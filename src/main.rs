use clap::{Parser, ValueEnum, parser::ValueSource}; // Corrected import path for ValueSource
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tracing::{error, info, Level}; // Removed unused `warn`
use std::error::Error;
use std::process;
use config::{Config, ConfigError, File}; // Removed unused `Environment`
use serde::Deserialize;

// Use the library crate name
use dead_simple_db::{api, error, db::{SimpleDb, SyncStrategy}};

// Keep DbError import if used directly, otherwise allow unused
#[allow(unused_imports)]
use dead_simple_db::error::DbError;


// --- Configuration Structures ---

// Structure for config file values
// Removed Default derive as SocketAddr and CliSyncStrategy don't implement it
#[derive(Deserialize, Debug)]
struct Settings {
    #[serde(default = "default_data_file")]
    data_file: PathBuf,
    index_file: Option<PathBuf>, // Make index_file optional in config
    #[serde(default = "default_listen_addr")]
    listen: SocketAddr,
    #[serde(default = "default_sync_strategy")]
    sync: CliSyncStrategy,
}

// Default value functions for Settings (remain the same)
fn default_data_file() -> PathBuf {
    PathBuf::from("data.dblog")
}

fn default_listen_addr() -> SocketAddr {
    "127.0.0.1:7878".parse().expect("Default listen address should be valid")
}

fn default_sync_strategy() -> CliSyncStrategy {
    CliSyncStrategy::Never
}

// Enum for Sync Strategy, used by both CLI and Config (remains the same)
#[derive(ValueEnum, Clone, Debug, Copy, Deserialize)]
#[serde(rename_all = "lowercase")]
enum CliSyncStrategy {
    Always,
    Never,
}

// Convert CLI/Config enum to internal DB enum (remains the same)
impl From<CliSyncStrategy> for SyncStrategy {
    fn from(cli_strategy: CliSyncStrategy) -> Self {
        match cli_strategy {
            CliSyncStrategy::Always => SyncStrategy::Always,
            CliSyncStrategy::Never => SyncStrategy::Never,
        }
    }
}

// --- Command Line Argument Structure --- (remains the same)

/// Simple Key-Value Database Server
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Path to a configuration file (TOML format)
    #[arg(short, long, value_name = "FILE")]
    config: Option<PathBuf>,

    /// Path to the database log file (overrides config)
    #[arg(short = 'd', long, value_name = "FILE")]
    data_file: Option<PathBuf>,

    /// Path to the index snapshot file (overrides config)
    #[arg(long, value_name = "FILE")]
    index_file: Option<PathBuf>,

    /// Network address to listen on (e.g., 127.0.0.1:7878) (overrides config)
    #[arg(short, long, value_name = "IP:PORT")]
    listen: Option<SocketAddr>,

    /// Write synchronization strategy (overrides config)
    #[arg(long, value_enum)]
    sync: Option<CliSyncStrategy>,
}


// --- Main Application Logic --- (remains mostly the same, minor adjustments might be needed if Default was relied upon implicitly, but wasn't here)

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Initialize tracing subscriber (early)
    tracing_subscriber::fmt()
        .with_max_level(Level::INFO)
        .with_target(true)
        .init();

    // --- Configuration Loading ---

    // 1. Parse CLI args *first* to potentially get config path
    let args = Args::parse();

    // 2. Build configuration layers
    let config_builder = Config::builder()
        // Start with base defaults defined in the Settings struct defaults
        // These provide the absolute fallback if nothing else is set
        .set_default("data_file", default_data_file().to_str().unwrap_or("data.dblog"))?
        .set_default("listen", default_listen_addr().to_string())?
        .set_default("sync", "never")?
        // index_file default is calculated later based on data_file
        ;

    // 3. Add config file source if specified
    let config_builder = match &args.config {
        Some(config_path) => {
            info!("Attempting to load configuration from: {:?}", config_path);
            // Make it required=true ONLY if the --config flag was explicitly provided
            let required = args.config.is_some();
             config_builder.add_source(File::from(config_path.clone()).required(required))
        }
        None => {
            // Try loading a default config file name (e.g., config.toml) optionally
            info!("No --config specified, attempting to load 'config.toml' optionally");
            config_builder.add_source(File::with_name("config").required(false))
        }
    };

    // 4. (Optional) Add environment variable source
    // config_builder = config_builder.add_source(config::Environment::with_prefix("APP").separator("__"));

    // 5. Finalize config build
    let cfg = match config_builder.build() {
         Ok(c) => c,
         Err(e) => {
             // Handle specific errors like file not found vs parsing errors
             match e {
                 ConfigError::FileParse { uri, cause } => {
                     error!("Error parsing configuration file {:?}: {}", uri.unwrap_or_else(|| "Unknown".to_string()), cause);
                     eprintln!("Error: Failed to parse configuration file. Please check the format.");
                     process::exit(1); // Exit on parse error
                 }
                 ConfigError::NotFound(path) if args.config.is_some() => {
                      // Only error if --config was EXPLICITLY provided and file not found
                      error!("Configuration file specified but not found: {}", path);
                      eprintln!("Error: Configuration file specified via --config was not found: {}", path);
                      process::exit(1); // Exit if specified config is missing
                 }
                 ConfigError::NotFound(_) => {
                     // Ignore if default config file wasn't found
                     info!("Default configuration file not found, using defaults and CLI arguments.");
                 }
                 _ => {
                     // Other errors (e.g., type mismatches)
                     error!("Error loading configuration: {}", e);
                     eprintln!("Error: Could not load configuration: {}", e);
                     process::exit(1); // Exit on other config errors
                 }
             }
             // If we ignored NotFound for default file, build minimal defaults
             Config::builder()
                 .set_default("data_file", default_data_file().to_str().unwrap_or("data.dblog"))?
                 .set_default("listen", default_listen_addr().to_string())?
                 .set_default("sync", "never")?
                 .build()?
         }
    };


    // 6. Deserialize base settings from config object
    let base_settings: Settings = match cfg.try_deserialize() {
         Ok(s) => s,
         Err(e) => {
             error!("Error deserializing configuration settings: {}", e);
             eprintln!("Error: Configuration values are invalid: {}", e);
             process::exit(1);
         }
     };


    // --- Merging CLI arguments over Config/Defaults ---

    // Determine final values, prioritizing CLI flags if they were explicitly provided
    let final_data_file = args.data_file.clone().unwrap_or(base_settings.data_file);

    // Special handling for index_file: default depends on data_file
    let final_index_file = args.index_file.clone()
        .or(base_settings.index_file) // Use config value if present
        .unwrap_or_else(|| final_data_file.with_extension("dblog.index")); // Calculate default based on final data_file

    let final_listen_addr = args.listen.unwrap_or(base_settings.listen);
    let final_sync_strategy_cli = args.sync.unwrap_or(base_settings.sync);
    let final_sync_strategy: SyncStrategy = final_sync_strategy_cli.into(); // Convert to internal type


    // --- Log final configuration ---
    info!("--- Final Configuration ---");
    info!("Data file: {:?}", final_data_file);
    info!("Index file: {:?}", final_index_file);
    info!("Listen address: {}", final_listen_addr);
    info!("Sync strategy: {:?}", final_sync_strategy);
    info!("---------------------------");


    // --- Database Initialization (using final config values) ---
    if let Some(parent) = final_data_file.parent() {
        if !parent.exists() {
            info!("Creating data directory: {:?}", parent);
            std::fs::create_dir_all(parent)?;
        }
    }
    if let Some(parent) = final_index_file.parent() {
        if !parent.exists() {
            info!("Creating index directory: {:?}", parent);
            std::fs::create_dir_all(parent)?;
        }
    }

    let db = match SimpleDb::open(&final_data_file, &final_index_file, final_sync_strategy) {
        Ok(db_instance) => Arc::new(db_instance),
        Err(e) => {
            error!("Failed to open database: {}", e);
            match e {
                 error::DbError::Io(ref io_err) if io_err.kind() == std::io::ErrorKind::PermissionDenied => {
                     eprintln!("Error: Permission denied accessing database files ({:?} or {:?}). Please check file/directory permissions.", final_data_file, final_index_file);
                 }
                 _ => {
                      eprintln!("Error: Could not initialize database: {}", e);
                 }
            }
            process::exit(1);
        }
    };
    info!("Database opened successfully.");


    // --- API Router Setup ---
    let app = api::create_router(db.clone());

    // --- Start Server ---
    let listener = match tokio::net::TcpListener::bind(final_listen_addr).await {
         Ok(l) => l,
         Err(e) => {
             error!("Failed to bind to address {}: {}", final_listen_addr, e);
             eprintln!("Error: Could not bind to address {}. Is it already in use?", final_listen_addr);
             process::exit(1);
         }
     };
    info!("Server listening on {}", final_listen_addr);


    // --- Graceful Shutdown Handling ---
    let shutdown_signal = async {
        tokio::signal::ctrl_c()
            .await
            .expect("Failed to install CTRL+C signal handler");
        info!("Received CTRL+C, initiating graceful shutdown...");
    };

    axum::serve(listener, app.into_make_service())
        .with_graceful_shutdown(shutdown_signal)
        .await
        .map_err(|e| {
             error!("Server error: {}", e);
             Box::new(e) as Box<dyn Error>
        })?;

    // --- Perform final actions before exiting ---
    info!("Server shut down gracefully. Performing final sync/cleanup...");
    if let Err(e) = db.save_index_snapshot() {
        error!("Error saving final index snapshot: {}", e);
    }
    if let Err(e) = db.sync() {
        error!("Error performing final sync: {}", e);
    }

    info!("Shutdown complete.");
    Ok(())
}