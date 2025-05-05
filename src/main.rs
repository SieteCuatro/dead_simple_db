// src/main.rs

use clap::{Parser, ValueEnum};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
// --- Change Level import if needed, or just use it directly ---
use tracing::{debug, error, info, Level, warn}; // Ensure Level is imported if not already
use std::error::Error;
use std::process;
use config::{Config, ConfigError, File};
use serde::Deserialize;
use tokio::time::Duration;

// Use the library crate name
use dead_simple_db::{api, error, db::{SimpleDb, SyncStrategy}};

#[allow(unused_imports)]
use dead_simple_db::error::DbError;

// --- Import TraceLayer ---
use tower_http::trace::TraceLayer;
// -------------------------


// --- Configuration Structures ---

// Structure for config file values
#[derive(Deserialize, Debug)]
struct Settings {
    #[serde(default = "default_data_file")]
    data_file: PathBuf,
    index_file: Option<PathBuf>,
    #[serde(default = "default_listen_addr")]
    listen: SocketAddr,
    #[serde(default = "default_sync_strategy")]
    sync: CliSyncStrategy,
    #[serde(default = "default_auto_compact_threshold")]
    auto_compact_threshold_bytes: u64,
    #[serde(default = "default_auto_snapshot_interval")]
    auto_snapshot_interval_secs: u64,
    #[serde(default = "default_maintenance_check_interval")]
    maintenance_check_interval_secs: u64,
}

// Default value functions for Settings
fn default_data_file() -> PathBuf { PathBuf::from("data.dblog") }
fn default_listen_addr() -> SocketAddr { "127.0.0.1:7878".parse().expect("Default listen address should be valid") }
fn default_sync_strategy() -> CliSyncStrategy { CliSyncStrategy::Never }
fn default_auto_compact_threshold() -> u64 { 0 } // Disabled by default
fn default_auto_snapshot_interval() -> u64 { 0 } // Disabled by default
fn default_maintenance_check_interval() -> u64 { 60 } // Check every 60 seconds

// Enum for Sync Strategy (remains the same)
#[derive(ValueEnum, Clone, Debug, Copy, Deserialize)]
#[serde(rename_all = "lowercase")]
enum CliSyncStrategy { Always, Never }

impl From<CliSyncStrategy> for SyncStrategy { // (remains the same)
    fn from(cli_strategy: CliSyncStrategy) -> Self {
        match cli_strategy {
            CliSyncStrategy::Always => SyncStrategy::Always,
            CliSyncStrategy::Never => SyncStrategy::Never,
        }
    }
}

// --- Command Line Argument Structure ---
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

    /// Automatic compaction threshold in bytes (0 to disable) (overrides config)
    #[arg(long, value_name = "BYTES")]
    auto_compact_threshold_bytes: Option<u64>,

    /// Automatic snapshot interval in seconds (0 to disable) (overrides config)
    #[arg(long, value_name = "SECONDS")]
    auto_snapshot_interval_secs: Option<u64>,

    /// Maintenance check interval in seconds (overrides config)
    #[arg(long, value_name = "SECONDS")]
    maintenance_check_interval_secs: Option<u64>,
}


// --- Main Application Logic ---

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Initialize tracing subscriber (early)
    // --- MODIFIED LOG LEVEL ---
    tracing_subscriber::fmt()
        .with_max_level(Level::DEBUG) // Changed from INFO to DEBUG
        .with_target(true) // Keep target info
        // .with_line_number(true) // Optional: Add line numbers for debugging
        .init();
    // --------------------------

    // --- Configuration Loading ---
    let args = Args::parse();

    // Build configuration layers
    let config_builder = Config::builder()
        .set_default("data_file", default_data_file().to_str().unwrap_or("data.dblog"))?
        .set_default("listen", default_listen_addr().to_string())?
        .set_default("sync", "never")?
        .set_default("auto_compact_threshold_bytes", default_auto_compact_threshold() as i64)?
        .set_default("auto_snapshot_interval_secs", default_auto_snapshot_interval() as i64)?
        .set_default("maintenance_check_interval_secs", default_maintenance_check_interval() as i64)?;

    // Add config file source if specified
    let config_builder = match &args.config {
        Some(config_path) => {
            info!("Attempting to load configuration from: {:?}", config_path);
            let required = args.config.is_some();
             config_builder.add_source(File::from(config_path.clone()).required(required))
        }
        None => {
            info!("No --config specified, attempting to load 'config.toml' optionally");
            config_builder.add_source(File::with_name("config").required(false))
        }
    };

    // Finalize config build
    let cfg = match config_builder.build() {
         Ok(c) => c,
         Err(e) => {
             match e {
                 ConfigError::FileParse { uri, cause } => {
                     error!("Error parsing configuration file {:?}: {}", uri.unwrap_or_else(|| "Unknown".to_string()), cause);
                     eprintln!("Error: Failed to parse configuration file. Please check the format.");
                     process::exit(1);
                 }
                 ConfigError::NotFound(path) if args.config.is_some() => {
                     error!("Configuration file specified but not found: {}", path);
                     eprintln!("Error: Configuration file specified via --config was not found: {}", path);
                     process::exit(1);
                 }
                 ConfigError::NotFound(_) => {
                     // This is okay if config file is optional
                     debug!("Optional default configuration file 'config.toml' not found.");
                 }
                 _ => {
                     error!("Error loading configuration: {}", e);
                     eprintln!("Error: Could not load configuration: {}", e);
                     process::exit(1);
                 }
             }
             // Build minimal defaults if config load failed non-critically
              Config::builder()
                 .set_default("data_file", default_data_file().to_str().unwrap_or("data.dblog"))?
                 .set_default("listen", default_listen_addr().to_string())?
                 .set_default("sync", "never")?
                 .set_default("auto_compact_threshold_bytes", default_auto_compact_threshold() as i64)?
                 .set_default("auto_snapshot_interval_secs", default_auto_snapshot_interval() as i64)?
                 .set_default("maintenance_check_interval_secs", default_maintenance_check_interval() as i64)?
                 .build()?
         }
    };

    // Deserialize base settings
    let base_settings: Settings = match cfg.try_deserialize() {
         Ok(s) => s,
         Err(e) => {
             error!("Error deserializing configuration settings: {}", e);
             eprintln!("Error: Configuration values are invalid: {}", e);
             process::exit(1);
         }
     };


    // --- Merging CLI arguments over Config/Defaults ---
    let final_data_file = args.data_file.clone().unwrap_or(base_settings.data_file);
    let final_index_file = args.index_file.clone()
        .or(base_settings.index_file)
        .unwrap_or_else(|| final_data_file.with_extension("dblog.index"));
    let final_listen_addr = args.listen.unwrap_or(base_settings.listen);
    let final_sync_strategy: SyncStrategy = args.sync.unwrap_or(base_settings.sync).into();
    let final_auto_compact_threshold = args.auto_compact_threshold_bytes.unwrap_or(base_settings.auto_compact_threshold_bytes);
    let final_auto_snapshot_interval_secs = args.auto_snapshot_interval_secs.unwrap_or(base_settings.auto_snapshot_interval_secs);
    let final_maintenance_check_interval_secs = args.maintenance_check_interval_secs.unwrap_or(base_settings.maintenance_check_interval_secs);

    // --- Log final configuration ---
    // Use debug! for detailed config logging now that DEBUG level is enabled
    debug!("--- Final Configuration ---");
    debug!("Data file: {:?}", final_data_file);
    debug!("Index file: {:?}", final_index_file);
    debug!("Listen address: {}", final_listen_addr);
    debug!("Sync strategy: {:?}", final_sync_strategy);
    debug!("Auto Compact Threshold: {} bytes (0=disabled)", final_auto_compact_threshold);
    debug!("Auto Snapshot Interval: {} seconds (0=disabled)", final_auto_snapshot_interval_secs);
    debug!("Maintenance Check Interval: {} seconds", final_maintenance_check_interval_secs);
    debug!("---------------------------");

    // --- Database Initialization ---
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

    // --- Spawn Maintenance Task ---
    if final_auto_compact_threshold > 0 || final_auto_snapshot_interval_secs > 0 {
        if final_maintenance_check_interval_secs == 0 {
            warn!("Automatic maintenance enabled but check interval is 0, disabling checks.");
        } else {
            info!("Spawning background maintenance task.");
            spawn_maintenance_task(
                db.clone(),
                final_auto_compact_threshold,
                final_auto_snapshot_interval_secs,
                final_maintenance_check_interval_secs,
            );
        }
    } else {
        info!("Automatic maintenance disabled by configuration.");
    }

    // --- API Router Setup ---
    let router = api::create_router(db.clone());

    // --- Apply TraceLayer ---
    let app = router.layer(TraceLayer::new_for_http());
    // ------------------------

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

    // --- Use the layered app ---
    axum::serve(listener, app.into_make_service())
        .with_graceful_shutdown(shutdown_signal)
        .await
        .map_err(|e| {
             error!("Server error: {}", e);
             Box::new(e) as Box<dyn Error>
        })?;
    // ---------------------------

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

// --- Maintenance Task ---

fn spawn_maintenance_task(
    db: Arc<SimpleDb>,
    compact_threshold_bytes: u64,
    snapshot_interval_secs: u64,
    check_interval_secs: u64,
) {
    // Flags to prevent concurrent runs of the *same* maintenance type
    let is_compacting = Arc::new(AtomicBool::new(false));
    let is_snapshotting = Arc::new(AtomicBool::new(false));

    tokio::spawn(async move {
        if check_interval_secs == 0 { return; } // Prevent division by zero / useless loop

        let mut interval = tokio::time::interval(Duration::from_secs(check_interval_secs));
        // Wait one interval before the first check
        interval.tick().await;

        info!("Maintenance task started. Check interval: {}s", check_interval_secs);

        loop {
            interval.tick().await;
            debug!("Maintenance check running..."); // Use debug here

            // --- Check Compaction ---
            if compact_threshold_bytes > 0 {
                // Check if already compacting
                if !is_compacting.load(Ordering::SeqCst) {
                    match db.get_log_size_for_compaction_check() {
                        Ok(current_size) => {
                            if current_size >= compact_threshold_bytes {
                                info!("Log size {} >= threshold {}, triggering compaction.", current_size, compact_threshold_bytes);
                                // Set flag *before* spawning task
                                if is_compacting.compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst).is_ok() {
                                    let db_clone = db.clone();
                                    let flag_clone = is_compacting.clone();
                                    tokio::spawn(async move {
                                        info!("Starting background compaction...");
                                        match db_clone.compact() {
                                            Ok(_) => info!("Background compaction finished successfully."),
                                            Err(e) => error!("Background compaction failed: {}", e),
                                        }
                                        // Always reset the flag
                                        flag_clone.store(false, Ordering::SeqCst);
                                    });
                                } else {
                                   debug!("Compaction already in progress, skipping trigger."); // Use debug
                                }
                            }
                        }
                        Err(e) => {
                            warn!("Failed to get log size for compaction check: {}", e);
                        }
                    }
                } else {
                     debug!("Compaction already running, skipping check."); // Use debug
                }
            }

            // --- Check Snapshotting ---
            if snapshot_interval_secs > 0 {
                // Check if already snapshotting
                if !is_snapshotting.load(Ordering::SeqCst) {
                    match db.get_last_snapshot_time().await {
                        Some(last_time) => {
                            if last_time.elapsed() >= Duration::from_secs(snapshot_interval_secs) {
                                info!("Snapshot interval exceeded ({}s elapsed >= {}s threshold), triggering snapshot.", last_time.elapsed().as_secs(), snapshot_interval_secs);
                                // Set flag *before* spawning task
                                if is_snapshotting.compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst).is_ok() {
                                    let db_clone = db.clone();
                                    let flag_clone = is_snapshotting.clone();
                                    tokio::spawn(async move {
                                        info!("Starting background snapshot...");
                                        match db_clone.save_index_snapshot() {
                                            Ok(_) => info!("Background snapshot finished successfully."),
                                            Err(e) => error!("Background snapshot failed: {}", e),
                                        }
                                        // Always reset the flag
                                        flag_clone.store(false, Ordering::SeqCst);
                                    });
                                } else {
                                   debug!("Snapshot already in progress, skipping trigger."); // Use debug
                                }
                            }
                        }
                        None => {
                            // No snapshot taken yet, trigger one immediately if interval > 0
                             info!("No previous snapshot time recorded, triggering initial snapshot.");
                             if is_snapshotting.compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst).is_ok() {
                                 let db_clone = db.clone();
                                 let flag_clone = is_snapshotting.clone();
                                 tokio::spawn(async move {
                                     info!("Starting background initial snapshot...");
                                     match db_clone.save_index_snapshot() {
                                         Ok(_) => info!("Background initial snapshot finished successfully."),
                                         Err(e) => error!("Background initial snapshot failed: {}", e),
                                     }
                                     flag_clone.store(false, Ordering::SeqCst);
                                 });
                             } else {
                                debug!("Snapshot already in progress, skipping initial trigger."); // Use debug
                             }
                        }
                    }
                } else {
                    debug!("Snapshot already running, skipping check."); // Use debug
                }
            }
        }
    });
}