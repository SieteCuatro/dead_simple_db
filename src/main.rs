use clap::{Parser, ValueEnum};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tracing::{error, info, Level};
use std::error::Error;
use std::process;

// Use the library crate name to bring items into scope
// This single line correctly imports everything needed from the library
use dead_simple_db::{api, error, db::{SimpleDb, SyncStrategy}};

// This specific import is still useful for using `DbError` directly below
// but we will use the qualified path `error::DbError` in the match for clarity
#[allow(unused_imports)]
use error::DbError;


/// Simple Key-Value Database Server
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Path to the database log file
    #[arg(short, long, value_name = "FILE", default_value = "data.dblog")]
    data_file: PathBuf,

    /// Path to the index snapshot file (default: data_file + .index)
    #[arg(long, value_name = "FILE")]
    index_file: Option<PathBuf>,

    /// Network address to listen on (e.g., 127.0.0.1:7878)
    #[arg(short, long, value_name = "IP:PORT", default_value = "127.0.0.1:7878")]
    listen: SocketAddr,

    /// Write synchronization strategy
    #[arg(long, value_enum, default_value_t = CliSyncStrategy::Never)]
    sync: CliSyncStrategy,
}

#[derive(ValueEnum, Clone, Debug, Copy)]
enum CliSyncStrategy {
    /// Sync every write to disk (safer, slower)
    Always,
    /// Let the OS cache writes (faster, risk of data loss on crash)
    Never,
}

// Convert CLI enum to internal enum
impl From<CliSyncStrategy> for SyncStrategy {
    fn from(cli_strategy: CliSyncStrategy) -> Self {
        match cli_strategy {
            CliSyncStrategy::Always => SyncStrategy::Always,
            CliSyncStrategy::Never => SyncStrategy::Never,
        }
    }
}


#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Initialize tracing subscriber
    tracing_subscriber::fmt()
        .with_max_level(Level::INFO) // Default INFO level
        .with_target(true)
        .init();

    // Parse command line arguments
    let args = Args::parse();

    info!("Starting Dead Simple DB Server...");
    info!("Data file: {:?}", args.data_file);
    let sync_strategy: SyncStrategy = args.sync.into();
    info!("Sync strategy: {:?}", sync_strategy);

    // Determine index file path
    let index_file_path = args
        .index_file
        .unwrap_or_else(|| args.data_file.with_extension("dblog.index"));
    info!("Index file: {:?}", index_file_path);


    // --- Database Initialization ---
    // Ensure parent directory exists
     if let Some(parent) = args.data_file.parent() {
        if !parent.exists() {
            info!("Creating data directory: {:?}", parent);
             std::fs::create_dir_all(parent)?;
         }
     }
    if let Some(parent) = index_file_path.parent() {
        if !parent.exists() {
             info!("Creating index directory: {:?}", parent);
             std::fs::create_dir_all(parent)?;
         }
     }


    let db = match SimpleDb::open(&args.data_file, &index_file_path, sync_strategy) {
        Ok(db_instance) => Arc::new(db_instance),
        Err(e) => {
            error!("Failed to open database: {}", e);
            // Provide more context for specific errors if possible
            match e {
                 // **FIX HERE**: Use the `error` module path
                 error::DbError::Io(ref io_err) if io_err.kind() == std::io::ErrorKind::PermissionDenied => {
                     eprintln!("Error: Permission denied accessing database files ({:?} or {:?}). Please check file/directory permissions.", args.data_file, index_file_path);
                 }
                 _ => {
                      eprintln!("Error: Could not initialize database: {}", e);
                 }
            }
            process::exit(1); // Exit cleanly on critical startup error
        }
    };
    info!("Database opened successfully.");


    // --- API Router Setup ---
    let app = api::create_router(db.clone()); // Clone Arc for router

    // --- Start Server ---
    let listener = match tokio::net::TcpListener::bind(args.listen).await {
         Ok(l) => l,
         Err(e) => {
             error!("Failed to bind to address {}: {}", args.listen, e);
             eprintln!("Error: Could not bind to address {}. Is it already in use?", args.listen);
             process::exit(1);
         }
     };

    info!("Server listening on {}", args.listen);

    // --- Graceful Shutdown Handling ---
    let shutdown_signal = async {
        tokio::signal::ctrl_c()
            .await
            .expect("Failed to install CTRL+C signal handler");
        info!("Received CTRL+C, initiating graceful shutdown...");
    };

    // Serve the application with graceful shutdown
    axum::serve(listener, app.into_make_service())
        .with_graceful_shutdown(shutdown_signal)
        .await
        .map_err(|e| {
             error!("Server error: {}", e);
             Box::new(e) as Box<dyn Error> // Map error for main's return type
        })?;

    // --- Perform final actions before exiting ---
    info!("Server shut down gracefully. Performing final sync/cleanup...");

    // Explicitly sync/save snapshot on shutdown (optional but good practice)
     if let Err(e) = db.save_index_snapshot() {
         error!("Error saving final index snapshot: {}", e);
     }
    if let Err(e) = db.sync() { // Force a final sync regardless of strategy
        error!("Error performing final sync: {}", e);
    }

     // The Arc going out of scope should trigger the Drop impl if this is the last reference.

    info!("Shutdown complete.");
    Ok(())
}