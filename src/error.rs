use thiserror::Error;
use bincode; // Import the crate itself

#[derive(Debug, Error)]
pub enum DbError {
    #[error("I/O Error: {0}")]
    Io(#[from] std::io::Error),

    // --- Updated Bincode Errors ---
    #[error("Serialization Error (Bincode Encode): {0}")]
    BincodeEncode(#[from] Box<bincode::error::EncodeError>), // Boxed encode error

    #[error("Deserialization Error (Bincode Decode): {0}")]
    BincodeDecode(#[from] Box<bincode::error::DecodeError>), // Boxed decode error
    // --- End Updated Bincode Errors ---

    #[error("Serialization Error (JSON): {0}")]
    JsonSerialization(#[from] serde_json::Error),

    #[error("Failed to serialize record: {0}")]
    Serialization(String),

    #[error("Failed to deserialize record: {0}")]
    Deserialization(String),

    #[error("Invalid record found at offset {offset}: {reason}")]
    InvalidRecord { offset: u64, reason: String },

    #[error("CRC mismatch for record at offset {offset}")]
    CrcMismatch { offset: u64 },

    #[error("Key not found")]
    KeyNotFound,

    #[error("Key too large (limit: {limit}, actual: {actual})")]
    KeyTooLarge { limit: usize, actual: usize },

    #[error("Value too large (limit: {limit}, actual: {actual})")]
    ValueTooLarge { limit: usize, actual: usize },

    #[error("Concurrency lock poisoned: {0}")]
    LockPoisoned(String),

    #[error("Database file format version mismatch")]
    VersionMismatch,

    #[error("Configuration Error: {0}")]
    Config(String),

    #[error("Index snapshot error: {0}")]
    SnapshotError(String), // Specific errors during snapshot load/save

    #[error("Compaction error: {0}")]
    CompactionError(String), // Specific errors during compaction

    #[error("Internal database error: {0}")]
    Internal(String), // Catch-all for unexpected states
}

// Result type alias for convenience
pub type DbResult<T> = Result<T, DbError>;