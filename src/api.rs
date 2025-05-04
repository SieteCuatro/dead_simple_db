use axum::{
    body::Bytes, // Use Bytes extractor directly
    extract::{Path, State},
    http::{header, HeaderValue, StatusCode}, // Removed unused HeaderMap, Method
    response::{IntoResponse, Response},
    routing::{get, post}, // Removed unused delete, put
    Json, Router,
};
use serde_json::Value;
use std::collections::HashMap; // Use HashMap directly
use std::sync::Arc;
use tracing::{debug, error, info, warn};

use crate::db::SimpleDb;
use crate::error::DbError;

// Define a helper type for API errors to implement IntoResponse
#[derive(Debug)]
pub enum ApiError {
    BadRequest(String),
    DatabaseError(DbError),
    Internal(String), // Added Internal variant
}

// Implement conversion from DbError to ApiError
impl From<DbError> for ApiError {
    fn from(err: DbError) -> Self {
        ApiError::DatabaseError(err)
    }
}

// Implement how ApiError converts into an HTTP response
impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, error_message) = match self {
            ApiError::BadRequest(msg) => {
                warn!("Bad request: {}", msg);
                (StatusCode::BAD_REQUEST, msg)
            }
            ApiError::Internal(msg) => { // Handle Internal variant
                error!("Internal API Error: {}", msg);
                (StatusCode::INTERNAL_SERVER_ERROR, "An internal server error occurred".to_string())
            }
            ApiError::DatabaseError(db_err) => {
                // Enhanced error mapping
                match db_err {
                    DbError::KeyNotFound => (StatusCode::NOT_FOUND, "Key not found".to_string()),
                    DbError::KeyTooLarge { .. } | DbError::ValueTooLarge { .. } => {
                        warn!("Payload too large: {}", db_err);
                        (StatusCode::PAYLOAD_TOO_LARGE, db_err.to_string())
                    }
                    DbError::Io(ref io_err) => {
                        error!("Internal Server Error (IO): {}", io_err);
                         (StatusCode::INTERNAL_SERVER_ERROR, "Internal storage error".to_string())
                    }
                    DbError::BincodeSerializaton(ref b_err) => {
                        error!("Internal Server Error (Snapshot Serialization): {}", b_err);
                        (StatusCode::INTERNAL_SERVER_ERROR, "Internal server error".to_string())
                    }
                    DbError::LockPoisoned(_) => {
                         error!("Internal Server Error (Concurrency): {}", db_err);
                         (StatusCode::INTERNAL_SERVER_ERROR, "Server concurrency issue".to_string())
                    }
                    DbError::CompactionError(ref msg) => {
                         error!("Internal Server Error (Compaction): {}", msg);
                         (StatusCode::INTERNAL_SERVER_ERROR, "Database maintenance error".to_string())
                     }
                     DbError::SnapshotError(ref msg) => {
                         error!("Internal Server Error (Snapshot): {}", msg);
                         (StatusCode::INTERNAL_SERVER_ERROR, "Database state error".to_string())
                     }
                    // Catch other internal DbError variants
                    _ => {
                        error!("Internal server error (DB): {}", db_err);
                        (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            "An unexpected internal database error occurred".to_string(),
                        )
                    }
                }
            }
        };

        let body = Json(serde_json::json!({ "error": error_message }));
        // Ensure correct Response construction
        Response::builder()
            .status(status)
            .header(header::CONTENT_TYPE, "application/json")
            .body(body.into_response().into_body()) // Convert Json response body
            .unwrap() // Should not fail if status/headers are valid
    }
}

// Type alias for Result using our ApiError
type ApiResult<T = Response> = Result<T, ApiError>;

// --- API Handlers ---

pub async fn get_key(
    State(db): State<Arc<SimpleDb>>,
    Path(key): Path<String>,
) -> ApiResult {
    debug!("API: GET request for key: {}", key);

    match db.get(key.as_bytes()) {
        Ok(Some(value_bytes)) => {
            // Try to return as JSON, fall back to octet-stream
            match serde_json::from_slice::<Value>(&value_bytes) {
                Ok(json_value) => Ok((StatusCode::OK, Json(json_value)).into_response()),
                Err(_) => {
                    warn!(
                        "Value for key '{}' is not valid JSON, returning as octet-stream.",
                        key
                    );
                    Response::builder()
                        .status(StatusCode::OK)
                        .header(
                            header::CONTENT_TYPE,
                            HeaderValue::from_static("application/octet-stream"),
                        )
                        .body(value_bytes.into()) // Convert Vec<u8> into Body
                        .map_err(|e| ApiError::Internal(format!("Response builder error: {}", e))) // Map builder error to ApiError::Internal
                }
            }
        }
        Ok(None) => {
            warn!("Key '{}' not found in database.", key);
            Err(ApiError::DatabaseError(DbError::KeyNotFound))
        }
        Err(e) => {
            error!("Error retrieving key '{}': {:?}", key, e);
            Err(ApiError::DatabaseError(e))
        }
    }
}

pub async fn put_key(
    State(db): State<Arc<SimpleDb>>,
    Path(key): Path<String>,
    body: Bytes, // Accept raw bytes
) -> ApiResult<StatusCode> {
    debug!(
        "API: PUT request for key: {}, body bytes: {}",
        key,
        body.len()
    );

    // No JSON validation here - store raw bytes
    if let Err(e) = db.put(key.as_bytes(), &body) {
        error!("Error storing key '{}': {:?}", key, e);
        return Err(ApiError::DatabaseError(e));
    }

    Ok(StatusCode::CREATED) // Use 201 Created for successful PUT
}

pub async fn delete_key(
    State(db): State<Arc<SimpleDb>>,
    Path(key): Path<String>,
) -> ApiResult<StatusCode> {
    debug!("API: DELETE request for key: {}", key);

    if let Err(e) = db.delete(key.as_bytes()) {
        // Handle KeyNotFound specifically if needed (maybe return 204 anyway?)
        if matches!(e, DbError::KeyNotFound) {
             warn!("Attempted to delete non-existent key '{}'", key);
             // Return 204 No Content even if key wasn't there (idempotent)
             return Ok(StatusCode::NO_CONTENT);
        }
        error!("Error deleting key '{}': {:?}", key, e);
        return Err(ApiError::DatabaseError(e));
    }

    Ok(StatusCode::NO_CONTENT) // 204 No Content on success
}

// Batch PUT: Expects {"key1": "value1", "key2": "value2", ...} where value can be any JSON type
pub async fn batch_put(
    State(db): State<Arc<SimpleDb>>,
    Json(payload): Json<HashMap<String, Value>>, // Accept JSON Value to store raw representation
) -> ApiResult<StatusCode> {
    debug!("API: Batch PUT request with {} items", payload.len());

    // Prepare data for optimized batch put
    // No need for `entries` variable here
    let mut value_bytes_store = Vec::with_capacity(payload.len()); // Store owned bytes

    for (key, value) in payload {
        // Serialize the JSON value back to bytes
        match serde_json::to_vec(&value) { // Use to_vec for efficiency
            Ok(value_bytes) => {
                value_bytes_store.push((key, value_bytes)); // Store owned data
            }
            Err(e) => {
                 warn!("Failed to serialize value for key '{}' in batch PUT: {}", key, e);
                 return Err(ApiError::BadRequest(format!("Invalid JSON value for key '{}': {}", key, e)));
            }
        }
    }

    // Create slices for the batch_put function
    let entries_slices: Vec<(&[u8], &[u8])> = value_bytes_store
        .iter()
        .map(|(k, v)| (k.as_bytes(), v.as_slice()))
        .collect();

    // Call the optimized batch put
    if let Err(e) = db.batch_put(&entries_slices) {
        error!("Error during batch PUT: {:?}", e);
        return Err(ApiError::DatabaseError(e));
    }

    Ok(StatusCode::OK) // 200 OK for batch success
}

// Batch GET: Expects ["key1", "key2", ...]
pub async fn batch_get(
    State(db): State<Arc<SimpleDb>>,
    Json(keys_to_get): Json<Vec<String>>, // Corrected: Expects a list of keys
) -> ApiResult<Json<HashMap<String, Option<Value>>>> { // Return JSON values or null
    debug!("API: Batch GET request for {} keys", keys_to_get.len());

    // Convert String keys to Vec<u8> for the db layer
    let keys_bytes: Vec<Vec<u8>> = keys_to_get.iter().map(|s| s.as_bytes().to_vec()).collect();

    match db.batch_get(&keys_bytes) {
        Ok(results_bytes_map) => {
            let mut final_results: HashMap<String, Option<Value>> = HashMap::new();
            for (key_bytes, value_opt_bytes) in results_bytes_map {
                // Use the new ApiError::Internal variant here
                let key_string = String::from_utf8(key_bytes)
                    .map_err(|e| ApiError::Internal(format!("Invalid UTF-8 key retrieved: {}", e)))?;

                if let Some(value_bytes) = value_opt_bytes {
                    // Attempt to parse value as JSON
                    match serde_json::from_slice::<Value>(&value_bytes) {
                        Ok(json_value) => final_results.insert(key_string, Some(json_value)),
                        Err(_) => {
                             warn!("Value for key '{}' in batch GET is not valid JSON, returning null.", key_string);
                             final_results.insert(key_string, None) // Represent non-JSON as null
                         }
                    };
                } else {
                    final_results.insert(key_string, None); // Key not found
                }
            }
            Ok(Json(final_results))
        }
        Err(e) => {
            error!("Error during batch GET: {:?}", e);
            Err(ApiError::DatabaseError(e))
        }
    }
}

// Admin endpoint for compaction
pub async fn trigger_compaction(State(db): State<Arc<SimpleDb>>) -> ApiResult<StatusCode> {
    info!("API: Received request to trigger compaction");
    // Spawn background task to avoid blocking the API response
    tokio::spawn(async move {
        info!("Starting background compaction task...");
        match db.compact() {
            Ok(_) => info!("Compaction completed successfully."),
            Err(e) => error!("Compaction failed: {}", e),
        }
        // Optionally save index after compaction?
         match db.save_index_snapshot() {
            Ok(_) => info!("Index snapshot saved successfully after compaction."),
            Err(e) => error!("Failed to save index snapshot after compaction: {}", e),
         }
         info!("Background compaction task finished.");
    });
    Ok(StatusCode::ACCEPTED) // 202 Accepted: Request received, processing started
}

// Admin endpoint for saving index snapshot
pub async fn trigger_save_snapshot(State(db): State<Arc<SimpleDb>>) -> ApiResult<StatusCode> {
    info!("API: Received request to trigger index snapshot save");
    match db.save_index_snapshot() {
        Ok(_) => {
            info!("Index snapshot saved successfully via API.");
            Ok(StatusCode::OK)
        }
        Err(e) => {
            error!("Failed to save index snapshot via API: {}", e);
            Err(ApiError::DatabaseError(e))
        }
    }
}


// Function to create the Axum router
pub fn create_router(db: Arc<SimpleDb>) -> Router {
    Router::new()
        .route(
            "/keys/:key", // Standard path parameter syntax
            // Use axum::routing::{get, put, delete} directly
            get(get_key).put(put_key).delete(delete_key),
        )
        .route("/keys/batch", post(batch_put)) // POST for batch put (map body)
        .route("/keys/batch/get", post(batch_get)) // POST for batch get (list body)
        // Admin routes (consider adding authentication/authorization later)
        .route("/admin/compact", post(trigger_compaction))
        .route("/admin/save_snapshot", post(trigger_save_snapshot))
        .with_state(db) // Provide the database state to the handlers
}