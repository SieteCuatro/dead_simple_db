// src/api.rs
use axum::{
    body::Bytes,
    extract::{Path, State},
    http::{header, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, error, info, warn};

use crate::db::SimpleDb;
use crate::error::DbError;

#[derive(Debug)]
pub enum ApiError {
    BadRequest(String),
    DatabaseError(DbError),
    Internal(String),
}

impl From<DbError> for ApiError {
    fn from(err: DbError) -> Self {
        ApiError::DatabaseError(err)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, error_message) = match self {
            ApiError::BadRequest(msg) => {
                warn!("Bad request: {}", msg);
                (StatusCode::BAD_REQUEST, msg)
            }
            ApiError::Internal(msg) => {
                error!("Internal API Error: {}", msg);
                (StatusCode::INTERNAL_SERVER_ERROR, "An internal server error occurred".to_string())
            }
            ApiError::DatabaseError(db_err) => {
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
                    DbError::BincodeEncode(ref b_err) => {
                        error!("Internal Server Error (Snapshot/Compact Encode): {}", b_err);
                        (StatusCode::INTERNAL_SERVER_ERROR, "Internal server error during state save".to_string())
                    }
                    DbError::BincodeDecode(ref b_err) => {
                         error!("Internal Server Error (Snapshot Decode): {}", b_err);
                         (StatusCode::INTERNAL_SERVER_ERROR, "Internal server error during state load".to_string())
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

pub type ApiResult<T = Response> = Result<T, ApiError>;

pub async fn get_key(
    State(db): State<Arc<SimpleDb>>,
    Path(key): Path<String>,
) -> ApiResult {
    debug!("API: GET request for key: {}", key);

    match db.get(key.as_bytes()) {
        Ok(Some(value_bytes)) => {
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
                        .body(value_bytes.into())
                        .map_err(|e| ApiError::Internal(format!("Response builder error: {}", e)))
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
    body: Bytes,
) -> ApiResult<StatusCode> {
    debug!(
        "API: PUT request for key: {}, body bytes: {}",
        key,
        body.len()
    );

    if let Err(e) = db.put(key.as_bytes(), &body) {
        error!("Error storing key '{}': {:?}", key, e);
        return Err(ApiError::DatabaseError(e));
    }

    Ok(StatusCode::CREATED)
}

pub async fn delete_key(
    State(db): State<Arc<SimpleDb>>,
    Path(key): Path<String>,
) -> ApiResult<StatusCode> {
    debug!("API: DELETE request for key: {}", key);

    if let Err(e) = db.delete(key.as_bytes()) {
        if matches!(e, DbError::KeyNotFound) {
             warn!("Attempted to delete non-existent key '{}'", key);
             return Ok(StatusCode::NO_CONTENT);
        }
        error!("Error deleting key '{}': {:?}", key, e);
        return Err(ApiError::DatabaseError(e));
    }

    Ok(StatusCode::NO_CONTENT)
}

pub async fn batch_put(
    State(db): State<Arc<SimpleDb>>,
    Json(payload): Json<HashMap<String, Value>>,
) -> ApiResult<StatusCode> {
    debug!("API: Batch PUT request with {} items", payload.len());
    let mut value_bytes_store = Vec::with_capacity(payload.len());

    for (key, value) in payload {
        match serde_json::to_vec(&value) {
            Ok(value_bytes) => {
                value_bytes_store.push((key, value_bytes));
            }
            Err(e) => {
                 warn!("Failed to serialize value for key '{}' in batch PUT: {}", key, e);
                 return Err(ApiError::BadRequest(format!("Invalid JSON value for key '{}': {}", key, e)));
            }
        }
    }

    let entries_slices: Vec<(&[u8], &[u8])> = value_bytes_store
        .iter()
        .map(|(k, v)| (k.as_bytes(), v.as_slice()))
        .collect();

    if let Err(e) = db.batch_put(&entries_slices) {
        error!("Error during batch PUT: {:?}", e);
        return Err(ApiError::DatabaseError(e));
    }

    Ok(StatusCode::OK)
}

pub async fn batch_get(
    State(db): State<Arc<SimpleDb>>,
    Json(keys_to_get): Json<Vec<String>>,
) -> ApiResult<Json<HashMap<String, Option<Value>>>> {
    debug!("API: Batch GET request for {} keys", keys_to_get.len());
    let keys_bytes: Vec<Vec<u8>> = keys_to_get.iter().map(|s| s.as_bytes().to_vec()).collect();

    match db.batch_get(&keys_bytes) {
        Ok(results_bytes_map) => {
            let mut final_results: HashMap<String, Option<Value>> = HashMap::new();
            for (key_bytes, value_opt_bytes) in results_bytes_map {
                let key_string = String::from_utf8(key_bytes)
                    .map_err(|e| ApiError::Internal(format!("Invalid UTF-8 key retrieved: {}", e)))?;

                if let Some(value_bytes) = value_opt_bytes {
                    match serde_json::from_slice::<Value>(&value_bytes) {
                        Ok(json_value) => final_results.insert(key_string, Some(json_value)),
                        Err(_) => {
                             warn!("Value for key '{}' in batch GET is not valid JSON, returning null.", key_string);
                             final_results.insert(key_string, None)
                         }
                    };
                } else {
                    final_results.insert(key_string, None);
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

pub async fn trigger_compaction(State(db): State<Arc<SimpleDb>>) -> ApiResult<StatusCode> {
    info!("API: Received request to trigger compaction");
    tokio::spawn(async move {
        info!("Starting background compaction task...");
        match db.compact() {
            Ok(_) => info!("Compaction completed successfully."),
            Err(e) => error!("Compaction failed: {}", e),
        }
         match db.save_index_snapshot() {
            Ok(_) => info!("Index snapshot saved successfully after compaction."),
            Err(e) => error!("Failed to save index snapshot after compaction: {}", e),
         }
         info!("Background compaction task finished.");
    });
    Ok(StatusCode::ACCEPTED)
}

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

pub fn create_router(db: Arc<SimpleDb>) -> Router {
    Router::new()
        .route(
            "/v1/keys/{key}", 
            get(get_key).put(put_key).delete(delete_key),
        )
        .route("/v1/keys/batch", post(batch_put)) // <-- Keep /v1 prefix
        .route("/v1/keys/batch/get", post(batch_get)) // <-- Keep /v1 prefix
        .route("/v1/admin/compact", post(trigger_compaction)) // <-- Keep /v1 prefix
        .route("/v1/admin/save_snapshot", post(trigger_save_snapshot)) // <-- Keep /v1 prefix
        .with_state(db)
}