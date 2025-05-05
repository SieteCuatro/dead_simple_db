// tests/api_integration.rs
use dead_simple_db::db::{SimpleDb, SyncStrategy}; // Access items from your crate
use dead_simple_db::api; // Access API router function

use std::net::{SocketAddr, TcpListener}; // For finding free port
use std::sync::Arc;
use tempfile::tempdir;
use reqwest::{Client, StatusCode};
use serde_json::{json, Value};
use std::collections::HashMap;

// Helper to spawn the server on a random available port
async fn spawn_app() -> (String, Arc<SimpleDb>) {
    // Find a free port
    let listener = TcpListener::bind("127.0.0.1:0").expect("Failed to bind random port");
    let port = listener.local_addr().unwrap().port();
    drop(listener); // Drop listener so the server can bind it

    let addr = format!("127.0.0.1:{}", port);
    let sock_addr: SocketAddr = addr.parse().unwrap();

    // Setup temp DB for the test server instance
    let dir = tempdir().expect("Failed to create temp dir for API test");
    let data_path = dir.path().join("api_test.dblog");
    let index_path = dir.path().join("api_test.dblog.index");
    let db = Arc::new(
        SimpleDb::open(data_path, index_path, SyncStrategy::Never)
            .expect("Failed to open temp DB for API")
    );

    // Keep temp dir alive by leaking it (simplest for test, RAII guard better in prod)
    // Alternatively, return the TempDir handle and keep it alive in the test function.
    std::mem::forget(dir);

    let app_db = db.clone(); // Clone Arc for the app
    let router = api::create_router(app_db);

    // Run the server in a background task
    tokio::spawn(async move {
        let listener = tokio::net::TcpListener::bind(sock_addr).await.unwrap();
         tracing::debug!("Test server listening on {}", sock_addr); // Requires tracing setup
        axum::serve(listener, router.into_make_service()).await.unwrap();
    });

    // Give the server a moment to start (crude, better check needed in real apps)
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    (format!("http://{}", addr), db) // Return base URL and DB handle for direct checks
}


#[tokio::test]
async fn test_api_put_get_delete_cycle() {
     // tracing_subscriber::fmt().init(); // Enable if needed for debugging a specific test
    let (addr, _db) = spawn_app().await; // Keep _db handle if direct verification needed
    let client = Client::new();

    let key = "api_cycle_key";
    let value_text = "api cycle value";
    // --- MODIFIED URL ---
    let url = format!("{}/v1/keys/{}", addr, key);
    // --------------------

    // 1. PUT the value
    let put_resp = client.put(&url)
        .body(value_text)
        .send()
        .await
        .expect("Failed to execute PUT request");
    assert_eq!(put_resp.status(), StatusCode::CREATED);

    // 2. GET the value back
    let get_resp = client.get(&url)
        .send()
        .await
        .expect("Failed to execute GET request");
    assert_eq!(get_resp.status(), StatusCode::OK);
    // Check Content-Type might be octet-stream as we didn't store JSON
     assert!(get_resp.headers().get("content-type").is_some());
     // assert_eq!(get_resp.headers()["content-type"], "application/octet-stream");
    assert_eq!(get_resp.text().await.unwrap(), value_text);


    // 3. DELETE the value
    let del_resp = client.delete(&url)
        .send()
        .await
        .expect("Failed to execute DELETE request");
    assert_eq!(del_resp.status(), StatusCode::NO_CONTENT);

    // 4. GET again, should be 404
    let get_after_del_resp = client.get(&url)
        .send()
        .await
        .expect("Failed to execute GET after DELETE");
    assert_eq!(get_after_del_resp.status(), StatusCode::NOT_FOUND);
    let error_body: Value = get_after_del_resp.json().await.unwrap();
    assert_eq!(error_body, json!({"error": "Key not found"}));

}

#[tokio::test]
async fn test_api_get_not_found() {
    let (addr, _db) = spawn_app().await;
    let client = Client::new();
    let key = "key_that_does_not_exist";
    // --- MODIFIED URL ---
    let url = format!("{}/v1/keys/{}", addr, key);
    // --------------------

    let resp = client.get(&url).send().await.expect("GET failed");
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    let error_body: Value = resp.json().await.unwrap();
    assert_eq!(error_body, json!({"error": "Key not found"}));
}

#[tokio::test]
async fn test_api_delete_not_found() {
    let (addr, _db) = spawn_app().await;
    let client = Client::new();
    let key = "delete_key_that_does_not_exist";
    // --- MODIFIED URL ---
    let url = format!("{}/v1/keys/{}", addr, key);
    // --------------------

    let resp = client.delete(&url).send().await.expect("DELETE failed");
    // Idempotent DELETE returns 204 even if key wasn't there
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);
}


#[tokio::test]
async fn test_api_batch_put() {
    let (addr, db) = spawn_app().await; // Need DB handle to verify
    let client = Client::new();
    // --- MODIFIED URL ---
    let url = format!("{}/v1/keys/batch", addr);
    // --------------------

    let payload = json!({
        "batch_api_1": "value_api_1",
        "batch_api_2": 42,
        "batch_api_3": {"is_json": true}
    });

    let resp = client.post(&url)
        .json(&payload)
        .send()
        .await
        .expect("Batch PUT failed");

    assert_eq!(resp.status(), StatusCode::OK);

    // Verify directly using DB handle
    assert_eq!(db.get(b"batch_api_1").unwrap().unwrap(), serde_json::to_vec("value_api_1").unwrap());
    assert_eq!(db.get(b"batch_api_2").unwrap().unwrap(), serde_json::to_vec(&42).unwrap());
    assert_eq!(db.get(b"batch_api_3").unwrap().unwrap(), serde_json::to_vec(&json!({"is_json": true})).unwrap());
}

#[tokio::test]
async fn test_api_batch_get() {
    let (addr, db) = spawn_app().await; // Need DB handle to setup
    let client = Client::new();
    // --- MODIFIED URL ---
    let batch_get_url = format!("{}/v1/keys/batch/get", addr);
    // --------------------
    // Prefix unused variable with underscore
    let _put_url_base = format!("{}/v1/keys", addr); // Also add /v1 here if used later

    // Setup some data
    db.put(b"batch_get_1", &serde_json::to_vec("value_get_1").unwrap()).unwrap();
    db.put(b"batch_get_2", &serde_json::to_vec(&json!({"num": 123})).unwrap()).unwrap();
    db.put(b"batch_get_plain", b"this is not json").unwrap(); // Store non-json

    let keys_to_get = json!(["batch_get_1", "batch_get_2", "batch_get_plain", "batch_get_missing"]);

    let resp = client.post(&batch_get_url)
        .json(&keys_to_get)
        .send()
        .await
        .expect("Batch GET failed");

    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(resp.headers()["content-type"], "application/json");

    let results: HashMap<String, Option<Value>> = resp.json().await.unwrap();

    assert_eq!(results.len(), 4);
    assert_eq!(results["batch_get_1"], Some(json!("value_get_1")));
    assert_eq!(results["batch_get_2"], Some(json!({"num": 123})));
    assert_eq!(results["batch_get_plain"], None); // Non-JSON data returned as null
    assert_eq!(results["batch_get_missing"], None); // Missing key returned as null

}

#[tokio::test]
async fn test_api_admin_endpoints() {
     let (addr, _db) = spawn_app().await;
     let client = Client::new();

     // Test compaction trigger
     // --- MODIFIED URL ---
     let compact_url = format!("{}/v1/admin/compact", addr);
     // --------------------
     let compact_resp = client.post(&compact_url).send().await.expect("Compact request failed");
     assert_eq!(compact_resp.status(), StatusCode::ACCEPTED);
     // Hard to verify background task finished easily here, check logs in debug

     // Test snapshot trigger
     // --- MODIFIED URL ---
     let snapshot_url = format!("{}/v1/admin/save_snapshot", addr);
     // --------------------
     let snapshot_resp = client.post(&snapshot_url).send().await.expect("Snapshot request failed");
     assert_eq!(snapshot_resp.status(), StatusCode::OK);
     // Could verify index file modification time changed if needed
}

// Add more tests:
// - PUT with large value (test 413)
// - Batch PUT with invalid JSON in request body (test 400)
// - Batch GET with invalid JSON array in request body (test 400)
// - Test URL encoding for keys with special characters
// - Test different Content-Types on PUT and corresponding GET results