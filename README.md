[![Rust](https://github.com/SieteCuatro/dead_simple_db/actions/workflows/rust.yml/badge.svg?branch=master)](https://github.com/SieteCuatro/dead_simple_db/actions/workflows/rust.yml) [![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](./LICENSE) [![dependency status](https://deps.rs/repo/github/SieteCuatro/dead_simple_db/status.svg)](https://deps.rs/repo/github/SieteCuatro/dead_simple_db)
 [![Rust](https://img.shields.io/badge/rust-%23E56F14.svg?style=flat&logo=rust)](https://www.rust-lang.org)
# Dead Simple DB

A basic, persistent key-value store implemented in Rust with an HTTP API built using Axum and Tokio. It focuses on simplicity and demonstrating core database concepts like append-only logging, indexing, snapshotting, compaction, and automatic background maintenance.

## Overview

Dead Simple DB provides a straightforward way to store and retrieve key-value data via an HTTP API. It persists data to an append-only log file, ensuring that writes are durable (depending on the chosen sync strategy). An in-memory hash map acts as an index, mapping keys directly to their latest data location in the log file for fast reads. To mitigate potentially long startup times caused by re-reading the entire log, the index can be periodically saved to a snapshot file. Log compaction is available to reclaim disk space occupied by outdated or deleted data. Automatic background tasks can trigger compaction based on log size and snapshotting based on time intervals.

While simple, it serves as a foundation and learning tool for understanding how key-value stores can be built.

## Features

*   **Persistent Storage:** All data modifications (PUTs and DELETEs) are written sequentially to a durable append-only log file (`.dblog`).
*   **In-Memory Indexing:** Utilizes a `std::collections::HashMap` for fast key lookups, mapping keys (`Vec<u8>`) to their byte offset (`u64`) in the log file.
*   **Index Snapshotting:** Allows saving the current state of the in-memory index and the corresponding log offset to a separate file (`.dblog.index`) using Bincode serialization. This significantly speeds up subsequent database startups. Can be triggered manually or automatically.
*   **Log Compaction:** Implements a process to create a new log file containing only the *latest* version of each key currently present in the index. This removes redundant data from deleted or overwritten keys, reclaiming disk space. The switch to the new log is done atomically. Can be triggered manually or automatically.
*   **Automatic Maintenance:** Configurable background task to automatically trigger:
    *   Log compaction when the log file exceeds a specified size threshold.
    *   Index snapshotting based on a time interval since the last snapshot.
*   **HTTP API (Axum):** Exposes a RESTful interface for interacting with the database:
    *   Individual key operations (GET, PUT, DELETE).
    *   Batch operations for efficient multi-key PUTs and GETs.
    *   Administrative endpoints to manually trigger compaction and snapshotting.
*   **Data Integrity:** Employs CRC32 checksums within each log record to help detect data corruption.
*   **Concurrency Control:** Uses `std::sync::RwLock` to allow multiple concurrent reads or exclusive write access to the database state, ensuring thread safety.
*   **Flexible Configuration:** Supports configuration via defaults, a TOML file (`config.toml`), and command-line arguments, with CLI arguments taking the highest precedence.
*   **Configurable Sync Strategy:** Offers control over disk synchronization for write operations:
    *   `Always`: Performs `fsync` after every write for maximum durability (at the cost of performance).
    *   `Never`: Relies on the operating system's page cache for writes (faster, but risks data loss on system crash or power failure before data is flushed).
*   **Command-Line Interface:** Simple CLI built with `clap` for configuring the server (file paths, listen address, sync strategy, maintenance options, config file path).
*   **Testing:** Includes unit tests for database logic and integration tests for the API and CLI behavior (including configuration layering).

## Getting Started

### Prerequisites

*   **Rust:** Ensure you have a recent Rust toolchain installed (>= 1.70 recommended). You can get it from [rustup.rs](https://rustup.rs/).

### Building

1.  Clone the repository:
    ```bash
    git clone <repository-url> # Replace with the actual URL
    cd dead_simple_db
    ```
2.  Build the project:
    ```bash
    # For a development build (faster compilation, less optimized)
    cargo build

    # For a release build (slower compilation, optimized executable)
    cargo build --release
    ```
    The executable will be located at `target/debug/dead_simple_db` or `target/release/dead_simple_db`.

### Running the Server

You can run the server directly using `cargo run` (which compiles and runs) or by executing the compiled binary. Configuration can be provided via a TOML file and/or command-line arguments.

**Configuration Precedence:**

Settings are determined in the following order (later steps override earlier ones):
1.  Internal Defaults (e.g., listen on `127.0.0.1:7878`, use `data.dblog`, `sync = never`, auto-maintenance disabled).
2.  Values from `config.toml` (if found in the current directory).
3.  Values from a config file specified using the `--config <FILE>` argument (overrides `config.toml` if both are present).
4.  Values provided directly via other command-line arguments (e.g., `--listen`, `--data-file`, `--sync`, `--auto-compact-threshold-bytes`).

**Example `config.toml`:**

Create a file named `config.toml` in the directory where you run the server:

```toml
# Example config.toml for dead_simple_db

# --- File Paths ---
# Optional: Specify the path to the main database log file where records are stored.
# Default: "data.dblog"
# data_file = "/var/db/dead_simple_db/data.dblog"

# Optional: Specify the path to the index snapshot file.
# Default: <data_file_path>.index (e.g., "data.dblog.index")
# index_file = "/var/db/dead_simple_db/data.dblog.index"

# --- Network Settings ---
# Optional: Specify the network IP address and port for the HTTP API server to listen on.
# Default: "127.0.0.1:7878"
# listen = "0.0.0.0:8000"

# --- Performance and Durability ---
# Optional: Specify the disk synchronization strategy for writes ("always" or "never").
# Default: "never"
# sync = "always"
sync = "never"

# --- Automatic Maintenance ---
# Optional: Automatically trigger log compaction when the log file exceeds this size in bytes.
# Set to 0 to disable automatic compaction based on size.
# Example: 1073741824 = 1 GiB
# Default: 0 (disabled)
# auto_compact_threshold_bytes = 1073741824
auto_compact_threshold_bytes = 0

# Optional: Automatically trigger index snapshotting at this interval in seconds.
# Set to 0 to disable time-based automatic snapshotting.
# Example: 3600 = 1 hour
# Default: 0 (disabled)
# auto_snapshot_interval_secs = 3600
auto_snapshot_interval_secs = 0

# Optional: Interval in seconds for checking maintenance conditions.
# How often the background task wakes up to check log size / snapshot time.
# Default: 60
# maintenance_check_interval_secs = 30
```

**Running Examples:**

```bash
# Run with internal defaults (will look for config.toml optionally)
./target/release/dead_simple_db

# Run using settings from ./config.toml (if it exists)
./target/release/dead_simple_db

# Specify a custom config file
./target/release/dead_simple_db --config /etc/dead_simple_db.toml

# Use cargo run and override listen address from config/defaults via CLI flag
cargo run -- --listen 127.0.0.1:8000

# Specify data file, sync strategy, and enable auto-compaction via CLI
# (These override any config file settings for these specific options)
./target/release/dead_simple_db --data-file /var/db/app.dblog --sync always --auto-compact-threshold-bytes 536870912
```

**Command-Line Options:**

*   `-c, --config <FILE>`: Path to a configuration file (TOML format).
*   `-d, --data-file <FILE>`: Path to the database log file. Overrides config file setting. (Default: `data.dblog`).
*   `--index-file <FILE>`: Path to the index snapshot file. Overrides config file setting. If omitted, defaults to data file path + `.index`.
*   `-l, --listen <IP:PORT>`: Network address and port to listen on. Overrides config file setting. (Default: `127.0.0.1:7878`).
*   `--sync <always|never>`: Write synchronization strategy. Overrides config file setting. (Default: `never`).
*   `--auto-compact-threshold-bytes <BYTES>`: Automatic compaction threshold in bytes (0 to disable). Overrides config file setting. (Default: 0).
*   `--auto-snapshot-interval-secs <SECONDS>`: Automatic snapshot interval in seconds (0 to disable). Overrides config file setting. (Default: 0).
*   `--maintenance-check-interval-secs <SECONDS>`: Maintenance check interval in seconds. Overrides config file setting. (Default: 60).
*   `-h, --help`: Print help information and exit.
*   `-V, --version`: Print version information and exit.

### Running Tests

Execute the comprehensive test suite (unit tests, API integration, CLI integration) using:

```bash
cargo test
```

## API Endpoints

All API endpoints are prefixed with `/v1`. The following examples assume the server is running on the default `http://127.0.0.1:7878`. Adjust the address if you configured it differently.

---

**`GET /v1/keys/{key}`**

*   **Method:** `GET`
*   **Description:** Retrieves the value associated with the given `{key}`.
*   **Path Parameter:**
    *   `{key}` (string): The key to retrieve (URL-encoded if necessary).
*   **Success Response:**
    *   **Code:** `200 OK`
    *   **Content:** The raw value associated with the key.
    *   **Headers:**
        *   `Content-Type`: `application/json` if the stored data is valid JSON, otherwise `application/octet-stream`.
*   **Error Responses:**
    *   **Code:** `404 Not Found`, Body: `{"error": "Key not found"}`
    *   **Code:** `500 Internal Server Error`, Body: `{"error": "..."}` (e.g., "Internal storage error")
*   **Example (`curl`):**
    ```bash
    # Get key 'mykey' (assuming value is plain text or binary)
    curl -v http://127.0.0.1:7878/v1/keys/mykey

    # Get key 'my_json_key' (assuming value is JSON)
    curl http://127.0.0.1:7878/v1/keys/my_json_key | jq .
    ```

---

**`PUT /v1/keys/{key}`**

*   **Method:** `PUT`
*   **Description:** Stores or updates the value for the given `{key}`. This is an upsert operation. The request body is stored as raw bytes.
*   **Path Parameter:**
    *   `{key}` (string): The key to store/update (URL-encoded if necessary).
*   **Request Body:** The raw bytes of the value to store.
*   **Headers (Request):** Clients should set `Content-Type` appropriately (e.g., `application/json`, `text/plain`, `application/octet-stream`), although the server currently stores the raw bytes regardless.
*   **Success Response:**
    *   **Code:** `201 Created` (No body)
*   **Error Responses:**
    *   **Code:** `413 Payload Too Large`, Body: `{"error": "Value too large (limit: ..., actual: ...)"}` or `{"error": "Key too large ..."}`
    *   **Code:** `500 Internal Server Error`, Body: `{"error": "..."}`
*   **Example (`curl`):**
    ```bash
    # Put plain text value
    curl -X PUT -v -H "Content-Type: text/plain" --data "This is the value" http://127.0.0.1:7878/v1/keys/mytextkey

    # Put JSON value
    curl -X PUT -v -H "Content-Type: application/json" --data '{"message": "hello", "count": 5}' http://127.0.0.1:7878/v1/keys/myjsonkey

    # Put binary data from a file
    curl -X PUT -v -H "Content-Type: application/octet-stream" --data-binary "@./image.jpg" http://127.0.0.1:7878/v1/keys/myimage
    ```

---

**`DELETE /v1/keys/{key}`**

*   **Method:** `DELETE`
*   **Description:** Removes the key and its associated value from the database by writing a deletion marker (tombstone) to the log.
*   **Path Parameter:**
    *   `{key}` (string): The key to delete (URL-encoded if necessary).
*   **Success Response:**
    *   **Code:** `204 No Content` (Returned even if the key did not exist previously, ensuring idempotency).
*   **Error Responses:**
    *   **Code:** `500 Internal Server Error`, Body: `{"error": "..."}`
*   **Example (`curl`):**
    ```bash
    curl -X DELETE -v http://127.0.0.1:7878/v1/keys/mykey_to_delete
    ```

---

**`POST /v1/keys/batch`**

*   **Method:** `POST`
*   **Description:** Stores or updates multiple key-value pairs efficiently in a single request. Values are internally serialized to their JSON byte representation before storage.
*   **Request Body:** Must be `application/json`. A JSON object where keys are the database keys (strings) and values are the corresponding values (any valid JSON type).
    ```json
    {
      "user:123": {"name": "Alice", "email": "alice@example.com"},
      "product:abc": {"price": 99.99, "in_stock": true},
      "config:feature_x": true
    }
    ```
*   **Success Response:**
    *   **Code:** `200 OK` (No body)
*   **Error Responses:**
    *   **Code:** `400 Bad Request`, Body: `{"error": "Invalid JSON value for key '...'..."}` (if a value fails internal serialization) or if the request body is not a valid JSON object.
    *   **Code:** `413 Payload Too Large`, Body: `{"error": "Value too large..."}` or `{"error": "Key too large ..."}`
    *   **Code:** `500 Internal Server Error`, Body: `{"error": "..."}`
*   **Example (`curl`):**
    ```bash
    curl -X POST -v -H "Content-Type: application/json" \
         --data '{"keyA": "valA", "keyB": 100, "keyC": null}' \
         http://127.0.0.1:7878/v1/keys/batch
    ```

---

**`POST /v1/keys/batch/get`**

*   **Method:** `POST`
*   **Description:** Retrieves the values for multiple specified keys efficiently in a single request.
*   **Request Body:** Must be `application/json`. A JSON array of strings, where each string is a key to retrieve.
    ```json
    [ "user:123", "product:abc", "config:feature_y", "key_not_found" ]
    ```
*   **Success Response:**
    *   **Code:** `200 OK`
    *   **Headers:** `Content-Type: application/json`
    *   **Body:** A JSON object mapping each requested key (string) to its corresponding value.
        *   If the key exists and the stored data is valid JSON, the parsed JSON value is returned.
        *   If the key exists but the stored data is *not* valid JSON (e.g., raw bytes stored via single PUT), `null` is returned for that key.
        *   If the key does not exist, `null` is returned for that key.
    ```json
    {
      "user:123": { "name": "Alice", "email": "alice@example.com" },
      "product:abc": { "price": 99.99, "in_stock": true },
      "config:feature_y": null, // Assuming this was stored as non-JSON or doesn't exist
      "key_not_found": null
    }
    ```
*   **Error Responses:**
    *   **Code:** `400 Bad Request`, if the request body is not a valid JSON array of strings.
    *   **Code:** `500 Internal Server Error`, Body: `{"error": "..."}`
*   **Example (`curl`):**
    ```bash
    curl -X POST -v -H "Content-Type: application/json" \
         --data '["keyA", "key_does_not_exist", "keyC"]' \
         http://127.0.0.1:7878/v1/keys/batch/get | jq .
    ```

---

**`POST /v1/admin/compact`**

*   **Method:** `POST`
*   **Description:** **Asynchronously** triggers the log compaction process. The server responds immediately while compaction runs in the background. See server logs for completion status or errors during compaction. This is idempotent; if compaction is already running (e.g., triggered automatically), the request will likely still return `202 Accepted` but will not start a second concurrent compaction.
*   **Success Response:**
    *   **Code:** `202 Accepted` (Indicates the request was received and processing started or is already in progress).
*   **Error Responses:**
    *   **Code:** `500 Internal Server Error`, Body: `{"error": "..."}` (If there's an issue initiating the background task itself).
*   **Example (`curl`):**
    ```bash
    curl -X POST -v http://127.0.0.1:7878/v1/admin/compact
    ```

---

**`POST /v1/admin/save_snapshot`**

*   **Method:** `POST`
*   **Description:** **Synchronously** saves the current state of the in-memory index to the configured index snapshot file. The server will wait for the snapshot to complete before responding. This is idempotent; if a snapshot is already running (e.g., triggered automatically), the request might block or fail depending on internal locking, but typically results in a successful response once a snapshot is complete.
*   **Success Response:**
    *   **Code:** `200 OK`
*   **Error Responses:**
    *   **Code:** `500 Internal Server Error`, Body: `{"error": "..."}` (If saving or syncing the snapshot file fails).
*   **Example (`curl`):**
    ```bash
    curl -X POST -v http://127.0.0.1:7878/v1/admin/save_snapshot
    ```

---

## Storage Format Details

### Log File (`<name>.dblog`)

This file stores a sequence of records representing database operations. It's designed to be append-only during normal operation (compaction replaces it).

**Record Structure:**

```
+---------------------------+-------------+----------------+-----------------+------------------+-------------------+--------------------+
| CRC32 Checksum (4 bytes)  | Timestamp   | Record Type    | Key Length      | Key Data         | Value Length      | Value Data         |
| (Big Endian, IEEE)        | (8 bytes)   | (1 byte)       | (4 bytes)       | (variable)       | (4 bytes, only PUT)| (variable, only PUT)|
|                           | (BE, nanos) | (0x01=PUT,     | (BE, len of Key)| (UTF-8 suggested | (BE, len of Value)| (Raw Bytes)        |
|                           |             |  0x02=DELETE)  |   Data)         |   but not enforced)| Data)             |                    |
+---------------------------+-------------+----------------+-----------------+------------------+-------------------+--------------------+
```

*   **CRC32 Checksum:** Calculated over all subsequent fields in the record (Timestamp to Value Data). Used to verify record integrity on reload. Uses the `CRC_32_ISCSI` polynomial.
*   **Timestamp:** Nanoseconds since the UNIX epoch when the record was written. Currently informational.
*   **Record Type:** `0x01` for a PUT operation (key/value stored), `0x02` for a DELETE operation (key marked for removal).
*   **Key Length:** The number of bytes in the `Key Data` field.
*   **Key Data:** The raw bytes representing the key.
*   **Value Length:** The number of bytes in the `Value Data` field. *Only present for PUT records.*
*   **Value Data:** The raw bytes representing the value. *Only present for PUT records.*

### Index File (`<name>.dblog.index`)

This is a binary file created using the `bincode` serialization library. It stores a snapshot of the in-memory index.

**Structure (Conceptual):**

```
+--------------------------------------------------------------------+
| Bincode Encoded `IndexSnapshot` Struct                             |
+--------------------------------------------------------------------+
| IndexSnapshot {                                                    |
|   offset: u64,          // Log offset up to which index is valid    |
|   index: HashMap<       // The actual key -> offset mapping         |
|              Vec<u8>,   // Key bytes                               |
|              u64        // Offset of PUT record in .dblog file     |
|            >,                                                      |
| }                                                                  |
+--------------------------------------------------------------------+
```

*   **offset:** Stores the byte offset in the `.dblog` file. When reloading, the server only needs to read log records *after* this offset to update the index loaded from the snapshot.
*   **index:** A direct serialization of the `HashMap` used internally to track the location of the latest value for each key.

## Architecture

The project is structured as a Rust library crate with a binary (`main.rs`) that uses the library to run the server.

*   **`main.rs`:**
    *   Entry point of the application.
    *   Parses command-line arguments using `clap`.
    *   Initializes `tracing` subscriber for logging.
    *   Loads configuration from defaults, optional TOML file, and CLI arguments.
    *   Creates and manages the `SimpleDb` instance within an `Arc`.
    *   Sets up the Axum HTTP router using `api::create_router`.
    *   Binds the TCP listener and starts the Axum server.
    *   Handles graceful shutdown on receiving a `CTRL+C` signal, attempting final DB sync/snapshot.
    *   **Spawns an optional background maintenance task** for automatic compaction and snapshotting based on configuration.
*   **`db.rs`:**
    *   Defines `SimpleDb` (the public interface) and `DbInner` (the internal state).
    *   `SimpleDb` holds an `Arc<RwLock<DbInner>>` providing thread-safe access.
    *   `DbInner` contains the `BufWriter` and `File` handles for the log, the `HashMap` index, file paths, sync state, and **last snapshot time**.
    *   Implements core database operations: `put`, `get`, `delete`, `batch_put`, `batch_get`.
    *   Handles log record serialization/deserialization and CRC32 checks.
    *   Manages index loading (from snapshot + log replay) via `update_index_from_log`.
    *   Implements index snapshotting (`save_index_snapshot`) and log compaction (`compact`).
    *   **Includes helper methods for the maintenance task** (`get_last_snapshot_time`, `get_log_size_for_compaction_check`).
    *   Includes unit tests (`#[cfg(test)] mod tests`).
*   **`api.rs`:**
    *   Defines Axum route handlers (`get_key`, `put_key`, `delete_key`, `batch_put`, `batch_get`, `trigger_compaction`, `trigger_save_snapshot`).
    *   Uses Axum extractors (`State`, `Path`, `Json`, `Bytes`) to access database state and request data.
    *   Defines `ApiError` enum for API-level errors and implements `IntoResponse` to map both `ApiError` and `DbError` to appropriate HTTP status codes and JSON error bodies.
    *   Contains the `create_router` function to build the Axum application router.
    *   The `trigger_compaction` handler now returns `202 Accepted` and spawns a background task.
*   **`error.rs`:**
    *   Defines the `DbError` enum using `thiserror` for structured database-related errors (I/O, serialization, not found, limits, etc.).
    *   Provides a `DbResult<T>` type alias.
*   **`lib.rs`:**
    *   Declares the public modules (`db`, `error`, `api`) making them available to the binary crate and external users if published.
*   **`tests/`:**
    *   `api_integration.rs`: Contains tests that spawn a real server instance on a random port and use an HTTP client (`reqwest`) to verify API endpoint behavior. (Should be updated to check for `202 Accepted` from `/admin/compact`).
    *   `cli_integration.rs`: Contains tests that run the compiled binary as a separate process (`assert_cmd`) to verify command-line argument handling, configuration layering (including new maintenance flags), and basic server startup/shutdown.

## Error Handling

Error handling occurs at two main levels:

1.  **`DbError` (`error.rs`):** Represents errors originating from the database core logic (I/O failures, data corruption like CRC mismatches, serialization issues during snapshotting, key/value size limits exceeded, lock poisoning).
2.  **`ApiError` (`api.rs`):** Represents errors specific to the API layer (e.g., bad requests) or acts as a wrapper around `DbError`. It implements `axum::response::IntoResponse` to translate these errors into user-friendly HTTP responses:
    *   `DbError::KeyNotFound` -> `404 Not Found`
    *   `DbError::KeyTooLarge`, `DbError::ValueTooLarge` -> `413 Payload Too Large`
    *   `ApiError::BadRequest` -> `400 Bad Request`
    *   Most other `DbError` variants (like IO, Bincode, LockPoisoned, CompactionError) and `ApiError::Internal` -> `500 Internal Server Error`

Internal Server Errors log the underlying detailed `DbError` or internal message to the server console/logs for debugging but return a generic error message to the client. Errors occurring within background maintenance tasks are currently only logged server-side.

## Concurrency

The database uses `Arc<RwLock<DbInner>>` to manage concurrent access:

*   `Arc`: Allows multiple owners of the database state pointer, making it cheap to share the `SimpleDb` instance across Axum handlers (which run in different Tokio tasks) and the background maintenance task.
*   `RwLock`: Provides read-write locking.
    *   Multiple threads/tasks can acquire a read lock simultaneously (e.g., for concurrent `GET` or `batch_get` operations, checking maintenance conditions like snapshot time or log size).
    *   Only one thread/task can acquire a write lock at a time (e.g., for `PUT`, `DELETE`, `batch_put`, `compact`, `save_index_snapshot`). Write locks block all other readers and writers until released.

This model allows high read concurrency while ensuring safety during write operations, index updates, and maintenance state changes. File operations themselves might involve internal OS-level locking, but the `RwLock` primarily protects the consistency of the in-memory index, the writer's position, and shared maintenance state (like `last_snapshot_time`). The background maintenance task uses `AtomicBool` flags to prevent redundant triggering of the *same* long-running operation (compaction or snapshotting) if conditions remain met across check intervals.

## Compaction & Snapshotting Explained

*   **Why?** Append-only logs grow indefinitely. Overwritten values and deleted keys leave "garbage" data in the log file, consuming disk space. Reading the entire log on startup to rebuild the index can become very slow for large logs.
*   **Snapshotting:** Periodically saves the current `HashMap` index (which only references *live* data offsets) to disk. On startup, the server loads this snapshot and only needs to read the *tail* of the log file (records added since the snapshot) to bring the index fully up-to-date. This dramatically reduces startup time.
    *   **Manual Trigger:** `POST /admin/save_snapshot` (Synchronous).
    *   **Automatic Trigger:** Occurs if `auto_snapshot_interval_secs` is non-zero and the configured time has elapsed since the last snapshot.
*   **Compaction:** Reads the current index, then iterates through it, reading the *value* for each live key from the *old* log file and writing a *new* PUT record (key + latest value) to a *new*, temporary log file. Once all live keys are written, the temporary log file and a corresponding new index snapshot are atomically swapped with the old files. This process effectively garbage collects the old log and creates an up-to-date snapshot.
    *   **Manual Trigger:** `POST /admin/compact` (Asynchronous trigger, returns `202 Accepted`).
    *   **Automatic Trigger:** Occurs if `auto_compact_threshold_bytes` is non-zero and the current log file size meets or exceeds the threshold.

## Future Improvements / Roadmap

*   **Security:**
    *   **Authentication/Authorization:** Implement API keys, JWT, or other mechanisms to secure the API.
    *   **TLS/HTTPS Support:** Add configuration options for encrypted communication.
*   **Operational:**
    *   **Enhanced Logging:** Add configuration for log level and file output.
    *   **Metrics:** Integrate Prometheus or similar for exposing operational metrics.
    *   **Rate Limiting:** Add middleware to prevent abuse.
    *   **Maintenance Feedback:** Provide API endpoint(s) to check the status of ongoing or last completed maintenance tasks.
    *   **Robustness:** Further improve error handling during compaction/snapshot swap phases.
*   **Core Features:**
    *   **Transactions:** Explore adding support for atomic multi-key operations.
    *   **Memory Usage Optimization:** Investigate options for handling datasets larger than RAM.
    *   **Streaming API:** Allow streaming large values.
    *   **Replication:** Introduce primary/secondary replication.
    *   **Time-To-Live (TTL):** Add support for key expiration.
    *   **Enhanced Querying:** Consider support for range scans, prefix searches.
    *   **Configurable Limits:** Allow key/value size limits to be set via configuration.
*   **Observability:** Integrate distributed tracing capabilities.

## License

This project is licensed under the MIT License. See the `LICENSE` file for details.
