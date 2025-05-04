use crate::error::{DbError, DbResult};
use crc::{Crc, CRC_32_ISCSI};
use serde::{Deserialize, Serialize}; // Needed for index snapshot
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{debug, error, info, warn};

const CRC_CALCULATOR: Crc<u32> = Crc::<u32>::new(&CRC_32_ISCSI);
const MAX_KEY_SIZE: usize = 1024 * 4; // 4 KiB limit for keys
const MAX_VALUE_SIZE: usize = 1024 * 1024 * 16; // 16 MiB limit for values

// Record type markers
const RECORD_TYPE_PUT: u8 = 0x01;
const RECORD_TYPE_DELETE: u8 = 0x02;

/// Strategy for syncing writes to disk.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SyncStrategy {
    /// Sync on every write operation (safer, slower).
    Always,
    /// Rely on OS caching, flush buffer only (faster, risk of data loss on crash).
    #[default]
    Never,
}

// Snapshot structure for serialization
#[derive(Serialize, Deserialize, Debug)]
struct IndexSnapshot {
    offset: u64, // Log offset up to which this index is valid
    index: HashMap<Vec<u8>, u64>,
}

// Contains the actual database state, protected by the RwLock in SimpleDb
#[derive(Debug)]
struct DbInner {
    writer: BufWriter<File>,
    // Keep the reader handle primarily for cloning during GET operations.
    reader_file: File,
    index: HashMap<Vec<u8>, u64>,
    path: PathBuf,       // Path to the main data file
    index_path: PathBuf, // Path to the index snapshot file
    last_sync_offset: u64, // Keep track for potential periodic syncing later
}

// The main database structure, providing thread-safe access via RwLock
#[derive(Debug, Clone)] // Clone is cheap (Arc increment)
pub struct SimpleDb {
    inner: Arc<RwLock<DbInner>>, // Use Arc for cheaper cloning in API state
    sync_strategy: SyncStrategy,
}

impl SimpleDb {
    /// Opens or creates the database file and index snapshot at the given paths.
    /// Loads the index from snapshot + log file on startup.
    pub fn open<P: AsRef<Path>>(
        path: P,
        index_path: P,
        sync_strategy: SyncStrategy,
    ) -> DbResult<Self> {
        let path = path.as_ref().to_path_buf();
        let index_path = index_path.as_ref().to_path_buf();
        info!(
            "Opening database file: {:?}, index path: {:?}",
            path, index_path
        );

        // 1. Load index from snapshot if available
        let mut index = HashMap::new();
        let mut log_start_offset = 0;

        if index_path.exists() {
            info!("Loading index snapshot from {:?}", index_path);
            match File::open(&index_path) {
                Ok(file) => {
                    let reader = BufReader::new(file);
                    match bincode::deserialize_from::<_, IndexSnapshot>(reader) {
                        Ok(snapshot) => {
                            index = snapshot.index;
                            log_start_offset = snapshot.offset;
                            info!(
                                "Loaded index snapshot with {} keys, valid up to log offset {}",
                                index.len(),
                                log_start_offset
                            );
                        }
                        Err(e) => {
                            warn!("Failed to deserialize index snapshot: {}. Rebuilding from full log.", e);
                            // Proceed as if snapshot didn't exist
                        }
                    }
                }
                Err(e) => {
                    warn!(
                        "Failed to open index snapshot file {:?}: {}. Rebuilding from full log.",
                        index_path, e
                    );
                    // Proceed as if snapshot didn't exist
                }
            }
        } else {
            info!("No index snapshot found. Index will be built from log.");
        }

        // 2. Open file for writing (append, create)
        let write_file = OpenOptions::new()
            .read(true) // Needed for sync_all
            .append(true)
            .create(true)
            .open(&path)?;

        // 3. Open the *same* file again for reading
        let read_file = File::open(&path)?;

        // 4. Update index by reading the log file *after* the snapshot offset
        if log_start_offset < write_file.metadata()?.len() {
            info!(
                "Updating index from log file starting at offset {}",
                log_start_offset
            );
            // Clone reader file handle for index update
            let read_file_for_index = read_file.try_clone()?;
            let mut reader_for_index = BufReader::new(read_file_for_index);
            Self::update_index_from_log(&mut reader_for_index, &mut index, log_start_offset)?;
        } else {
            info!("Log file does not contain new records since last snapshot (or is empty).");
        }

        // 5. Prepare the writer and store state
        let mut writer = BufWriter::new(write_file);
        // Ensure writer points to the end for appending
        let last_sync_offset = writer.seek(SeekFrom::End(0))?;

        let inner = DbInner {
            writer,
            reader_file: read_file, // Keep original handle for cloning
            index,
            path,
            index_path,
            last_sync_offset,
        };

        info!(
            "Database opened successfully. Final index contains {} keys.",
            inner.index.len()
        );
        Ok(SimpleDb {
            inner: Arc::new(RwLock::new(inner)),
            sync_strategy,
        })
    }

    /// Updates the in-memory index by reading the log file sequentially from a given offset.
    fn update_index_from_log(
        reader: &mut BufReader<File>,
        index: &mut HashMap<Vec<u8>, u64>,
        start_offset: u64,
    ) -> DbResult<()> {
        let mut current_offset = match reader.seek(SeekFrom::Start(start_offset)) {
            Ok(offset) if offset == start_offset => offset,
            Ok(offset) => {
                 error!("Failed to seek to desired start offset {} for index update, got {}", start_offset, offset);
                 return Err(DbError::Io(std::io::Error::new(std::io::ErrorKind::Other, "Failed to seek for index update")));
            }
            Err(e) => {
                error!("Failed to seek to start offset {} for index update: {:?}", start_offset, e);
                return Err(DbError::Io(e));
            }
        };
        let file_len = reader.get_ref().metadata()?.len();
        info!("Reading log from offset {} to {}", current_offset, file_len);

        while current_offset < file_len {
            let record_start_offset = current_offset;
            debug!("IndexUpdate: Reading record at offset: {}", record_start_offset);

            // --- Read Header Fields ---
            let mut header_buf = [0u8; 4 + 8 + 1 + 4]; // CRC, TS, Type, KeyLen
            if let Err(e) = reader.read_exact(&mut header_buf) {
                 if e.kind() == std::io::ErrorKind::UnexpectedEof && current_offset == file_len {
                     debug!("Clean EOF reached at end of log file.");
                     break; // Expected end of file
                 }
                 warn!("Unexpected EOF or I/O error reading header at offset {}: {}. Stopping index update.", record_start_offset, e);
                 // Decide if this is fatal or just stops indexing? Let's stop.
                 return Err(DbError::Io(e));
             }

            let stored_crc = u32::from_be_bytes(header_buf[0..4].try_into().unwrap());
            let ts_bytes = &header_buf[4..12];
            let record_type = header_buf[12];
            let key_len_bytes = &header_buf[13..17];
            let key_len = u32::from_be_bytes(key_len_bytes.try_into().unwrap()) as usize;

            // Basic sanity check
             if key_len > MAX_KEY_SIZE {
                 warn!("Record at offset {} has oversized key ({} bytes). Skipping corrupt record.", record_start_offset, key_len);
                 // Attempt to recover by finding next record? Hard. Stop for now.
                 return Err(DbError::InvalidRecord{offset: record_start_offset, reason: format!("Key size {} exceeds limit {}", key_len, MAX_KEY_SIZE)});
             }

            // --- Read Variable Length Data ---
            let mut key_data = vec![0u8; key_len];
            if let Err(e) = reader.read_exact(&mut key_data) { /* Handle error similar to header read */ return Err(DbError::Io(e)); }

            let mut value_len: u32 = 0;
            let mut value_len_bytes = [0u8; 4];
            let mut value_data: Option<Vec<u8>> = None;

            if record_type == RECORD_TYPE_PUT {
                if let Err(e) = reader.read_exact(&mut value_len_bytes) { /* Handle error */ return Err(DbError::Io(e)); }
                value_len = u32::from_be_bytes(value_len_bytes);
                if value_len as usize > MAX_VALUE_SIZE {
                    warn!("Record at offset {} has oversized value ({} bytes). Skipping corrupt record.", record_start_offset, value_len);
                    return Err(DbError::InvalidRecord{offset: record_start_offset, reason: format!("Value size {} exceeds limit {}", value_len, MAX_VALUE_SIZE)});
                }
                let mut val_bytes = vec![0u8; value_len as usize];
                if let Err(e) = reader.read_exact(&mut val_bytes) { /* Handle error */ return Err(DbError::Io(e)); }
                value_data = Some(val_bytes);
            } else if record_type != RECORD_TYPE_DELETE {
                warn!("Skipping unknown record type {} at offset {}", record_type, record_start_offset);
                // Calculate skip length carefully
                 let record_header_key_len = 4 + 8 + 1 + 4 + key_len as u64;
                 current_offset = record_start_offset + record_header_key_len;
                 // Value length field and value data are only present for PUT
                 reader.seek(SeekFrom::Start(current_offset))?; // Seek past header+key
                 continue; // Move to next record attempt
            }

            // --- Verify CRC ---
            let mut bytes_for_crc = Vec::with_capacity(8 + 1 + 4 + key_len + if value_data.is_some() { 4 + value_len as usize } else { 0 });
            bytes_for_crc.extend_from_slice(ts_bytes);
            bytes_for_crc.push(record_type);
            bytes_for_crc.extend_from_slice(key_len_bytes);
            bytes_for_crc.extend_from_slice(&key_data);
            if let Some(ref val_data) = value_data {
                bytes_for_crc.extend_from_slice(&value_len_bytes);
                bytes_for_crc.extend_from_slice(val_data);
            }

            let calculated_crc = CRC_CALCULATOR.checksum(&bytes_for_crc);

            if calculated_crc != stored_crc {
                warn!(
                    "CRC mismatch at offset {}: Stored=0x{:08x}, Calculated=0x{:08x}. Skipping record.",
                    record_start_offset, stored_crc, calculated_crc
                );
                 // Calculate skip length
                 let record_header_key_len = 4 + 8 + 1 + 4 + key_len as u64;
                 current_offset = record_start_offset + record_header_key_len;
                 if record_type == RECORD_TYPE_PUT {
                     current_offset += 4 + value_len as u64; // Value len bytes + value data
                 }
                 reader.seek(SeekFrom::Start(current_offset))?; // Try seeking past it
                 continue;
            }

            // --- Update Index ---
            if record_type == RECORD_TYPE_PUT {
                debug!("IndexUpdate: Indexing PUT for key (len {}) at offset {}", key_data.len(), record_start_offset);
                index.insert(key_data, record_start_offset);
            } else if record_type == RECORD_TYPE_DELETE {
                 debug!("IndexUpdate: Processing DELETE for key (len {}) at offset {}", key_data.len(), record_start_offset);
                index.remove(&key_data);
            }

            // --- Advance Offset ---
             current_offset = record_start_offset + (4 + bytes_for_crc.len()) as u64;
             let actual_pos = reader.stream_position()?;
             if actual_pos != current_offset {
                 warn!("Reader position mismatch after record read: expected {}, got {}. Stopping index update.", current_offset, actual_pos);
                  // This might indicate partial read or corruption, safer to stop.
                  return Err(DbError::Internal("Reader position mismatch during index build".to_string()));
             }

        } // End while loop

        info!("Index update complete. Current index size: {}", index.len());
        Ok(())
    }


    /// Internal helper to write a record (PUT or DELETE) to the log file.
    /// Returns the starting offset of the written record.
    fn write_record(&self, key: &[u8], value: Option<&[u8]>) -> DbResult<u64> {
        let mut inner = self
            .inner
            .write()
            .map_err(|e| DbError::LockPoisoned(e.to_string()))?;

        // Ensure writer is at the end (should be due to append mode, but check)
        let current_offset = inner.writer.seek(SeekFrom::End(0))?;

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;

        let record_type = if value.is_some() {
            RECORD_TYPE_PUT
        } else {
            RECORD_TYPE_DELETE
        };
        let key_len = key.len() as u32;
        let value_len = value.map_or(0, |v| v.len() as u32);

        // --- Prepare data for CRC calculation ---
        let mut bytes_for_crc = Vec::new();
        let ts_bytes = timestamp.to_be_bytes();
        let key_len_bytes = key_len.to_be_bytes();
        let value_len_bytes = value_len.to_be_bytes();

        bytes_for_crc.extend_from_slice(&ts_bytes);
        bytes_for_crc.push(record_type);
        bytes_for_crc.extend_from_slice(&key_len_bytes);
        bytes_for_crc.extend_from_slice(key);
        if let Some(val) = value {
            bytes_for_crc.extend_from_slice(&value_len_bytes);
            bytes_for_crc.extend_from_slice(val);
        }

        let crc_value = CRC_CALCULATOR.checksum(&bytes_for_crc);

        // --- Write full record ---
        inner.writer.write_all(&crc_value.to_be_bytes())?;
        inner.writer.write_all(&bytes_for_crc)?;

        let bytes_written = (4 + bytes_for_crc.len()) as u64;
        let next_offset = current_offset + bytes_written;

        // --- Conditional Flush & Sync ---
        // Always flush the BufWriter to ensure data is passed to OS buffer
        inner.writer.flush()?; // Error handled below

        if self.sync_strategy == SyncStrategy::Always {
             if let Err(e) = inner.writer.get_ref().sync_all() {
                 error!("Failed to sync file after write: {:?}", e);
                 // Even if sync fails, data might be in OS cache. Should we rollback index? Complex.
                 // Let's return error and leave index potentially inconsistent until next build.
                 return Err(DbError::Io(e));
             }
             inner.last_sync_offset = next_offset; // Record that we synced up to here
             debug!("Writer flushed and synced (Strategy: Always)");
         } else {
             debug!("Writer flushed (Strategy: Never)");
         }

        // --- Update Index (only if write and sync (if applicable) were okay) ---
        if record_type == RECORD_TYPE_PUT {
            inner.index.insert(key.to_vec(), current_offset);
            debug!(
                "PUT record written for key (len {}) at offset {}",
                key.len(),
                current_offset
            );
        } else {
            // DELETE
            inner.index.remove(key);
            debug!(
                "DELETE record written for key (len {}) at offset {}",
                key.len(),
                current_offset
            );
        }

        Ok(current_offset)
    }


    /// Optimized internal helper to read only the value data for a key at a specific offset.
    /// Assumes the offset is valid and points to a PUT record. NO CRC CHECK performed here.
    fn read_value_only_at_offset(&self, offset: u64) -> DbResult<Vec<u8>> {
        // Acquire read lock only to clone the file handle
        let read_file_clone = {
            let inner = self
                .inner
                .read()
                .map_err(|e| DbError::LockPoisoned(e.to_string()))?;
            inner.reader_file.try_clone()?
        };

        let mut reader = BufReader::new(read_file_clone);

        // Seek to the record's start offset
        reader.seek(SeekFrom::Start(offset))?;
        debug!("OptimizedRead: Reading value at offset: {}", offset);

        // Read/skip fixed headers up to key_len
        // CRC (4) + Timestamp (8) + Type (1) = 13 bytes
        reader.seek_relative(13)?;

        // Read key length
        let mut key_len_bytes = [0u8; 4];
        reader.read_exact(&mut key_len_bytes)?;
        let key_len = u32::from_be_bytes(key_len_bytes) as u64;

        // Skip the key data
        reader.seek_relative(key_len as i64)?;

        // Read value length
        let mut value_len_bytes = [0u8; 4];
        reader.read_exact(&mut value_len_bytes)?;
        let value_len = u32::from_be_bytes(value_len_bytes) as usize;

        // Read the value data directly
        let mut value_data = vec![0u8; value_len];
        reader.read_exact(&mut value_data)?;

        debug!(
            "OptimizedRead: Successfully read value ({} bytes) at offset {}",
            value_data.len(),
            offset
        );
        Ok(value_data)
    }


    /// Puts a key-value pair into the database.
    pub fn put(&self, key: &[u8], value: &[u8]) -> DbResult<()> {
        if key.len() > MAX_KEY_SIZE {
            return Err(DbError::KeyTooLarge {
                limit: MAX_KEY_SIZE,
                actual: key.len(),
            });
        }
        if value.len() > MAX_VALUE_SIZE {
            return Err(DbError::ValueTooLarge {
                limit: MAX_VALUE_SIZE,
                actual: value.len(),
            });
        }
        debug!(
            "PUT operation: key length = {}, value length = {}",
            key.len(),
            value.len()
        );
        // write_record updates index internally
        self.write_record(key, Some(value))?;
        Ok(())
    }

    /// Gets a value associated with a key from the database. Uses optimized read path.
    pub fn get(&self, key: &[u8]) -> DbResult<Option<Vec<u8>>> {
        debug!("GET operation: key length = {}", key.len());
        let offset_opt = { // Scope for read lock
            let inner = self
                .inner
                .read()
                .map_err(|e| DbError::LockPoisoned(e.to_string()))?;
            inner.index.get(key).copied() // .copied() creates an Option<u64>
        }; // Read lock released

        match offset_opt {
            Some(off) => {
                debug!("Key found in index at offset {}", off);
                // Use the optimized read function
                let value = self.read_value_only_at_offset(off)?;
                Ok(Some(value))
            }
            None => {
                debug!("Key not found in index");
                Ok(None) // Key not found is not an error for GET
            }
        }
    }

    /// Deletes a key from the database.
    pub fn delete(&self, key: &[u8]) -> DbResult<()> {
        debug!("DELETE operation: key length = {}", key.len());
        // Check existence using read lock first for efficiency
        let key_exists = {
            let inner = self
                .inner
                .read()
                .map_err(|e| DbError::LockPoisoned(e.to_string()))?;
            inner.index.contains_key(key)
        };

        if !key_exists {
            debug!("Attempted to delete non-existent key");
            // Consistent with GET returning None, DELETE on non-existent key is often idempotent.
            // Returning Ok here is common, but KeyNotFound might be preferred by some APIs.
            // Let's return KeyNotFound for explicitness as per original plan.
             return Err(DbError::KeyNotFound);
           // return Ok(());
        }

        // Key exists, write the tombstone record (acquires write lock internally)
        self.write_record(key, None)?;
        Ok(())
    }

    /// Stores multiple key-value pairs in the database efficiently.
    pub fn batch_put(&self, entries: &[(&[u8], &[u8])]) -> DbResult<()> { // Take slice
         if entries.is_empty() {
             return Ok(());
         }
         debug!("Starting optimized batch PUT for {} entries", entries.len());

         let mut inner = self
             .inner
             .write()
             .map_err(|e| DbError::LockPoisoned(e.to_string()))?;

         let mut offsets = Vec::with_capacity(entries.len());
         let mut keys_written = Vec::with_capacity(entries.len());

         // Seek only once before starting the batch write
         let mut current_offset = inner.writer.seek(SeekFrom::End(0))?;

         for (key, value) in entries {
             // Size checks
              if key.len() > MAX_KEY_SIZE { return Err(DbError::KeyTooLarge { limit: MAX_KEY_SIZE, actual: key.len() }); }
              if value.len() > MAX_VALUE_SIZE { return Err(DbError::ValueTooLarge { limit: MAX_VALUE_SIZE, actual: value.len() }); }

             let record_start_offset = current_offset;

             let timestamp = SystemTime::now()
                 .duration_since(UNIX_EPOCH)
                 .unwrap_or_default()
                 .as_nanos() as u64;
             let record_type = RECORD_TYPE_PUT;
             let key_len = key.len() as u32;
             let value_len = value.len() as u32;

             // --- Prepare data for CRC calculation ---
             let mut bytes_for_crc = Vec::new();
             let ts_bytes = timestamp.to_be_bytes();
             let key_len_bytes = key_len.to_be_bytes();
             let value_len_bytes = value_len.to_be_bytes();

             bytes_for_crc.extend_from_slice(&ts_bytes);
             bytes_for_crc.push(record_type);
             bytes_for_crc.extend_from_slice(&key_len_bytes);
             bytes_for_crc.extend_from_slice(key);
             bytes_for_crc.extend_from_slice(&value_len_bytes);
             bytes_for_crc.extend_from_slice(value);

             let crc_value = CRC_CALCULATOR.checksum(&bytes_for_crc);

             // Write record to BufWriter (buffered)
             inner.writer.write_all(&crc_value.to_be_bytes())?;
             inner.writer.write_all(&bytes_for_crc)?;

             // Update offset for the *next* record
             let record_len = (4 + bytes_for_crc.len()) as u64;
             current_offset += record_len;

             // Store info needed for index update *after* sync
             offsets.push(record_start_offset);
             keys_written.push(key.to_vec()); // Clone key needed for index map
         }

         // --- Flush & Sync Once (Respect Strategy) ---
         inner.writer.flush()?; // Flush buffer to OS first

         let next_sync_offset = current_offset; // Offset after the last write
         if self.sync_strategy == SyncStrategy::Always {
              if let Err(e) = inner.writer.get_ref().sync_all() {
                   error!("Failed to sync file after batch write: {:?}", e);
                   return Err(DbError::Io(e)); // Fail the whole batch on sync error
              }
              inner.last_sync_offset = next_sync_offset;
              debug!("Batch flushed and synced (Strategy: Always)");
          } else {
              debug!("Batch flushed (Strategy: {:?})", self.sync_strategy);
          }

         // --- Update Index (still within write lock) ---
         for (key_vec, offset) in keys_written.into_iter().zip(offsets) {
             inner.index.insert(key_vec, offset);
         }
         debug!("Index updated for batch PUT");

         Ok(())
         // Write lock released when `inner` goes out of scope
     }


     /// Retrieves values for multiple keys from the database efficiently.
     /// Takes Vec<Vec<u8>> as input.
     /// Returns None for keys not found. Returns Err on the first read error.
     pub fn batch_get(&self, keys: &[Vec<u8>]) -> DbResult<HashMap<Vec<u8>, Option<Vec<u8>>>> {
          if keys.is_empty() {
             return Ok(HashMap::new());
         }
         debug!("Starting optimized batch GET for {} keys", keys.len());

         // 1. Get offsets within read lock
         let mut key_offset_pairs: Vec<(Vec<u8>, Option<u64>)> = Vec::with_capacity(keys.len());
         {
             let inner = self
                 .inner
                 .read()
                 .map_err(|e| DbError::LockPoisoned(e.to_string()))?;
             for key in keys {
                 // Use key directly as it's already Vec<u8>
                 let offset = inner.index.get(key).copied();
                 key_offset_pairs.push((key.clone(), offset)); // Clone key needed for result map
             }
         } // Read lock released here

         // 2. Read values outside the lock
         let mut results = HashMap::with_capacity(keys.len());
         for (key_vec, offset_opt) in key_offset_pairs {
             match offset_opt {
                 Some(offset) => {
                     match self.read_value_only_at_offset(offset) {
                         Ok(value_data) => {
                             results.insert(key_vec, Some(value_data));
                         }
                         Err(e) => {
                              error!("Error reading value for key {:?} at offset {}: {}", String::from_utf8_lossy(&key_vec), offset, e);
                              // Fail fast on first error
                              return Err(e);
                         }
                     }
                 }
                 None => {
                     // Key wasn't found in index, insert None
                     results.insert(key_vec, None);
                 }
             }
         }

         Ok(results)
     }


    /// Saves the current in-memory index to the snapshot file.
    pub fn save_index_snapshot(&self) -> DbResult<()> {
        info!("Saving index snapshot...");
        let (index_clone, current_log_offset, index_path) = {
            let inner = self
                .inner
                .read()
                .map_err(|e| DbError::LockPoisoned(e.to_string()))?;
            // Clone index while holding read lock
            // Get current log position *before* potential writes happen
             // Note: This offset might not perfectly align with last *synced* data if SyncStrategy::Never
             let log_offset = inner.writer.buffer().len() as u64 + inner.last_sync_offset; // Estimate current end
            (inner.index.clone(), log_offset, inner.index_path.clone())
        }; // Read lock released

        let snapshot = IndexSnapshot {
            offset: current_log_offset,
            index: index_clone,
        };

        // Write to temporary file first for atomicity
        let temp_path = index_path.with_extension("index.tmp");
        let file = File::create(&temp_path)?; // Use ? for IO errors
        let mut writer = BufWriter::new(file);

        bincode::serialize_into(&mut writer, &snapshot)?; // Use ? for bincode errors
        writer.flush()?; // Flush BufWriter
        writer.get_ref().sync_all()?; // Ensure index snapshot is durable

        // Atomically replace the old snapshot
        fs::rename(&temp_path, &index_path)?;

        info!(
            "Index snapshot saved successfully to {:?} ({} keys, log offset {})",
            index_path,
            snapshot.index.len(),
            snapshot.offset
        );
        Ok(())
    }


    /// Compacts the log file by writing only the latest values to a new file.
    /// This is a potentially long-running operation.
    pub fn compact(&self) -> DbResult<()> {
        info!("Starting log compaction...");

        // --- Phase 1: Preparation (Minimal Locking) ---
        let (
            index_clone,
            compaction_log_path,
            compaction_index_path,
            original_log_path,
            original_index_path,
        ) = {
            let inner = self.inner.read()
                .map_err(|e| DbError::LockPoisoned(e.to_string()))?;
            // Get paths and clone index under read lock
            (
                inner.index.clone(),
                inner.path.with_extension("log.compacting"),
                inner.index_path.with_extension("index.compacting"),
                inner.path.clone(),
                inner.index_path.clone(),
            )
        }; // Read lock released


        info!("Compacting {} keys into temporary files: {:?}, {:?}",
              index_clone.len(), compaction_log_path, compaction_index_path);

        // --- Phase 2: Rewrite Data (No Locking) ---
        let mut new_index: HashMap<Vec<u8>, u64> = HashMap::with_capacity(index_clone.len());
        let mut compacted_log_offset = 0u64;

        { // Scope for temp writer/files
            let compaction_file = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true) // Start fresh
                .open(&compaction_log_path)?;
            let mut compaction_writer = BufWriter::new(compaction_file);

            // Iterate through the state of the index *at the start of compaction*
            for (key, original_offset) in index_clone.iter() {
                // Read the value from the *original* log file using the *original* offset
                // Use the optimized read function as the offset is trusted from the index
                 match self.read_value_only_at_offset(*original_offset) {
                     Ok(value) => {
                         // Write this key-value pair to the new compacted log
                         let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos() as u64;
                         let record_type = RECORD_TYPE_PUT;
                         let key_len = key.len() as u32;
                         let value_len = value.len() as u32;

                         let mut bytes_for_crc = Vec::new();
                         // *** CORRECTED TYPO HERE ***
                         bytes_for_crc.extend_from_slice(&timestamp.to_be_bytes());
                         bytes_for_crc.push(record_type);
                         bytes_for_crc.extend_from_slice(&key_len.to_be_bytes());
                         bytes_for_crc.extend_from_slice(key);
                         bytes_for_crc.extend_from_slice(&value_len.to_be_bytes());
                         bytes_for_crc.extend_from_slice(&value);

                         let crc_value = CRC_CALCULATOR.checksum(&bytes_for_crc);

                         let record_start_offset = compacted_log_offset;

                         compaction_writer.write_all(&crc_value.to_be_bytes())?;
                         compaction_writer.write_all(&bytes_for_crc)?;

                         let bytes_written = (4 + bytes_for_crc.len()) as u64;
                         compacted_log_offset += bytes_written;

                         // Store the *new* offset in the *new* index map
                         new_index.insert(key.clone(), record_start_offset);
                     }
                     Err(DbError::CrcMismatch { offset }) | Err(DbError::InvalidRecord { offset, .. }) => {
                         // If reading the supposedly valid entry fails, log it but skip it in the compacted log
                         warn!("Compaction: Skipping record for key {:?} due to read error at original offset {}: {}", String::from_utf8_lossy(key), offset, "CRC/Invalid");
                         continue;
                     }
                     Err(DbError::Io(e)) => {
                          // IO errors during read are more serious, abort compaction
                          error!("Compaction: Aborting due to I/O error reading key {:?} at offset {}: {}", String::from_utf8_lossy(key), original_offset, e);
                          // Clean up temp files?
                          let _ = fs::remove_file(&compaction_log_path);
                          return Err(DbError::Io(e));
                      }
                      // Other errors like LockPoisoned shouldn't happen here as we hold no lock
                      e => {
                           error!("Compaction: Aborting due to unexpected error reading key {:?} at offset {}: {:?}", String::from_utf8_lossy(key), original_offset, e);
                           let _ = fs::remove_file(&compaction_log_path);
                           return Err(e.unwrap_err());
                      }
                 }
            } // End loop through cloned index

            // Ensure the compacted log is fully written and durable
            compaction_writer.flush()?;
            compaction_writer.get_ref().sync_all()?;
             info!("Compacted log data written and synced to {:?}", compaction_log_path);

        } // Compaction writer/file closed here

        // --- Phase 3: Save New Index Snapshot (No Locking) ---
         let new_snapshot = IndexSnapshot {
            offset: compacted_log_offset, // The end offset of the *new* log
            index: new_index,
         };
        {
             let index_file = File::create(&compaction_index_path)?;
             let mut index_writer = BufWriter::new(index_file);
             bincode::serialize_into(&mut index_writer, &new_snapshot)?;
             index_writer.flush()?;
             index_writer.get_ref().sync_all()?;
              info!("Compacted index snapshot written and synced to {:?}", compaction_index_path);
         }


        // --- Phase 4: Atomically Replace Files & Update State (Exclusive Lock Needed) ---
        info!("Acquiring exclusive lock to switch to compacted files...");
        { // Scope for write lock
            let mut inner = self.inner.write()
                .map_err(|e| DbError::LockPoisoned(e.to_string()))?;

            // Replace writer and reader_file with dummy values before dropping
            inner.writer = std::mem::replace(&mut inner.writer, BufWriter::new(File::create("dummy").unwrap()));
            inner.reader_file = std::mem::replace(&mut inner.reader_file, File::create("dummy").unwrap());

            info!("Replacing {:?} -> {:?}", compaction_log_path, original_log_path);
            fs::rename(&compaction_log_path, &original_log_path)?;
            info!("Replacing {:?} -> {:?}", compaction_index_path, &original_index_path);
            fs::rename(&compaction_index_path, &original_index_path)?;


             // Re-open the *newly renamed* log file for appending
             let new_write_file = OpenOptions::new()
                 .read(true)
                 .append(true)
                 // No create needed, we just renamed it
                 .open(&original_log_path)?;
             let new_reader_file = File::open(&original_log_path)?;

             // Create new writer, ensure it points to the end
             let mut new_writer = BufWriter::new(new_write_file);
             let end_offset = new_writer.seek(SeekFrom::End(0))?; // Should match compacted_log_offset

             // Update the live DbInner state
             inner.writer = new_writer;
             inner.reader_file = new_reader_file;
             inner.index = new_snapshot.index; // Replace the index map
             inner.last_sync_offset = end_offset; // Reset sync offset to the end of compacted log

             info!("Compaction complete. Switched to new log and index. New log end offset: {}", end_offset);

        } // Write lock released

        // Optional: Clean up the original log file (e.g., move to backup)
        // fs::remove_file(original_log_path.with_extension("log.bak"))?;

        Ok(())
    }

    /// Checks if the database is empty (based on the index).
    pub fn is_empty(&self) -> bool {
        self.inner
            .read()
            .map_or(true, |inner| inner.index.is_empty())
    }

    // Force sync, useful after operations if strategy is Never
    pub fn sync(&self) -> DbResult<()> {
        debug!("Manual sync requested.");
        let mut inner = self.inner.write().map_err(|e| DbError::LockPoisoned(e.to_string()))?;
        inner.writer.flush()?;
        inner.writer.get_ref().sync_all()?;
        inner.last_sync_offset = inner.writer.seek(SeekFrom::Current(0))?; // Update sync offset
        info!("Manual sync completed.");
        Ok(())
    }

}

// Implement Drop to ensure data is flushed on shutdown
impl Drop for SimpleDb {
    fn drop(&mut self) {
        // Try to get exclusive access via Arc::get_mut first, then RwLock::get_mut
        // Use `if let Some(...)` because the expression returns an Option
        if let Some(inner) = Arc::get_mut(&mut self.inner).and_then(|rwlock| rwlock.get_mut().ok())
        {
            info!("Shutting down database, ensuring final flush/sync...");
            if let Err(e) = inner.writer.flush() {
                error!("Error flushing writer during drop: {:?}", e);
            }
            // Always try to sync on drop unless Never strategy? Or just always try? Let's always try.
            if let Err(e) = inner.writer.get_ref().sync_all() {
                 error!("Error syncing file during drop: {:?}", e);
            }
             info!("Final flush/sync attempted.");
        } else {
            // This might happen if other Arcs still exist, or lock is poisoned.
             warn!("Could not get exclusive access during drop for final sync. Data might not be fully flushed/synced.");
        }
    }
}

// src/db.rs
// ... (existing code) ...

#[cfg(test)]
mod tests {
    use super::*; // Import everything from the outer module (SimpleDb, DbError, etc.)
    use tempfile::tempdir; // Use tempdir for isolation

    // Helper to create a temporary DB for a test
    fn setup_temp_db(sync: SyncStrategy) -> (SimpleDb, PathBuf) {
        let dir = tempdir().expect("Failed to create temp dir");
        let data_path = dir.path().join("test.dblog");
        let index_path = dir.path().join("test.dblog.index");
        let db = SimpleDb::open(&data_path, &index_path, sync)
            .expect("Failed to open temp DB");
        // Keep the dir handle until the end of the test to prevent deletion
        (db, dir.into_path()) // Return path to prevent premature cleanup
    }

    // Initialize tracing for tests (run once)
    use std::sync::Once;
    static TRACING: Once = Once::new();
    fn setup_tracing() {
        TRACING.call_once(|| {
            tracing_subscriber::fmt()
                .with_max_level(tracing::Level::DEBUG) // More verbose for tests
                .with_test_writer() // Write to test output
                .init();
        });
    }

    #[tokio::test]
    async fn test_put_get_simple() {
        setup_tracing();
        let (db, _dir) = setup_temp_db(SyncStrategy::Never); // Keep _dir alive

        let key = b"hello";
        let value = b"world";

        db.put(key, value).expect("PUT failed");

        let retrieved = db.get(key).expect("GET failed");
        assert_eq!(retrieved, Some(value.to_vec()));
    }

    #[tokio::test]
    async fn test_get_non_existent() {
        setup_tracing();
        let (db, _dir) = setup_temp_db(SyncStrategy::Never);

        let retrieved = db.get(b"non_existent_key").expect("GET failed");
        assert_eq!(retrieved, None);
    }

    #[tokio::test]
    async fn test_put_overwrite() {
        setup_tracing();
        let (db, _dir) = setup_temp_db(SyncStrategy::Never);

        let key = b"key_to_overwrite";
        let value1 = b"value1";
        let value2 = b"value2_new";

        db.put(key, value1).expect("First PUT failed");
        let retrieved1 = db.get(key).expect("First GET failed");
        assert_eq!(retrieved1, Some(value1.to_vec()));

        db.put(key, value2).expect("Second PUT (overwrite) failed");
        let retrieved2 = db.get(key).expect("Second GET failed");
        assert_eq!(retrieved2, Some(value2.to_vec()));
    }

    #[tokio::test]
    async fn test_delete_simple() {
         setup_tracing();
        let (db, _dir) = setup_temp_db(SyncStrategy::Never);

        let key = b"key_to_delete";
        let value = b"some_value";

        db.put(key, value).expect("PUT before delete failed");
        assert!(db.get(key).expect("GET before delete failed").is_some());

        db.delete(key).expect("DELETE failed");
        assert_eq!(db.get(key).expect("GET after delete failed"), None);
    }

     #[tokio::test]
    async fn test_delete_non_existent() {
        setup_tracing();
        let (db, _dir) = setup_temp_db(SyncStrategy::Never);

        let result = db.delete(b"non_existent_for_delete");
        // Expect KeyNotFound error
        assert!(matches!(result, Err(DbError::KeyNotFound)));
    }

    #[tokio::test]
    async fn test_persistence_and_reload() {
        setup_tracing();
        let dir = tempdir().expect("Failed to create temp dir");
        let data_path = dir.path().join("persist.dblog");
        let index_path = dir.path().join("persist.dblog.index");

        let key1 = b"persist_key1";
        let val1 = b"persist_val1";
        let key2 = b"persist_key2";
        let val2 = b"persist_val2";

        { // Scope for first DB instance
            let db1 = SimpleDb::open(&data_path, &index_path, SyncStrategy::Never)
                .expect("Failed to open DB 1");
            db1.put(key1, val1).expect("DB1 PUT key1 failed");
            db1.put(key2, val2).expect("DB1 PUT key2 failed");
            // db1 is dropped here, Drop impl should flush/sync
        }

        // Re-open the database
        let db2 = SimpleDb::open(&data_path, &index_path, SyncStrategy::Never)
            .expect("Failed to open DB 2");

        assert_eq!(db2.get(key1).expect("DB2 GET key1 failed"), Some(val1.to_vec()));
        assert_eq!(db2.get(key2).expect("DB2 GET key2 failed"), Some(val2.to_vec()));
    }

    #[tokio::test]
    async fn test_batch_put_get() {
        setup_tracing();
        let (db, _dir) = setup_temp_db(SyncStrategy::Never);

        let entries = vec![
            (b"batch1".as_slice(), b"val1".as_slice()),
            (b"batch2".as_slice(), b"val2".as_slice()),
            (b"batch3".as_slice(), b"val3".as_slice()),
        ];
        db.batch_put(&entries).expect("Batch PUT failed");

        assert_eq!(db.get(b"batch1").unwrap(), Some(b"val1".to_vec()));
        assert_eq!(db.get(b"batch2").unwrap(), Some(b"val2".to_vec()));
        assert_eq!(db.get(b"batch3").unwrap(), Some(b"val3".to_vec()));

        let keys_to_get = vec![b"batch2".to_vec(), b"batch_not_exist".to_vec(), b"batch1".to_vec()];
        let results = db.batch_get(&keys_to_get).expect("Batch GET failed");

        assert_eq!(results.len(), 3);
        assert_eq!(results.get(b"batch1".as_slice()), Some(&Some(b"val1".to_vec())));
        assert_eq!(results.get(b"batch2".as_slice()), Some(&Some(b"val2".to_vec())));
        assert_eq!(results.get(b"batch_not_exist".as_slice()), Some(&None));
    }

    #[tokio::test]
    async fn test_compaction() {
        setup_tracing();
        let (db, _dir) = setup_temp_db(SyncStrategy::Never);

        let key_a = b"keyA"; // Keep
        let key_b = b"keyB"; // Overwrite
        let key_c = b"keyC"; // Delete
        let key_d = b"keyD"; // Keep

        db.put(key_a, b"valA1").unwrap();
        db.put(key_b, b"valB1").unwrap();
        db.put(key_c, b"valC1").unwrap();
        db.put(key_d, b"valD1").unwrap();
        db.put(key_b, b"valB2_latest").unwrap(); // Overwrite B
        db.delete(key_c).unwrap(); // Delete C

        let log_size_before = db.inner.read().unwrap().path.metadata().unwrap().len();

        // Compact
        db.compact().expect("Compaction failed");

        let log_size_after = db.inner.read().unwrap().path.metadata().unwrap().len();

        // Verify state after compaction
        assert_eq!(db.get(key_a).unwrap(), Some(b"valA1".to_vec()));
        assert_eq!(db.get(key_b).unwrap(), Some(b"valB2_latest".to_vec()));
        assert_eq!(db.get(key_c).unwrap(), None); // Should be gone
        assert_eq!(db.get(key_d).unwrap(), Some(b"valD1".to_vec()));

        // Verify log size reduced (usually, unless values are tiny)
        assert!(log_size_after < log_size_before, "Log size should decrease after compaction");

        // Put something *after* compaction
        db.put(b"keyE_after", b"valE").unwrap();
        assert_eq!(db.get(b"keyE_after").unwrap(), Some(b"valE".to_vec()));
    }

    #[tokio::test]
    async fn test_snapshotting() {
        setup_tracing();
        let dir = tempdir().expect("Failed to create temp dir");
        let data_path = dir.path().join("snap.dblog");
        let index_path = dir.path().join("snap.dblog.index");

        let key1 = b"snap_key1";
        let val1 = b"snap_val1";

        { // Scope for first DB instance
            let db1 = SimpleDb::open(&data_path, &index_path, SyncStrategy::Never)
                .expect("Failed to open DB 1 for snap");
            db1.put(key1, val1).expect("DB1 snap PUT key1 failed");
            db1.save_index_snapshot().expect("Failed to save snapshot");
            // Put another key *after* snapshot
            db1.put(b"key_after_snap", b"val_after").unwrap();
        } // DB1 dropped

        // Re-open: Should load from snapshot then read the last record
        let db2 = SimpleDb::open(&data_path, &index_path, SyncStrategy::Never)
            .expect("Failed to open DB 2 after snap");

        assert_eq!(db2.get(key1).expect("DB2 snap GET key1 failed"), Some(val1.to_vec()));
        assert_eq!(db2.get(b"key_after_snap").expect("DB2 snap GET key_after failed"), Some(b"val_after".to_vec()));
        assert!(index_path.exists(), "Index file should exist");
    }

     #[tokio::test]
     async fn test_key_value_size_limits() {
         setup_tracing();
         let (db, _dir) = setup_temp_db(SyncStrategy::Never);
         let large_key = vec![0u8; MAX_KEY_SIZE + 1];
         let large_value = vec![1u8; MAX_VALUE_SIZE + 1];
         let ok_key = b"ok_key";
         let ok_value = b"ok_value";

         // Test large key
         let key_res = db.put(&large_key, ok_value);
         assert!(matches!(key_res, Err(DbError::KeyTooLarge { .. })));

         // Test large value
         let val_res = db.put(ok_key, &large_value);
         assert!(matches!(val_res, Err(DbError::ValueTooLarge { .. })));

         // Test max size ok (optional - can be slow)
         // let max_key = vec![0u8; MAX_KEY_SIZE];
         // let max_value = vec![1u8; MAX_VALUE_SIZE];
         // assert!(db.put(&max_key, b"test").is_ok());
         // assert!(db.put(b"test", &max_value).is_ok());
     }

     // Add more tests:
     // - Test recovery from partially written records (hard to simulate reliably)
     // - Test recovery from corrupt CRC (hard to simulate reliably)
     // - Test behaviour with empty keys/values if allowed (currently not explicitly handled)
     // - Test sync strategy always (check logs, harder to assert disk state)
     // - Basic concurrency tests (multiple threads reading/writing - more complex)

} // end mod tests