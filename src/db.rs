use crate::error::{DbError, DbResult};
use crc::{Crc, CRC_32_ISCSI};
use serde::{Deserialize, Serialize};
use bincode::{config::standard as bincode_standard_config, Decode, Encode};
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::time::Instant; // Use tokio's Instant for async context
use tracing::{debug, error, info, warn}; // Make sure Instant is imported

const CRC_CALCULATOR: Crc<u32> = Crc::<u32>::new(&CRC_32_ISCSI);
const MAX_KEY_SIZE: usize = 1024 * 4;
const MAX_VALUE_SIZE: usize = 1024 * 1024 * 16; // 16 MiB

const RECORD_TYPE_PUT: u8 = 0x01;
const RECORD_TYPE_DELETE: u8 = 0x02;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SyncStrategy {
    Always,
    #[default]
    Never,
}

#[derive(Serialize, Deserialize, Debug, Encode, Decode)]
struct IndexSnapshot {
    offset: u64,
    index: HashMap<Vec<u8>, u64>,
}

#[derive(Debug)]
struct DbInner {
    writer: BufWriter<File>,
    reader_file: File, // This holds the handle for cloning
    index: HashMap<Vec<u8>, u64>,
    path: PathBuf,
    index_path: PathBuf,
    last_sync_offset: u64,
    last_snapshot_time: Option<Instant>, // Track last successful snapshot
}

#[derive(Debug, Clone)]
pub struct SimpleDb {
    inner: Arc<RwLock<DbInner>>,
    sync_strategy: SyncStrategy,
}

impl SimpleDb {
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

        let mut index = HashMap::new();
        let mut log_start_offset = 0;
        let mut last_snapshot_time = None; // Initialize last snapshot time

        if index_path.exists() {
            info!("Loading index snapshot from {:?}", index_path);
            match File::open(&index_path) {
                Ok(file) => {
                    let mut reader = BufReader::new(file);
                    match bincode::decode_from_std_read::<IndexSnapshot, _, _>(
                        &mut reader,
                        bincode_standard_config(),
                    ) {
                        Ok(snapshot) => {
                            index = snapshot.index;
                            log_start_offset = snapshot.offset;
                            info!(
                                "Loaded index snapshot with {} keys, valid up to log offset {}",
                                index.len(),
                                log_start_offset
                            );
                           // If snapshot loaded successfully, record the time
                           last_snapshot_time = Some(Instant::now());
                        }
                        Err(e) => {
                            // Log the decode error but don't wrap it in DbError here,
                            // just proceed with full log replay.
                            warn!(
                                "Failed to deserialize index snapshot (bincode decode error): {}. Rebuilding from full log.",
                                e
                            );
                            log_start_offset = 0;
                            index = HashMap::new();
                        }
                    }
                }
                Err(e) => {
                    warn!(
                        "Failed to open index snapshot file {:?}: {}. Rebuilding from full log.",
                        index_path, e
                    );
                    // Ensure reset if file open failed
                    log_start_offset = 0;
                    index = HashMap::new();
                }
            }
        } else {
            info!("No index snapshot found. Index will be built from log.");
        }

        let write_file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(&path)?;

        let read_file = File::open(&path)?; // This is the handle we keep

        let file_len = write_file.metadata()?.len();
        if log_start_offset < file_len {
            info!(
                "Updating index from log file starting at offset {}",
                log_start_offset
            );
            let read_file_for_index = read_file.try_clone()?;
            let mut reader_for_index = BufReader::new(read_file_for_index);
            // update_index_from_log might return an error which should be propagated
            Self::update_index_from_log(&mut reader_for_index, &mut index, log_start_offset)?;
        } else {
            info!("Log file does not contain new records since last snapshot (or is empty).");
        }

        let mut writer = BufWriter::new(write_file);
        let current_offset_at_end = writer.seek(SeekFrom::End(0))?;
        let initial_sync_offset = log_start_offset;

        let mut inner = DbInner {
            writer,
            reader_file: read_file, // Assign the correct handle
            index,
            path,
            index_path,
            last_sync_offset: initial_sync_offset,
            last_snapshot_time, // Assign loaded/initial time
        };

        // Update last_sync_offset after potential log replay if no snapshot was loaded
        if log_start_offset == 0 {
             inner.last_sync_offset = current_offset_at_end;
        }

        Ok(SimpleDb {
            inner: Arc::new(RwLock::new(inner)),
            sync_strategy,
        })
    }

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

            let mut header_buf = [0u8; 4 + 8 + 1 + 4]; // CRC, TS, Type, KeyLen
            if let Err(e) = reader.read_exact(&mut header_buf) {
                 if e.kind() == std::io::ErrorKind::UnexpectedEof && current_offset == file_len {
                     debug!("Clean EOF reached at end of log file.");
                     break;
                 }
                 warn!("Unexpected EOF or I/O error reading header at offset {}: {}. Stopping index update.", record_start_offset, e);
                 return Err(DbError::Io(e));
             }

            let stored_crc = u32::from_be_bytes(header_buf[0..4].try_into().unwrap());
            let ts_bytes = &header_buf[4..12];
            let record_type = header_buf[12];
            let key_len_bytes = &header_buf[13..17];
            let key_len = u32::from_be_bytes(key_len_bytes.try_into().unwrap()) as usize;

             if key_len > MAX_KEY_SIZE {
                 warn!("Record at offset {} has oversized key ({} bytes). Skipping corrupt record.", record_start_offset, key_len);
                 return Err(DbError::InvalidRecord{offset: record_start_offset, reason: format!("Key size {} exceeds limit {}", key_len, MAX_KEY_SIZE)});
             }

            let mut key_data = vec![0u8; key_len];
            if let Err(e) = reader.read_exact(&mut key_data) { return Err(DbError::Io(e)); }

            let mut value_len: u32 = 0;
            let mut value_len_bytes = [0u8; 4];
            let mut value_data: Option<Vec<u8>> = None;

            if record_type == RECORD_TYPE_PUT {
                if let Err(e) = reader.read_exact(&mut value_len_bytes) { return Err(DbError::Io(e)); }
                value_len = u32::from_be_bytes(value_len_bytes);
                if value_len as usize > MAX_VALUE_SIZE {
                    warn!("Record at offset {} has oversized value ({} bytes). Skipping corrupt record.", record_start_offset, value_len);
                    return Err(DbError::InvalidRecord{offset: record_start_offset, reason: format!("Value size {} exceeds limit {}", value_len, MAX_VALUE_SIZE)});
                }
                let mut val_bytes = vec![0u8; value_len as usize];
                if let Err(e) = reader.read_exact(&mut val_bytes) { return Err(DbError::Io(e)); }
                value_data = Some(val_bytes);
            } else if record_type != RECORD_TYPE_DELETE {
                warn!("Skipping unknown record type {} at offset {}", record_type, record_start_offset);
                 let record_header_key_len = 4 + 8 + 1 + 4 + key_len as u64;
                 current_offset = record_start_offset + record_header_key_len;
                 reader.seek(SeekFrom::Start(current_offset))?;
                 continue;
            }

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
                 let record_header_key_len = 4 + 8 + 1 + 4 + key_len as u64;
                 current_offset = record_start_offset + record_header_key_len;
                 if record_type == RECORD_TYPE_PUT {
                     current_offset += 4 + value_len as u64;
                 }
                 reader.seek(SeekFrom::Start(current_offset))?;
                 continue;
            }

            if record_type == RECORD_TYPE_PUT {
                debug!("IndexUpdate: Indexing PUT for key (len {}) at offset {}", key_data.len(), record_start_offset);
                index.insert(key_data, record_start_offset);
            } else if record_type == RECORD_TYPE_DELETE {
                 debug!("IndexUpdate: Processing DELETE for key (len {}) at offset {}", key_data.len(), record_start_offset);
                index.remove(&key_data);
            }

             current_offset = record_start_offset + (4 + bytes_for_crc.len()) as u64;
             let actual_pos = reader.stream_position()?;
             if actual_pos != current_offset {
                 warn!("Reader position mismatch after record read: expected {}, got {}. Stopping index update.", current_offset, actual_pos);
                  return Err(DbError::Internal("Reader position mismatch during index build".to_string()));
             }
        }

        info!("Index update complete. Current index size: {}", index.len());
        Ok(())
    }

    fn write_record(&self, key: &[u8], value: Option<&[u8]>) -> DbResult<u64> {
         let mut inner = self
            .inner
            .write()
            .map_err(|e| DbError::LockPoisoned(e.to_string()))?;

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

        inner.writer.write_all(&crc_value.to_be_bytes())?;
        inner.writer.write_all(&bytes_for_crc)?;

        let bytes_written = (4 + bytes_for_crc.len()) as u64;
        let next_offset = current_offset + bytes_written;

        inner.writer.flush()?;

        if self.sync_strategy == SyncStrategy::Always {
             if let Err(e) = inner.writer.get_ref().sync_all() {
                 error!("Failed to sync file after write: {:?}", e);
                 return Err(DbError::Io(e));
             }
             inner.last_sync_offset = next_offset;
             debug!("Writer flushed and synced (Strategy: Always)");
         } else {
             debug!("Writer flushed (Strategy: Never)");
         }

        if record_type == RECORD_TYPE_PUT {
            inner.index.insert(key.to_vec(), current_offset);
            debug!(
                "PUT record written for key (len {}) at offset {}",
                key.len(),
                current_offset
            );
        } else {
            inner.index.remove(key);
            debug!(
                "DELETE record written for key (len {}) at offset {}",
                key.len(),
                current_offset
            );
        }

        Ok(current_offset)
    }

    fn read_value_only_at_offset(&self, offset: u64) -> DbResult<Vec<u8>> {
         let read_file_clone = {
            let inner = self
                .inner
                .read()
                .map_err(|e| DbError::LockPoisoned(e.to_string()))?;
            inner.reader_file.try_clone()?
        };

        let mut reader = BufReader::new(read_file_clone);

        reader.seek(SeekFrom::Start(offset))?;
        debug!("OptimizedRead: Reading value at offset: {}", offset);

        // Seek past: CRC (4), TS (8), Type (1) = 13 bytes
        reader.seek_relative(13)?;

        let mut key_len_bytes = [0u8; 4];
        reader.read_exact(&mut key_len_bytes)?;
        let key_len = u32::from_be_bytes(key_len_bytes) as u64;

        // Seek past key data
        reader.seek_relative(key_len as i64)?;

        // Read value length (4 bytes)
        let mut value_len_bytes = [0u8; 4];
        reader.read_exact(&mut value_len_bytes)?;
        let value_len = u32::from_be_bytes(value_len_bytes) as usize;

        // Read value data
        let mut value_data = vec![0u8; value_len];
        reader.read_exact(&mut value_data)?;

        debug!(
            "OptimizedRead: Successfully read value ({} bytes) at offset {}",
            value_data.len(),
            offset
        );
        Ok(value_data)
    }

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
        self.write_record(key, Some(value))?;
        Ok(())
    }

    pub fn get(&self, key: &[u8]) -> DbResult<Option<Vec<u8>>> {
         debug!("GET operation: key length = {}", key.len());
        let offset_opt = {
            let inner = self
                .inner
                .read()
                .map_err(|e| DbError::LockPoisoned(e.to_string()))?;
            inner.index.get(key).copied()
        };

        match offset_opt {
            Some(off) => {
                debug!("Key found in index at offset {}", off);
                let value = self.read_value_only_at_offset(off)?;
                Ok(Some(value))
            }
            None => {
                debug!("Key not found in index");
                Ok(None)
            }
        }
    }

    pub fn delete(&self, key: &[u8]) -> DbResult<()> {
         debug!("DELETE operation: key length = {}", key.len());
        let key_exists = {
            let inner = self
                .inner
                .read()
                .map_err(|e| DbError::LockPoisoned(e.to_string()))?;
            inner.index.contains_key(key)
        };

        if !key_exists {
            debug!("Attempted to delete non-existent key");
             return Err(DbError::KeyNotFound);
        }

        self.write_record(key, None)?;
        Ok(())
    }

    pub fn batch_put(&self, entries: &[(&[u8], &[u8])]) -> DbResult<()> {
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

         let mut current_offset = inner.writer.seek(SeekFrom::End(0))?;

         for (key, value) in entries {
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

             inner.writer.write_all(&crc_value.to_be_bytes())?;
             inner.writer.write_all(&bytes_for_crc)?;

             let record_len = (4 + bytes_for_crc.len()) as u64;
             current_offset += record_len;

             offsets.push(record_start_offset);
             keys_written.push(key.to_vec());
         }

         inner.writer.flush()?;

         let next_sync_offset = current_offset;
         if self.sync_strategy == SyncStrategy::Always {
              if let Err(e) = inner.writer.get_ref().sync_all() {
                   error!("Failed to sync file after batch write: {:?}", e);
                   return Err(DbError::Io(e));
              }
              inner.last_sync_offset = next_sync_offset;
              debug!("Batch flushed and synced (Strategy: Always)");
          } else {
              debug!("Batch flushed (Strategy: {:?})", self.sync_strategy);
          }

         for (key_vec, offset) in keys_written.into_iter().zip(offsets) {
             inner.index.insert(key_vec, offset);
         }
         debug!("Index updated for batch PUT");

         Ok(())
     }


     pub fn batch_get(&self, keys: &[Vec<u8>]) -> DbResult<HashMap<Vec<u8>, Option<Vec<u8>>>> {
           if keys.is_empty() {
             return Ok(HashMap::new());
         }
         debug!("Starting optimized batch GET for {} keys", keys.len());

         let mut key_offset_pairs: Vec<(Vec<u8>, Option<u64>)> = Vec::with_capacity(keys.len());
         {
             let inner = self
                 .inner
                 .read()
                 .map_err(|e| DbError::LockPoisoned(e.to_string()))?;
             for key in keys {
                 let offset = inner.index.get(key).copied();
                 key_offset_pairs.push((key.clone(), offset));
             }
         }

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
                              return Err(e);
                         }
                     }
                 }
                 None => {
                     results.insert(key_vec, None);
                 }
             }
         }

         Ok(results)
     }


    /// Saves the current in-memory index to the snapshot file using Bincode 2.0.
    pub fn save_index_snapshot(&self) -> DbResult<()> {
        // Use std::time::Instant here as this function is synchronous for now
        let start_time = std::time::Instant::now();
        info!("Saving index snapshot...");

        // NOTE: Removed path_clone_for_sync as it was unused
        let (index_clone, index_path) = {
            let inner = self
                .inner
                .read() // Keep read lock shorter
                .map_err(|e| DbError::LockPoisoned(e.to_string()))?;
            // We need the path later for syncing the directory entry if using rename
            (
                inner.index.clone(),
                inner.index_path.clone(),
                // inner.path.clone(), // Removed unused path clone
            )
        };

         // Calculate log offset *after* flushing buffer
         let flushed_log_offset = self.flush_writer_get_offset()?;


        let snapshot = IndexSnapshot {
            offset: flushed_log_offset,
            index: index_clone,
        };

        let temp_path = index_path.with_extension("dblog.index.tmp"); // Changed extension slightly
        let file = match File::create(&temp_path) {
            Ok(f) => f,
            Err(e) => {
                error!("Failed to create temporary snapshot file {:?}: {}", temp_path, e);
                return Err(DbError::SnapshotError(format!("Failed to create temp file: {}", e)));
            }
        };
        let mut writer = BufWriter::new(file);

        bincode::encode_into_std_write(
            &snapshot,
            &mut writer,
            bincode_standard_config(),
        )
        .map_err(|e| {
            error!("Failed to encode snapshot data: {}", e);
            DbError::BincodeEncode(Box::new(e))
        })?;

        writer.flush()?;
        writer.get_ref().sync_all()?;

        // Atomically replace the old index file
        fs::rename(&temp_path, &index_path)?;

        // --- Optional: Sync parent directory (for durability on some filesystems) ---
         if let Some(parent_dir) = index_path.parent() {
             if let Ok(dir_handle) = File::open(parent_dir) {
                 if let Err(e) = dir_handle.sync_all() {
                     warn!("Failed to sync directory containing index file {:?}: {}", parent_dir, e);
                     // Don't fail the whole snapshot for this, but log it
                 }
             } else {
                 warn!("Could not open parent directory {:?} to sync for index file.", parent_dir);
             }
         }
        // --- End Optional Sync ---

        // Update the last snapshot time *after* successful save and rename
         {
            let mut inner = self.inner.write().map_err(|e| DbError::LockPoisoned(e.to_string()))?;
            inner.last_snapshot_time = Some(Instant::now());
         }

        info!(
            "Index snapshot saved successfully to {:?} ({} keys, log offset {}, took {:?})",
            index_path,
            snapshot.index.len(),
            snapshot.offset,
            start_time.elapsed() // Use std::time::Instant here
        );
        Ok(())
    }

    // Helper to flush buffer and get reliable end offset (needs write lock)
    fn flush_writer_get_offset(&self) -> DbResult<u64> {
          let mut inner = self.inner.write().map_err(|e| DbError::LockPoisoned(e.to_string()))?;
          inner.writer.flush()?; // Flush the buffer first
          let offset = inner.writer.seek(SeekFrom::End(0))?; // Get offset after flush
          Ok(offset)
    }


    /// Compacts the log file by writing only the latest values to a new file.
    /// This is currently a synchronous operation.
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
            (
                inner.index.clone(),
                inner.path.with_extension("dblog.compacting"),
                inner.index_path.with_extension("dblog.index.compacting"),
                inner.path.clone(),
                inner.index_path.clone(),
            )
        };

        info!("Compacting {} keys into temporary files: {:?}, {:?}",
              index_clone.len(), compaction_log_path, compaction_index_path);

        // --- Phase 2: Rewrite Data (No Locking) ---
        let mut new_index: HashMap<Vec<u8>, u64> = HashMap::with_capacity(index_clone.len());
        let mut current_compacted_offset = 0u64; // Use a local var for tracking offset during rewrite
        let rewrite_result: DbResult<()> = (|| {
            let compaction_file = OpenOptions::new()
                .write(true).create(true).truncate(true)
                .open(&compaction_log_path)?;
            let mut compaction_writer = BufWriter::new(compaction_file);

            for (key, original_offset) in index_clone.iter() {
                 match self.read_value_only_at_offset(*original_offset) {
                     Ok(value) => {
                         let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos() as u64;
                         let record_type = RECORD_TYPE_PUT;
                         let key_len = key.len() as u32;
                         let value_len = value.len() as u32;
                         let mut bytes_for_crc = Vec::new();
                         bytes_for_crc.extend_from_slice(&timestamp.to_be_bytes()); // Corrected variable name
                         bytes_for_crc.push(record_type);
                         bytes_for_crc.extend_from_slice(&key_len.to_be_bytes());
                         bytes_for_crc.extend_from_slice(key);
                         bytes_for_crc.extend_from_slice(&value_len.to_be_bytes());
                         bytes_for_crc.extend_from_slice(&value);
                         let crc_value = CRC_CALCULATOR.checksum(&bytes_for_crc);
                         let record_start_offset = current_compacted_offset; // Use local offset var
                         compaction_writer.write_all(&crc_value.to_be_bytes())?;
                         compaction_writer.write_all(&bytes_for_crc)?;
                         let bytes_written = (4 + bytes_for_crc.len()) as u64;
                         current_compacted_offset += bytes_written; // Increment local offset var
                         new_index.insert(key.clone(), record_start_offset);
                     }
                     Err(DbError::CrcMismatch { offset }) | Err(DbError::InvalidRecord { offset, .. }) => {
                         warn!("Compaction: Skipping record for key {:?} due to read error at original offset {}: {}", String::from_utf8_lossy(key), offset, "CRC/Invalid");
                         continue; // Skip this key
                     }
                     Err(e) => {
                        error!("Compaction: Aborting due to error reading key {:?} at offset {}: {}", String::from_utf8_lossy(key), original_offset, e);
                        return Err(DbError::CompactionError(format!("Read error: {}", e)));
                     }
                 }
            }
            compaction_writer.flush()?;
            compaction_writer.get_ref().sync_all()?;
             info!("Compacted log data written and synced to {:?}", compaction_log_path);
            Ok(())
        })();

        if let Err(e) = rewrite_result {
             error!("Compaction Phase 2 (Rewrite) failed: {}", e);
             let _ = fs::remove_file(&compaction_log_path); // Attempt cleanup
             let _ = fs::remove_file(&compaction_index_path);
             return Err(e);
        }


        // --- Phase 3: Save New Index Snapshot (No Locking) ---
         let new_snapshot = IndexSnapshot {
            offset: current_compacted_offset, // Use final local offset
            index: new_index,
         };
        let snapshot_result: DbResult<()> = (|| {
             let index_file = File::create(&compaction_index_path)?;
             let mut index_writer = BufWriter::new(index_file);
             bincode::encode_into_std_write(
                 &new_snapshot,
                 &mut index_writer,
                 bincode_standard_config(),
             )
              .map_err(|e| DbError::BincodeEncode(Box::new(e)))?;

             index_writer.flush()?;
             index_writer.get_ref().sync_all()?;
             info!("Compacted index snapshot written and synced to {:?}", compaction_index_path);
             Ok(())
        })();

        if let Err(e) = snapshot_result {
            error!("Compaction Phase 3 (Snapshot) failed: {}", e);
            let _ = fs::remove_file(&compaction_log_path); // Attempt cleanup
            let _ = fs::remove_file(&compaction_index_path);
            return Err(DbError::CompactionError(format!("Snapshotting failed: {}", e)));
        }


        // --- Phase 4: Atomically Replace Files & Update State ---
        info!("Acquiring exclusive lock to switch to compacted files...");

        // Rename files *first* while nothing holds handles to the originals
        fs::rename(&compaction_log_path, &original_log_path)
            .map_err(|e| DbError::CompactionError(format!("Failed to rename compacted log {:?} -> {:?}: {}", compaction_log_path, original_log_path, e)))?;
        info!("Renamed log: {:?} -> {:?}", compaction_log_path, original_log_path);

        fs::rename(&compaction_index_path, &original_index_path)
            .map_err(|e| DbError::CompactionError(format!("Failed to rename compacted index {:?} -> {:?}: {}", compaction_index_path, original_index_path, e)))?;
        info!("Renamed index: {:?} -> {:?}", compaction_index_path, original_index_path);

       // --- Optional: Sync parent directory (for durability on some filesystems) ---
        Self::sync_directory_for_paths(&[&original_log_path, &original_index_path]);
       // --- End Optional Sync ---

        // Now acquire lock to update internal state and handles
        let swap_result: DbResult<()> = {
            let mut inner = self.inner.write()
                .map_err(|e| DbError::LockPoisoned(e.to_string()))?;

             // Open the *newly renamed* files
             let new_write_file = OpenOptions::new()
                 .read(true).append(true).create(false) // Should exist now
                 .open(&original_log_path)?;
             let new_reader_file = File::open(&original_log_path)?;

             let mut new_writer = BufWriter::new(new_write_file);
             let end_offset = new_writer.seek(SeekFrom::End(0))?;
             if end_offset != current_compacted_offset {
                 warn!("Compaction: New log end offset {} does not match expected offset {} after rewrite. Index offset might be slightly off.", end_offset, current_compacted_offset);
             }

             // Replace internal state. Old handles are dropped here.
             inner.writer = new_writer;
             inner.reader_file = new_reader_file;
             inner.index = new_snapshot.index;
             // Reset last_snapshot_time as the new index file is now the latest snapshot
             inner.last_snapshot_time = Some(Instant::now());
             inner.last_sync_offset = end_offset;

             Ok(())
        };

        if let Err(e) = swap_result {
            error!("Compaction Phase 4 (State Swap) failed: {}", e);
            // State is inconsistent now. Log prominently. Recovery is hard.
             return Err(DbError::CompactionError(format!("Failed to update internal state after rename: {}", e)));
        }

        let final_offset = self.inner.read().unwrap().last_sync_offset;
        info!("Compaction complete. Switched to new log and index. New log end offset: {}", final_offset);

        Ok(())
    }

     /// Helper to sync parent directories for given paths. Logs warnings on failure.
     fn sync_directory_for_paths(paths: &[&PathBuf]) {
         for path in paths {
             if let Some(parent_dir) = path.parent() {
                 if parent_dir.as_os_str().is_empty() { // Handle root dir case? Unlikely needed.
                      continue;
                 }
                 match File::open(parent_dir) {
                     Ok(dir_handle) => {
                         if let Err(e) = dir_handle.sync_all() {
                             warn!("Failed to sync directory {:?} for file {:?}: {}", parent_dir, path.file_name().unwrap_or_default(), e);
                         } else {
                              debug!("Synced directory {:?} for file {:?}", parent_dir, path.file_name().unwrap_or_default());
                         }
                     }
                     Err(e) => {
                         warn!("Could not open parent directory {:?} to sync for file {:?}: {}", parent_dir, path.file_name().unwrap_or_default(), e);
                     }
                 }
             }
         }
     }

     // --- Methods for Maintenance Task ---
     pub async fn get_last_snapshot_time(&self) -> Option<Instant> {
          let inner = self.inner.read().map_err(|e| DbError::LockPoisoned(e.to_string())).ok()?; // Use ok() to convert Result to Option
          inner.last_snapshot_time
      }

    pub fn is_empty(&self) -> bool {
         self.inner
            .read()
            .map_or(true, |inner| inner.index.is_empty())
    }

    pub fn sync(&self) -> DbResult<()> {
         debug!("Manual sync requested.");
        let mut inner = self.inner.write().map_err(|e| DbError::LockPoisoned(e.to_string()))?;
        inner.writer.flush()?;
        let current_pos = inner.writer.stream_position()?;
        inner.writer.get_ref().sync_all()?;
        inner.last_sync_offset = current_pos;
        info!("Manual sync completed up to offset {}.", current_pos);
        Ok(())
    }

   /// Gets the current log file size. Used by maintenance task. Avoids locking if possible.
   pub fn get_log_size_for_compaction_check(&self) -> DbResult<u64> {
      // Read path without lock first
      let path = self.inner.read().map_err(|e| DbError::LockPoisoned(e.to_string()))?.path.clone();
      // Get metadata directly from the path
      match fs::metadata(&path) {
          Ok(metadata) => Ok(metadata.len()),
          Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(0), // File might not exist yet or after compaction swap error
          Err(e) => Err(DbError::Io(e)), // Propagate other I/O errors
      }
  }

} // impl SimpleDb

impl Drop for SimpleDb {
     fn drop(&mut self) {
        if let Some(inner_lock) = Arc::get_mut(&mut self.inner) {
            if let Some(mut inner) = inner_lock.try_write().ok() {
                info!("Shutting down database, ensuring final flush/sync...");
                if let Err(e) = inner.writer.flush() {
                    error!("Error flushing writer during drop: {:?}", e);
                }
                if let Err(e) = inner.writer.get_ref().sync_all() {
                     error!("Error syncing file during drop: {:?}", e);
                }
                 info!("Final flush/sync attempted.");
            } else {
                 warn!("Could not acquire write lock during drop (possibly poisoned). Final sync may not have occurred.");
            }
        } else {
             warn!("Could not get exclusive access via Arc during drop for final sync. Other references might exist. Data might not be fully flushed/synced.");
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    use std::sync::Once;
    static TRACING: Once = Once::new();
    fn setup_tracing() {
        TRACING.call_once(|| {
            tracing_subscriber::fmt()
                .with_max_level(tracing::Level::DEBUG)
                .with_test_writer()
                .init();
        });
    }
     fn setup_temp_db(sync: SyncStrategy) -> (SimpleDb, PathBuf) {
        let dir = tempdir().expect("Failed to create temp dir");
        let data_path = dir.path().join("test.dblog");
        let index_path = dir.path().join("test.dblog.index");
        let db = SimpleDb::open(&data_path, &index_path, sync)
            .expect("Failed to open temp DB");
        (db, dir.into_path()) // Return path to prevent premature cleanup
    }

    #[tokio::test]
    async fn test_put_get_simple() {
        setup_tracing();
        let (db, _dir) = setup_temp_db(SyncStrategy::Never);
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
        {
            let db1 = SimpleDb::open(&data_path, &index_path, SyncStrategy::Never)
                .expect("Failed to open DB 1");
            db1.put(key1, val1).expect("DB1 PUT key1 failed");
            db1.put(key2, val2).expect("DB1 PUT key2 failed");
            // db1 goes out of scope, Drop should be called
        }
        // Reopen
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
          let (db, dir) = setup_temp_db(SyncStrategy::Never); // Keep dir handle
          let data_path = dir.as_path().join("test.dblog");

          let key_a = b"keyA";
          let key_b = b"keyB";
          let key_c = b"keyC";
          let key_d = b"keyD";
          db.put(key_a, b"valA1").unwrap();
          db.put(key_b, b"valB1").unwrap();
          db.put(key_c, b"valC1").unwrap();
          db.put(key_d, b"valD1").unwrap();
          db.put(key_b, b"valB2_latest").unwrap();
          db.delete(key_c).unwrap();

          let log_size_before = fs::metadata(&data_path).unwrap().len();
          // --- CORRECTED LINE ---
          let index_size_before = db.inner.read().unwrap().index.len();
          // --- END CORRECTION ---
          assert_eq!(index_size_before, 3); // A, B, D should be present

          db.compact().expect("Compaction failed");

          let log_size_after = fs::metadata(&data_path).unwrap().len();
          let index_size_after = db.inner.read().unwrap().index.len();

          assert_eq!(index_size_after, 3);
          assert_eq!(db.get(key_a).unwrap(), Some(b"valA1".to_vec()));
          assert_eq!(db.get(key_b).unwrap(), Some(b"valB2_latest".to_vec()));
          assert_eq!(db.get(key_c).unwrap(), None); // Key C should still be gone
          assert_eq!(db.get(key_d).unwrap(), Some(b"valD1".to_vec()));
          assert!(log_size_after < log_size_before || log_size_before == 0, "Log size should decrease after compaction or stay 0");

          // Test writes after compaction
          db.put(b"keyE_after", b"valE").unwrap();
          assert_eq!(db.get(b"keyE_after").unwrap(), Some(b"valE".to_vec()));
          assert_eq!(db.inner.read().unwrap().index.len(), 4); // Now A, B, D, E
      }

    #[tokio::test]
    async fn test_snapshotting() {
        setup_tracing();
        let dir = tempdir().expect("Failed to create temp dir");
        let data_path = dir.path().join("snap.dblog");
        let index_path = dir.path().join("snap.dblog.index");
        let key1 = b"snap_key1";
        let val1 = b"snap_val1";
        let key_after = b"key_after_snap";
        let val_after = b"val_after";

        let snapshot_offset_check;
        {
            let db1 = SimpleDb::open(&data_path, &index_path, SyncStrategy::Never)
                .expect("Failed to open DB 1 for snap");
            db1.put(key1, val1).expect("DB1 snap PUT key1 failed");

            db1.save_index_snapshot().expect("Failed to save snapshot");

            snapshot_offset_check = { // Re-read snapshot to check offset
                let file = File::open(&index_path).unwrap();
                let mut reader = BufReader::new(file);
                let snapshot: IndexSnapshot = bincode::decode_from_std_read(&mut reader, bincode_standard_config()).unwrap();
                snapshot.offset
            };
            // Check snapshot time was set
            assert!(db1.inner.read().unwrap().last_snapshot_time.is_some());

            // Write *after* snapshot
            db1.put(key_after, val_after).unwrap();
        } // db1 is dropped

        // Reopen
        let db2 = SimpleDb::open(&data_path, &index_path, SyncStrategy::Never)
            .expect("Failed to open DB 2 after snap");

        // Check that the offset read from the snapshot file matches the file size *before* the last write
        let log_size_after_reopen = fs::metadata(&data_path).unwrap().len();
        let expected_log_size_at_snapshot = snapshot_offset_check; // This IS the offset recorded
        assert!(log_size_after_reopen > expected_log_size_at_snapshot, "Log size after reopen should be greater than snapshot offset");


        assert_eq!(db2.get(key1).expect("DB2 snap GET key1 failed"), Some(val1.to_vec()));
        assert_eq!(db2.get(key_after).expect("DB2 snap GET key_after failed"), Some(val_after.to_vec())); // Should be replayed from log
        assert!(index_path.exists(), "Index file should exist");
        // Check snapshot time was loaded
        assert!(db2.inner.read().unwrap().last_snapshot_time.is_some());
    }

     #[tokio::test]
     async fn test_key_value_size_limits() {
         setup_tracing();
         let (db, _dir) = setup_temp_db(SyncStrategy::Never);
         let large_key = vec![0u8; MAX_KEY_SIZE + 1];
         let large_value = vec![1u8; MAX_VALUE_SIZE + 1];
         let ok_key = b"ok_key";
         let ok_value = b"ok_value";

         let key_res = db.put(&large_key, ok_value);
         assert!(matches!(key_res, Err(DbError::KeyTooLarge { .. })));

         let val_res = db.put(ok_key, &large_value);
         assert!(matches!(val_res, Err(DbError::ValueTooLarge { .. })));

         // Test boundaries
         let max_key = vec![0u8; MAX_KEY_SIZE];
         let max_value = vec![1u8; MAX_VALUE_SIZE];
         assert!(db.put(&max_key, ok_value).is_ok());
         assert!(db.put(ok_key, &max_value).is_ok());
     }

      #[tokio::test]
      async fn test_get_log_size_check() {
          setup_tracing();
          let (db, _dir) = setup_temp_db(SyncStrategy::Never);
          let size_empty = db.get_log_size_for_compaction_check().unwrap();
          // May not be exactly 0 if header written on open, check <= some small value
          assert!(size_empty <= 64, "Initial log size should be very small (got {})", size_empty);

          db.put(b"somekey", b"somevalue").unwrap();
          // Force flush/sync to ensure metadata reflects write for test consistency
          db.sync().unwrap();

          let size_after_put = db.get_log_size_for_compaction_check().unwrap();
          assert!(size_after_put > size_empty, "Log size should increase after put (was {}, now {})", size_empty, size_after_put);
      }
}