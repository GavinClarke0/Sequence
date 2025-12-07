use crate::log_segment::LogSegmentError;
use crate::log_value::{LogData, LogValueDeserialized, LogValueSerialized};
use async_trait::async_trait;
use fjall::compaction::Fifo;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};

use fjall::{compaction::Strategy, Config, Keyspace, PartitionCreateOptions, PartitionHandle};

#[derive(Debug, thiserror::Error)]
pub enum LogError {
    #[error("Log segment error: {0}")]
    SegmentError(#[from] LogSegmentError),

    #[error("Index {0} not found in log")]
    IndexNotFound(u32),

    #[error("Log is empty")]
    LogEmpty,

    #[error("Database error: {0}")]
    DatabaseError(#[from] fjall::Error),

    #[error("Serialization error: {0}")]
    SerializationError(String),
}

type LogId = String;


#[async_trait]
pub trait Log<D: LogData> {
    fn id(&self) -> &LogId;
    async fn append(&self, log_value: LogValueDeserialized<D>) -> Result<u32, LogError>;
    async fn get(&self, index: u32) -> Result<LogValueDeserialized<D>, LogError>;
    async fn size_bytes(&self) -> u64;
}

/// Block cache size for fjall database (64 MB)
const FJALL_MEM_CACHE_SIZE: u64 = 64 * 1024 * 1024;
/// Max stored data in partition before oldest segments
/// are dropped.
const FJALL_LOG_MAX_SIZE: u64 = 64 * 1024 * 1024;
/// Time-to-live for log entries (None = no TTL)
const FJALL_LOG_TTL: Option<u64> = None;
/// Number of flush worker threads for memtable flushing
const FJALL_FLUSH_WORKERS: usize = 2;
/// Number of compaction worker threads for LSM tree compaction
const FJALL_COMPACTION_WORKERS: usize = 2;

pub struct FjallLog {
    log_id: LogId,
    partition: PartitionHandle,
    /// Atomic counter for the next available index
    next_index: AtomicU32,
}

impl FjallLog {
    /// Create a new FjallLog with the given partition, inferring the next index from existing data
    pub fn new(log_id: LogId, partition: PartitionHandle) -> Result<Self, LogError> {
        let next_index_value = Self::infer_next_index(&partition)?;
        let next_index = AtomicU32::new(next_index_value);

        Ok(FjallLog {
            log_id,
            partition,
            next_index,
        })
    }

    /// Scan the partition to find the maximum index and initialize next_index
    fn infer_next_index(partition: &PartitionHandle) -> Result<u32, LogError> {
        let mut max_index: Option<u32> = None;

        // Last key must be highest valued index since we use BE u32 as indexes. 
        if let Some(kv) = partition.last_key_value()? {
            let (key, _) = kv; 
            if key.len() == 4 {
                if let Ok(key_bytes) = <[u8; 4]>::try_from(key.as_ref()) {
                    let index = u32::from_be_bytes(key_bytes);
                    max_index = Some(max_index.map_or(index, |max| max.max(index)));
                }
            }
        }

        // Next index is max_index + 1, or 0 if no entries exist
        Ok(max_index.map_or(0, |max| max.saturating_add(1)))
    }

    /// Get the current index and increment atomically
    fn get_and_increment_index(&self) -> u32 {
        self.next_index.fetch_add(1, Ordering::SeqCst)
    }
}

#[async_trait]
impl<D: LogData> Log<D> for FjallLog {
    fn id(&self) -> &LogId {
        &self.log_id
    }

    async fn append(&self, log_value: LogValueDeserialized<D>) -> Result<u32, LogError> {
        // Get current index and increment
        let index = self.get_and_increment_index();

        // Serialize using the LogData trait implementation
        let serialized = log_value
            .to_serialized()
            .map_err(|e| LogError::SerializationError(e.to_string()))?;

        // Serialize with rkyv for storage. Todo: use arena.
        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&serialized)
            .map_err(|e| LogError::SerializationError(e.to_string()))?;

        // Store in partition with index as key
        let key = index.to_be_bytes();
        self.partition.insert(key, bytes.as_ref())?;

        Ok(index)
    }

    async fn get(&self, index: u32) -> Result<LogValueDeserialized<D>, LogError> {
        // Retrieve from partition
        let key = index.to_be_bytes();
        let value = self.partition
            .get(key)?
            .ok_or(LogError::IndexNotFound(index))?;

        // Copy to aligned buffer for rkyv deserialization
        let mut aligned_bytes: rkyv::util::AlignedVec<16> = rkyv::util::AlignedVec::new();
        aligned_bytes.extend_from_slice(value.as_ref());

        // Deserialize from rkyv
        let serialized: LogValueSerialized = rkyv::from_bytes::<LogValueSerialized, rkyv::rancor::Error>(&aligned_bytes)
            .map_err(|e: rkyv::rancor::Error| LogError::SerializationError(format!("rkyv deserialization failed: {}", e)))?;

        // Deserialize using the LogData trait implementation
        let deserialized = serialized
            .to_deserialized()
            .map_err(|e| LogError::SerializationError(e.to_string()))?;

        Ok(deserialized)
    }

    async fn size_bytes(&self) -> u64 {
        // Use partition's approximate size method
        self.partition.disk_space() as u64
    }
}

pub struct FjallDatabaseState {
    key_space: Keyspace,
}

impl FjallDatabaseState {
    pub fn new(file_path: PathBuf) -> anyhow::Result<Self> {
        let key_space = Config::new(file_path)
            .cache_size(FJALL_MEM_CACHE_SIZE)
            .flush_workers(FJALL_FLUSH_WORKERS)
            .compaction_workers(FJALL_COMPACTION_WORKERS)
            .open()?;
        Ok(Self { key_space })
    }

    pub fn new_log(&self, log_id: LogId) -> Result<FjallLog, LogError> {
        // Configure partition with FIFO compaction strategy
        let options = PartitionCreateOptions::default()
            .compaction_strategy(Strategy::Fifo(Fifo{
                limit: FJALL_LOG_MAX_SIZE,
                ttl_seconds: FJALL_LOG_TTL, 
            }));

        let partition = self
            .key_space
            .open_partition(&log_id, options)?;

        FjallLog::new(log_id, partition)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};
    use tempfile::TempDir;

    #[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
    struct TestData {
        value: i32,
        name: String,
    }

    impl LogData for TestData {
        fn to_bytes(&self) -> anyhow::Result<Vec<u8>> {
            serde_json::to_vec(self).map_err(|e| anyhow::anyhow!("Serialization failed: {}", e))
        }

        fn from_bytes(bytes: &[u8]) -> anyhow::Result<Self> {
            serde_json::from_slice(bytes)
                .map_err(|e| anyhow::anyhow!("Deserialization failed: {}", e))
        }
    }

    fn create_test_db() -> (FjallDatabaseState, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let db_state = FjallDatabaseState::new(temp_dir.path().to_path_buf()).unwrap();
        (db_state, temp_dir)
    }

    #[tokio::test]
    async fn test_create_log_and_append() {
        let (db_state, _temp_dir) = create_test_db();
        let log: FjallLog = db_state.new_log("test_log".to_string()).unwrap();

        let entry = LogValueDeserialized {
            key: 123,
            data: TestData {
                value: 42,
                name: "test".to_string(),
            },
            metadata: vec![],
        };

        let index = log.append(entry).await.unwrap();
        assert_eq!(index, 0, "First entry should have index 0");
    }

    #[tokio::test]
    async fn test_append_and_read() {
        let (db_state, _temp_dir) = create_test_db();
        let log: FjallLog = db_state.new_log("test_log".to_string()).unwrap();

        let entry = LogValueDeserialized {
            key: 456,
            data: TestData {
                value: 99,
                name: "hello".to_string(),
            },
            metadata: vec![("key1".to_string(), "value1".to_string())],
        };

        let index = log.append(entry.clone()).await.unwrap();

        let retrieved: LogValueDeserialized<TestData> = log.get(index).await.unwrap();
        assert_eq!(retrieved.key, 456);
        assert_eq!(retrieved.data.value, 99);
        assert_eq!(retrieved.data.name, "hello");
        assert_eq!(retrieved.metadata.len(), 1);
        assert_eq!(retrieved.metadata[0].0, "key1");
        assert_eq!(retrieved.metadata[0].1, "value1");
    }

    #[tokio::test]
    async fn test_sequential_index_assignment() {
        let (db_state, _temp_dir) = create_test_db();
        let log: FjallLog = db_state.new_log("test_log".to_string()).unwrap();

        let mut indexes = Vec::new();
        for i in 0..10 {
            let entry = LogValueDeserialized {
                key: i as u128,
                data: TestData {
                    value: i,
                    name: format!("entry_{}", i),
                },
                metadata: vec![],
            };
            let index = log.append(entry).await.unwrap();
            indexes.push(index);
        }

        assert_eq!(indexes, vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);

        for (i, &index) in indexes.iter().enumerate() {
            let retrieved: LogValueDeserialized<TestData> = log.get(index).await.unwrap();
            assert_eq!(retrieved.data.value, i as i32);
            assert_eq!(retrieved.data.name, format!("entry_{}", i));
        }
    }

    #[tokio::test]
    async fn test_index_not_found() {
        let (db_state, _temp_dir) = create_test_db();
        let log: FjallLog = db_state.new_log("test_log".to_string()).unwrap();

        let result: Result<LogValueDeserialized<TestData>, LogError> = log.get(999).await;
        assert!(result.is_err());
        match result.unwrap_err() {
            LogError::IndexNotFound(index) => assert_eq!(index, 999),
            _ => panic!("Expected IndexNotFound error"),
        }
    }

    #[tokio::test]
    async fn test_next_index_inference_on_reopen() {
        let temp_dir = TempDir::new().unwrap();
        let log_id = "test_log_reopen".to_string();

        {
            let db_state = FjallDatabaseState::new(temp_dir.path().to_path_buf()).unwrap();
            let log: FjallLog = db_state.new_log(log_id.clone()).unwrap();

            for i in 0..5 {
                let entry = LogValueDeserialized {
                    key: i,
                    data: TestData {
                        value: i as i32,
                        name: format!("entry_{}", i),
                    },
                    metadata: vec![],
                };
                log.append(entry).await.unwrap();
            }
        }

        {
            let db_state = FjallDatabaseState::new(temp_dir.path().to_path_buf()).unwrap();
            let log: FjallLog = db_state.new_log(log_id.clone()).unwrap();

            let entry = LogValueDeserialized {
                key: 100,
                data: TestData {
                    value: 100,
                    name: "new_entry".to_string(),
                },
                metadata: vec![],
            };

            let index = log.append(entry).await.unwrap();
            assert_eq!(index, 5, "Next index should be 5 after reopening");

            let retrieved: LogValueDeserialized<TestData> = log.get(0).await.unwrap();
            assert_eq!(retrieved.data.value, 0);

            let retrieved_new: LogValueDeserialized<TestData> = log.get(5).await.unwrap();
            assert_eq!(retrieved_new.data.value, 100);
        }
    }

    #[tokio::test]
    async fn test_empty_log_next_index() {
        let (db_state, _temp_dir) = create_test_db();
        let log: FjallLog = db_state.new_log("empty_log".to_string()).unwrap();

        let entry = LogValueDeserialized {
            key: 1,
            data: TestData {
                value: 1,
                name: "first".to_string(),
            },
            metadata: vec![],
        };

        let index = log.append(entry).await.unwrap();
        assert_eq!(index, 0, "Empty log should start at index 0");
    }

    #[tokio::test]
    async fn test_size_bytes() {
        let (db_state, _temp_dir) = create_test_db();
        let log = db_state.new_log("test_log".to_string()).unwrap();

        let initial_size = <FjallLog as Log<TestData>>::size_bytes(&log).await;

        let entry = LogValueDeserialized {
            key: 1,
            data: TestData {
                value: 42,
                name: "test_data".to_string(),
            },
            metadata: vec![],
        };

        <FjallLog as Log<TestData>>::append(&log, entry).await.unwrap();

        let after_append_size = <FjallLog as Log<TestData>>::size_bytes(&log).await;
        assert!(
            after_append_size >= initial_size,
            "Size should increase after appending data"
        );
    }

    #[tokio::test]
    async fn test_concurrent_appends() {
        let (db_state, _temp_dir) = create_test_db();
        let log = std::sync::Arc::new(db_state.new_log("concurrent_log".to_string()).unwrap());

        let mut handles = vec![];
        for i in 0..10 {
            let log_clone = log.clone();
            let handle = tokio::spawn(async move {
                let entry = LogValueDeserialized {
                    key: i,
                    data: TestData {
                        value: i as i32,
                        name: format!("concurrent_{}", i),
                    },
                    metadata: vec![],
                };
                log_clone.append(entry).await.unwrap()
            });
            handles.push(handle);
        }

        let mut indexes = vec![];
        for handle in handles {
            indexes.push(handle.await.unwrap());
        }

        indexes.sort();
        assert_eq!(indexes, vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);

        for index in 0..10 {
            let retrieved: LogValueDeserialized<TestData> = log.get(index).await.unwrap();
            assert!(retrieved.data.value >= 0 && retrieved.data.value < 10);
        }
    }

    #[tokio::test]
    async fn test_log_id() {
        let (db_state, _temp_dir) = create_test_db();
        let log = db_state.new_log("my_test_log".to_string()).unwrap();

        assert_eq!(<FjallLog as Log<TestData>>::id(&log), "my_test_log");
    }

    #[tokio::test]
    async fn test_multiple_logs_independent() {
        let (db_state, _temp_dir) = create_test_db();
        let log1: FjallLog = db_state.new_log("log1".to_string()).unwrap();
        let log2: FjallLog = db_state.new_log("log2".to_string()).unwrap();

        let entry1 = LogValueDeserialized {
            key: 1,
            data: TestData {
                value: 100,
                name: "log1_entry".to_string(),
            },
            metadata: vec![],
        };

        let entry2 = LogValueDeserialized {
            key: 2,
            data: TestData {
                value: 200,
                name: "log2_entry".to_string(),
            },
            metadata: vec![],
        };

        let index1 = log1.append(entry1).await.unwrap();
        let index2 = log2.append(entry2).await.unwrap();

        assert_eq!(index1, 0);
        assert_eq!(index2, 0);

        let retrieved1: LogValueDeserialized<TestData> = log1.get(0).await.unwrap();
        let retrieved2: LogValueDeserialized<TestData> = log2.get(0).await.unwrap();

        assert_eq!(retrieved1.data.value, 100);
        assert_eq!(retrieved2.data.value, 200);
    }
}
