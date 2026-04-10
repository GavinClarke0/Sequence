use crate::log::{Log, LogError};
use crate::log_value::{LogData, LogValueDeserialized};
use std::future::Future;
use std::marker::PhantomData;
use std::sync::Arc;

/// A source is an entry point into the computation graph.
/// Sources are backed by a Log from which data is read and to which data is written.
///
/// Uses RPITIT (return position impl trait) instead of async_trait to avoid boxing.
pub trait Source: Send + Sync + 'static {
    type Data: LogData;

    /// Unique identifier for this source
    fn id(&self) -> &str;

    /// Write a value to the source log.
    /// Returns the index at which the value was written.
    fn write(
        &self,
        value: LogValueDeserialized<Self::Data>,
    ) -> impl Future<Output = Result<u32, LogError>> + Send;

    /// Read a value from the source log at the given index.
    fn read(
        &self,
        index: u32,
    ) -> impl Future<Output = Result<LogValueDeserialized<Self::Data>, LogError>> + Send;

    /// Get the current size of the backing log in bytes.
    fn size_bytes(&self) -> impl Future<Output = u64> + Send;
}

/// Concrete source backed by a typed log.
pub struct LogSource<D, L>
where
    D: LogData,
    L: Log<D>,
{
    id: String,
    log: Arc<L>,
    _marker: PhantomData<D>,
}

impl<D, L> LogSource<D, L>
where
    D: LogData,
    L: Log<D>,
{
    pub fn new(id: impl Into<String>, log: L) -> Self {
        Self {
            id: id.into(),
            log: Arc::new(log),
            _marker: PhantomData,
        }
    }

    /// Get a reference to the underlying log.
    pub fn log(&self) -> &L {
        &self.log
    }
}

impl<D, L> Source for LogSource<D, L>
where
    D: LogData,
    L: Log<D> + Send + Sync + 'static,
{
    type Data = D;

    fn id(&self) -> &str {
        &self.id
    }

    async fn write(&self, value: LogValueDeserialized<Self::Data>) -> Result<u32, LogError> {
        self.log.append(value).await
    }

    async fn read(&self, index: u32) -> Result<LogValueDeserialized<Self::Data>, LogError> {
        self.log.get(index).await
    }

    async fn size_bytes(&self) -> u64 {
        self.log.size_bytes().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::log::FjallDatabaseState;
    use serde::{Deserialize, Serialize};
    use tempfile::TempDir;

    #[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
    struct TestEvent {
        name: String,
        value: i32,
    }

    impl LogData for TestEvent {
        fn to_bytes(&self) -> anyhow::Result<Vec<u8>> {
            serde_json::to_vec(self).map_err(|e| anyhow::anyhow!("Serialization failed: {}", e))
        }

        fn from_bytes(bytes: &[u8]) -> anyhow::Result<Self> {
            serde_json::from_slice(bytes)
                .map_err(|e| anyhow::anyhow!("Deserialization failed: {}", e))
        }
    }

    #[tokio::test]
    async fn test_source_write_and_read() {
        let temp_dir = TempDir::new().unwrap();
        let db = FjallDatabaseState::new(temp_dir.path().to_path_buf()).unwrap();
        let log = db.new_log("test_source".to_string()).unwrap();

        let source = LogSource::new("events", log);

        let event = LogValueDeserialized::new(
            1,
            TestEvent {
                name: "test".to_string(),
                value: 42,
            },
            vec![],
        );

        let index = source.write(event.clone()).await.unwrap();
        assert_eq!(index, 0);

        let retrieved = source.read(index).await.unwrap();
        assert_eq!(retrieved.key, 1);
        assert_eq!(retrieved.data.name, "test");
        assert_eq!(retrieved.data.value, 42);
    }

    #[tokio::test]
    async fn test_source_sequential_writes() {
        let temp_dir = TempDir::new().unwrap();
        let db = FjallDatabaseState::new(temp_dir.path().to_path_buf()).unwrap();
        let log = db.new_log("test_source".to_string()).unwrap();

        let source = LogSource::new("events", log);

        for i in 0..5 {
            let event = LogValueDeserialized::new(
                i as u128,
                TestEvent {
                    name: format!("event_{}", i),
                    value: i,
                },
                vec![],
            );
            let index = source.write(event).await.unwrap();
            assert_eq!(index, i as u32);
        }

        for i in 0..5 {
            let retrieved = source.read(i as u32).await.unwrap();
            assert_eq!(retrieved.data.name, format!("event_{}", i));
            assert_eq!(retrieved.data.value, i);
        }
    }
}
