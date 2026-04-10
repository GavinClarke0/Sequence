use crate::log::{Log, LogError};
use crate::log_value::{LogData, LogValueDeserialized};
use std::future::Future;
use std::marker::PhantomData;
use std::sync::Arc;

/// A sink is an exit point from the computation graph.
/// External programs consume data from the sink's backing log.
///
/// Sinks are strongly typed - the Data type must implement LogData,
/// ensuring serializability for external consumers.
///
/// Uses RPITIT (return position impl trait) instead of async_trait to avoid boxing.
pub trait Sink: Send + Sync + 'static {
    type Data: LogData;

    /// Unique identifier for this sink
    fn id(&self) -> &str;

    /// Consume a value from the sink log at the given index.
    /// Called by external consumers to read processed data.
    fn consume(
        &self,
        index: u32,
    ) -> impl Future<Output = Result<LogValueDeserialized<Self::Data>, LogError>> + Send;

    /// Internal method: write a value to the sink log.
    /// Called by the runtime when data flows to this sink.
    fn write_internal(
        &self,
        value: LogValueDeserialized<Self::Data>,
    ) -> impl Future<Output = Result<u32, LogError>> + Send;

    /// Get the current size of the backing log in bytes.
    fn size_bytes(&self) -> impl Future<Output = u64> + Send;
}

/// Concrete sink backed by a typed log.
pub struct LogSink<D, L>
where
    D: LogData,
    L: Log<D>,
{
    id: String,
    log: Arc<L>,
    _marker: PhantomData<D>,
}

impl<D, L> LogSink<D, L>
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

impl<D, L> Sink for LogSink<D, L>
where
    D: LogData,
    L: Log<D> + Send + Sync + 'static,
{
    type Data = D;

    fn id(&self) -> &str {
        &self.id
    }

    async fn consume(&self, index: u32) -> Result<LogValueDeserialized<Self::Data>, LogError> {
        self.log.get(index).await
    }

    async fn write_internal(
        &self,
        value: LogValueDeserialized<Self::Data>,
    ) -> Result<u32, LogError> {
        self.log.append(value).await
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
    struct ProcessedEvent {
        original_name: String,
        processed_value: i32,
    }

    impl LogData for ProcessedEvent {
        fn to_bytes(&self) -> anyhow::Result<Vec<u8>> {
            serde_json::to_vec(self).map_err(|e| anyhow::anyhow!("Serialization failed: {}", e))
        }

        fn from_bytes(bytes: &[u8]) -> anyhow::Result<Self> {
            serde_json::from_slice(bytes)
                .map_err(|e| anyhow::anyhow!("Deserialization failed: {}", e))
        }
    }

    #[tokio::test]
    async fn test_sink_write_and_consume() {
        let temp_dir = TempDir::new().unwrap();
        let db = FjallDatabaseState::new(temp_dir.path().to_path_buf()).unwrap();
        let log = db.new_log("test_sink".to_string()).unwrap();

        let sink = LogSink::new("processed", log);

        let event = LogValueDeserialized::new(
            1,
            ProcessedEvent {
                original_name: "test".to_string(),
                processed_value: 84,
            },
            vec![],
        );

        // Runtime writes to sink
        let index = sink.write_internal(event.clone()).await.unwrap();
        assert_eq!(index, 0);

        // External consumer reads from sink
        let consumed = sink.consume(index).await.unwrap();
        assert_eq!(consumed.key, 1);
        assert_eq!(consumed.data.original_name, "test");
        assert_eq!(consumed.data.processed_value, 84);
    }

    #[tokio::test]
    async fn test_sink_sequential_consumption() {
        let temp_dir = TempDir::new().unwrap();
        let db = FjallDatabaseState::new(temp_dir.path().to_path_buf()).unwrap();
        let log = db.new_log("test_sink".to_string()).unwrap();

        let sink = LogSink::new("processed", log);

        // Runtime writes multiple values
        for i in 0..5 {
            let event = LogValueDeserialized::new(
                i as u128,
                ProcessedEvent {
                    original_name: format!("event_{}", i),
                    processed_value: i * 2,
                },
                vec![],
            );
            let index = sink.write_internal(event).await.unwrap();
            assert_eq!(index, i as u32);
        }

        // External consumer reads all values
        for i in 0..5 {
            let consumed = sink.consume(i as u32).await.unwrap();
            assert_eq!(consumed.data.original_name, format!("event_{}", i));
            assert_eq!(consumed.data.processed_value, i * 2);
        }
    }
}
