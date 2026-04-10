use fjall::{
    compaction::{Fifo, Strategy},
    Config, Keyspace, PartitionCreateOptions, PartitionHandle,
};
use futures::Stream;
use std::path::PathBuf;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use tokio::sync::Notify;

const FJALL_MEM_CACHE_SIZE: u64 = 64 * 1024 * 1024;
const FJALL_QUEUE_MAX_SIZE: u64 = 64 * 1024 * 1024;

#[derive(Debug, thiserror::Error)]
pub enum QueueError {
    #[error("Database error: {0}")]
    Db(#[from] fjall::Error),
    #[error("Deserialization failed: {0}")]
    Decode(String),
    #[error("Sequence number {requested} has been deleted (current oldest: {oldest})")]
    Lagged { requested: u64, oldest: u64 },
}

/// The contract for types that can be stored in a `FjallQueue`.
pub trait Queueable: Sized + Send + Sync + 'static {
    fn serialize(&self) -> Vec<u8>;
    fn deserialize(bytes: &[u8]) -> Result<Self, Box<dyn std::error::Error + Send + Sync>>;
}

#[derive(Debug, Clone)]
pub struct Message<T> {
    pub seq: u64,
    pub data: T,
}

pub struct FjallQueue<T: Queueable> {
    partition: PartitionHandle,
    next_seq: Arc<AtomicU64>,
    notifier: Arc<Notify>,
    _marker: std::marker::PhantomData<T>,
}

impl<T: Queueable> FjallQueue<T> {
    pub fn new(partition: PartitionHandle) -> Self {
        let last_key = partition
            .iter()
            .rev()
            .next()
            .and_then(|res| res.ok())
            .map(|(k, _)| u64::from_be_bytes(k[..8].try_into().unwrap_or([0; 8])));

        let start_seq = last_key.map(|s| s + 1).unwrap_or(0);

        Self {
            partition,
            next_seq: Arc::new(AtomicU64::new(start_seq)),
            notifier: Arc::new(Notify::new()),
            _marker: std::marker::PhantomData,
        }
    }

    /// Appends an item, returning its monotonically increasing sequence number.
    pub fn append(&self, item: &T) -> Result<u64, QueueError> {
        let seq = self.next_seq.fetch_add(1, Ordering::SeqCst);
        self.partition.insert(seq.to_be_bytes(), item.serialize())?;
        self.notifier.notify_waiters();
        Ok(seq)
    }

    /// Fetches up to `limit` messages starting at `start_seq`.
    /// Errors if `start_seq` has already been pruned by the retention policy.
    pub fn fetch_batch(&self, start_seq: u64, limit: usize) -> Result<Vec<Message<T>>, QueueError> {
        if let Some(Ok((first_key, _))) = self.partition.iter().next() {
            let oldest = u64::from_be_bytes(first_key[..8].try_into().unwrap_or([0; 8]));
            if start_seq < oldest {
                return Err(QueueError::Lagged {
                    requested: start_seq,
                    oldest,
                });
            }
        }

        self.partition
            .range(start_seq.to_be_bytes()..)
            .take(limit)
            .map(|res| {
                let (k, v) = res.map_err(QueueError::Db)?;
                let seq = u64::from_be_bytes(k[..8].try_into().unwrap());
                let data = T::deserialize(&v).map_err(|e| QueueError::Decode(e.to_string()))?;
                Ok(Message { seq, data })
            })
            .collect()
    }

    /// Returns a stream starting at `start_seq`. Replays historical messages
    /// then waits for new ones, making it suitable for use in transformer loops.
    ///
    /// Takes `Arc<Self>` so the returned stream is `'static` and can be spawned.
    pub fn subscribe(
        self: Arc<Self>,
        start_seq: u64,
    ) -> impl Stream<Item = Result<Message<T>, QueueError>> + 'static {
        let mut cursor = start_seq;
        async_stream::stream! {
            loop {
                match self.fetch_batch(cursor, 100) {
                    Ok(batch) => {
                        if batch.is_empty() {
                            self.notifier.notified().await;
                        } else {
                            for msg in batch {
                                cursor = msg.seq + 1;
                                yield Ok(msg);
                            }
                        }
                    }
                    Err(e) => {
                        yield Err(e);
                        break;
                    }
                }
            }
        }
    }
}

/// Owns the fjall `Keyspace` and opens named queues.
pub struct FjallDatabase {
    keyspace: Keyspace,
}

impl FjallDatabase {
    pub fn new(path: PathBuf) -> anyhow::Result<Self> {
        let keyspace = Config::new(path)
            .cache_size(FJALL_MEM_CACHE_SIZE)
            .open()?;
        Ok(Self { keyspace })
    }

    /// Opens (or creates) a named queue with FIFO compaction.
    pub fn open_queue<T: Queueable>(&self, name: &str) -> Result<FjallQueue<T>, QueueError> {
        let options = PartitionCreateOptions::default()
            .compaction_strategy(Strategy::Fifo(Fifo {
                limit: FJALL_QUEUE_MAX_SIZE,
                ttl_seconds: None,
            }));
        let partition = self.keyspace.open_partition(name, options)?;
        Ok(FjallQueue::new(partition))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    struct StringItem(String);

    impl Queueable for StringItem {
        fn serialize(&self) -> Vec<u8> {
            self.0.as_bytes().to_vec()
        }
        fn deserialize(bytes: &[u8]) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
            Ok(StringItem(String::from_utf8(bytes.to_vec())?))
        }
    }

    fn open_test_queue(dir: &TempDir) -> FjallQueue<StringItem> {
        let db = FjallDatabase::new(dir.path().to_path_buf()).unwrap();
        db.open_queue("test").unwrap()
    }

    #[test]
    fn test_append_and_fetch() {
        let dir = TempDir::new().unwrap();
        let queue = open_test_queue(&dir);

        let seq = queue.append(&StringItem("hello".into())).unwrap();
        assert_eq!(seq, 0);

        let batch = queue.fetch_batch(0, 10).unwrap();
        assert_eq!(batch.len(), 1);
        assert_eq!(batch[0].seq, 0);
        assert_eq!(batch[0].data.0, "hello");
    }

    #[test]
    fn test_sequential_seq_numbers() {
        let dir = TempDir::new().unwrap();
        let queue = open_test_queue(&dir);

        for i in 0..5u64 {
            let seq = queue.append(&StringItem(i.to_string())).unwrap();
            assert_eq!(seq, i);
        }
    }

    #[test]
    fn test_resumes_after_reopen() {
        let dir = TempDir::new().unwrap();
        {
            let db = FjallDatabase::new(dir.path().to_path_buf()).unwrap();
            let queue: FjallQueue<StringItem> = db.open_queue("q").unwrap();
            queue.append(&StringItem("a".into())).unwrap();
            queue.append(&StringItem("b".into())).unwrap();
        }
        {
            let db = FjallDatabase::new(dir.path().to_path_buf()).unwrap();
            let queue: FjallQueue<StringItem> = db.open_queue("q").unwrap();
            let seq = queue.append(&StringItem("c".into())).unwrap();
            assert_eq!(seq, 2);
        }
    }

    #[tokio::test]
    async fn test_subscribe_streams_existing_then_new() {
        use futures::StreamExt;

        let dir = TempDir::new().unwrap();
        let db = FjallDatabase::new(dir.path().to_path_buf()).unwrap();
        let queue = Arc::new(db.open_queue::<StringItem>("q").unwrap());

        queue.append(&StringItem("a".into())).unwrap();
        queue.append(&StringItem("b".into())).unwrap();

        let producer = Arc::clone(&queue);
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            producer.append(&StringItem("c".into())).unwrap();
        });

        let mut stream = std::pin::pin!(Arc::clone(&queue).subscribe(0));
        let mut collected = vec![];
        for _ in 0..3 {
            let msg = stream.next().await.unwrap().unwrap();
            collected.push(msg.data.0.clone());
        }

        assert_eq!(collected, vec!["a", "b", "c"]);
    }
}
