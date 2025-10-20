use async_trait::async_trait;
use parking_lot::{RwLock};
use std::{mem, cell::UnsafeCell};
use crate::log_value::{LogValueDeserialized, Data};


/// High-level async trait for writing log entries to an active segment
#[async_trait]
pub trait LogSegmentWriter<D: Data>: Send + Sync {
    /// Append a log value and return its assigned index
    async fn append(&self, log_value: LogValueDeserialized<D>) -> Result<u32, LogSegmentError>;
    /// Seal the segment, making it immutable and ready for reads
    async fn seal(self) -> Result<(), LogSegmentError>;
    /// Min log index in the segment
    fn min_index(&self) -> u32;
    /// Max log segment in the log segment.
    fn max_index(&self) -> u32;
}

/// High-level async trait for reading log entries from a sealed segment
#[async_trait]
pub trait LogSegmentReader<D: Data>: Send + Sync {
    /// Get a log value by its index (returns serialized bytes)
    async fn get(&self, index: u32) -> Result<LogValueDeserialized<D>, LogSegmentError>;
    /// Min index in the log segment
    fn min_index(&self) -> u32;
    /// Maximum index in the log segment.
    fn max_index(&self) -> u32;
}

#[derive(Debug, thiserror::Error)]
pub enum LogSegmentError {
    #[error("Index {index} out of bounds (min: {min}, max: {max})")]
    IndexOutOfBounds { index: u32, min: u32, max: u32 },

    #[error("Failed to read entry: {0}")]
    ReadError(#[from] anyhow::Error),

    #[error("Failed to write entry: {0}")]
    WriteError(String),

    #[error("Segment is sealed and cannot accept writes")]
    SegmentSealed,

    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Serialization error: {0}")]
    SerializationError(String),
}

/// Maximum segment size in bytes (1 MB)
const MAX_SEGMENT_SIZE: u32 = 1024 * 1024;

/// Compile-time function to estimate entry size for a given data type.
/// Uses conservative lower bound.
const fn estimate_entry_size<D: Data>() -> u32 {
    // TODO: panic if > u32. We cannot handle 4Gb values anyway
    mem::size_of::<LogValueDeserialized<D>>() as u32
}

/// Active memory log segment that supports both reading and writing
/// Uses a pre-allocated Vec for efficient storage of log entries.
/// 
/// This is a DUMB implementation that will be replaced. Using
/// UnsafeCells here as a bit of a learning exercise. 
pub struct ActiveMemoryLogSegment<D: Data> {
    lock: RwLock<()>,
    /// Vector of log entries (protected by RwLock for thread-safety)
    entries: UnsafeCell<Vec<LogValueDeserialized<D>>>,
    /// Current approximate size in bytes (protected by RwLock)
    current_size: UnsafeCell<u32>,
    /// Index within the log segment
    /// that the next write will take
    /// place at.
    write_index: UnsafeCell<u32>,
    /// The index in the global log
    /// that this segment starts at and should
    /// not be mutated.
    log_index_offset: u32,
}

// SAFETY: ActiveMemoryLogSegment uses internal RwLock for synchronization,
// making it safe to send across thread boundaries and share between threads.
// All mutable access to internal state (entries, current_size, write_index)
// is protected by the RwLock. log_index_offset is immutable after creation.
unsafe impl<D: Data> Send for ActiveMemoryLogSegment<D> {}

unsafe impl<D: Data> Sync for ActiveMemoryLogSegment<D> {}

impl<D: Data> ActiveMemoryLogSegment<D> {
    /// Create a new active memory log segment with pre-allocated capacity
    pub fn new(log_index_offset: u32) -> Self {
        let estimated_entry_size = estimate_entry_size::<D>();
        let estimated_entries = MAX_SEGMENT_SIZE / estimated_entry_size.max(1);

        Self {
            entries: UnsafeCell::new(Vec::with_capacity(estimated_entries as usize)),
            lock: RwLock::new(()),
            current_size: UnsafeCell::new(0),
            write_index: UnsafeCell::new(0), // max < min when empty
            log_index_offset,
        }
    }

    /// Get the current size in bytes
    pub fn size(&self) -> u32 {
        let _lock = self.lock.read();
        // SAFETY: We hold the read lock, so no writers can modify current_size
        unsafe { *self.current_size.get() }
    }

    /// Check if the segment is full
    pub fn is_full(&self) -> bool {
        let _lock = self.lock.read();
        // SAFETY: We hold the read lock, so no writers can modify current_size
        unsafe { *self.current_size.get() >= MAX_SEGMENT_SIZE }
    }

    /// Approximate size of a LogValueDeserialized entry
    fn approximate_entry_size(entry: &LogValueDeserialized<D>) -> u32 {
        (mem::size_of::<u128>() // key
            + mem::size_of::<D>() // data (conservative estimate)
            + entry.metadata.iter().map(|(k, v)| k.len() + v.len()).sum::<usize>() // metadata
            + mem::size_of::<Vec<(String, String)>>() // Vec overhead
            ) as u32 // TODO: panic or error on wrap. 
    }
}

#[async_trait]
impl<D: Data + Clone> LogSegmentWriter<D> for ActiveMemoryLogSegment<D> {
    async fn append(&self, log_value: LogValueDeserialized<D>) -> Result<u32, LogSegmentError> {
        let entry_size = Self::approximate_entry_size(&log_value);

        let _lock = self.lock.write();

        // SAFETY: We hold the write lock, so we have exclusive access to modify the fields
        unsafe {
            let entries = &mut *self.entries.get();
            let current_size = &mut *self.current_size.get();
            let write_index = &mut *self.write_index.get();

            if *current_size + entry_size > MAX_SEGMENT_SIZE {
                return Err(LogSegmentError::WriteError(
                    format!("Segment full: {} + {} > {}", *current_size, entry_size, MAX_SEGMENT_SIZE)
                ));
            }

            entries.push(log_value);
            *current_size += entry_size;
            *write_index += 1;

            Ok(*write_index - 1)
        }
    }

    async fn seal(self) -> Result<(), LogSegmentError> {
        let _lock = self.lock.write();
        // In a real implementation, you might store this sealed segment somewhere
        // For now, we just consume self and return success
        Ok(())
    }

    fn min_index(&self) -> u32 {
        let _lock = self.lock.read();
        self.log_index_offset
    }

    fn max_index(&self) -> u32 {
        let _lock = self.lock.read();
        // SAFETY: We hold the read lock
        let write_index = unsafe { *self.write_index.get() };
        self.log_index_to_segment_index(write_index - 1)
    }
}

impl<D: Data> ActiveMemoryLogSegment<D> {

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        let _lock = self.lock.read();
        // SAFETY: We hold the read lock
        unsafe { (*self.entries.get()).is_empty() }
    }

    #[inline]
    fn log_index_to_segment_index(&self, i: u32) -> u32 {
        // No lock needed - log_index_offset is immutable
        i.saturating_sub(self.log_index_offset)
    }
    fn segment_index_to_log_index(&self, i: u32) -> u32 {
        // No lock needed - log_index_offset is immutable
        i + (self.log_index_offset)
    }
}

#[async_trait]
impl<D: Data + Clone> LogSegmentReader<D> for ActiveMemoryLogSegment<D> {
    async fn get(&self, index: u32) -> Result<LogValueDeserialized<D>, LogSegmentError> {
        let _lock = self.lock.read();

        let index = self.log_index_to_segment_index(index);
        // SAFETY: We hold the read lock
        unsafe {
            let entries = &*self.entries.get();
            let write_index = *self.write_index.get();

            entries.get(index as usize)
                .cloned()
                .ok_or(LogSegmentError::IndexOutOfBounds {
                    index,
                    min: 0,
                    max: self.segment_index_to_log_index(write_index - 1),
                })
        }
    }

    fn min_index(&self) -> u32 {
        let _lock = self.lock.read();
        self.log_index_offset
    }

    fn max_index(&self) -> u32 {
        let _lock = self.lock.read();
        // SAFETY: We hold the read lock
        let write_index = unsafe { *self.write_index.get() };
        self.log_index_to_segment_index(write_index - 1)
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Serialize, Deserialize};

    #[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
    struct TestData {
        value: i32,
        name: String,
    }

    #[tokio::test]
    async fn test_active_segment_append() {
        let segment = ActiveMemoryLogSegment::<TestData>::new(0);

        let entry = LogValueDeserialized {
            key: 123,
            data: TestData {
                value: 42,
                name: "test".to_string(),
            },
            metadata: vec![],
        };

        let index = segment.append(entry).await.unwrap();
        assert_eq!(index, 0);
        assert_eq!(LogSegmentWriter::min_index(&segment), 0);
        assert_eq!(LogSegmentWriter::max_index(&segment), 0);
    }

    #[tokio::test]
    async fn test_active_segment_read() {
        let segment = ActiveMemoryLogSegment::<TestData>::new(0);

        let entry = LogValueDeserialized {
            key: 123,
            data: TestData {
                value: 42,
                name: "test".to_string(),
            },
            metadata: vec![],
        };

        segment.append(entry).await.unwrap();

        // Use the LogSegmentReader trait to get the entry
        let retrieved = LogSegmentReader::get(&segment, 0).await.unwrap();
        assert_eq!(retrieved.key, 123);
        assert_eq!(retrieved.data.value, 42);
        assert_eq!(retrieved.data.name, "test");
    }

    // TODO: Uncomment when SealedMemoryLogSegment is implemented
    // #[tokio::test]
    // async fn test_sealed_segment_read_only() {
    //     let mut active = ActiveMemoryLogSegment::<TestData>::new(0);
    //
    //     let entry = LogValueDeserialized {
    //         key: 456,
    //         data: TestData {
    //             value: 99,
    //             name: "sealed".to_string(),
    //         },
    //         metadata: vec![],
    //     };
    //
    //     active.append(entry).await.unwrap();
    //     let sealed = SealedMemoryLogSegment::from(active);
    //
    //     let retrieved = sealed.get(0).await.unwrap();
    //     assert_eq!(retrieved.key, 456);
    // }
}
