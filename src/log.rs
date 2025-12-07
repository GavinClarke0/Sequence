use parking_lot::RwLock;
use std::sync::Arc;
use std::marker::PhantomData;
use crate::log_segment::{LogSegmentWriter, LogSegmentReader, LogSegmentError};
use crate::log_value::{Data, LogValueDeserialized};

#[derive(Debug, thiserror::Error)]
pub enum LogError {
    #[error("Log segment error: {0}")]
    SegmentError(#[from] LogSegmentError),

    #[error("Index {0} not found in log")]
    IndexNotFound(u32),

    #[error("Log is empty")]
    LogEmpty,
}

/// Trait for managing log state (segments and indices)
/// Implementations must be Send + Sync for concurrent access
pub trait LogState<D: Data, S>: Send + Sync
where
    S: LogSegmentWriter<D> + LogSegmentReader<D>,
{
    /// Get the current active segment (cheap clone of Arc)
    fn active_segment(&self) -> Arc<S>;

    /// Get all sealed segments (clones Arcs)
    fn sealed_segments(&self) -> Vec<Arc<S>>;

    /// Attempt to rotate the active segment if it matches the expected current segment
    /// Returns true if rotation occurred
    fn try_rotate_segment(&self, expected_current: &Arc<S>, new_segment: S) -> bool;

    /// Find the segment containing the given index
    fn find_segment_for_read(&self, index: u32) -> Option<Arc<S>>;

    /// Get the next available index (atomically if possible)
    fn next_index(&self) -> u32;
}

/// Atomic implementation of LogState using Arc and atomics for thread safety
pub struct AtomicLogState<D: Data, S>
where
    S: LogSegmentWriter<D> + LogSegmentReader<D>,
{
    /// Active segment protected by RwLock for rotation
    active: RwLock<Arc<S>>,
    /// Sealed segments protected by RwLock
    sealed: RwLock<Vec<Arc<S>>>,
    /// Phantom data for D
    _phantom: PhantomData<D>,
}

impl<D: Data, S> AtomicLogState<D, S>
where
    S: LogSegmentWriter<D> + LogSegmentReader<D>,
{
    pub fn new(initial_segment: S) -> Self {
        Self {
            active: RwLock::new(Arc::new(initial_segment)),
            sealed: RwLock::new(Vec::new()),
            _phantom: PhantomData,
        }
    }
}

impl<D: Data, S> LogState<D, S> for AtomicLogState<D, S>
where
    S: LogSegmentWriter<D> + LogSegmentReader<D>,
{
    fn active_segment(&self) -> Arc<S> {
        Arc::clone(&*self.active.read())
    }

    fn sealed_segments(&self) -> Vec<Arc<S>> {
        self.sealed.read().clone()
    }

    fn try_rotate_segment(&self, expected_current: &Arc<S>, new_segment: S) -> bool {
        let mut active = self.active.write();

        // Check if still the same segment (another thread might have rotated)
        if Arc::ptr_eq(&*active, expected_current) {
            let old_active = std::mem::replace(&mut *active, Arc::new(new_segment));
            self.sealed.write().push(old_active);
            true
        } else {
            false
        }
    }

    fn find_segment_for_read(&self, index: u32) -> Option<Arc<S>> {
        // Check sealed segments first
        for seg in self.sealed.read().iter() {
            if !LogSegmentReader::is_empty(seg.as_ref())
                && index >= LogSegmentReader::min_index(seg.as_ref())
                && index <= LogSegmentReader::max_index(seg.as_ref()) {
                return Some(Arc::clone(seg));
            }
        }

        // Check active segment
        let active = self.active_segment();
        if !LogSegmentReader::is_empty(active.as_ref())
            && index >= LogSegmentReader::min_index(active.as_ref())
            && index <= LogSegmentReader::max_index(active.as_ref()) {
            return Some(active);
        }

        None
    }

    fn next_index(&self) -> u32 {
        // For now, get from active segment
        // Can be optimized with AtomicU32 if needed
        let active = self.active.read();
        if LogSegmentReader::is_empty(active.as_ref()) {
            LogSegmentWriter::min_index(active.as_ref())
        } else {
            LogSegmentWriter::max_index(active.as_ref()) + 1
        }
    }
}

/// Log - contains all business logic for managing segments
/// Generic over segment implementation and state management
pub struct Log<D: Data, S, State>
where
    S: LogSegmentWriter<D> + LogSegmentReader<D>,
    State: LogState<D, S>,
{
    state: State,
    _phantom: PhantomData<(D, S)>,
}

impl<D: Data + Clone, S, State> Log<D, S, State>
where
    S: LogSegmentWriter<D> + LogSegmentReader<D>,
    State: LogState<D, S>,
{
    /// Create a new log with the given state
    pub fn new(state: State) -> Self {
        Self {
            state,
            _phantom: PhantomData,
        }
    }

    /// Append a log value and return its assigned global index
    pub async fn append(&self, log_value: LogValueDeserialized<D>, new_segment_fn: impl Fn(u32) -> S) -> Result<u32, LogError> {
        loop {
            // Get active segment (cheap Arc clone)
            let active_segment = self.state.active_segment();

            // Try to append without holding any locks
            let result = active_segment.append(log_value.clone()).await;

            match result {
                Ok(index) => {
                    return Ok(index);
                }
                Err(LogSegmentError::WriteError(_)) => {
                    // Segment is full - try to rotate
                    let next_index = self.state.next_index();
                    let new_segment = new_segment_fn(next_index);

                    // Try to rotate (atomically checks if still the same segment)
                    self.state.try_rotate_segment(&active_segment, new_segment);

                    // Loop will retry with (potentially) new active segment
                }
                Err(e) => return Err(LogError::SegmentError(e)),
            }
        }
    }

    /// Get a log value by its global index
    pub async fn get(&self, index: u32) -> Result<LogValueDeserialized<D>, LogError> {
        // Find the segment (uses state's optimized search)
        let segment = self.state.find_segment_for_read(index)
            .ok_or(LogError::IndexNotFound(index))?;

        // Read from segment without holding any log-level locks
        segment.get(index).await.map_err(LogError::SegmentError)
    }

    /// Get the minimum index in the log
    pub fn min_index(&self) -> Option<u32> {
        // Check sealed segments first
        let sealed = self.state.sealed_segments();
        if let Some(first) = sealed.first() {
            return Some(LogSegmentReader::min_index(first.as_ref()));
        }

        // Check active segment
        let active = self.state.active_segment();
        if !LogSegmentReader::is_empty(active.as_ref()) {
            return Some(LogSegmentReader::min_index(active.as_ref()));
        }

        None
    }

    /// Get the maximum index in the log
    pub fn max_index(&self) -> Option<u32> {
        let active = self.state.active_segment();

        // Check active segment first
        if !LogSegmentReader::is_empty(active.as_ref()) {
            return Some(LogSegmentReader::max_index(active.as_ref()));
        }

        // Fall back to last sealed segment
        self.state.sealed_segments().last().map(|s| LogSegmentReader::max_index(s.as_ref()))
    }
}

// ============================================================================
// Memory Log Implementation
// ============================================================================

use crate::log_segment::ActiveMemoryLogSegment;

/// Type alias for a memory-backed log with atomic state management
pub type MemoryLog<D> = Log<D, ActiveMemoryLogSegment<D>, AtomicLogState<D, ActiveMemoryLogSegment<D>>>;

// Helper functions for MemoryLog
pub fn new_memory_log<D: Data + Clone>() -> MemoryLog<D> {
    let state = AtomicLogState::new(ActiveMemoryLogSegment::new(0));
    Log::new(state)
}

pub async fn append_to_memory_log<D: Data + Clone>(
    log: &MemoryLog<D>,
    log_value: LogValueDeserialized<D>,
) -> Result<u32, LogError> {
    log.append(log_value, |start_index| ActiveMemoryLogSegment::new(start_index)).await
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
    async fn test_log_append_single_entry() {
        let log = new_memory_log::<TestData>();

        let entry = LogValueDeserialized {
            key: 123,
            data: TestData {
                value: 42,
                name: "test".to_string(),
            },
            metadata: vec![],
        };

        let index = append_to_memory_log(&log, entry).await.unwrap();
        assert_eq!(index, 0);
        assert_eq!(log.min_index(), Some(0));
        assert_eq!(log.max_index(), Some(0));
    }

    #[tokio::test]
    async fn test_log_append_multiple_entries() {
        let log = new_memory_log::<TestData>();

        for i in 0..10 {
            let entry = LogValueDeserialized {
                key: i as u128,
                data: TestData {
                    value: i as i32,
                    name: format!("entry_{}", i),
                },
                metadata: vec![],
            };

            let index = append_to_memory_log(&log, entry).await.unwrap();
            assert_eq!(index, i);
        }

        assert_eq!(log.min_index(), Some(0));
        assert_eq!(log.max_index(), Some(9));
    }

    #[tokio::test]
    async fn test_log_read_from_active_segment() {
        let log = new_memory_log::<TestData>();

        let entry = LogValueDeserialized {
            key: 456,
            data: TestData {
                value: 99,
                name: "active".to_string(),
            },
            metadata: vec![],
        };

        append_to_memory_log(&log, entry).await.unwrap();

        let retrieved = log.get(0).await.unwrap();
        assert_eq!(retrieved.key, 456);
        assert_eq!(retrieved.data.value, 99);
        assert_eq!(retrieved.data.name, "active");
    }

    #[tokio::test]
    async fn test_log_read_multiple_entries() {
        let log = new_memory_log::<TestData>();

        // Write multiple entries
        for i in 0..5 {
            let entry = LogValueDeserialized {
                key: i as u128,
                data: TestData {
                    value: (i * 10) as i32,
                    name: format!("entry_{}", i),
                },
                metadata: vec![],
            };
            append_to_memory_log(&log, entry).await.unwrap();
        }

        // Read them back
        for i in 0..5 {
            let retrieved = log.get(i).await.unwrap();
            assert_eq!(retrieved.key, i as u128);
            assert_eq!(retrieved.data.value, (i * 10) as i32);
            assert_eq!(retrieved.data.name, format!("entry_{}", i));
        }
    }

    #[tokio::test]
    async fn test_log_empty_read() {
        let log = new_memory_log::<TestData>();

        let result = log.get(0).await;
        assert!(result.is_err());
        match result {
            Err(LogError::IndexNotFound(0)) => {},
            _ => panic!("Expected IndexNotFound error"),
        }
    }

    #[tokio::test]
    async fn test_log_out_of_bounds() {
        let log = new_memory_log::<TestData>();

        let entry = LogValueDeserialized {
            key: 1,
            data: TestData {
                value: 1,
                name: "test".to_string(),
            },
            metadata: vec![],
        };

        append_to_memory_log(&log, entry).await.unwrap();

        // Try to read beyond what exists
        let result = log.get(100).await;
        assert!(result.is_err());
        match result {
            Err(LogError::IndexNotFound(100)) => {},
            _ => panic!("Expected IndexNotFound error"),
        }
    }

    #[tokio::test]
    async fn test_log_segment_rotation() {
        let log = new_memory_log::<TestData>();

        // Create large entries to force segment rotation
        let large_string = "x".repeat(10_000); // 10KB per entry

        let mut indices = vec![];
        for i in 0..150 {  // This should trigger at least one rotation
            let entry = LogValueDeserialized {
                key: i as u128,
                data: TestData {
                    value: i as i32,
                    name: large_string.clone(),
                },
                metadata: vec![],
            };

            let index = append_to_memory_log(&log, entry).await.unwrap();
            indices.push(index);
        }

        // Verify indices are sequential
        for (i, &index) in indices.iter().enumerate() {
            assert_eq!(index, i as u32);
        }

        // Verify min/max
        assert_eq!(log.min_index(), Some(0));
        assert_eq!(log.max_index(), Some(149));

        // Verify we can read across segments
        for i in 0..150 {
            let retrieved = log.get(i).await.unwrap();
            assert_eq!(retrieved.key, i as u128);
            assert_eq!(retrieved.data.value, i as i32);
        }
    }

    #[tokio::test]
    async fn test_log_read_from_sealed_segment() {
        let log = new_memory_log::<TestData>();

        // Fill up first segment to force rotation
        let large_string = "x".repeat(10_000);

        for i in 0..120 {
            let entry = LogValueDeserialized {
                key: i as u128,
                data: TestData {
                    value: i as i32,
                    name: large_string.clone(),
                },
                metadata: vec![],
            };
            append_to_memory_log(&log, entry).await.unwrap();
        }

        // Now read from what should be a sealed segment (early indices)
        let retrieved = log.get(0).await.unwrap();
        assert_eq!(retrieved.key, 0);
        assert_eq!(retrieved.data.value, 0);

        let retrieved = log.get(50).await.unwrap();
        assert_eq!(retrieved.key, 50);
        assert_eq!(retrieved.data.value, 50);
    }

    #[tokio::test]
    async fn test_log_read_across_segments() {
        let log = new_memory_log::<TestData>();

        let large_string = "x".repeat(10_000);

        // Write enough to span multiple segments
        for i in 0..200 {
            let entry = LogValueDeserialized {
                key: i as u128,
                data: TestData {
                    value: i as i32,
                    name: large_string.clone(),
                },
                metadata: vec![],
            };
            append_to_memory_log(&log, entry).await.unwrap();
        }

        // Read from beginning (sealed segment)
        let retrieved = log.get(0).await.unwrap();
        assert_eq!(retrieved.data.value, 0);

        // Read from middle (potentially another sealed segment)
        let retrieved = log.get(100).await.unwrap();
        assert_eq!(retrieved.data.value, 100);

        // Read from end (active segment)
        let retrieved = log.get(199).await.unwrap();
        assert_eq!(retrieved.data.value, 199);
    }
}
