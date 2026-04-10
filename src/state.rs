use crate::log_value::LogData;
use std::fmt::Debug;
use std::future::Future;

/// Key for transformer state in the shared keyspace.
/// Composed of transformer ID and the source log index.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct StateKey {
    /// Transformer ID
    pub transformer_id: String,
    /// Source log index this state corresponds to
    pub index: u32,
}

impl StateKey {
    pub fn new(transformer_id: impl Into<String>, index: u32) -> Self {
        Self {
            transformer_id: transformer_id.into(),
            index,
        }
    }

    /// Convert to bytes for storage key.
    /// Format: transformer_id bytes followed by big-endian u32 index.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = self.transformer_id.as_bytes().to_vec();
        bytes.push(b':');
        bytes.extend_from_slice(&self.index.to_be_bytes());
        bytes
    }

    /// Parse from bytes
    pub fn from_bytes(bytes: &[u8]) -> anyhow::Result<Self> {
        let separator_pos = bytes
            .iter()
            .rposition(|&b| b == b':')
            .ok_or_else(|| anyhow::anyhow!("Invalid state key format: no separator"))?;

        let transformer_id = String::from_utf8(bytes[..separator_pos].to_vec())?;
        let index_bytes: [u8; 4] = bytes[separator_pos + 1..]
            .try_into()
            .map_err(|_| anyhow::anyhow!("Invalid state key format: wrong index length"))?;
        let index = u32::from_be_bytes(index_bytes);

        Ok(Self {
            transformer_id,
            index,
        })
    }
}

/// A snapshot of transformer state (serialized form for storage).
#[derive(Clone, Debug)]
pub struct StateSnapshotBytes {
    /// The serialized state data
    pub state_bytes: Vec<u8>,
    /// The log index this state was computed up to (inclusive)
    pub committed_index: u32,
}

impl StateSnapshotBytes {
    pub fn new(state_bytes: Vec<u8>, committed_index: u32) -> Self {
        Self {
            state_bytes,
            committed_index,
        }
    }

    /// Deserialize into a typed snapshot
    pub fn into_typed<S: TransformerState>(self) -> anyhow::Result<StateSnapshot<S>> {
        let state = S::from_bytes(&self.state_bytes)?;
        Ok(StateSnapshot {
            state,
            committed_index: self.committed_index,
        })
    }
}

/// A snapshot of transformer state (typed form for use in transformers).
#[derive(Clone, Debug)]
pub struct StateSnapshot<S> {
    /// The state data
    pub state: S,
    /// The log index this state was computed up to (inclusive)
    pub committed_index: u32,
}

impl<S> StateSnapshot<S> {
    pub fn new(state: S, committed_index: u32) -> Self {
        Self {
            state,
            committed_index,
        }
    }
}

impl<S: TransformerState> StateSnapshot<S> {
    /// Serialize into bytes snapshot for storage
    pub fn into_bytes(self) -> anyhow::Result<StateSnapshotBytes> {
        let state_bytes = self.state.to_bytes()?;
        Ok(StateSnapshotBytes {
            state_bytes,
            committed_index: self.committed_index,
        })
    }
}

#[derive(Debug, thiserror::Error)]
pub enum StateStoreError {
    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("Storage error: {0}")]
    Storage(String),

    #[error("State not found for key: {0:?}")]
    NotFound(StateKey),
}

/// Pluggable interface for transformer state storage.
/// All transformers share a single StateStore instance.
///
/// The runtime is responsible for:
/// - Loading snapshots on startup via `load()`
/// - Saving snapshots periodically via `save()`
/// - Tracking committed log indices with each snapshot
///
/// This trait operates on raw bytes, allowing flexibility in the
/// underlying storage mechanism.
///
/// Uses RPITIT (return position impl trait) instead of async_trait to avoid boxing.
pub trait StateStore: Send + Sync + 'static {
    /// Load a transformer's state snapshot as bytes.
    /// Returns None if no snapshot exists (transformer will start fresh).
    fn load(
        &self,
        key: &StateKey,
    ) -> impl Future<Output = Result<Option<StateSnapshotBytes>, StateStoreError>> + Send;

    /// Save a transformer's state snapshot as bytes.
    /// Called by the runtime during checkpointing.
    fn save(
        &self,
        key: &StateKey,
        snapshot: &StateSnapshotBytes,
    ) -> impl Future<Output = Result<(), StateStoreError>> + Send;

    /// Delete a transformer's state (for cleanup/migration).
    fn delete(&self, key: &StateKey) -> impl Future<Output = Result<(), StateStoreError>> + Send;
}

/// State that a transformer can maintain between invocations.
/// Must be serializable via LogData for persistence.
pub trait TransformerState: LogData + Default + Clone + Send + Sync + 'static {}

/// Blanket impl: any type meeting bounds is TransformerState
impl<T> TransformerState for T where T: LogData + Default + Clone + Send + Sync + 'static {}

/// Unit state for stateless transformers.
#[derive(Default, Clone, Debug, PartialEq)]
pub struct NoState;

impl LogData for NoState {
    fn to_bytes(&self) -> anyhow::Result<Vec<u8>> {
        Ok(vec![])
    }

    fn from_bytes(_bytes: &[u8]) -> anyhow::Result<Self> {
        Ok(NoState)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_state_key_to_bytes_roundtrip() {
        let key = StateKey::new("my_transformer", 12345);
        let bytes = key.to_bytes();
        let restored = StateKey::from_bytes(&bytes).unwrap();

        assert_eq!(restored.transformer_id, "my_transformer");
        assert_eq!(restored.index, 12345);
    }

    #[test]
    fn test_no_state_serialization() {
        let state = NoState;
        let bytes = state.to_bytes().unwrap();
        assert!(bytes.is_empty());

        let restored = NoState::from_bytes(&bytes).unwrap();
        assert_eq!(restored, NoState);
    }

    #[test]
    fn test_snapshot_bytes_roundtrip() {
        #[derive(Default, Clone, Debug, PartialEq)]
        struct TestState {
            count: u32,
        }

        impl LogData for TestState {
            fn to_bytes(&self) -> anyhow::Result<Vec<u8>> {
                Ok(self.count.to_le_bytes().to_vec())
            }

            fn from_bytes(bytes: &[u8]) -> anyhow::Result<Self> {
                let count = u32::from_le_bytes(bytes.try_into()?);
                Ok(TestState { count })
            }
        }

        let snapshot = StateSnapshot::new(TestState { count: 42 }, 100);
        let bytes_snapshot = snapshot.into_bytes().unwrap();

        assert_eq!(bytes_snapshot.committed_index, 100);

        let restored: StateSnapshot<TestState> = bytes_snapshot.into_typed().unwrap();
        assert_eq!(restored.state.count, 42);
        assert_eq!(restored.committed_index, 100);
    }
}
