use rkyv::{Archive, Deserialize, Serialize, bytecheck};
use rkyv::string::ArchivedString;
use rkyv::vec::ArchivedVec;
use anyhow::Result;
use serde::{Serialize as SerdeSerialize, Deserialize as SerdeDeserialize};

pub type LogValueKey = u128;

/// Zero-copy serializable log value with rkyv
/// - Key: up to 128 bits (u128)
/// - Data: dynamic byte array
/// - Metadata: array of string-string pairs
#[derive(Archive, Deserialize, Serialize, Debug, PartialEq)]
pub struct LogValueSerialized {
    /// 128-bit key
    pub key: LogValueKey,
    /// Dynamic data payload
    pub data: Vec<u8>,
    /// Metadata as key-value pairs
    pub metadata: Vec<(String, String)>,
}

/// Trait for data types that can be serialized/deserialized using serde
/// Supports multiple formats: JSON, Protobuf, MessagePack, etc.
pub trait Data: SerdeSerialize + for<'de> SerdeDeserialize<'de> + Send + Sync + 'static {}

/// Blanket implementation - any type meeting the serde bounds is automatically Data
impl<T> Data for T where T: SerdeSerialize + for<'de> SerdeDeserialize<'de> + Send + Sync + 'static {}

#[derive(Clone)]
pub struct LogValueDeserialized<D: Data> {
    /// 128-bit key
    pub key: LogValueKey,
    /// Dynamic data payload
    pub data: D,
    /// Metadata as key-value pairs
    pub metadata: Vec<(String, String)>,
}

impl LogValueSerialized {
    pub fn new(key: u128, data: Vec<u8>, metadata: Vec<(String, String)>) -> Self {
        LogValueSerialized { key, data, metadata }
    }

    /// Deserialize the data payload into a typed value using the specified format
    ///
    /// # Type Parameters
    /// - `D`: The target data type (must implement Data trait)
    /// - `E`: The error type from the deserializer
    /// - `F`: The serde format function to use (e.g., serde_json::from_slice)
    ///
    /// # Example
    /// ```ignore
    /// let deserialized = serialized.into_deserialized(serde_json::from_slice)?;
    /// ```
    pub fn into_deserialized<D, E, F>(
        self,
        deserialize_fn: F,
    ) -> Result<LogValueDeserialized<D>>
    where
        D: Data,
        E: std::error::Error + Send + Sync + 'static,
        F: FnOnce(&[u8]) -> std::result::Result<D, E>,
    {
        let data = deserialize_fn(&self.data)
            .map_err(|e| anyhow::anyhow!("Failed to deserialize data: {}", e))?;

        Ok(LogValueDeserialized {
            key: self.key,
            data,
            metadata: self.metadata,
        })
    }
}

impl<D: Data> LogValueDeserialized<D> {
    pub fn new(key: u128, data: D, metadata: Vec<(String, String)>) -> Self {
        LogValueDeserialized { key, data, metadata }
    }

    /// Serialize the data payload into bytes using the specified format
    ///
    /// # Type Parameters
    /// - `E`: The error type from the serializer
    /// - `F`: The serde format function to use (e.g., serde_json::to_vec)
    ///
    /// # Example
    /// ```ignore
    /// let serialized = deserialized.into_serialized(serde_json::to_vec)?;
    /// ```
    pub fn into_serialized<E, F>(
        self,
        serialize_fn: F,
    ) -> Result<LogValueSerialized>
    where
        E: std::error::Error + Send + Sync + 'static,
        F: FnOnce(&D) -> std::result::Result<Vec<u8>, E>,
    {
        let data = serialize_fn(&self.data)
            .map_err(|e| anyhow::anyhow!("Failed to serialize data: {}", e))?;

        Ok(LogValueSerialized {
            key: self.key,
            data,
            metadata: self.metadata,
        })
    }
}
