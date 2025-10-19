use rkyv::{Archive, Deserialize, Serialize, bytecheck};
use rkyv::string::ArchivedString;
use rkyv::vec::ArchivedVec;
use anyhow::Result;

pub type LogValueKey = u128;

/// Zero-copy serializable log value with rkyv
/// - Key: up to 128 bits (u128)
/// - Data: dynamic byte array
/// - Metadata: array of string-string pairs
#[derive(Archive, Deserialize, Serialize, Debug, PartialEq)]
#[archive_attr(derive(bytecheck::CheckBytes))]
pub struct LogValue {
    /// 128-bit key
    pub key: LogValueKey,
    /// Dynamic data payload
    pub data: Vec<u8>,
    /// Metadata as key-value pairs
    pub metadata: Vec<(String, String)>,
}

impl LogValue {
    pub fn new(key: u128, data: Vec<u8>, metadata: Vec<(String, String)>) -> Self {
        LogValue { key, data, metadata }
    }
}

/// Zero-copy view into an archived LogValue
pub struct ArchivedLogValueView<'a> {
    archived: &'a ArchivedLogValue,
}

impl<'a> ArchivedLogValueView<'a> {
    /// Create a zero-copy view from archived bytes
    ///
    /// # Safety
    /// The bytes must be properly aligned and contain valid archived data
    pub fn new(bytes: &'a [u8]) -> Result<Self> {
        let archived = unsafe {
            rkyv::archived_root::<LogValue>(bytes)
        };
        Ok(ArchivedLogValueView { archived })
    }

    /// Get the key (zero-copy)
    pub fn key(&self) -> u128 {
        self.archived.key
    }

    /// Get the data slice (zero-copy)
    pub fn data(&self) -> &'a [u8] {
        self.archived.data.as_slice()
    }

    /// Get number of metadata entries
    pub fn metadata_len(&self) -> usize {
        self.archived.metadata.len()
    }

    /// Get a specific metadata value by key (zero-copy)
    pub fn get_metadata(&self, key: &str) -> Option<&'a str> {
        for item in self.archived.metadata.iter() {
            if item.0.as_str() == key {
                return Some(item.1.as_str());
            }
        }
        None
    }

    /// Iterator over metadata entries (zero-copy)
    pub fn metadata_iter(&self) -> impl Iterator<Item = (&'a str, &'a str)> + 'a {
        self.archived.metadata.iter()
            .map(|item| (item.0.as_str(), item.1.as_str()))
    }
}
