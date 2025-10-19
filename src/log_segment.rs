
use std::{io::Write, marker::PhantomData};
use anyhow::{Result, Context, bail};
use async_trait::async_trait;
use crc_fast::{Digest, CrcAlgorithm::Crc32IsoHdlc};
use crate::log_value::{LogValue, ArchivedLogValueView};

/// High-level async trait for writing log entries to an active segment
#[async_trait]
pub trait LogSegmentWriter: Send {
    /// Append a log value and return its assigned index
    async fn append(&mut self, log_value: LogValue) -> Result<u32, LogSegmentError>;

    /// Seal the segment, making it immutable and ready for reads
    async fn seal(self) -> Result<(), LogSegmentError>;

    fn min_index(&self) -> u32;
    fn max_index(&self) -> u32;
}

/// High-level async trait for reading log entries from a sealed segment
#[async_trait]
pub trait LogSegmentReader: Send + Sync {
    /// Get a log value by its index (zero-copy from in-memory buffer)
    async fn get(&self, index: u32) -> Result<LogEntryView<'_>, LogSegmentError>;

    fn min_index(&self) -> u32;
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
}

/// Sealed (read-only) log segment
/// Data is loaded into memory on open, reads are zero-copy from memory buffer
pub struct SealedLogSegment<D: LogSegmentDataStore, I: LogSegmentIndexStore> {
    data_store: D,
    index_store: I,
    min_index: u32,
    max_index: u32,
}

impl<D: LogSegmentDataStore, I: LogSegmentIndexStore> SealedLogSegment<D, I> {
    pub fn new(data_store: D, index_store: I) -> Self {
        let min_index = index_store.min_index();
        let max_index = index_store.max_index();
        Self {
            data_store,
            index_store,
            min_index,
            max_index,
        }
    }
}

pub struct ActiveLogSegment<D: LogSegmentDataStore, I: LogSegmentIndexStore> {
    data_store: D,
    index_store: I,
    next_index: u32,
    min_index: u32,
}

impl<D: LogSegmentDataStore, I: LogSegmentIndexStore> ActiveLogSegment<D, I> {
    pub fn new(data_store: D, index_store: I, min_index: u32) -> Self {
        Self {
            data_store,
            index_store,
            next_index: min_index,
            min_index,
        }
    }
}

#[async_trait]
impl<D: LogSegmentDataStore, I: LogSegmentIndexStore> LogSegmentWriter for ActiveLogSegment<D, I> {
    async fn append(&mut self, log_value: LogValue) -> Result<u32, LogSegmentError> {
        // Build log entry with CRC validation
        let entry = LogEntry::from_log_value(&log_value)
            .map_err(|e| LogSegmentError::WriteError(format!("{}", e)))?;
        let entry_bytes = entry.build();

        // Write to data store (O_DIRECT)
        let (offset, _) = self.data_store.append(&entry_bytes).await?;

        // Persist index mapping before making visible
        let index = self.next_index;
        self.index_store.write_offset(index, offset).await?;
        self.index_store.flush().await?;

        // TODO commit to internal pre-parsed data structure. 

        self.next_index += 1;
        Ok(index)
    }

    async fn seal(mut self) -> Result<(), LogSegmentError> {
        self.data_store.flush().await?;
        self.index_store.flush().await?;
        Ok(())
    }

    fn min_index(&self) -> u32 {
        self.min_index
    }

    fn max_index(&self) -> u32 {
        self.next_index.saturating_sub(1)
    }
}

#[async_trait]
impl<D: LogSegmentDataStore, I: LogSegmentIndexStore> LogSegmentReader for SealedLogSegment<D, I> {
    async fn get(&self, index: u32) -> Result<LogEntryView<'_>, LogSegmentError> {
        if index < self.min_index || index > self.max_index {
            return Err(LogSegmentError::IndexOutOfBounds {
                index,
                min: self.min_index,
                max: self.max_index,
            });
        }

        // Get offset from in-memory index buffer
        let offset = self.index_store.get_offset(index)
            .ok_or_else(|| LogSegmentError::IndexOutOfBounds {
                index,
                min: self.min_index,
                max: self.max_index,
            })?;

        // Zero-copy read from in-memory data buffer
        let data = self.data_store.get_at_offset(offset).await?;
        LogEntryView::new(data).map_err(LogSegmentError::ReadError)
    }

    fn min_index(&self) -> u32 {
        self.min_index
    }

    fn max_index(&self) -> u32 {
        self.max_index
    }
}


/// Low-level async trait for the data file backing
/// Contains the actual log entries in sequential format with headers
#[async_trait]
pub trait LogSegmentDataStore: Send + Sync {
    /// Get entry data at a specific byte offset (zero-copy)
    async fn get_at_offset(&self, offset: u32) -> Result<&[u8]>;

    /// Append an entry to disk via O_DIRECT
    /// Returns (offset, byte_length) of the written entry
    async fn append(&mut self, entry: &[u8]) -> Result<(u32, u32)>;

    /// Flush buffered writes to disk
    async fn flush(&mut self) -> Result<()>;
}

/// Low-level async trait for the index file
/// Maps logical index -> byte offset in data file
/// Uses 1MB arena-allocated in-memory buffer for reads
#[async_trait]
pub trait LogSegmentIndexStore: Send + Sync {
    /// Write an offset mapping for a given index
    /// Must be persisted before making the entry visible to reads
    async fn write_offset(&mut self, index: u32, offset: u32) -> Result<()>;

    /// Get the byte offset for a logical index from in-memory buffer
    fn get_offset(&self, index: u32) -> Option<u32>;

    /// Get the current maximum index
    fn max_index(&self) -> u32;

    /// Get the minimum index
    fn min_index(&self) -> u32;

    /// Flush index writes to disk
    async fn flush(&mut self) -> Result<()>;
}

/// Custom errors for log entry validation
#[derive(Debug, thiserror::Error)]
pub enum LogEntryError {
    #[error("Entry too small: expected at least {expected} bytes, got {actual}")]
    TooSmall { expected: usize, actual: usize },

    #[error("Invalid byte length: {0}")]
    InvalidByteLength(usize),

    #[error("Header CRC mismatch: expected {expected:#x}, got {actual:#x}")]
    HeaderCrcMismatch { expected: u32, actual: u32 },

    #[error("Message CRC mismatch: expected {expected:#x}, got {actual:#x}")]
    MessageCrcMismatch { expected: u32, actual: u32 },

    #[error("Serialization error: {0}")]
    SerializationError(String),
}


// Raw header format on disk
#[repr(C)]
#[derive(Clone, Copy)]
struct LogEntryHeader {
    byte_length: u32,
    version: u32,
    header_crc: u32,
    message_crc: u32,
}

impl LogEntryHeader {
    const SIZE: usize = std::mem::size_of::<Self>();

    /// Calculate CRC32 of header (excluding the header_crc field itself)
    fn calculate_header_crc(&self) -> u32 {
        let mut digest = Digest::new(Crc32IsoHdlc);
        digest.write(&self.byte_length.to_le_bytes());
        digest.write(&self.version.to_le_bytes());
        digest.write(&self.message_crc.to_le_bytes());
        digest.finalize() as u32
    }

    /// Calculate CRC32C of message data
    fn calculate_message_crc(data: &[u8]) -> u32 {
        let mut digest = Digest::new(Crc32IsoHdlc);
        digest.write(data);
        digest.finalize() as u32
    }
}

/// Builder for creating log entries with proper CRC validation
pub struct LogEntry {
    message: Vec<u8>,
}

impl LogEntry {
    /// Create a new log entry from a LogValue
    pub fn from_log_value(log_value: &LogValue) -> Result<Self, LogEntryError> {
        let message = rkyv::to_bytes::<rkyv::rancor::Error>(log_value)
            .map_err(|e| LogEntryError::SerializationError(format!("{}", e)))?
            .to_vec();
        Ok(Self { message })
    }

    /// Create a new log entry from raw message bytes
    pub fn from_bytes(message: Vec<u8>) -> Self {
        Self { message }
    }

    /// Build the complete entry with header and CRCs
    pub fn build(&self) -> Vec<u8> {
        let message_crc = LogEntryHeader::calculate_message_crc(&self.message);
        let byte_length = (LogEntryHeader::SIZE + self.message.len()) as u32;

        let mut header = LogEntryHeader {
            byte_length,
            version: 1,
            header_crc: 0,
            message_crc,
        };
        header.header_crc = header.calculate_header_crc();

        let mut entry = Vec::with_capacity(byte_length as usize);
        // Write header as raw bytes
        unsafe {
            let header_bytes = std::slice::from_raw_parts(
                &header as *const _ as *const u8,
                LogEntryHeader::SIZE,
            );
            entry.extend_from_slice(header_bytes);
        }
        entry.extend_from_slice(&self.message);
        entry
    }

    /// Get the message size
    pub fn message_len(&self) -> usize {
        self.message.len()
    }

    /// Get the total entry size (header + message)
    pub fn total_len(&self) -> usize {
        LogEntryHeader::SIZE + self.message.len()
    }
}


// To be used for sealed logs that are not
// actively being written to. 
pub struct LogEntryView<'a> {
    data: &'a [u8],
    _phantom: PhantomData<&'a LogEntryHeader>,
}

// Zero copy view into log entry.
impl<'a> LogEntryView<'a> {

    pub fn new(data: &'a [u8]) -> Result<Self> {
        // Validate minimum size
        if data.len() < LogEntryHeader::SIZE {
            bail!(LogEntryError::TooSmall {
                expected: LogEntryHeader::SIZE,
                actual: data.len()
            });
        }

        let byte_length = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;
        if byte_length < LogEntryHeader::SIZE {
            bail!(LogEntryError::InvalidByteLength(byte_length));
        }
        if data.len() < byte_length {
            bail!(LogEntryError::TooSmall {
                expected: byte_length,
                actual: data.len()
            });
        }

        // Create view with validated data
        let view = LogEntryView {
            data: &data[..byte_length],
            _phantom: PhantomData,
        };

        // Verify header CRC
        let header = view.header();
        let calculated_header_crc = header.calculate_header_crc();
        if calculated_header_crc != header.header_crc {
            bail!(LogEntryError::HeaderCrcMismatch {
                expected: header.header_crc,
                actual: calculated_header_crc
            });
        }

        // Verify message CRC
        let message = view.message();
        let calculated_message_crc = LogEntryHeader::calculate_message_crc(message);
        if calculated_message_crc != header.message_crc {
            bail!(LogEntryError::MessageCrcMismatch {
                expected: header.message_crc,
                actual: calculated_message_crc
            });
        }

        Ok(view)
    }

    /// Get header as a reference (zero-copy)
    fn header(&self) -> &'a LogEntryHeader {
        unsafe { &*(self.data.as_ptr() as *const LogEntryHeader) }
    }

    pub fn byte_length(&self) -> u32 {
        self.header().byte_length
    }

    pub fn version(&self) -> u32 {
        self.header().version
    }

    pub fn header_crc(&self) -> u32 {
        self.header().header_crc
    }

    pub fn message_crc(&self) -> u32 {
        self.header().message_crc
    }

    /// Get message portion (zero-copy)
    pub fn message(&self) -> &'a [u8] {
        &self.data[LogEntryHeader::SIZE..]
    }

    /// Get the entire raw bytes
    pub fn as_bytes(&self) -> &'a [u8] {
        self.data
    }

    /// Parse message as archived LogValue (zero-copy)
    pub fn as_log_value(&self) -> Result<ArchivedLogValueView<'a>> {
        ArchivedLogValueView::new(self.message())
            .context("Failed to parse message as LogValue")
    }
}

pub struct LogEntryIter<'a> {
    data: &'a [u8],
    offset: usize,
}

impl<'a> Iterator for LogEntryIter<'a> {
    type Item = Result<LogEntryView<'a>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.offset >= self.data.len() {
            return None;
        }

        match LogEntryView::new(&self.data[self.offset..]) {
            Ok(entry) => {
                let entry_len = entry.byte_length() as usize;
                self.offset += entry_len;
                Some(Ok(entry))
            }
            Err(e) => Some(Err(e)),
        }
    }
}
