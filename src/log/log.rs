use async_trait::async_trait;
use tokio::sync::{Mutex, RwLock};
use uuid::Uuid;
use anyhow::{Result, anyhow};
use std::collections::HashMap;
use std::sync::{Mutex, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone)]
#[repr(C)]
pub struct LeaseDeadLine {
    lease_id: Uuid,
    deadline: u64,
}

enum LogRecordType {
    Value,
    MetaData,
}

pub struct Metadata {
    log_id: str,
}

#[derive(Debug, Clone)]
#[repr(C)]
pub struct LogResult {
    record_type: LogRecordType,
    metadata: Option<Metadata>,
    value_payload: Option<Box<[u8]>>,
}

#[derive(Debug, Clone)]
#[repr(C)]
pub struct LogPosition {
    position: u64,
    rotation: Uuid,
}

#[async_trait]
pub trait LogRing {
    async fn create_log(&self, log_id: &str, size: u64) -> Result<()>;
    async fn try_take_lease(&self, log_id: &str) -> Result<LeaseDeadLine>;
    async fn renew_lease(&self, log_id: &str, lead_uuid: Uuid) -> u64;
    async fn write_log(&self, log_id: &str, lease_uuid: Uuid, payload: &[u8]) -> Result<LogPosition>;
    async fn read(&self, log_id: &str, position: &LogPosition) -> Result<Option<LogResult>>;
    async fn read_next_after(&self, log_id: &str, position: &LogPosition) -> Result<Option<LogResult>>;
    async fn head_pos(&self, log_id: &str) -> LogPosition;
    async fn list_logs(&self, log_id: &str) -> Vec<str>;
}

// InMemoryLogRing temp structure to provide functionality for distributed log ring.
pub struct InMemoryLogRing {
    logs: RwLock<HashMap<String, Mutex<RingBuffer>>>,
}

impl InMemoryLogRing {
    pub fn new() -> Self {
        Self {
            logs: RwLock::new(HashMap::new()),
        }
    }
}

#[async_trait]
impl LogRing for InMemoryLogRing {
    async fn create_log(&self, log_id: &str, size: u64) -> Result<()> {
        let mut logs = self.logs.write().await;
        if logs.contains_key(log_id) {
            return Err(anyhow!("Log already exists"));
        }
        logs.insert(log_id.to_string(), Mutex::new(RingBuffer::new(size)));
        Ok(())
    }

    async fn try_take_lease(&self, log_id: &str) -> Result<LeaseDeadLine> {
        let logs = self.logs.read().await;
        let log = logs.get(log_id).ok_or_else(|| anyhow!("Log not found"))?;
        let mut log = log.lock().await;
        log.try_take_lease()
    }

    async fn renew_lease(&self, log_id: &str, lead_uuid: Uuid) -> u64 {
        let logs = self.logs.read().await;
        let log = logs.get(log_id).expect("Log not found");
        let mut log = log.lock().await;
        log.renew_lease(lead_uuid)
    }

    async fn write_log(&self, log_id: &str, lease_uuid: Uuid, payload: &[u8]) -> Result<LogPosition> {
        let logs = self.logs.read().await;
        let log = logs.get(log_id).ok_or_else(|| anyhow!("Log not found"))?;
        let mut log = log.lock().await;
        log.write(lease_uuid, payload)
    }

    async fn read(&self, log_id: &str, position: &LogPosition) -> Result<Option<LogResult>> {
        let logs = self.logs.read().await;
        let log = logs.get(log_id).ok_or_else(|| anyhow!("Log not found"))?;
        let log = log.lock().await;
        Ok(log.read(position))
    }

    async fn head_pos(&self, log_id: &str) -> LogPosition {
        let logs = self.logs.read().await;
        let log = logs.get(log_id).expect("Log not found");
        let log = log.lock().await;
        log.head_pos()
    }

    async fn list_logs(&self, log_id: &str) -> Vec<str> {
        todo!()
    }
}

struct RingBuffer {
    buffer: Vec<Option<Vec<u8>>>,
    capacity: u64,
    head: u64,
    tail: u64,
    rotation: Uuid,
    lease: Option<LeaseDeadLine>,
}

impl RingBuffer {
    fn new(size: u64) -> Self {
        Self {
            buffer: vec![None; size as usize],
            capacity: size,
            head: 0,
            tail: 0,
            rotation: Uuid::new_v4(),
            lease: None,
        }
    }

    fn write(&mut self, lease_uuid: Uuid, payload: &[u8]) -> Result<LogPosition> {
        if let Some(lease) = &self.lease {
            if lease.lease_id != lease_uuid {
                return Err(anyhow!("Lease mismatch: Write not allowed"));
            }
        } else {
            return Err(anyhow!("No active lease"));
        }

        if (self.tail + 1) % self.capacity == self.head {
            self.head = (self.head + 1) % self.capacity; // Overwrite oldest entry
        }

        self.buffer[self.tail as usize] = Some(payload.to_vec());
        let position = LogPosition {
            position: self.tail,
            rotation: self.rotation,
        };
        self.tail = (self.tail + 1) % self.capacity;

        Ok(position)
    }

    fn read(&self, position: &LogPosition) -> Option<LogResult> {
        if position.rotation != self.rotation {
            return None; // Rotation mismatch, log position invalid
        }

        self.buffer
            .get(position.position as usize)
            .and_then(|opt| opt.as_ref().map(|data| LogResult {
                payload: data.clone().into_boxed_slice(),
            }))
    }

    fn renew_lease(&mut self, lease_uuid: Uuid) -> u64 {
        let deadline = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 10;
        self.lease = Some(LeaseDeadLine {
            lease_id: lease_uuid,
            deadline,
        });
        deadline
    }

    fn try_take_lease(&mut self) -> Result<LeaseDeadLine> {
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
        if let Some(lease) = &self.lease {
            if lease.deadline > now {
                return Err(anyhow!("Lease is currently held"));
            }
        }
        let new_lease = LeaseDeadLine {
            lease_id: Uuid::new_v4(),
            deadline: now + 10,
        };
        self.lease = Some(new_lease.clone());
        Ok(new_lease)
    }

    fn head_pos(&self) -> LogPosition {
        LogPosition {
            position: self.head,
            rotation: self.rotation,
        }
    }
}
