use foundationdb::{Database, Transaction};
use foundationdb::tuple::{pack, unpack};
use foundationdb::future::FdbValue;
use foundationdb::options::{MutationType};
use std::time::{SystemTime, UNIX_EPOCH};
use anyhow::{Result, anyhow};
use serde::{Serialize, Deserialize};

const LEASE_PREFIX: &[u8] = b"lease/";
const LOG_PREFIX: &[u8] = b"log/";

#[derive(Serialize, Deserialize, Debug)]
struct LeaseMeta {
    leader_id: String,
    expires_at: u64,
    fence: u64,
}

fn current_unix_ts_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64
}

pub async fn acquire_lease(db: &Database, key: &str, node_id: &str, ttl_ms: u64) -> Result<u64> {
    let lease_key = [LEASE_PREFIX, key.as_bytes()].concat();
    let now = current_unix_ts_ms();

    db.transact_boxed(|tr| {
        let lease_key = lease_key.clone();
        let node_id = node_id.to_string();
        Box::pin(async move {
            let existing = tr.get(&lease_key, false).await?;
            let grant = match existing {
                Some(val) => {
                    let meta: LeaseMeta = serde_json::from_slice(&val)?;
                    if meta.expires_at > now {
                        return Err(anyhow!("Lease still valid, held by {}", meta.leader_id));
                    }
                    true
                },
                None => true
            };

            if grant {
                let fence = tr.get_read_version().await?;
                let meta = LeaseMeta {
                    leader_id: node_id,
                    expires_at: now + ttl_ms,
                    fence,
                };
                tr.set(&lease_key, serde_json::to_vec(&meta)?.as_slice());
                Ok(fence)
            } else {
                Err(anyhow!("Lease not granted"))
            }
        })
    }).await
}

pub async fn write_log(db: &Database, key: &str, fence: u64, payload: &[u8]) -> Result<()> {
    let log_key = [LOG_PREFIX, key.as_bytes()].concat();

    db.transact_boxed(|tr| {
        let log_key = log_key.clone();
        Box::pin(async move {
            let lease_key = [LEASE_PREFIX, key.as_bytes()].concat();
            let meta_bytes = tr.get(&lease_key, true).await?.ok_or_else(|| anyhow!("No lease found"))?;
            let meta: LeaseMeta = serde_json::from_slice(&meta_bytes)?;
            if meta.fence != fence {
                return Err(anyhow!("Fencing token mismatch"));
            }
            tr.set(&log_key, payload);
            Ok(())
        })
    }).await
}

// Expose via any API layer (Actix, Axum, etc.)
// Example API function
pub async fn create_log_entry(db: &Database, key: &str, node_id: &str, ttl_ms: u64, payload: &[u8]) -> Result<()> {
    let fence = acquire_lease(db, key, node_id, ttl_ms).await?;
    write_log(db, key, fence, payload).await
}
