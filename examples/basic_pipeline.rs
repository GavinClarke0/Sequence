//! Basic pipeline example demonstrating the sequence library.
//!
//! Shows a producer writing to a queue and a consumer subscribing to it.
//!
//! Run with: cargo run --example basic_pipeline

use anyhow::Result;
use futures::StreamExt;
use sequence::{FjallDatabase, Message, Queueable};
use std::sync::Arc;
use tempfile::TempDir;

#[derive(Debug, Clone)]
struct Event {
    source: String,
    value: i32,
}

impl Queueable for Event {
    fn serialize(&self) -> Vec<u8> {
        serde_json::to_vec(&serde_json::json!({
            "source": self.source,
            "value": self.value,
        }))
        .unwrap()
    }

    fn deserialize(bytes: &[u8]) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let v: serde_json::Value = serde_json::from_slice(bytes)?;
        Ok(Event {
            source: v["source"].as_str().unwrap_or("").to_string(),
            value: v["value"].as_i64().unwrap_or(0) as i32,
        })
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    println!("=== Sequence Example ===\n");

    let temp_dir = TempDir::new()?;
    let db = FjallDatabase::new(temp_dir.path().to_path_buf())?;
    let queue = Arc::new(db.open_queue::<Event>("events")?);

    // Consumer: subscribe from seq 0
    let consumer_queue = Arc::clone(&queue);
    let consumer = tokio::spawn(async move {
        let mut stream = std::pin::pin!(consumer_queue.subscribe(0));
        while let Some(result) = stream.next().await {
            match result {
                Ok(Message { seq, data }) => println!("  consumed [{seq:04}] {:?}", data),
                Err(e) => { eprintln!("  error: {e}"); break; }
            }
        }
    });

    // Producer: write 5 events with small delays
    for i in 0..5 {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let seq = queue.append(&Event { source: format!("sensor_{i}"), value: i * 10 })?;
        println!("produced [{seq:04}] sensor_{i}");
    }

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    consumer.abort();

    println!("\n=== Done ===");
    Ok(())
}
