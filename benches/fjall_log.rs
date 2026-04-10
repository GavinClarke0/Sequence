use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use sequence::{FjallDatabase, FjallQueue, Queueable};
use std::sync::Arc;
use tempfile::TempDir;

#[derive(Debug, Clone)]
struct BenchmarkData {
    value: i64,
    name: String,
    timestamp: u64,
}

impl Queueable for BenchmarkData {
    fn serialize(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(8 + 8 + self.name.len());
        bytes.extend_from_slice(&self.value.to_le_bytes());
        bytes.extend_from_slice(&self.timestamp.to_le_bytes());
        bytes.extend_from_slice(self.name.as_bytes());
        bytes
    }

    fn deserialize(bytes: &[u8]) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let value = i64::from_le_bytes(bytes[..8].try_into()?);
        let timestamp = u64::from_le_bytes(bytes[8..16].try_into()?);
        let name = String::from_utf8(bytes[16..].to_vec())?;
        Ok(BenchmarkData { value, name, timestamp })
    }
}

fn make_queue() -> (Arc<FjallQueue<BenchmarkData>>, TempDir) {
    let dir = TempDir::new().unwrap();
    let db = FjallDatabase::new(dir.path().to_path_buf()).unwrap();
    let queue = Arc::new(db.open_queue("bench").unwrap());
    (queue, dir)
}

fn item(i: usize) -> BenchmarkData {
    BenchmarkData {
        value: i as i64,
        name: format!("entry_{}", i),
        timestamp: i as u64,
    }
}

fn sequential_writes_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("fjall_sequential_writes");

    for &write_count in [100usize, 1000, 5000].iter() {
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{}_writes", write_count)),
            &write_count,
            |b, &n| {
                b.to_async(tokio::runtime::Runtime::new().unwrap()).iter(|| async {
                    let (queue, _dir) = make_queue();
                    for i in 0..n {
                        black_box(queue.append(&item(i)).unwrap());
                    }
                });
            },
        );
    }

    group.finish();
}

fn sequential_reads_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("fjall_sequential_reads");

    for &entry_count in [100usize, 1000, 5000].iter() {
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{}_reads", entry_count)),
            &entry_count,
            |b, &n| {
                b.to_async(tokio::runtime::Runtime::new().unwrap()).iter(|| async {
                    let (queue, _dir) = make_queue();
                    for i in 0..n {
                        queue.append(&item(i)).unwrap();
                    }
                    let batch = queue.fetch_batch(0, n).unwrap();
                    for msg in batch {
                        black_box(msg);
                    }
                });
            },
        );
    }

    group.finish();
}

fn concurrent_writes_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("fjall_concurrent_writes");

    for &num_writers in [2usize, 4, 8].iter() {
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{}_writers", num_writers)),
            &num_writers,
            |b, &num_writers| {
                b.to_async(tokio::runtime::Runtime::new().unwrap()).iter(|| async {
                    let (queue, _dir) = make_queue();
                    let writes_per_writer = 100;
                    let mut handles = vec![];

                    for writer_id in 0..num_writers {
                        let q = Arc::clone(&queue);
                        handles.push(tokio::spawn(async move {
                            for i in 0..writes_per_writer {
                                q.append(&BenchmarkData {
                                    value: i as i64,
                                    name: format!("w{}_e{}", writer_id, i),
                                    timestamp: i as u64,
                                })
                                .unwrap();
                            }
                        }));
                    }

                    for h in handles {
                        let _ = h.await;
                    }
                });
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    sequential_writes_benchmark,
    sequential_reads_benchmark,
    concurrent_writes_benchmark,
);
criterion_main!(benches);
