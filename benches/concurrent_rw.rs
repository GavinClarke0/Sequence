use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use sequence::{FjallDatabase, FjallQueue, Queueable};
use std::sync::Arc;
use tempfile::TempDir;

#[derive(Debug, Clone)]
struct BenchmarkData {
    value: i64,
    name: String,
}

impl Queueable for BenchmarkData {
    fn serialize(&self) -> Vec<u8> {
        let mut bytes = self.value.to_le_bytes().to_vec();
        bytes.extend_from_slice(self.name.as_bytes());
        bytes
    }

    fn deserialize(bytes: &[u8]) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let value = i64::from_le_bytes(bytes[..8].try_into()?);
        let name = String::from_utf8(bytes[8..].to_vec())?;
        Ok(BenchmarkData { value, name })
    }
}

fn open_queue(dir: &TempDir) -> Arc<FjallQueue<BenchmarkData>> {
    let db = FjallDatabase::new(dir.path().to_path_buf()).unwrap();
    Arc::new(db.open_queue("bench").unwrap())
}

fn concurrent_rw_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("concurrent_rw");

    for write_count in [100u64, 1000, 5000].iter() {
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{}_writes", write_count)),
            write_count,
            |b, &write_count| {
                b.to_async(tokio::runtime::Runtime::new().unwrap())
                    .iter(|| async {
                        let dir = TempDir::new().unwrap();
                        let queue = open_queue(&dir);

                        let write_queue = Arc::clone(&queue);
                        let read_queue = Arc::clone(&queue);

                        let writer = tokio::spawn(async move {
                            for i in 0..write_count {
                                write_queue
                                    .append(&BenchmarkData {
                                        value: i as i64,
                                        name: format!("entry_{}", i),
                                    })
                                    .unwrap();
                            }
                        });

                        let target = write_count / 2;
                        let reader = tokio::spawn(async move {
                            let mut fetched = 0u64;
                            while fetched < target {
                                let batch = read_queue.fetch_batch(fetched, 100).unwrap();
                                for msg in batch {
                                    black_box(&msg.data);
                                    fetched = msg.seq + 1;
                                }
                                if fetched < target {
                                    tokio::task::yield_now().await;
                                }
                            }
                        });

                        let _ = tokio::join!(writer, reader);
                    });
            },
        );
    }

    group.finish();
}

fn write_heavy_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("write_heavy");

    for num_writers in [2usize, 4, 8].iter() {
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{}_writers", num_writers)),
            num_writers,
            |b, &num_writers| {
                b.to_async(tokio::runtime::Runtime::new().unwrap())
                    .iter(|| async {
                        let dir = TempDir::new().unwrap();
                        let queue = open_queue(&dir);
                        let writes_per_writer = 100u64;

                        let mut handles = vec![];
                        for writer_id in 0..num_writers {
                            let q = Arc::clone(&queue);
                            handles.push(tokio::spawn(async move {
                                for i in 0..writes_per_writer {
                                    q.append(&BenchmarkData {
                                        value: i as i64,
                                        name: format!("w{}_e{}", writer_id, i),
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

criterion_group!(benches, concurrent_rw_benchmark, write_heavy_benchmark);
criterion_main!(benches);
