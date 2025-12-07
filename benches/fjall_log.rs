use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use sequence::log::{FjallDatabaseState, FjallLog, Log};
use sequence::log_value::{LogData, LogValueDeserialized};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tempfile::TempDir;

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
struct BenchmarkData {
    value: i64,
    name: String,
    timestamp: u64,
}

impl LogData for BenchmarkData {
    fn to_bytes(&self) -> anyhow::Result<Vec<u8>> {
        serde_json::to_vec(self).map_err(|e| anyhow::anyhow!("Serialization failed: {}", e))
    }

    fn from_bytes(bytes: &[u8]) -> anyhow::Result<Self> {
        serde_json::from_slice(bytes)
            .map_err(|e| anyhow::anyhow!("Deserialization failed: {}", e))
    }
}

fn create_test_log() -> (FjallLog, TempDir) {
    let temp_dir = TempDir::new().unwrap();
    let db_state = FjallDatabaseState::new(temp_dir.path().to_path_buf()).unwrap();
    let log = db_state.new_log("bench_log".to_string()).unwrap();
    (log, temp_dir)
}

/// Benchmark sequential writes to FjallLog
fn sequential_writes_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("fjall_sequential_writes");

    for write_count in [100, 1000, 5000].iter() {
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{}_writes", write_count)),
            write_count,
            |b, &write_count| {
                b.to_async(tokio::runtime::Runtime::new().unwrap())
                    .iter(|| async {
                        let (log, _temp_dir) = create_test_log();

                        for i in 0..write_count {
                            let entry = LogValueDeserialized {
                                key: i as u128,
                                data: BenchmarkData {
                                    value: i as i64,
                                    name: format!("entry_{}", i),
                                    timestamp: i as u64,
                                },
                                metadata: vec![],
                            };

                            let _ = black_box(log.append(entry).await.unwrap());
                        }
                    });
            },
        );
    }

    group.finish();
}

/// Benchmark sequential reads from FjallLog
fn sequential_reads_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("fjall_sequential_reads");

    for entry_count in [100, 1000, 5000].iter() {
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{}_reads", entry_count)),
            entry_count,
            |b, &entry_count| {
                b.to_async(tokio::runtime::Runtime::new().unwrap()).iter(|| async {
                    let (log, _temp_dir) = create_test_log();

                    // Pre-populate the log
                    for i in 0..entry_count {
                        let entry = LogValueDeserialized {
                            key: i as u128,
                            data: BenchmarkData {
                                value: i as i64,
                                name: format!("entry_{}", i),
                                timestamp: i as u64,
                            },
                            metadata: vec![],
                        };
                        log.append(entry).await.unwrap();
                    }

                    // Benchmark reads
                    for i in 0..entry_count {
                        let entry: LogValueDeserialized<BenchmarkData> =
                            black_box(log.get(i).await.unwrap());
                        black_box(entry);
                    }
                });
            },
        );
    }

    group.finish();
}

/// Benchmark random reads from FjallLog
fn random_reads_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("fjall_random_reads");

    for entry_count in [100, 1000, 5000].iter() {
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{}_entries", entry_count)),
            entry_count,
            |b, &entry_count| {
                b.to_async(tokio::runtime::Runtime::new().unwrap()).iter(|| async {
                    let (log, _temp_dir) = create_test_log();

                    // Pre-populate the log
                    for i in 0..entry_count {
                        let entry = LogValueDeserialized {
                            key: i as u128,
                            data: BenchmarkData {
                                value: i as i64,
                                name: format!("entry_{}", i),
                                timestamp: i as u64,
                            },
                            metadata: vec![],
                        };
                        log.append(entry).await.unwrap();
                    }

                    // Benchmark random reads
                    use std::collections::hash_map::RandomState;
                    use std::hash::{BuildHasher, Hash, Hasher};
                    let random_state = RandomState::new();

                    for i in 0..entry_count {
                        let mut hasher = random_state.build_hasher();
                        i.hash(&mut hasher);
                        let random_index = (hasher.finish() as u32) % entry_count;

                        let entry: LogValueDeserialized<BenchmarkData> =
                            black_box(log.get(random_index).await.unwrap());
                        black_box(entry);
                    }
                });
            },
        );
    }

    group.finish();
}

/// Benchmark concurrent writes to FjallLog
fn concurrent_writes_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("fjall_concurrent_writes");

    for num_writers in [2, 4, 8].iter() {
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{}_writers", num_writers)),
            num_writers,
            |b, &num_writers| {
                b.to_async(tokio::runtime::Runtime::new().unwrap())
                    .iter(|| async {
                        let (log, _temp_dir) = create_test_log();
                        let log = Arc::new(log);

                        let writes_per_writer = 100;

                        // Spawn multiple writer tasks
                        let mut writer_handles = vec![];
                        for writer_id in 0..num_writers {
                            let log_clone = Arc::clone(&log);
                            let handle = tokio::spawn(async move {
                                for i in 0..writes_per_writer {
                                    let entry = LogValueDeserialized {
                                        key: (writer_id * writes_per_writer + i) as u128,
                                        data: BenchmarkData {
                                            value: i as i64,
                                            name: format!("writer_{}_entry_{}", writer_id, i),
                                            timestamp: i as u64,
                                        },
                                        metadata: vec![],
                                    };

                                    let _ = log_clone.append(entry).await.unwrap();
                                }
                            });
                            writer_handles.push(handle);
                        }

                        // Wait for all tasks
                        for handle in writer_handles {
                            let _ = handle.await;
                        }
                    });
            },
        );
    }

    group.finish();
}

/// Benchmark concurrent reads from FjallLog
fn concurrent_reads_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("fjall_concurrent_reads");

    for num_readers in [2, 4, 8].iter() {
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{}_readers", num_readers)),
            num_readers,
            |b, &num_readers| {
                b.to_async(tokio::runtime::Runtime::new().unwrap())
                    .iter(|| async {
                        let (log, _temp_dir) = create_test_log();
                        let log = Arc::new(log);

                        let entry_count = 500;

                        // Pre-populate the log
                        for i in 0..entry_count {
                            let entry = LogValueDeserialized {
                                key: i as u128,
                                data: BenchmarkData {
                                    value: i as i64,
                                    name: format!("entry_{}", i),
                                    timestamp: i as u64,
                                },
                                metadata: vec![],
                            };
                            log.append(entry).await.unwrap();
                        }

                        // Multiple reader tasks
                        let mut reader_handles = vec![];
                        for reader_id in 0..num_readers {
                            let log_clone = Arc::clone(&log);
                            let handle = tokio::spawn(async move {
                                let reads_per_reader = entry_count / num_readers;
                                let start_index = reader_id * reads_per_reader;
                                let end_index = start_index + reads_per_reader;

                                for i in start_index..end_index {
                                    let entry: LogValueDeserialized<BenchmarkData> =
                                        log_clone.get(i).await.unwrap();
                                    black_box(entry);
                                }
                            });
                            reader_handles.push(handle);
                        }

                        // Wait for all tasks
                        for handle in reader_handles {
                            let _ = handle.await;
                        }
                    });
            },
        );
    }

    group.finish();
}

/// Benchmark mixed read/write workload
fn mixed_workload_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("fjall_mixed_workload");

    for workload_size in [100, 500, 1000].iter() {
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{}_ops", workload_size)),
            workload_size,
            |b, &workload_size| {
                b.to_async(tokio::runtime::Runtime::new().unwrap())
                    .iter(|| async {
                        let (log, _temp_dir) = create_test_log();
                        let log = Arc::new(log);

                        // Pre-populate with half the entries
                        for i in 0..(workload_size / 2) {
                            let entry = LogValueDeserialized {
                                key: i as u128,
                                data: BenchmarkData {
                                    value: i as i64,
                                    name: format!("entry_{}", i),
                                    timestamp: i as u64,
                                },
                                metadata: vec![],
                            };
                            log.append(entry).await.unwrap();
                        }

                        let write_log = Arc::clone(&log);
                        let read_log = Arc::clone(&log);

                        // Writer task
                        let writer = tokio::spawn(async move {
                            for i in (workload_size / 2)..workload_size {
                                let entry = LogValueDeserialized {
                                    key: i as u128,
                                    data: BenchmarkData {
                                        value: i as i64,
                                        name: format!("entry_{}", i),
                                        timestamp: i as u64,
                                    },
                                    metadata: vec![],
                                };
                                let _ = write_log.append(entry).await.unwrap();
                            }
                        });

                        // Reader task
                        let reader = tokio::spawn(async move {
                            for i in 0..(workload_size / 2) {
                                let entry: LogValueDeserialized<BenchmarkData> =
                                    read_log.get(i).await.unwrap();
                                black_box(entry);
                            }
                        });

                        // Wait for both tasks
                        let _ = tokio::join!(writer, reader);
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
    random_reads_benchmark,
    concurrent_writes_benchmark,
    concurrent_reads_benchmark,
    mixed_workload_benchmark
);
criterion_main!(benches);
