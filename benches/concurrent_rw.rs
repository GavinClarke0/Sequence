use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId};
use sequence::log_segment::{ActiveMemoryLogSegment, LogSegmentWriter, LogSegmentReader};
use sequence::log_value::LogValueDeserialized;
use serde::{Serialize, Deserialize};
use std::sync::Arc;

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
struct BenchmarkData {
    value: i64,
    name: String,
    timestamp: u64,
}

/// Benchmark concurrent writes and reads on a shared log segment
/// - Writer task continuously appends entries
/// - Reader task continuously reads entries
fn concurrent_rw_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("concurrent_rw");

    // Test with different write/read ratios
    for write_count in [100, 1000, 5000].iter() {
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{}_writes", write_count)),
            write_count,
            |b, &write_count| {
                b.to_async(tokio::runtime::Runtime::new().unwrap())
                    .iter(|| async {
                        // Create a shared log segment wrapped in Arc
                        let segment = Arc::new(ActiveMemoryLogSegment::<BenchmarkData>::new(0));

                        let write_segment = Arc::clone(&segment);
                        let read_segment = Arc::clone(&segment);

                        // Writer task
                        let writer = tokio::spawn(async move {
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
                                let res = write_segment.append(entry).await;

                                if res.is_err() {
                                    panic!("bad stuff")
                                }
                            }
                        });

                        // Reader task - reads entries as they become available
                        let reader = tokio::spawn(async move {
                            let mut read_count = 0;
                            let target_reads = write_count / 2; // Read half of what's written

                            while read_count < target_reads {
                                // Try to read at current position
                                if let Ok(entry) = LogSegmentReader::get(&*read_segment, read_count).await {
                                    black_box(entry);
                                    read_count += 1;
                                } else {
                                    // Entry not yet available, yield and retry
                                    tokio::task::yield_now().await;

                                    
                                }
                            }
                        });

                        // Wait for both tasks to complete
                        let _ = tokio::join!(writer, reader);
                    });
            },
        );
    }

    group.finish();
}

/// Benchmark write-heavy workload (multiple writers, single reader)
fn write_heavy_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("write_heavy");

    for num_writers in [2, 4, 8].iter() {
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{}_writers", num_writers)),
            num_writers,
            |b, &num_writers| {
                b.to_async(tokio::runtime::Runtime::new().unwrap())
                    .iter(|| async {
                        let segment = Arc::new(ActiveMemoryLogSegment::<BenchmarkData>::new(0));

                        let writes_per_writer = 100;

                        // Spawn multiple writer tasks
                        let mut writer_handles = vec![];
                        for writer_id in 0..num_writers {
                            let write_segment = Arc::clone(&segment);
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

                                    let _ = write_segment.append(entry).await;
                                }
                            });
                            writer_handles.push(handle);
                        }

                        // Single reader task
                        let read_segment = Arc::clone(&segment);
                        let reader = tokio::spawn(async move {
                            let total_writes = num_writers * writes_per_writer;
                            let mut read_count = 0;

                            while read_count < total_writes / 2 {
                                if let Ok(entry) = LogSegmentReader::get(&*read_segment, read_count).await {
                                    black_box(entry);
                                    read_count += 1;
                                } else {
                                    tokio::task::yield_now().await;
                                }
                            }
                        });

                        // Wait for all tasks
                        for handle in writer_handles {
                            let _ = handle.await;
                        }
                        let _ = reader.await;
                    });
            },
        );
    }

    group.finish();
}

/// Benchmark read-heavy workload (single writer, multiple readers)
fn read_heavy_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("read_heavy");

    for num_readers in [2, 4, 8].iter() {
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{}_readers", num_readers)),
            num_readers,
            |b, &num_readers| {
                b.to_async(tokio::runtime::Runtime::new().unwrap())
                    .iter(|| async {
                        let segment = Arc::new(ActiveMemoryLogSegment::<BenchmarkData>::new(0));

                        let write_count = 500;

                        // Single writer task
                        let write_segment = Arc::clone(&segment);
                        let writer = tokio::spawn(async move {
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

                                let _ = write_segment.append(entry).await;
                            }
                        });

                        // Multiple reader tasks
                        let mut reader_handles = vec![];
                        for reader_id in 0..num_readers {
                            let read_segment = Arc::clone(&segment);
                            let handle = tokio::spawn(async move {
                                let reads_per_reader = write_count / num_readers;
                                let start_index = reader_id * reads_per_reader;
                                let end_index = start_index + reads_per_reader;

                                for i in start_index..end_index {
                                    loop {
                                        if let Ok(entry) = LogSegmentReader::get(&*read_segment, i).await {
                                            black_box(entry);
                                            break;
                                        } else {
                                            tokio::task::yield_now().await;
                                        }
                                    }
                                }
                            });
                            reader_handles.push(handle);
                        }

                        // Wait for all tasks
                        let _ = writer.await;
                        for handle in reader_handles {
                            let _ = handle.await;
                        }
                    });
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    concurrent_rw_benchmark,
    write_heavy_benchmark,
    read_heavy_benchmark
);
criterion_main!(benches);
