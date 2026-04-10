# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

`sequence` is a Rust library implementing a durable, file-backed log segment system with zero-copy read semantics. It's designed for high-performance sequential data storage with CRC32 validation for data integrity. Data is written to Kakfa like topics where it can be processed by a local rust function (which can hold state between function runs) or read by external data sinks via a tbh protpcol.

Sequence is designed for a single machine and is NUMA aware. 

## Interface

The code is expressed as a compile time graph where you describe each log, each transformer and each sink (Where non rust programs can access data). This should be created with enums and started automatically.

Users describe their log as a graph and then the systems create the required segments etc. Later thought will be needed for migrations. 

## Architecture - Log an Log Segments

A log segment holds a fixed size of data (see the current in memory log_segment) and a log is comprised of multiple log segment. We only have one segment active (actively written too) and all other segments are are read only (once sealed). The monitcity increasing value is across log segment ideas. 

We keep previous log segments only for reading and will read them into memory before serving queries. The log segment type is responsible for it's level of durability. However the log manager should be aware of it's contents.



## Build Commands

```bash
# Build the library
cargo build

# Build with optimizations
cargo build --release

# Run all tests
cargo testWould 

# Run a specific test
cargo test <test_name>

# Run tests with output
cargo test -- --nocapture

# Check code without building
cargo check

# Format code
cargo fmt

# Run linter
cargo clippy
```

### Key Design Patterns

**Zero-Copy Semantics**: All read operations avoid allocation by returning views into mmap'd or buffered data. Never deserialize unless explicitly required.

**CRC Validation**: Two-level integrity checking:
- Header CRC validates the entry metadata (byte_length, version, message_crc)
- Message CRC validates the actual payload data

**Separation of Concerns**:
- Data format (`LogValue`) is independent of storage format (`LogEntryHeader`)
- Storage layer traits (`LogSegmentWriter/Reader`) are independent of file implementation
- Index and data files are separate concerns (`FsLogSegmentData` vs `FsLogSegmentIndex`)

### Dependencies

- `rkyv`: Zero-copy serialization framework (core dependency)
- `crc-fast`: CRC32 computation for integrity checks
- `tokio`: Async runtime (features = "full")
- `anyhow`, `thiserror`: Error handling

