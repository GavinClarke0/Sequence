# Sequence Library - Development Context

## Project Overview

`sequence` is a Rust library for building durable, log-backed data processing pipelines. Data flows through Kafka-like append-only logs where transformers process events and can maintain state between invocations.

## Architecture

```
Source → Input Log → Transformer (with State) → Output Log → Sink
```

### Core Concepts

- **Log**: Durable append-only storage (backed by fjall LSM-tree database)
- **Transformer**: Processes input events → output events, can maintain state
- **Worker**: Runtime component that runs a transformer's processing loop
- **State Store**: Persists transformer state for recovery
- **Source/Sink**: Entry/exit points for external I/O

## Current Implementation Status

### Completed Features

1. **Linear Pipeline** (`src/graph.rs`)
   - `PipelineBuilder` creates single-transformer pipelines
   - Works: source → transformer → sink

2. **DAG Support** (`src/dag/`) - JUST IMPLEMENTED
   - `DagBuilder` for multi-transformer graphs
   - Compile-time type-safe edge connections
   - Fan-out: one output feeds multiple downstream transformers
   - Fan-in (merge): multiple transformers write to same downstream log (same type required)
   - Cycle detection at build time

### DAG Module Structure

```
src/dag/
├── mod.rs          # Module exports
├── error.rs        # DagError enum
├── handles.rs      # OutputPort<D>, InputPort<D>, type-safe handles
├── wiring.rs       # WiringContext for edge definitions
├── builder.rs      # DagBuilder, WiredDagBuilder
└── dag.rs          # Dag struct, TypedDagRunner
```

### DAG API Usage

```rust
// 1. Build the DAG structure (defines nodes and edges)
let dag = DagBuilder::new(log_builder, state_store)
    .source::<RawEvent>("input")
    .transformer(Parser::new("parser"))       // RawEvent -> ParsedEvent
    .transformer(Enricher::new("enricher"))   // ParsedEvent -> EnrichedEvent
    .sink::<EnrichedEvent>("output")
    .connect(|ctx| {
        // Type-safe edges - mismatched types won't compile
        ctx.edge(ctx.source::<RawEvent>("input"), ctx.input::<RawEvent>("parser"));
        ctx.edge(ctx.output::<ParsedEvent>("parser"), ctx.input::<ParsedEvent>("enricher"));
        ctx.edge(ctx.output::<EnrichedEvent>("enricher"), ctx.sink::<EnrichedEvent>("output"));
    })
    .build()?;

// 2. Create typed runner and add workers (preserves type info)
let runner = TypedDagRunner::new(dag)
    .add_worker(Parser::new("parser"), "input_out", "parser_out")
    .add_worker(Enricher::new("enricher"), "parser_out", "enricher_out");

// 3. External I/O
runner.dag().write_source("input", event).await?;

// 4. Run the DAG
let handle = runner.run().await?;
// ... later ...
handle.shutdown().await?;
```

## What is a "Worker"?

A **Worker** is the runtime component that executes a transformer's processing loop. Located in `src/runtime.rs`:

```rust
pub struct TransformerWorker<T, IL, OL, SS> {
    transformer: T,          // The transformer logic
    input_log: Arc<IL>,      // Reads from this log
    output_log: Arc<OL>,     // Writes to this log
    state_store: Arc<SS>,    // Persists state checkpoints
    config: WorkerConfig,    // Batch size, checkpoint intervals, etc.
}
```

**Worker responsibilities:**
1. Subscribe to input log notifications (`log.subscribe()`)
2. Read batches of events from input log
3. Call `transformer.transform()` for each event
4. Write output events to output log
5. Periodically checkpoint state to StateStore
6. Handle errors with exponential backoff
7. Gracefully shutdown on signal

**Worker lifecycle:**
```
initialize() → load state from StateStore
     ↓
run_loop() → wait for notifications → process batch → checkpoint
     ↓
shutdown → final checkpoint
```

## Key Design Decisions

1. **Type erasure during building, type safety via handles**
   - `DagBuilder` stores nodes as type-erased `NodeSpec`
   - `OutputPort<D>` and `InputPort<D>` carry types via PhantomData
   - `ctx.edge()` requires matching types at compile time

2. **Logs are shared via Arc**
   - Fan-out: multiple workers subscribe to same `Arc<Log>`
   - Fan-in: multiple workers append to same `Arc<Log>`

3. **Workers added separately from DAG structure**
   - `DagBuilder` defines graph topology and creates logs
   - `TypedDagRunner::add_worker()` adds actual transformer instances
   - This preserves type information that would be lost with type erasure

4. **Event-driven processing**
   - Workers wait on `Notify` from input log
   - No polling - immediate wake on new data

## Future Work (Stage 2)

- **JoinTransformer**: Combine events from different-typed streams by key
- **More sophisticated fan-in**: Currently only supports merge (same type)
- **Dynamic DAG modification**: Add/remove nodes at runtime

## File Locations

| Component | File |
|-----------|------|
| Log trait & FjallLog | `src/log.rs` |
| Transformer trait | `src/transformer.rs` |
| Worker & Runtime | `src/runtime.rs` |
| State management | `src/state.rs` |
| Linear Pipeline | `src/graph.rs` |
| DAG module | `src/dag/` |
| Example | `examples/basic_pipeline.rs` |
| Benchmarks | `benches/fjall_log.rs` |

## Running Tests

```bash
cargo test              # All tests
cargo test dag::        # DAG-specific tests
cargo test --release    # Optimized tests
```

## Dependencies

- `fjall`: LSM-tree storage engine
- `rkyv`: Zero-copy serialization
- `tokio`: Async runtime
- `serde`: User data serialization
