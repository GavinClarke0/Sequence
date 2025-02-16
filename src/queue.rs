

// interface for queue abstraction which holds work for transformers and topics.
use std::error::Error;
use crate::tick::Tick;

// Thinking when re-partition values in queue are left in queue but are rewritten to other queue.
// What does a repartition of a topic and a transformer look like from the queue perspective.
// Start with the Transformer as it is more complicated.
// 1. Create new queue on machine it is scheduled on. We can assume this may be a different machine.
// 2. Receive ack that queue is ready to accept writes.
// 3. Preform transformer init method (Latency sensitive as it may set up db connections)
// 4. Receive ack from transformer.
// 5. Snapshot transformer state, via transformer partition snapshot method and send state.
//   5.1 Ack will tell dependent system to begin writing to writing values to new partition.
//      5.1.1 This will need some consensus on what the partition is at all writers. I.e need quorum
//            or consistent view of cluster partition state. Since a out of sync will write data to old
//            queue handling partition. My thinking is that, Since queue that was partitioned should
//            have most up to data view of state it can reach consensus by writing values to new partition it
//            created. This is possible because it has the latest snapshot that affects partitioning of
//            it's queue.
// 6. After partition state snapshot, begin writing partitioned values to new queue. Can be async
//    to read as separate process.
// 7. new transformer begin processing state.

// what happens if we relax write order within a topic.

pub trait Queue {
    /// Writes a byte slice to the queue and returns the sequence value or an error.
    fn write(&mut self, partition_key: &[u8], data: &[u8]) -> Result<Tick, Box<dyn Error>>;

    /// Reads data from the queue given a sequence number. Returns None if the sequence does not exist.
    fn read(&self, sequence: Tick) -> Option<Vec<u8>>;

    /// Distance of sequence from head position. Helpful to determine number of queue size.
    fn distance(sequence: Tick) -> u64;
}


// responsible for accepting writes and
pub trait Acceptor {

}

pub trait Committer {

}
