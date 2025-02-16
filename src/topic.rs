use std::sync::{Arc, mpsc, RwLock};
use std::sync::mpsc::SendError;
use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;

// Partition scoped topic
#[async_trait]
pub trait TopicState<T> {
    // Propose partitioning an existing partition. Allows creation and write of new partition but
    // no reads. Maybe block writes until new partition is created ? Or transfer of state
    async fn propose_new_topic_partition(&self, topic_id: &str, partition: PartitionId) -> PartitionId;
    // Commits proposed partition and allows it for reads.
    async fn commit_new_topic_partition(&self, topic_id: &str, partition: PartitionId);
    // Determines uuid of partition to write topic to.
    fn partition<T>(&self, topic_id: &str, key: T) -> PartitionId;
    // List partitions.
    fn list_partitions(&self) -> Vec<PartitionData<T>>;
}

// We need a leader to decide who will preform the partition!
// TODO: what is the algo for cross machine partitions.
// 1. Option leader per topic.
// 2. Leader per partition.


// topics are configured and partitions created. In program definition language.
// topics own partitions and know their current believed partition state by the Topic State struct.
// A leader is needed if someone will sync all writes, otherwise we make the log responsible. We do
// need a leader to lead the partition, but not writes if log is responsible for ordering writes. Leader
// be needed for fail over in the event writes cannot be preformed.
//
// Topics are joined by transforms and readers and writers. Consumers register at the topic, their are
// two types of consumers a) user consumers and b) topic consumers (These need a transform or fitter between them).
// topic consumers need a writer to determine what partition to write too ( wrapper around object
// that writes partition (should this reroute if incorrect partition is chosen.)

pub struct Topic<T> {
    topic_id: String,
    log: dyn LogRing,
    partitions: TopicPartitions<T>
}

impl Topic<T> {
    pub fn new(topic_id: String, log: dyn LogRing, partition_fn: PartitionFunc<T>) -> Self {
        Self { topic_id, log, partitions: TopicPartitions::new(partition_fn) }
    }

    // runtime is responsible for messaging passing, committing processed state, ensuring liveliness
    // of writers.
    pub fn start_runtime() {
        // Starts readers and writers
    }


    pub fn start_write_runtime() {

        // 1. read from all writers and write topic to partition which is leader or log if leader.
        // 1.1 Need some method of committing last processed value by log, to allow recovery in the event of failure to write values.
        // 2. heart beat check leaders for each partition.
        // like leader election via log we can likely also due this via the log via a special message
        // type. This should mean electing consumer and write leaders is simple and does not
        // involve high levels of coordination. The log can agree on one single log/order and then\
        // use it's primitives to create consensus amongst many leaders.
        // consumer leaders can now easily report, exhaustion and

    }

    pub fn start_read_runtime() {

        // 1. If node has consumers of partition, read values.
        // todo: we only want one consumer active for each type of consumer on each node. How can
        // we ensure this property + health check. Whats

    }
}


// The case I want to solve, in a multi node system, a consistent view on partitions is difficult.
// A partition may be written to that is in the process of being split and receives a write for the
// new partition, when it is in fact the old. It should be able to handle this case and redirect the
// write. As this case could only ever happen cross machine, we need a write director per topic,
// per machine.


// 2. TopicWriter is aware of all partitions and decides on what partition to write too.
// 3. Life cycle of a request is a topic writer is responsible, it knows what partitions
// are handled on which machine and writes accordingly.


// Cross machine only happens if a partition is being written too on 2 machines. Partition leader
// solves this.
// Try and write to who I think owns the partition. If can't, do metadata query for current owner
// if that owner and failure is the same, due query
pub struct TopicWriter<T> {
    write_producer_queue: mpsc::Sender<T>
}


// Topic writer is a lightweight writer, that is cheap to create. Once write has
// been called it is the responsibly of the engine to ensure the write reaches the log.=
impl TopicWriter<T> {

    pub fn new(write_producer_queue: mpsc::Sender<T>) -> Self {
        return Self { write_producer_queue }
    }

    async fn write(&self, value: T) -> anyhow::Result<()> {
        return match self.write_producer_queue.send(value){
            Err(e) => {
                // TODO: catch common error cases/
                Anyhow::Error("failed to write")
            }
        }
    }
}

// 1. Topic readers is aware of the partitions and reads from the correct position.
pub trait TopicReader {
    fn new() -> Self;

}

type PartitionFunc<T> = fn (input: T) -> u64;

type PartitionId = u64;

pub struct PartitionData<T> {
    partition_id: PartitionId,
    partition_func: PartitionFunc<T>,
    low: u64,
    high: u64
}

pub struct PartitionAddress {
    address: String,
    partition_id: PartitionId,
}


pub struct TopicPartitions<T> {
    partition_fn: PartitionFunc<T>, // Partitioning function
    partitions: Arc<RwLock<Arc<Vec<Arc<PartitionData<T>>>>>>, // Copy-on-write storage for partitions
    // todo: Should determine who owns the partitions. This may be simplified by the
    // fact that this is a lightweight list which does not hold any refs to compliocated valytes
}

impl<T> TopicPartitions<T> {
    pub fn new(partition_fn: PartitionFunc<T>) -> Self {
        Self {
            partition_fn,
            partitions: Arc::new(RwLock::new(Arc::new(Vec::new()))),
            //todo: make it so a new value can be instantiated.
        }
    }

    /// Determines which partition a given key belongs to
    pub fn partition(&self, key: T) -> PartitionId {
        let key_hash = (self.partition_fn)(key);
        let partitions = self.partitions.read().unwrap();
        for partition in partitions.iter() {
            if partition.low <= key_hash && key_hash < partition.high {
                return partition.partition_id;
            }
        }
        0 // Return a default partition ID if no match is found
    }

    /// Returns a copy of the current partitions list
    pub fn list_partitions(&self) -> Arc<Vec<Arc<PartitionData<T>>>> {
        self.partitions.read().unwrap().clone()
    }

    /// Splits an existing partition into two, ensuring the list remains sorted
    pub fn split_partition(&self, partition_id: PartitionId) -> Result<(PartitionId, PartitionId), PartitionError> {
        loop {
            let partitions = self.partitions.read().unwrap().as_ref().clone();
            if let Some(index) = partitions.iter().position(|p| p.partition_id == partition_id) {
                let old_partition = &partitions[index];
                let new_partition_split_val = (old_partition.low + old_partition.high) / 2;
                let new_partition_id = new_partition_split_val;

                // Create two new partitions with updated bounds
                // TODO: this partition should the same as the old as it may hold unread values.
                let new_low_partition = Arc::new(PartitionData {
                    partition_id: new_partition_id,
                    partition_func: old_partition.partition_func,
                    low: old_partition.low,
                    high: new_partition_split_val,
                });
                let new_high_partition = Arc::new(PartitionData {
                    partition_id: old_partition.partition_id,
                    partition_func: old_partition.partition_func,
                    low: new_partition_split_val,
                    high: old_partition.high,
                });

                // Perform copy-on-write update of the partition list. In theory this works as all
                // previous values should see the old list which is valid until the list is sent.
                let mut new_partitions = partitions.clone();
                new_partitions[index] = new_low_partition; // Replace the old partition with the first new partition
                new_partitions.push(new_high_partition); // Add the second new partition

                // Ensure the list remains sorted by the `low` key range. Copy and sort is estimated
                // to be faster then maintaining a sorted list since number of partitions is expected to
                // be low.
                new_partitions.sort_by_key(|p| p.low);

                let mut write_lock = self.partitions.write().unwrap();
                return if Arc::ptr_eq(&*write_lock, &partitions) {
                    // If no other thread modified `self.partitions`, update it
                    *write_lock = Arc::new(new_partitions);
                    Ok((new_id1, new_partition_id))
                } else {
                    Err(PartitionError::ConcurrentModification)
                }
            } else {
                return Err(PartitionError::NotFound);
            }
        }
    }
}
