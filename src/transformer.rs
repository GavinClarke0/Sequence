// Transformer needs to know a few things
// 1. What queue it is pulling from. Should we deal with primary
// 2. If primary goes, what to do about transformer state. Replicate state ?
//    2.1 could use log of state to replay
// 3.
//

use anyhow::Error;
use anyhow::Result;
use crate::tick::Tick;

pub trait Transformer<TransformerFunction> {
    fn function();
}

// Holds the function we will actually excute
pub trait TransformerFunction<T, U> {
    fn snap_shot() -> SnapShot;
    fn invoke(x: T, sequence: u64) -> anyhow::Result<U>;
}

#[repr(C)]
pub struct SnapShot {
    snap_shot_id: u64,
    snap_shot_buf:  Box<[u8]>,
    queue_tick: Tick,
}


// Chain Replication 