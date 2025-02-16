use anyhow::Error;
use anyhow::Result;
use crate::tick::Tick;

pub trait Consumer<TransformerFunction> {
    fn function();
}

// Holds the function we will actually excute
pub trait ConsumerFunction<T> {
    fn invoke(x: T, sequence: u64) -> anyhow::Result<()>;
}

