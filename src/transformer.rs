use crate::queue::{Message, Queueable};
use anyhow::Result;
use std::future::Future;

/// Processes a stream of typed messages, optionally maintaining state between calls.
///
/// Implement this trait to define the transformation logic for a pipeline stage.
/// - `State = ()` for stateless transformers (the default is already `Default`).
/// - Return `Ok(None)` to filter (drop) a message without emitting output.
pub trait Transformer: Send + Sync + 'static {
    type Input: Queueable;
    type Output: Queueable;
    /// In-memory state carried across invocations. Use `()` for stateless transformers.
    type State: Default + Send + Sync + 'static;

    fn transform(
        &self,
        msg: Message<Self::Input>,
        state: &mut Self::State,
    ) -> impl Future<Output = Result<Option<Self::Output>>> + Send;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::queue::Queueable;

    struct IntItem(i32);

    impl Queueable for IntItem {
        fn serialize(&self) -> Vec<u8> {
            self.0.to_le_bytes().to_vec()
        }
        fn deserialize(bytes: &[u8]) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
            let arr: [u8; 4] = bytes.try_into()?;
            Ok(IntItem(i32::from_le_bytes(arr)))
        }
    }

    struct Doubler;

    impl Transformer for Doubler {
        type Input = IntItem;
        type Output = IntItem;
        type State = ();

        async fn transform(
            &self,
            msg: Message<Self::Input>,
            _state: &mut (),
        ) -> Result<Option<IntItem>> {
            Ok(Some(IntItem(msg.data.0 * 2)))
        }
    }

    struct CountingDoubler;

    #[derive(Default)]
    struct Count(u32);

    impl Transformer for CountingDoubler {
        type Input = IntItem;
        type Output = IntItem;
        type State = Count;

        async fn transform(
            &self,
            msg: Message<Self::Input>,
            state: &mut Count,
        ) -> Result<Option<IntItem>> {
            state.0 += 1;
            Ok(Some(IntItem(msg.data.0 * 2)))
        }
    }

    #[tokio::test]
    async fn test_stateless_transform() {
        let t = Doubler;
        let msg = Message { seq: 0, data: IntItem(21) };
        let out = t.transform(msg, &mut ()).await.unwrap().unwrap();
        assert_eq!(out.0, 42);
    }

    #[tokio::test]
    async fn test_stateful_transform() {
        let t = CountingDoubler;
        let mut state = Count::default();

        for i in 0..3 {
            let msg = Message { seq: i, data: IntItem(1) };
            t.transform(msg, &mut state).await.unwrap();
        }

        assert_eq!(state.0, 3);
    }
}
