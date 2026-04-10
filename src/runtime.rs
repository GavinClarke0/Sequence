use crate::queue::{FjallQueue, QueueError};
use crate::transformer::Transformer;
use anyhow::Result;
use futures::StreamExt;
use std::sync::Arc;
use tokio::{sync::watch, task::JoinHandle};

/// Handle for a running transformer task. Call `shutdown()` to stop it gracefully.
pub struct TransformerHandle {
    shutdown_tx: watch::Sender<bool>,
    task: JoinHandle<Result<()>>,
}

impl TransformerHandle {
    /// Signal the transformer to stop and wait for it to finish.
    pub async fn shutdown(self) -> Result<()> {
        let _ = self.shutdown_tx.send(true);
        self.task
            .await
            .map_err(|e| anyhow::anyhow!("transformer task panicked: {}", e))?
    }

    /// Returns true if the transformer task has already finished.
    pub fn is_finished(&self) -> bool {
        self.task.is_finished()
    }
}

/// Spawns a transformer that reads from `input`, calls `T::transform` on each
/// message, and appends non-`None` results to `output`.
///
/// `start_seq` controls where in the input queue the transformer begins.
/// Pass `0` to replay from the beginning, or persist the last processed
/// sequence number yourself and pass that to resume.
pub fn spawn_transformer<T>(
    transformer: T,
    input: Arc<FjallQueue<T::Input>>,
    output: Arc<FjallQueue<T::Output>>,
    start_seq: u64,
) -> TransformerHandle
where
    T: Transformer,
{
    let (shutdown_tx, mut shutdown_rx) = watch::channel(false);

    let task = tokio::spawn(async move {
        let mut state = T::State::default();
        let stream = input.subscribe(start_seq);
        tokio::pin!(stream);

        loop {
            tokio::select! {
                _ = shutdown_rx.changed() => break,
                msg = stream.next() => {
                    match msg {
                        Some(Ok(msg)) => {
                            match transformer.transform(msg, &mut state).await? {
                                Some(out) => { output.append(&out)?; }
                                None => {}
                            }
                        }
                        Some(Err(QueueError::Lagged { requested, oldest })) => {
                            return Err(anyhow::anyhow!(
                                "transformer fell behind: requested seq {requested} but oldest available is {oldest}"
                            ));
                        }
                        Some(Err(e)) => return Err(e.into()),
                        None => break,
                    }
                }
            }
        }

        Ok(())
    });

    TransformerHandle { shutdown_tx, task }
}
