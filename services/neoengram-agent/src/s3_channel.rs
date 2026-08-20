//! Agent side of the dedicated full-duplex S3 read channel.

use std::{sync::Arc, time::Duration};

use bytes::Bytes;
use neoengram_domain::protocol::{
    S3ReadChannelDecoder, S3ReadChannelFrame, S3ReadChannelHello, S3ReadFrame,
};
use tokio::{sync::mpsc, task::JoinHandle, time};

use crate::{AgentSessionClient, AgentSessionClientError, S3ReadExecutor};

const CHANNEL_FRAME_BUFFER: usize = 64;
const INITIAL_RECONNECT_DELAY: Duration = Duration::from_millis(100);
const MAX_RECONNECT_DELAY: Duration = Duration::from_secs(30);

#[derive(Clone)]
pub struct AgentS3ReadChannelWriter {
    outgoing: mpsc::Sender<Bytes>,
}

impl AgentS3ReadChannelWriter {
    pub(crate) fn new(outgoing: mpsc::Sender<Bytes>) -> Self {
        Self { outgoing }
    }

    pub async fn send(&self, frame: S3ReadChannelFrame) -> Result<(), AgentSessionClientError> {
        let encoded = frame
            .encode()
            .map(Bytes::from)
            .map_err(AgentSessionClientError::protocol)?;
        self.outgoing.send(encoded).await.map_err(|_| {
            AgentSessionClientError::transport("Agent S3 read request stream is closed")
        })
    }
}

pub struct AgentS3ReadChannelConnection {
    writer: AgentS3ReadChannelWriter,
    incoming: mpsc::Receiver<Result<S3ReadChannelFrame, AgentSessionClientError>>,
    tasks: Vec<JoinHandle<()>>,
}

impl std::fmt::Debug for AgentS3ReadChannelConnection {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AgentS3ReadChannelConnection")
            .finish_non_exhaustive()
    }
}

impl AgentS3ReadChannelConnection {
    pub(crate) fn new(
        writer: AgentS3ReadChannelWriter,
        incoming: mpsc::Receiver<Result<S3ReadChannelFrame, AgentSessionClientError>>,
        tasks: Vec<JoinHandle<()>>,
    ) -> Self {
        Self {
            writer,
            incoming,
            tasks,
        }
    }

    #[must_use]
    pub fn writer(&self) -> AgentS3ReadChannelWriter {
        self.writer.clone()
    }

    pub async fn receive(&mut self) -> Result<Option<S3ReadChannelFrame>, AgentSessionClientError> {
        self.incoming.recv().await.transpose()
    }
}

impl Drop for AgentS3ReadChannelConnection {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
    }
}

/// Converts arbitrary H2 DATA chunks into complete binary channel frames.
pub(crate) fn spawn_s3_response_decoder(
    mut chunks: mpsc::Receiver<Result<Bytes, AgentSessionClientError>>,
) -> (
    mpsc::Receiver<Result<S3ReadChannelFrame, AgentSessionClientError>>,
    JoinHandle<()>,
) {
    let (frames_tx, frames_rx) = mpsc::channel(CHANNEL_FRAME_BUFFER);
    let reader = tokio::spawn(async move {
        let mut decoder = S3ReadChannelDecoder::new();
        while let Some(chunk) = chunks.recv().await {
            let chunk = match chunk {
                Ok(chunk) => chunk,
                Err(error) => {
                    let _ = frames_tx.send(Err(error)).await;
                    return;
                }
            };
            let frames = match decoder.push(&chunk) {
                Ok(frames) => frames,
                Err(error) => {
                    let _ = frames_tx
                        .send(Err(AgentSessionClientError::protocol(error)))
                        .await;
                    return;
                }
            };
            for frame in frames {
                if frames_tx.send(Ok(frame)).await.is_err() {
                    return;
                }
            }
        }
        if let Err(error) = decoder.finish() {
            let _ = frames_tx
                .send(Err(AgentSessionClientError::protocol(error)))
                .await;
        }
    });
    (frames_rx, reader)
}

/// Abort-on-drop task that keeps the read channel connected for one immutable Agent session.
pub(crate) struct AgentS3ReadChannelTask(JoinHandle<()>);

impl Drop for AgentS3ReadChannelTask {
    fn drop(&mut self) {
        self.0.abort();
    }
}

pub(crate) fn spawn_s3_read_channel<C>(
    client: Arc<C>,
    hello: S3ReadChannelHello,
    executor: S3ReadExecutor,
) -> AgentS3ReadChannelTask
where
    C: AgentSessionClient + 'static,
{
    AgentS3ReadChannelTask(tokio::spawn(async move {
        let mut delay = INITIAL_RECONNECT_DELAY;
        loop {
            match client.connect_s3_read_channel(&hello).await {
                Ok(mut channel) => {
                    delay = INITIAL_RECONNECT_DELAY;
                    if let Err(error) = run_connected(&hello, &executor, &mut channel).await {
                        tracing::warn!(%error, "Agent S3 read channel disconnected");
                    }
                }
                Err(error) => {
                    tracing::warn!(%error, "Agent S3 read channel connection failed");
                }
            }
            time::sleep(delay).await;
            delay = delay.saturating_mul(2).min(MAX_RECONNECT_DELAY);
        }
    }))
}

async fn run_connected(
    hello: &S3ReadChannelHello,
    executor: &S3ReadExecutor,
    channel: &mut AgentS3ReadChannelConnection,
) -> Result<(), AgentSessionClientError> {
    let ready = time::timeout(Duration::from_secs(10), channel.receive())
        .await
        .map_err(|_| AgentSessionClientError::transport("Gateway S3 channel Ready timed out"))??
        .ok_or_else(|| {
            AgentSessionClientError::transport("Gateway S3 channel ended before Ready")
        })?;
    let S3ReadChannelFrame::Ready(ready) = ready else {
        return Err(AgentSessionClientError::protocol(
            "Gateway S3 channel must start with Ready",
        ));
    };
    if ready.agent_id != hello.agent_id || ready.session_generation != hello.session_generation {
        return Err(AgentSessionClientError::protocol(
            "Gateway S3 channel Ready has a different session fence",
        ));
    }

    let writer = channel.writer();
    let mut reads = tokio::task::JoinSet::new();
    loop {
        tokio::select! {
            frame = channel.receive() => {
                let Some(frame) = frame? else {
                    return Err(AgentSessionClientError::transport("Gateway S3 read channel ended"));
                };
                let S3ReadChannelFrame::Read(frame) = frame else {
                    return Err(AgentSessionClientError::protocol(
                        "Gateway sent a handshake frame after S3 channel Ready",
                    ));
                };
                // Opening a read resolves frozen metadata through SessionExecutionBridge. That
                // synchronous bridge owns a Tokio Handle and must run outside a runtime worker.
                let executor = executor.clone();
                let handled = run_blocking(move || executor.handle_frame(frame)).await?;
                match handled {
                    Ok(Some(mut response)) => {
                        let writer = writer.clone();
                        reads.spawn(async move {
                            while let Some(frame) = response.recv().await {
                                writer
                                    .send(S3ReadChannelFrame::Read(frame))
                                    .await?;
                            }
                            Ok::<(), AgentSessionClientError>(())
                        });
                    }
                    Ok(None) => {}
                    Err(error) => {
                        writer
                            .send(S3ReadChannelFrame::Read(S3ReadFrame::Error(error)))
                            .await?;
                    }
                }
            }
            completed = reads.join_next(), if !reads.is_empty() => {
                match completed {
                    Some(Ok(Ok(()))) => {}
                    Some(Ok(Err(error))) => return Err(error),
                    Some(Err(error)) => {
                        return Err(AgentSessionClientError::transport(format!(
                            "Agent S3 read worker failed: {error}"
                        )));
                    }
                    None => {}
                }
            }
        }
    }
}

async fn run_blocking<T, F>(operation: F) -> Result<T, AgentSessionClientError>
where
    T: Send + 'static,
    F: FnOnce() -> T + Send + 'static,
{
    tokio::task::spawn_blocking(operation)
        .await
        .map_err(|_| AgentSessionClientError::transport("Agent S3 read dispatcher failed"))
}

#[cfg(test)]
mod tests {
    use super::run_blocking;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn read_dispatch_runs_outside_the_async_runtime_worker() {
        let runtime = tokio::runtime::Handle::current();
        let value = run_blocking(move || runtime.block_on(async { 7_u8 }))
            .await
            .unwrap();

        assert_eq!(value, 7);
    }
}
