use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    fmt,
    sync::Arc,
};

use bytes::Bytes;
use neoengram_agent::LedgerRecord;
use neoengram_protocol::{
    AgentChannelDownstreamFrame, AgentChannelNdjsonDecoder, AgentChannelUpstreamFrame,
    AssignmentOperation, JobAssignment, JobDecision, JobId, SessionGeneration, TenantId,
};
use tokio::{sync::mpsc, task::JoinHandle};

use crate::{
    AgentDaemonError, AgentDaemonResult, AgentMessageProcessor, AgentSessionClientError,
    SharedSessionFence,
};

const CHANNEL_BUFFER: usize = 64;
const MAX_CONCURRENT_JOBS: usize = 16;

#[derive(Clone)]
pub struct AgentChannelWriter {
    outgoing: mpsc::Sender<Bytes>,
}

impl fmt::Debug for AgentChannelWriter {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AgentChannelWriter")
            .finish_non_exhaustive()
    }
}

impl AgentChannelWriter {
    pub(crate) async fn send(
        &self,
        frame: &AgentChannelUpstreamFrame,
    ) -> Result<(), AgentSessionClientError> {
        let bytes = frame
            .encode_ndjson()
            .map(Bytes::from)
            .map_err(AgentSessionClientError::protocol)?;
        self.outgoing.send(bytes).await.map_err(|_| {
            AgentSessionClientError::transport("Agent control channel request stream is closed")
        })
    }
}

pub struct AgentChannelConnection {
    pub(crate) writer: AgentChannelWriter,
    incoming: mpsc::Receiver<Result<AgentChannelDownstreamFrame, AgentSessionClientError>>,
    readers: Vec<JoinHandle<()>>,
}

impl fmt::Debug for AgentChannelConnection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AgentChannelConnection")
            .field("writer", &self.writer)
            .finish_non_exhaustive()
    }
}

impl AgentChannelConnection {
    pub(crate) fn new(
        outgoing: mpsc::Sender<Bytes>,
        incoming: mpsc::Receiver<Result<AgentChannelDownstreamFrame, AgentSessionClientError>>,
        readers: Vec<JoinHandle<()>>,
    ) -> Self {
        Self {
            writer: AgentChannelWriter { outgoing },
            incoming,
            readers,
        }
    }

    pub(crate) async fn receive(
        &mut self,
    ) -> Result<Option<AgentChannelDownstreamFrame>, AgentSessionClientError> {
        self.incoming.recv().await.transpose()
    }
}

impl Drop for AgentChannelConnection {
    fn drop(&mut self) {
        for reader in self.readers.drain(..) {
            reader.abort();
        }
    }
}

pub(crate) fn channel_buffers() -> (mpsc::Sender<Bytes>, mpsc::Receiver<Bytes>) {
    mpsc::channel(CHANNEL_BUFFER)
}

pub(crate) fn spawn_response_reader(
    mut chunks: mpsc::Receiver<Result<Bytes, AgentSessionClientError>>,
) -> (
    mpsc::Receiver<Result<AgentChannelDownstreamFrame, AgentSessionClientError>>,
    JoinHandle<()>,
) {
    let (frames_tx, frames_rx) = mpsc::channel(CHANNEL_BUFFER);
    let reader = tokio::spawn(async move {
        let mut decoder = AgentChannelNdjsonDecoder::new();
        while let Some(chunk) = chunks.recv().await {
            let lines = match chunk.and_then(|bytes| {
                decoder
                    .push(&bytes)
                    .map_err(AgentSessionClientError::protocol)
            }) {
                Ok(lines) => lines,
                Err(error) => {
                    let _ = frames_tx.send(Err(error)).await;
                    return;
                }
            };
            for line in lines {
                let frame = AgentChannelDownstreamFrame::decode_json(&line)
                    .map_err(AgentSessionClientError::protocol);
                let failed = frame.is_err();
                if frames_tx.send(frame).await.is_err() || failed {
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

#[derive(Debug)]
pub(crate) enum AgentWork {
    Assignment(JobAssignment),
    Recovery(LedgerRecord),
    Decision(JobDecision),
}

impl AgentWork {
    fn job_id(&self) -> &JobId {
        match self {
            Self::Assignment(assignment) => match &assignment.assignment {
                AssignmentOperation::Add { input, .. } => &input.job_id,
                AssignmentOperation::WorkspaceMaterialize { input, .. } => &input.job_id,
                AssignmentOperation::SnapshotMount { input, .. } => &input.job_id,
            },
            Self::Recovery(record) => &record.key.job_id,
            Self::Decision(decision) => &decision.job_id,
        }
    }

    fn workspace_delivery_token(&self) -> Option<String> {
        let Self::Assignment(assignment) = self else {
            return None;
        };
        let (job_id, assignment_id, generation) = match &assignment.assignment {
            AssignmentOperation::WorkspaceMaterialize { input, .. } => (
                &input.job_id,
                &input.assignment_id,
                input.assignment_generation,
            ),
            AssignmentOperation::SnapshotMount { input, .. } => (
                &input.job_id,
                &input.assignment_id,
                input.assignment_generation,
            ),
            AssignmentOperation::Add { .. } => return None,
        };
        Some(format!(
            "{}\0{}\0{}",
            job_id,
            assignment_id,
            generation.get()
        ))
    }
}

#[derive(Debug)]
pub(crate) struct FencedAgentWork {
    pub(crate) generation: SessionGeneration,
    pub(crate) work: AgentWork,
}

pub(crate) struct AgentWorkDispatcher {
    pub(crate) sender: mpsc::Sender<FencedAgentWork>,
    pub(crate) errors: mpsc::Receiver<AgentDaemonError>,
    task: JoinHandle<()>,
}

impl Drop for AgentWorkDispatcher {
    fn drop(&mut self) {
        self.task.abort();
    }
}

pub(crate) fn spawn_work_dispatcher(
    tenant_id: TenantId,
    processor: Arc<dyn AgentMessageProcessor>,
    fence: SharedSessionFence,
) -> AgentWorkDispatcher {
    let (sender, mut receiver) = mpsc::channel::<FencedAgentWork>(CHANNEL_BUFFER);
    let (errors_tx, errors) = mpsc::channel(1);
    let task = tokio::spawn(async move {
        let mut queued = BTreeMap::<JobId, VecDeque<FencedAgentWork>>::new();
        let mut running = BTreeSet::<JobId>::new();
        // The dispatcher survives H2 reconnects. Keep the immutable materialization claim so a
        // reconnect cannot enqueue the same physical checkout behind an already-running copy.
        // Process restart intentionally clears this set; the Server then redelivers non-terminal
        // work and the materializer verifies/replays the atomically published directory.
        let mut workspace_deliveries = BTreeSet::<String>::new();
        let mut workers = tokio::task::JoinSet::<(JobId, AgentDaemonResult<()>)>::new();
        loop {
            while workers.len() < MAX_CONCURRENT_JOBS {
                let next_job = queued
                    .iter()
                    .find(|(job_id, items)| !items.is_empty() && !running.contains(*job_id))
                    .map(|(job_id, _)| job_id.clone());
                let Some(job_id) = next_job else { break };
                let item = queued
                    .get_mut(&job_id)
                    .and_then(VecDeque::pop_front)
                    .expect("selected Job queue must contain work");
                running.insert(job_id.clone());
                let tenant_id = tenant_id.clone();
                let processor = Arc::clone(&processor);
                let fence = fence.clone();
                workers.spawn(async move {
                    let result = execute_fenced_work(&tenant_id, processor, &fence, item).await;
                    (job_id, result)
                });
            }

            tokio::select! {
                item = receiver.recv() => {
                    let Some(item) = item else {
                        while let Some(joined) = workers.join_next().await {
                            if let Ok((_, Err(error))) = joined {
                                let _ = errors_tx.send(error).await;
                            }
                        }
                        return;
                    };
                    if item
                        .work
                        .workspace_delivery_token()
                        .is_some_and(|token| !workspace_deliveries.insert(token))
                    {
                        continue;
                    }
                    queued.entry(item.work.job_id().clone()).or_default().push_back(item);
                }
                joined = workers.join_next(), if !workers.is_empty() => {
                    match joined {
                        Some(Ok((job_id, result))) => {
                            running.remove(&job_id);
                            if queued.get(&job_id).is_some_and(VecDeque::is_empty) {
                                queued.remove(&job_id);
                            }
                            if let Err(error) = result {
                                let _ = errors_tx.send(error).await;
                                return;
                            }
                        }
                        Some(Err(error)) => {
                            let _ = errors_tx.send(AgentDaemonError::Session(format!(
                                "Agent Job worker failed: {error}"
                            ))).await;
                            return;
                        }
                        None => {}
                    }
                }
            }
        }
    });
    AgentWorkDispatcher {
        sender,
        errors,
        task,
    }
}

async fn execute_fenced_work(
    tenant_id: &TenantId,
    processor: Arc<dyn AgentMessageProcessor>,
    fence: &SharedSessionFence,
    item: FencedAgentWork,
) -> AgentDaemonResult<()> {
    if fence.get()?.session_generation != item.generation {
        return Err(AgentDaemonError::Session(
            "discarded control message from a stale session generation".to_owned(),
        ));
    }
    match item.work {
        AgentWork::Assignment(assignment) => processor.handle_assignment(assignment).await,
        AgentWork::Recovery(record) => processor.recover_assignment(record).await,
        AgentWork::Decision(decision) => processor.handle_decision(tenant_id, decision).await,
    }
}

#[cfg(test)]
mod tests {
    use neoengram_protocol::{
        ControlError, ErrorCode, Extensions, MessageId, ProtocolVersion, SequenceNumber,
        SessionGeneration, UnixMillis, MAX_AGENT_CHANNEL_FRAME_BYTES,
    };

    use super::*;

    fn error_frame(sequence: u64) -> AgentChannelDownstreamFrame {
        AgentChannelDownstreamFrame {
            protocol_version: ProtocolVersion::V1,
            sequence: SequenceNumber::new(sequence),
            message_id: MessageId::new(format!("message-{sequence}")).unwrap(),
            correlation_id: None,
            session_generation: SessionGeneration::new(3),
            sent_at_unix_ms: UnixMillis::new(10),
            message: neoengram_protocol::AgentChannelDownstreamMessage::Error(ControlError {
                code: ErrorCode::new("TEST_ERROR").unwrap(),
                message: "test".to_owned(),
                retryable: false,
                retry_after_ms: None,
                extensions: Extensions::new(),
            }),
            extensions: Extensions::new(),
        }
    }

    #[tokio::test]
    async fn response_reader_handles_coalesced_and_split_frames() {
        let (chunks_tx, chunks_rx) = mpsc::channel(4);
        let (mut frames, reader) = spawn_response_reader(chunks_rx);
        let first = error_frame(1).encode_ndjson().unwrap();
        let second = error_frame(2).encode_ndjson().unwrap();
        let split = first.len() / 2;
        chunks_tx
            .send(Ok(Bytes::copy_from_slice(&first[..split])))
            .await
            .unwrap();
        let mut coalesced = first[split..].to_vec();
        coalesced.extend_from_slice(&second);
        chunks_tx.send(Ok(Bytes::from(coalesced))).await.unwrap();
        drop(chunks_tx);

        assert_eq!(frames.recv().await.unwrap().unwrap(), error_frame(1));
        assert_eq!(frames.recv().await.unwrap().unwrap(), error_frame(2));
        assert!(frames.recv().await.is_none());
        reader.await.unwrap();
    }

    #[tokio::test]
    async fn response_reader_enforces_limit_and_final_lf() {
        let (chunks_tx, chunks_rx) = mpsc::channel(4);
        let (mut frames, reader) = spawn_response_reader(chunks_rx);
        chunks_tx
            .send(Ok(Bytes::from(vec![
                b' ';
                MAX_AGENT_CHANNEL_FRAME_BYTES + 1
            ])))
            .await
            .unwrap();
        drop(chunks_tx);
        assert!(frames.recv().await.unwrap().is_err());
        reader.await.unwrap();

        let (chunks_tx, chunks_rx) = mpsc::channel(4);
        let (mut frames, reader) = spawn_response_reader(chunks_rx);
        chunks_tx.send(Ok(Bytes::from_static(b"{}"))).await.unwrap();
        drop(chunks_tx);
        assert!(frames.recv().await.unwrap().is_err());
        reader.await.unwrap();
    }

    #[tokio::test]
    async fn response_reader_rejects_duplicate_json_keys() {
        let (chunks_tx, chunks_rx) = mpsc::channel(4);
        let (mut frames, reader) = spawn_response_reader(chunks_rx);
        chunks_tx
            .send(Ok(Bytes::from_static(
                b"{\"protocol_version\":\"1\",\"protocol_version\":\"1\"}\n",
            )))
            .await
            .unwrap();
        drop(chunks_tx);
        assert!(frames.recv().await.unwrap().is_err());
        reader.await.unwrap();
    }
}
