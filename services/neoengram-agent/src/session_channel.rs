use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    fmt,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use crate::LedgerRecord;
use bytes::Bytes;
use neoengram_domain::protocol::{
    AgentChannelDownstreamFrame, AgentChannelNdjsonDecoder, AgentChannelUpstreamFrame,
    AgentResourceLifecycleAssignment, AssignmentOperation, JobAssignment, JobDecision,
    ReplicationAssignment, SessionGeneration, TenantId,
};
use tokio::{
    sync::mpsc,
    task::JoinHandle,
    time::{self, MissedTickBehavior},
};

use crate::{
    AgentDaemonError, AgentDaemonResult, AgentMessageProcessor, AgentSessionClientError,
    SharedSessionFence,
};

const CHANNEL_BUFFER: usize = 64;
const MAX_CONCURRENT_JOBS: usize = 16;
const REPLICATION_HISTORY_SWEEP_INTERVAL: Duration = Duration::from_secs(60);

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
            // EOF with bytes still buffered means the authenticated transport was truncated in
            // flight (for example, a Gateway closed after forwarding only a partial frame). It is
            // different from a complete LF-terminated frame that fails JSON/protocol validation:
            // the former must drive session reconnect so durable outbox work can be retried.
            let _ = frames_tx
                .send(Err(AgentSessionClientError::transport(error)))
                .await;
        }
    });
    (frames_rx, reader)
}

#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
pub(crate) enum AgentWork {
    Assignment(JobAssignment),
    Replication(ReplicationAssignment),
    Lifecycle(AgentResourceLifecycleAssignment),
    Recovery(LedgerRecord),
    Decision(JobDecision),
}

impl AgentWork {
    /// The key that determines execution ordering. Replication attempts for one logical
    /// replication must never run concurrently, even when Central has already advanced to a
    /// later attempt while an older worker is still unwinding.
    fn execution_key(&self) -> String {
        match self {
            Self::Assignment(assignment) => match &assignment.assignment {
                AssignmentOperation::Add { input, .. } => input.job_id.to_string(),
                AssignmentOperation::WorkspaceMaterialize { input, .. } => input.job_id.to_string(),
                AssignmentOperation::SnapshotDelivery { input, .. } => input.job_id.to_string(),
            },
            Self::Replication(assignment) => format!("replication:{}", assignment.replication_id),
            Self::Lifecycle(assignment) => {
                // Every phase and retry in one deletion batch mutates the same durable
                // quarantine journal. Keep those commands ordered even when Central redelivers
                // adjacent Saga steps on the same control stream.
                format!("lifecycle:{}", assignment.assignment.deletion_id)
            }
            Self::Recovery(record) => record.key.job_id.to_string(),
            Self::Decision(decision) => decision.job_id.to_string(),
        }
    }

    /// The key used to coalesce a redelivered command. The attempt is intentionally part of this
    /// key so a newly retried attempt can queue behind an older attempt without being discarded.
    fn redelivery_key(&self) -> String {
        match self {
            Self::Replication(assignment) => format!(
                "replication:{}:{}",
                assignment.replication_id, assignment.attempt
            ),
            _ => self.execution_key(),
        }
    }

    /// Replication assignments are intentionally redeliverable until Central observes the
    /// terminal publication.  Other command types use their message ID as a per-connection
    /// delivery fence and must not be admitted again on the same channel.
    const fn is_redeliverable(&self) -> bool {
        matches!(self, Self::Replication(_))
    }

    fn redelivery_deadline_unix_ms(&self) -> Option<u64> {
        match self {
            Self::Replication(assignment) => {
                Some(assignment.signed_ticket.as_ticket().deadline_unix_ms.get())
            }
            _ => None,
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
            AssignmentOperation::SnapshotDelivery { input, .. } => (
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ReplicationAttemptHistory {
    attempt: u64,
    retain_until_unix_ms: u64,
}

fn prune_replication_history(
    now_unix_ms: u64,
    active_replications: &BTreeSet<String>,
    completed_redeliveries: &mut BTreeMap<String, u64>,
    highest_replication_attempts: &mut BTreeMap<String, ReplicationAttemptHistory>,
) {
    completed_redeliveries.retain(|_, deadline| *deadline > now_unix_ms);
    highest_replication_attempts.retain(|execution_key, history| {
        history.retain_until_unix_ms > now_unix_ms || active_replications.contains(execution_key)
    });
}

fn current_unix_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|value| value.as_millis().min(u64::MAX as u128) as u64)
        .unwrap_or(0)
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
        let mut queued = BTreeMap::<String, VecDeque<FencedAgentWork>>::new();
        let mut running = BTreeSet::<String>::new();
        // A Replication stays deliverable until Central consumes its terminal report. Remember
        // locally completed attempts so that window does not execute the same transfer again.
        // The attempt is part of the redelivery key, so an explicit retry remains admissible.
        let mut completed_redeliveries = BTreeMap::<String, u64>::new();
        // Tracks queued and running redeliverable attempts. This is separate from the execution
        // key: attempt N+1 may be queued while attempt N is still running, but duplicate delivery
        // of either concrete attempt must be coalesced.
        let mut in_flight_redeliveries = BTreeMap::<String, u64>::new();
        // Preserve one coalesced delivery that arrived while its concrete attempt was already in
        // flight. If the worker exits with a transient transport error, this closes the race where
        // that delivery would otherwise be dropped just before the in-flight marker is cleared.
        let mut pending_redeliveries = BTreeMap::<String, FencedAgentWork>::new();
        // Central advances attempts monotonically. Once this dispatcher has observed attempt N,
        // an older delivery must not execute after N and overwrite its shared transfer staging.
        let mut highest_replication_attempts = BTreeMap::<String, ReplicationAttemptHistory>::new();
        // The dispatcher survives H2 reconnects. Keep the immutable materialization claim so a
        // reconnect cannot enqueue the same physical checkout behind an already-running copy.
        // Process restart intentionally clears this set; the Server then redelivers non-terminal
        // work and the materializer verifies/replays the atomically published directory.
        let mut workspace_deliveries = BTreeSet::<String>::new();
        let mut workers =
            tokio::task::JoinSet::<(String, Option<(String, u64)>, AgentDaemonResult<()>)>::new();
        let mut history_sweep = time::interval(REPLICATION_HISTORY_SWEEP_INTERVAL);
        history_sweep.set_missed_tick_behavior(MissedTickBehavior::Delay);
        history_sweep.tick().await;
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
                let redelivery = item.work.is_redeliverable().then(|| {
                    (
                        item.work.redelivery_key(),
                        item.work
                            .redelivery_deadline_unix_ms()
                            .expect("redeliverable work must carry a deadline"),
                    )
                });
                let execution_key = job_id.clone();
                workers.spawn(async move {
                    let result = execute_fenced_work(&tenant_id, processor, &fence, item).await;
                    (execution_key, redelivery, result)
                });
            }

            tokio::select! {
                item = receiver.recv() => {
                    let Some(item) = item else {
                        while let Some(joined) = workers.join_next().await {
                            if let Ok((_, _, Err(error))) = joined {
                                let _ = errors_tx.send(error).await;
                            }
                        }
                        return;
                    };
                    let execution_key = item.work.execution_key();
                    let redelivery = item.work.is_redeliverable().then(|| {
                        (
                            item.work.redelivery_key(),
                            item.work
                                .redelivery_deadline_unix_ms()
                                .expect("redeliverable work must carry a deadline"),
                        )
                    });
                    if let AgentWork::Replication(assignment) = &item.work {
                        let highest_attempt = highest_replication_attempts
                            .get(&execution_key)
                            .map(|history| history.attempt)
                            .unwrap_or(0);
                        if assignment.attempt < highest_attempt {
                            continue;
                        }
                        if assignment.attempt > highest_attempt {
                            highest_replication_attempts.insert(
                                execution_key.clone(),
                                ReplicationAttemptHistory {
                                    attempt: assignment.attempt,
                                    retain_until_unix_ms: assignment
                                        .signed_ticket
                                        .as_ticket()
                                        .deadline_unix_ms
                                        .get(),
                                },
                            );
                            // A still-queued older attempt has not touched staging yet and can be
                            // superseded immediately. A running attempt remains fenced by the
                            // execution key and is allowed to unwind before the new attempt starts.
                            if let Some(items) = queued.get_mut(&execution_key) {
                                items.retain(|queued_item| {
                                    let obsolete = matches!(
                                        &queued_item.work,
                                        AgentWork::Replication(queued_assignment)
                                            if queued_assignment.attempt < assignment.attempt
                                    );
                                    if obsolete {
                                        in_flight_redeliveries
                                            .remove(&queued_item.work.redelivery_key());
                                    }
                                    !obsolete
                                });
                            }
                            pending_redeliveries.retain(|_, pending_item| {
                                pending_item.work.execution_key() != execution_key
                                    || !matches!(
                                        &pending_item.work,
                                        AgentWork::Replication(pending_assignment)
                                            if pending_assignment.attempt < assignment.attempt
                                )
                            });
                        } else if let Some(history) =
                            highest_replication_attempts.get_mut(&execution_key)
                        {
                            history.retain_until_unix_ms = history.retain_until_unix_ms.max(
                                assignment
                                    .signed_ticket
                                    .as_ticket()
                                    .deadline_unix_ms
                                    .get(),
                            );
                        }
                    }
                    // The Gateway periodically redelivers active replication attempts. Coalesce
                    // one concrete attempt while it is running or queued, retaining the newest
                    // signed ticket so a queued attempt does not start with an avoidably stale
                    // capability. A later attempt still enters the same execution queue.
                    if let Some((redelivery_key, redelivery_deadline)) = redelivery.as_ref() {
                        if let Some(completed_deadline) =
                            completed_redeliveries.get_mut(redelivery_key)
                        {
                            *completed_deadline = (*completed_deadline).max(*redelivery_deadline);
                            continue;
                        }
                        if let Some(in_flight_deadline) =
                            in_flight_redeliveries.get_mut(redelivery_key)
                        {
                            *in_flight_deadline = (*in_flight_deadline).max(*redelivery_deadline);
                            if let Some(queued_item) = queued
                                .get_mut(&execution_key)
                                .and_then(|items| {
                                    items.iter_mut().find(|queued_item| {
                                        queued_item.work.is_redeliverable()
                                            && queued_item.work.redelivery_key() == *redelivery_key
                                    })
                                })
                            {
                                *queued_item = item;
                                continue;
                            }
                            pending_redeliveries.insert(redelivery_key.clone(), item);
                            continue;
                        }
                        in_flight_redeliveries
                            .insert(redelivery_key.clone(), *redelivery_deadline);
                    }
                    if item
                        .work
                        .workspace_delivery_token()
                        .is_some_and(|token| !workspace_deliveries.insert(token))
                    {
                        continue;
                    }
                    queued.entry(execution_key).or_default().push_back(item);
                }
                joined = workers.join_next(), if !workers.is_empty() => {
                    match joined {
                        Some(Ok((execution_key, redelivery, result))) => {
                            running.remove(&execution_key);
                            if queued.get(&execution_key).is_some_and(VecDeque::is_empty) {
                                queued.remove(&execution_key);
                            }
                            let observed_deadline = redelivery.as_ref().map(|(key, deadline)| {
                                in_flight_redeliveries.remove(key).unwrap_or(*deadline)
                            });
                            match result {
                                Ok(()) => {
                                    if let Some((redelivery_key, deadline)) = redelivery {
                                        pending_redeliveries.remove(&redelivery_key);
                                        completed_redeliveries.insert(
                                            redelivery_key,
                                            observed_deadline.unwrap_or(deadline),
                                        );
                                    }
                                }
                                // A transient Replication transport failure deliberately keeps
                                // the attempt active. Do not terminate the control dispatcher;
                                // use one delivery coalesced while the worker was running, or wait
                                // for Central's next delivery tick when there is no pending item.
                                Err(AgentDaemonError::SessionTransport(_))
                                    if redelivery.is_some() => {
                                        let (redelivery_key, deadline) = redelivery
                                            .expect("guarded redelivery must be present");
                                        if let Some(item) = pending_redeliveries.remove(&redelivery_key) {
                                            let execution_key = item.work.execution_key();
                                            let current = match &item.work {
                                                AgentWork::Replication(assignment) => {
                                                    highest_replication_attempts
                                                        .get(&execution_key)
                                                        .is_none_or(|highest| assignment.attempt == highest.attempt)
                                                }
                                                _ => true,
                                            };
                                            if current {
                                                let pending_deadline = item
                                                    .work
                                                    .redelivery_deadline_unix_ms()
                                                    .unwrap_or(deadline);
                                                in_flight_redeliveries.insert(
                                                    redelivery_key,
                                                    observed_deadline
                                                        .unwrap_or(deadline)
                                                        .max(pending_deadline),
                                                );
                                                queued
                                                    .entry(execution_key)
                                                    .or_default()
                                                    .push_front(item);
                                            }
                                        }
                                    }
                                Err(error) => {
                                    let _ = errors_tx.send(error).await;
                                    return;
                                }
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
                _ = history_sweep.tick() => {
                    let mut active_replications = running.clone();
                    active_replications.extend(
                        queued
                            .iter()
                            .filter(|(_, items)| !items.is_empty())
                            .map(|(execution_key, _)| execution_key.clone()),
                    );
                    active_replications.extend(
                        pending_redeliveries
                            .values()
                            .map(|item| item.work.execution_key()),
                    );
                    prune_replication_history(
                        current_unix_millis(),
                        &active_replications,
                        &mut completed_redeliveries,
                        &mut highest_replication_attempts,
                    );
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
        AgentWork::Replication(assignment) => processor.handle_replication(assignment).await,
        AgentWork::Lifecycle(assignment) => {
            if assignment.session_generation != item.generation {
                return Err(AgentDaemonError::Session(
                    "discarded lifecycle command from a stale session generation".to_owned(),
                ));
            }
            processor.handle_lifecycle_assignment(assignment).await
        }
        AgentWork::Recovery(record) => processor.recover_assignment(record).await,
        AgentWork::Decision(decision) => processor.handle_decision(tenant_id, decision).await,
    }
}

#[cfg(test)]
mod tests {
    use crate::AgentSessionFence;
    use async_trait::async_trait;
    use neoengram_domain::protocol::{
        AgentId, ArtifactId, CentralSignedPayload, CertificateGeneration, ControlError, DecimalU64,
        Ed25519Signature, EdgeClusterId, ErrorCode, Extensions, GatewayOpaqueBytes, GatewayPoolId,
        MessageId, MountGeneration, ObjectSet, PlacementId, RouteGeneration, SequenceNumber,
        SessionGeneration, SessionId, SignedTransferTicket, StorageVolumeId, TransferEndpoint,
        TransferId, TransferTicket, UnixMillis, CURRENT_WIRE_VERSION,
        MAX_AGENT_CHANNEL_FRAME_BYTES,
    };
    use neoengram_domain::{CommitId, ContentDigest};

    use super::*;

    fn error_frame(sequence: u64) -> AgentChannelDownstreamFrame {
        AgentChannelDownstreamFrame {
            wire_version: CURRENT_WIRE_VERSION,
            sequence: SequenceNumber::new(sequence),
            message_id: MessageId::new(format!("message-{sequence}")).unwrap(),
            correlation_id: None,
            session_generation: SessionGeneration::new(3),
            sent_at_unix_ms: UnixMillis::new(10),
            central_signature: None,
            message: neoengram_domain::protocol::AgentChannelDownstreamMessage::Error(
                ControlError {
                    code: ErrorCode::new("TEST_ERROR").unwrap(),
                    message: "test".to_owned(),
                    retryable: false,
                    retry_after_ms: None,
                    extensions: Extensions::new(),
                },
            ),
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
        let error = frames.recv().await.unwrap().unwrap_err();
        assert!(!error.retryable());
        assert!(!error.transient());
        reader.await.unwrap();

        let (chunks_tx, chunks_rx) = mpsc::channel(4);
        let (mut frames, reader) = spawn_response_reader(chunks_rx);
        chunks_tx.send(Ok(Bytes::from_static(b"{}"))).await.unwrap();
        drop(chunks_tx);
        let error = frames.recv().await.unwrap().unwrap_err();
        assert!(error.retryable());
        assert!(error.transient());
        reader.await.unwrap();
    }

    #[tokio::test]
    async fn response_reader_rejects_duplicate_json_keys() {
        let (chunks_tx, chunks_rx) = mpsc::channel(4);
        let (mut frames, reader) = spawn_response_reader(chunks_rx);
        chunks_tx
            .send(Ok(Bytes::from_static(
                b"{\"wire_version\":\"1\",\"wire_version\":\"1\"}\n",
            )))
            .await
            .unwrap();
        drop(chunks_tx);
        assert!(frames.recv().await.unwrap().is_err());
        reader.await.unwrap();
    }

    #[test]
    fn replication_history_prunes_only_expired_idle_entries() {
        let mut completed_redeliveries = BTreeMap::from([
            ("completed-expired".to_owned(), 100),
            ("completed-current".to_owned(), 101),
        ]);
        let mut highest_replication_attempts = BTreeMap::from([
            (
                "replication:expired-idle".to_owned(),
                ReplicationAttemptHistory {
                    attempt: 3,
                    retain_until_unix_ms: 100,
                },
            ),
            (
                "replication:expired-active".to_owned(),
                ReplicationAttemptHistory {
                    attempt: 4,
                    retain_until_unix_ms: 99,
                },
            ),
            (
                "replication:current".to_owned(),
                ReplicationAttemptHistory {
                    attempt: 5,
                    retain_until_unix_ms: 101,
                },
            ),
        ]);
        let active_replications = BTreeSet::from(["replication:expired-active".to_owned()]);

        prune_replication_history(
            100,
            &active_replications,
            &mut completed_redeliveries,
            &mut highest_replication_attempts,
        );

        assert_eq!(
            completed_redeliveries,
            BTreeMap::from([("completed-current".to_owned(), 101)])
        );
        assert!(!highest_replication_attempts.contains_key("replication:expired-idle"));
        assert!(highest_replication_attempts.contains_key("replication:expired-active"));
        assert!(highest_replication_attempts.contains_key("replication:current"));
    }

    #[derive(Debug)]
    struct ControlledReplicationProcessor {
        starts: tokio::sync::mpsc::UnboundedSender<(u64, u64)>,
        results: tokio::sync::Mutex<tokio::sync::mpsc::UnboundedReceiver<AgentDaemonResult<()>>>,
    }

    #[async_trait]
    impl AgentMessageProcessor for ControlledReplicationProcessor {
        async fn handle_assignment(&self, _assignment: JobAssignment) -> AgentDaemonResult<()> {
            Ok(())
        }

        async fn handle_replication(
            &self,
            assignment: ReplicationAssignment,
        ) -> AgentDaemonResult<()> {
            self.starts
                .send((
                    assignment.attempt,
                    assignment
                        .signed_ticket
                        .central_signature
                        .signed_at_unix_ms
                        .get(),
                ))
                .map_err(|_| {
                    AgentDaemonError::Session("replication start observer closed".to_owned())
                })?;
            self.results.lock().await.recv().await.unwrap_or_else(|| {
                Err(AgentDaemonError::Session(
                    "replication result controller closed".to_owned(),
                ))
            })
        }

        async fn handle_lifecycle_assignment(
            &self,
            _assignment: AgentResourceLifecycleAssignment,
        ) -> AgentDaemonResult<()> {
            Ok(())
        }

        async fn recover_assignment(&self, _record: LedgerRecord) -> AgentDaemonResult<()> {
            Ok(())
        }

        async fn handle_decision(
            &self,
            _tenant_id: &TenantId,
            _decision: JobDecision,
        ) -> AgentDaemonResult<()> {
            Ok(())
        }
    }

    fn replication_endpoint(name: &str) -> TransferEndpoint {
        TransferEndpoint {
            placement_id: PlacementId::new(format!("placement-{name}")).unwrap(),
            agent_id: AgentId::new(format!("agent-{name}")).unwrap(),
            gateway_pool_id: GatewayPoolId::new(format!("pool-{name}")).unwrap(),
            edge_cluster_id: EdgeClusterId::new(format!("cluster-{name}")).unwrap(),
            storage_volume_id: Some(StorageVolumeId::new(format!("volume-{name}")).unwrap()),
        }
    }

    fn replication_assignment(attempt: u64) -> ReplicationAssignment {
        replication_assignment_signed_at(attempt, 1)
    }

    fn replication_assignment_signed_at(
        attempt: u64,
        signed_at_unix_ms: u64,
    ) -> ReplicationAssignment {
        let tenant_id = TenantId::new("tenant-redelivery").unwrap();
        let artifact_id = ArtifactId::new("artifact-redelivery").unwrap();
        let commit_id = CommitId::from_bytes([7; 32]);
        let object_set = ObjectSet::new(Vec::new()).unwrap();
        let ticket = TransferTicket {
            transfer_id: TransferId::new("transfer-redelivery").unwrap(),
            tenant_id: tenant_id.clone(),
            artifact_id: artifact_id.clone(),
            commit_id,
            object_set_digest: object_set.object_set_digest,
            source: replication_endpoint("source"),
            target: replication_endpoint("target"),
            source_session_generation: SessionGeneration::new(1),
            source_mount_generation: MountGeneration::new(1),
            source_route_generation: RouteGeneration::new(1),
            session_generation: SessionGeneration::new(1),
            mount_generation: MountGeneration::new(1),
            route_generation: RouteGeneration::new(1),
            deadline_unix_ms: UnixMillis::new(u64::MAX),
            max_bytes: DecimalU64::new(0),
            allowed_objects: Vec::new(),
        };
        let payload =
            GatewayOpaqueBytes::new(SignedTransferTicket::payload_bytes(&ticket).unwrap()).unwrap();
        let signed_ticket = SignedTransferTicket::new(
            ticket,
            CentralSignedPayload {
                key_id: "test-redelivery".to_owned(),
                certificate_generation: CertificateGeneration::new(1),
                signed_at_unix_ms: UnixMillis::new(signed_at_unix_ms),
                expires_at_unix_ms: UnixMillis::new(u64::MAX),
                payload_digest: ContentDigest::hash(payload.as_bytes()),
                payload,
                signature: Ed25519Signature::from_bytes([0; 64]),
                extensions: Extensions::new(),
            },
        )
        .unwrap();
        ReplicationAssignment {
            replication_id: neoengram_domain::protocol::ReplicationId::new(
                "replication-redelivery",
            )
            .unwrap(),
            tenant_id,
            artifact_id,
            commit_id,
            attempt,
            signed_ticket,
            object_set,
            extensions: Extensions::new(),
        }
    }

    async fn send_replication(
        dispatcher: &AgentWorkDispatcher,
        attempt: u64,
    ) -> AgentDaemonResult<()> {
        send_replication_assignment(dispatcher, replication_assignment(attempt)).await
    }

    async fn send_replication_assignment(
        dispatcher: &AgentWorkDispatcher,
        assignment: ReplicationAssignment,
    ) -> AgentDaemonResult<()> {
        dispatcher
            .sender
            .send(FencedAgentWork {
                generation: SessionGeneration::new(1),
                work: AgentWork::Replication(assignment),
            })
            .await
            .map_err(|_| AgentDaemonError::Session("dispatcher closed".to_owned()))
    }

    async fn next_start(
        starts: &mut tokio::sync::mpsc::UnboundedReceiver<(u64, u64)>,
    ) -> (u64, u64) {
        tokio::time::timeout(std::time::Duration::from_secs(1), starts.recv())
            .await
            .expect("replication did not start")
            .expect("replication start observer closed")
    }

    async fn assert_no_start(starts: &mut tokio::sync::mpsc::UnboundedReceiver<(u64, u64)>) {
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(50), starts.recv())
                .await
                .is_err(),
            "an overlapping or duplicate replication started"
        );
    }

    #[tokio::test]
    async fn replication_transient_redelivery_stops_after_a_newer_attempt_is_observed() {
        let fence = SharedSessionFence::default();
        fence
            .replace(Some(AgentSessionFence {
                session_id: SessionId::new("session-redelivery").unwrap(),
                session_generation: SessionGeneration::new(1),
            }))
            .unwrap();
        let (starts_tx, mut starts_rx) = tokio::sync::mpsc::unbounded_channel();
        let (results_tx, results_rx) = tokio::sync::mpsc::unbounded_channel();
        let processor = Arc::new(ControlledReplicationProcessor {
            starts: starts_tx,
            results: tokio::sync::Mutex::new(results_rx),
        });
        let dispatcher = spawn_work_dispatcher(
            TenantId::new("tenant-redelivery").unwrap(),
            processor,
            fence,
        );

        send_replication(&dispatcher, 1).await.unwrap();
        assert_eq!(next_start(&mut starts_rx).await, (1, 1));
        send_replication(&dispatcher, 1).await.unwrap();
        assert_no_start(&mut starts_rx).await;

        results_tx
            .send(Err(AgentDaemonError::SessionTransport(
                "temporary QUIC outage".to_owned(),
            )))
            .unwrap();
        // The duplicate delivered while attempt 1 was running is retained and admitted only
        // after that worker reports the transient transport failure.
        assert_eq!(next_start(&mut starts_rx).await, (1, 1));

        // Once attempt 2 is observed, another delivery of attempt 1 is stale even though the
        // currently running attempt 1 has not completed yet.
        send_replication(&dispatcher, 2).await.unwrap();
        send_replication(&dispatcher, 1).await.unwrap();
        assert_no_start(&mut starts_rx).await;
        results_tx.send(Ok(())).unwrap();
        assert_eq!(next_start(&mut starts_rx).await, (2, 1));
        send_replication(&dispatcher, 1).await.unwrap();
        results_tx.send(Ok(())).unwrap();
        assert_no_start(&mut starts_rx).await;
    }

    #[tokio::test]
    async fn newer_replication_attempt_discards_queued_stale_attempts() {
        let fence = SharedSessionFence::default();
        fence
            .replace(Some(AgentSessionFence {
                session_id: SessionId::new("session-supersession").unwrap(),
                session_generation: SessionGeneration::new(1),
            }))
            .unwrap();
        let (starts_tx, mut starts_rx) = tokio::sync::mpsc::unbounded_channel();
        let (results_tx, results_rx) = tokio::sync::mpsc::unbounded_channel();
        let processor = Arc::new(ControlledReplicationProcessor {
            starts: starts_tx,
            results: tokio::sync::Mutex::new(results_rx),
        });
        let dispatcher = spawn_work_dispatcher(
            TenantId::new("tenant-redelivery").unwrap(),
            processor,
            fence,
        );

        send_replication(&dispatcher, 1).await.unwrap();
        assert_eq!(next_start(&mut starts_rx).await, (1, 1));
        send_replication(&dispatcher, 2).await.unwrap();
        send_replication(&dispatcher, 3).await.unwrap();
        assert_no_start(&mut starts_rx).await;

        // Attempt 3 supersedes queued attempt 2 but still waits for running attempt 1 to unwind.
        results_tx.send(Ok(())).unwrap();
        assert_eq!(next_start(&mut starts_rx).await, (3, 1));
        send_replication(&dispatcher, 2).await.unwrap();
        send_replication(&dispatcher, 3).await.unwrap();
        assert_no_start(&mut starts_rx).await;
        results_tx.send(Ok(())).unwrap();
        assert_no_start(&mut starts_rx).await;
    }

    #[tokio::test]
    async fn queued_replication_redelivery_replaces_an_older_signed_ticket() {
        let fence = SharedSessionFence::default();
        fence
            .replace(Some(AgentSessionFence {
                session_id: SessionId::new("session-ticket-refresh").unwrap(),
                session_generation: SessionGeneration::new(1),
            }))
            .unwrap();
        let (starts_tx, mut starts_rx) = tokio::sync::mpsc::unbounded_channel();
        let (results_tx, results_rx) = tokio::sync::mpsc::unbounded_channel();
        let processor = Arc::new(ControlledReplicationProcessor {
            starts: starts_tx,
            results: tokio::sync::Mutex::new(results_rx),
        });
        let dispatcher = spawn_work_dispatcher(
            TenantId::new("tenant-redelivery").unwrap(),
            processor,
            fence,
        );

        send_replication(&dispatcher, 1).await.unwrap();
        assert_eq!(next_start(&mut starts_rx).await, (1, 1));
        send_replication_assignment(&dispatcher, replication_assignment_signed_at(2, 10))
            .await
            .unwrap();
        send_replication_assignment(&dispatcher, replication_assignment_signed_at(2, 20))
            .await
            .unwrap();
        assert_no_start(&mut starts_rx).await;

        results_tx.send(Ok(())).unwrap();
        assert_eq!(next_start(&mut starts_rx).await, (2, 20));
        results_tx.send(Ok(())).unwrap();
    }
}
