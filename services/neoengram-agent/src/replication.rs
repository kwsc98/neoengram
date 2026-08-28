//! Agent-side object replication worker.
//!
//! Execution is deliberately separate from Central authority. The supplied progress sink
//! persists checkpoints and publishes the complete PlacementSet only after every object has
//! passed the runtime CAS verification.

use std::{
    collections::BTreeMap,
    future::Future,
    sync::{Arc, Mutex},
};

use neoengram_domain::core::{ContentDigest, ObjectId};
use neoengram_domain::protocol::{
    AgentId, CommitObject, MountGeneration, ReplicationAssignment, ReplicationId,
    ReplicationObjectState, ReplicationProgressReport, ReplicationState, SignedTransferTicket,
    TenantId, TransferId, TransferTicket, UnixMillis,
};
use neoengram_runtime::{
    ObjectBackend, ObjectSetTransferExecutor, ObjectTransferOutcome, TransferSink, TransferSource,
};

use crate::{AgentDaemonError, AgentDaemonResult, AgentReport, Clock, OutboundReportQueue};
use crate::{
    CentralCommandTrustBundle, FilesystemExecution, QuicTransferClientConfig, QuicTransferIdentity,
    QuicTransferNetwork, SharedSessionFence,
};

const REMOTE_TRANSFER_MAX_RETRIES: u32 = 8;
const REMOTE_TRANSFER_INITIAL_RETRY_DELAY: std::time::Duration =
    std::time::Duration::from_millis(100);
const REMOTE_TRANSFER_MAX_RETRY_DELAY: std::time::Duration = std::time::Duration::from_secs(5);

fn block_on<F>(future: F) -> F::Output
where
    F: Future,
{
    // Replication assignments are dispatched from `spawn_blocking`. Re-enter the process
    // runtime when available; the fallback keeps the public executor usable from synchronous
    // embedders and unit tests.
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        handle.block_on(future)
    } else {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("failed to create replication runtime")
            .block_on(future)
    }
}

/// Executes one Central-issued replication command. The data-plane adapter is deliberately
/// injected: a production Agent may connect the ticket's source/target Gateways, while tests and
/// same-host development can use the local CAS backend without changing control semantics.
pub trait ReplicationAssignmentExecutor: Send + Sync {
    fn execute(
        &self,
        assignment: &ReplicationAssignment,
        progress: &dyn ReplicationProgressSink,
    ) -> AgentDaemonResult<()>;
}

/// Artifact-scoped replication adapter for a mounted Agent Volume.
///
/// The object transfer kernel is intentionally reusable across transports. Until the Gateway
/// QUIC source/sink is installed, this production adapter supports only the explicit same-volume
/// case; a cross-Agent assignment fails closed after emitting a durable `Failed` checkpoint rather
/// than reading an arbitrary local path as if it were the signed source placement.
#[derive(Debug, Clone)]
pub struct MountedVolumeReplicationExecutor {
    execution: Arc<FilesystemExecution>,
    local_tenant_id: TenantId,
    local_agent_id: AgentId,
    local_volume_id: neoengram_domain::protocol::StorageVolumeId,
    trust_bundle: Option<Arc<CentralCommandTrustBundle>>,
    /// Optional Gateway-routed QUIC network. When absent, remote source assignments fail closed.
    network: Option<Arc<QuicTransferNetwork>>,
    /// The live control-session fence used to reject tickets issued for a replaced target session.
    session_fence: Option<SharedSessionFence>,
    local_mount_generation: Option<MountGeneration>,
    clock: Arc<dyn Clock>,
    worker: ReplicationWorker,
}

impl MountedVolumeReplicationExecutor {
    #[must_use]
    pub fn new(
        execution: Arc<FilesystemExecution>,
        local_tenant_id: TenantId,
        local_agent_id: AgentId,
        local_volume_id: neoengram_domain::protocol::StorageVolumeId,
        trust_bundle: Option<Arc<CentralCommandTrustBundle>>,
        clock: Arc<dyn Clock>,
    ) -> Self {
        Self::new_with_network(
            execution,
            local_tenant_id,
            local_agent_id,
            local_volume_id,
            trust_bundle,
            clock,
            None,
        )
    }

    #[must_use]
    pub fn new_with_network(
        execution: Arc<FilesystemExecution>,
        local_tenant_id: TenantId,
        local_agent_id: AgentId,
        local_volume_id: neoengram_domain::protocol::StorageVolumeId,
        trust_bundle: Option<Arc<CentralCommandTrustBundle>>,
        clock: Arc<dyn Clock>,
        network: Option<Arc<QuicTransferNetwork>>,
    ) -> Self {
        Self::new_with_network_and_session_fence(
            execution,
            local_tenant_id,
            local_agent_id,
            local_volume_id,
            trust_bundle,
            clock,
            network,
            None,
            None,
        )
    }

    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn new_with_network_and_session_fence(
        execution: Arc<FilesystemExecution>,
        local_tenant_id: TenantId,
        local_agent_id: AgentId,
        local_volume_id: neoengram_domain::protocol::StorageVolumeId,
        trust_bundle: Option<Arc<CentralCommandTrustBundle>>,
        clock: Arc<dyn Clock>,
        network: Option<Arc<QuicTransferNetwork>>,
        session_fence: Option<SharedSessionFence>,
        local_mount_generation: Option<MountGeneration>,
    ) -> Self {
        Self {
            execution,
            local_tenant_id,
            local_agent_id,
            local_volume_id,
            trust_bundle,
            network,
            session_fence,
            local_mount_generation,
            clock,
            worker: ReplicationWorker::default(),
        }
    }

    fn fail(
        assignment: &ReplicationAssignment,
        progress: &dyn ReplicationProgressSink,
        message: impl Into<String>,
    ) -> AgentDaemonResult<()> {
        let message = message.into();
        let _ = progress.state(&assignment.replication_id, ReplicationState::Failed);
        Err(AgentDaemonError::Session(message))
    }
}

impl ReplicationAssignmentExecutor for MountedVolumeReplicationExecutor {
    fn execute(
        &self,
        assignment: &ReplicationAssignment,
        progress: &dyn ReplicationProgressSink,
    ) -> AgentDaemonResult<()> {
        let ticket = assignment.signed_ticket.as_ticket();
        if assignment.tenant_id != self.local_tenant_id {
            return Self::fail(
                assignment,
                progress,
                "replication assignment tenant is not bound to this Agent",
            );
        }
        if ticket.target.agent_id != self.local_agent_id
            || ticket.target.storage_volume_id.as_ref() != Some(&self.local_volume_id)
        {
            return Self::fail(
                assignment,
                progress,
                "replication assignment target is not bound to this Agent Volume",
            );
        }
        let Some(trust_bundle) = &self.trust_bundle else {
            return Self::fail(
                assignment,
                progress,
                "Central transfer-ticket trust bundle is not configured",
            );
        };
        let target = self
            .execution
            .replication_backend(assignment.tenant_id.clone(), assignment.artifact_id.clone())?;
        let same_local_source = ticket.source.agent_id == self.local_agent_id
            && ticket.source.storage_volume_id.as_ref() == Some(&self.local_volume_id);
        if !same_local_source {
            let Some(network) = &self.network else {
                return Self::fail(
                    assignment,
                    progress,
                    "remote replication source transport is not configured on this Agent",
                );
            };
            progress.state(&assignment.replication_id, ReplicationState::Planning)?;
            progress.state(&assignment.replication_id, ReplicationState::Transferring)?;
            let mut retry_delay = REMOTE_TRANSFER_INITIAL_RETRY_DELAY;
            let mut transfer_error = None;
            let transfer_result = 'transfer: {
                for retry in 0..=REMOTE_TRANSFER_MAX_RETRIES {
                    let now = self.clock.now_unix_ms()?;
                    if now >= ticket.deadline_unix_ms.get() {
                        transfer_error = Some(crate::QuicTransferError::Expired);
                        break;
                    }
                    let identity = match (&self.session_fence, self.local_mount_generation) {
                        (Some(session_fence), Some(local_mount_generation)) => {
                            let current = session_fence.get().inspect_err(|_| {
                                let _ = progress
                                    .state(&assignment.replication_id, ReplicationState::Failed);
                            })?;
                            if ticket.session_generation != current.session_generation
                                || ticket.mount_generation != local_mount_generation
                            {
                                return Self::fail(
                                    assignment,
                                    progress,
                                    "replication ticket target session or mount generation is stale",
                                );
                            }
                            QuicTransferIdentity::new(self.local_agent_id.clone())
                                .with_session_mount(
                                    current.session_generation.get(),
                                    local_mount_generation.get(),
                                )
                        }
                        // Test/embedded callers that do not own a live Agent session retain the
                        // strict static identity API. Production startup always supplies the
                        // shared fence.
                        _ => QuicTransferIdentity::new(self.local_agent_id.clone())
                            .with_generations(
                                ticket.session_generation.get(),
                                ticket.mount_generation.get(),
                                ticket.route_generation.get(),
                            ),
                    };
                    let attempt = block_on(network.connect_gateway()).and_then(|connection| {
                        block_on(crate::run_quic_sink_stream(
                            connection,
                            &assignment.signed_ticket,
                            &assignment.object_set,
                            &target,
                            trust_bundle,
                            now,
                            QuicTransferClientConfig::default(),
                            Some(identity),
                        ))
                    });
                    match attempt {
                        Ok(()) => break 'transfer Ok(()),
                        Err(error)
                            if (error.is_transient()
                                || matches!(error, crate::QuicTransferError::Expired))
                                && retry < REMOTE_TRANSFER_MAX_RETRIES
                                && self.clock.now_unix_ms()? < ticket.deadline_unix_ms.get() =>
                        {
                            tracing::debug!(
                                replication_id = %assignment.replication_id,
                                retry,
                                error = %error,
                                "remote replication transport is unavailable; retrying"
                            );
                            std::thread::sleep(retry_delay);
                            retry_delay = retry_delay
                                .saturating_mul(2)
                                .min(REMOTE_TRANSFER_MAX_RETRY_DELAY);
                        }
                        Err(error) => {
                            transfer_error = Some(error);
                            break;
                        }
                    }
                }
                Err(transfer_error
                    .take()
                    .unwrap_or(crate::QuicTransferError::Expired))
            };
            transfer_result.map_err(|error| {
                let transient =
                    error.is_transient() || matches!(error, crate::QuicTransferError::Expired);
                if !transient {
                    let _ = progress.state(&assignment.replication_id, ReplicationState::Failed);
                }
                let message = format!("remote replication transfer failed: {error}");
                if transient {
                    AgentDaemonError::SessionTransport(message)
                } else {
                    AgentDaemonError::Session(message)
                }
            })?;
            progress.state(&assignment.replication_id, ReplicationState::Verifying)?;
            for object in &assignment.object_set.objects {
                progress.object(
                    &assignment.replication_id,
                    &object.object_id,
                    object.size.get(),
                    ReplicationObjectState::Verified,
                )?;
            }
            progress.publish(
                &assignment.replication_id,
                &assignment.tenant_id,
                &assignment.commit_id,
                &assignment.object_set.object_set_digest,
            )?;
            progress.state(&assignment.replication_id, ReplicationState::Published)?;
            return Ok(());
        }
        let source = self
            .execution
            .replication_backend(assignment.tenant_id.clone(), assignment.artifact_id.clone())?;
        let now = UnixMillis::new(self.clock.now_unix_ms()?);
        self.worker
            .run_signed(
                &assignment.replication_id,
                &assignment.signed_ticket,
                trust_bundle,
                now,
                &assignment.object_set.objects,
                &assignment.tenant_id,
                &ticket.transfer_id,
                &source,
                &target,
                progress,
            )
            .map(|_| ())
            .inspect_err(|_| {
                let _ = progress.state(&assignment.replication_id, ReplicationState::Failed);
            })
    }
}

pub trait ReplicationProgressSink: Send + Sync {
    fn state(
        &self,
        replication_id: &ReplicationId,
        state: ReplicationState,
    ) -> AgentDaemonResult<()>;
    fn object(
        &self,
        replication_id: &ReplicationId,
        object_id: &ObjectId,
        offset: u64,
        state: ReplicationObjectState,
    ) -> AgentDaemonResult<()>;
    fn publish(
        &self,
        replication_id: &ReplicationId,
        tenant_id: &TenantId,
        commit_id: &neoengram_domain::CommitId,
        object_set_digest: &ContentDigest,
    ) -> AgentDaemonResult<()>;
}

/// Durable progress adapter used by the control-channel processor.
///
/// Every callback becomes one idempotent Agent outbox record. Object offsets are retained in
/// memory for aggregate counters; the authoritative per-object boundary is the durable report
/// itself and Central remains responsible for the attempt fence.
pub struct DurableReplicationProgressSink {
    assignment: ReplicationAssignment,
    reports: Arc<dyn OutboundReportQueue>,
    clock: Arc<dyn Clock>,
    offsets: Mutex<BTreeMap<ObjectId, u64>>,
}

impl std::fmt::Debug for DurableReplicationProgressSink {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DurableReplicationProgressSink")
            .field("replication_id", &self.assignment.replication_id)
            .field("attempt", &self.assignment.attempt)
            .finish_non_exhaustive()
    }
}

impl DurableReplicationProgressSink {
    #[must_use]
    pub fn new(
        assignment: ReplicationAssignment,
        reports: Arc<dyn OutboundReportQueue>,
        clock: Arc<dyn Clock>,
    ) -> Self {
        Self {
            assignment,
            reports,
            clock,
            offsets: Mutex::new(BTreeMap::new()),
        }
    }

    fn enqueue(&self, report: ReplicationProgressReport) -> AgentDaemonResult<()> {
        report
            .validate()
            .map_err(|error| AgentDaemonError::Session(error.to_string()))?;
        let now = self.clock.now_unix_ms().map_err(AgentDaemonError::from)?;
        self.reports
            .enqueue(
                AgentReport::Replication(Box::new(report)),
                UnixMillis::new(now),
            )
            .map(|_| ())
            .map_err(AgentDaemonError::from)
    }

    fn object_size(&self, object_id: &ObjectId) -> AgentDaemonResult<u64> {
        self.assignment
            .object_set
            .objects
            .iter()
            .find(|object| &object.object_id == object_id)
            .map(|object| object.size.get())
            .ok_or_else(|| {
                AgentDaemonError::Session(format!(
                    "replication progress references an object outside the assigned ObjectSet: {object_id}"
                ))
            })
    }
}

impl ReplicationProgressSink for DurableReplicationProgressSink {
    fn state(
        &self,
        replication_id: &ReplicationId,
        state: ReplicationState,
    ) -> AgentDaemonResult<()> {
        if replication_id != &self.assignment.replication_id {
            return Err(AgentDaemonError::Session(
                "replication progress identity does not match its assignment".to_owned(),
            ));
        }
        let offsets = self.offsets.lock().map_err(|_| {
            AgentDaemonError::Session("replication progress lock was poisoned".to_owned())
        })?;
        let completed_objects = offsets.len() as u64;
        let completed_bytes = offsets.values().copied().sum();
        drop(offsets);
        self.enqueue(ReplicationProgressReport::State {
            replication_id: self.assignment.replication_id.clone(),
            tenant_id: self.assignment.tenant_id.clone(),
            attempt: self.assignment.attempt,
            state,
            completed_objects,
            completed_bytes,
            issue_code: None,
            issue_message: None,
            extensions: neoengram_domain::protocol::Extensions::new(),
        })
    }

    fn object(
        &self,
        replication_id: &ReplicationId,
        object_id: &ObjectId,
        offset: u64,
        state: ReplicationObjectState,
    ) -> AgentDaemonResult<()> {
        if replication_id != &self.assignment.replication_id {
            return Err(AgentDaemonError::Session(
                "replication progress identity does not match its assignment".to_owned(),
            ));
        }
        let size = self.object_size(object_id)?;
        if offset > size {
            return Err(AgentDaemonError::Session(format!(
                "replication object offset {offset} exceeds object size {size}"
            )));
        }
        self.offsets
            .lock()
            .map_err(|_| {
                AgentDaemonError::Session("replication progress lock was poisoned".to_owned())
            })?
            .insert(*object_id, offset);
        self.enqueue(ReplicationProgressReport::Object {
            replication_id: self.assignment.replication_id.clone(),
            tenant_id: self.assignment.tenant_id.clone(),
            attempt: self.assignment.attempt,
            object_id: *object_id,
            offset,
            state,
            extensions: neoengram_domain::protocol::Extensions::new(),
        })
    }

    fn publish(
        &self,
        replication_id: &ReplicationId,
        tenant_id: &TenantId,
        commit_id: &neoengram_domain::CommitId,
        object_set_digest: &ContentDigest,
    ) -> AgentDaemonResult<()> {
        if replication_id != &self.assignment.replication_id
            || tenant_id != &self.assignment.tenant_id
            || commit_id != &self.assignment.commit_id
            || object_set_digest != &self.assignment.object_set.object_set_digest
        {
            return Err(AgentDaemonError::Session(
                "replication publication identity does not match its assignment".to_owned(),
            ));
        }
        // Object checkpoints are emitted individually after the initial Verifying transition.
        // Refresh the aggregate counters before the publication fence so Central's atomic
        // finalize sees the complete ObjectSet in the same durable report order.
        self.state(replication_id, ReplicationState::Verifying)?;
        self.enqueue(ReplicationProgressReport::Published {
            replication_id: self.assignment.replication_id.clone(),
            tenant_id: self.assignment.tenant_id.clone(),
            attempt: self.assignment.attempt,
            commit_id: *commit_id,
            object_set_digest: *object_set_digest,
            extensions: neoengram_domain::protocol::Extensions::new(),
        })
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct ReplicationWorker {
    executor: ObjectSetTransferExecutor,
}

#[cfg(test)]
#[allow(clippy::items_after_test_module)]
mod tests {
    use std::sync::Mutex;

    use neoengram_domain::protocol::{
        ArtifactId, ContentDigest, DecimalU64, EdgeClusterId, GatewayPoolId, MountGeneration,
        ObjectEncoding, PlacementId, ReplicationObjectState, ReplicationState, RouteGeneration,
        SessionGeneration, TenantId, TransferEndpoint, UnixMillis,
    };
    use neoengram_domain::{CommitId, ObjectId};
    use neoengram_runtime::{ObjectPutOutcome, ObjectSpec, VolumeCasBackend};

    use super::*;

    #[derive(Default)]
    struct Progress {
        states: Mutex<Vec<ReplicationState>>,
        objects: Mutex<Vec<(ObjectId, u64, ReplicationObjectState)>>,
        published: Mutex<bool>,
    }

    impl ReplicationProgressSink for Progress {
        fn state(
            &self,
            _replication_id: &ReplicationId,
            state: ReplicationState,
        ) -> AgentDaemonResult<()> {
            self.states.lock().unwrap().push(state);
            Ok(())
        }

        fn object(
            &self,
            _replication_id: &ReplicationId,
            object_id: &ObjectId,
            offset: u64,
            state: ReplicationObjectState,
        ) -> AgentDaemonResult<()> {
            self.objects
                .lock()
                .unwrap()
                .push((*object_id, offset, state));
            Ok(())
        }

        fn publish(
            &self,
            _replication_id: &ReplicationId,
            _tenant_id: &TenantId,
            _commit_id: &CommitId,
            _object_set_digest: &ContentDigest,
        ) -> AgentDaemonResult<()> {
            *self.published.lock().unwrap() = true;
            Ok(())
        }
    }

    fn endpoint(name: &str) -> TransferEndpoint {
        TransferEndpoint {
            placement_id: PlacementId::new(format!("placement-{name}")).unwrap(),
            agent_id: neoengram_domain::AgentId::new(format!("agent-{name}")).unwrap(),
            gateway_pool_id: GatewayPoolId::new(format!("pool-{name}")).unwrap(),
            edge_cluster_id: EdgeClusterId::new(format!("cluster-{name}")).unwrap(),
            storage_volume_id: None,
        }
    }

    #[test]
    fn worker_copies_artifact_scoped_object_and_publishes_progress() {
        let source_root = tempfile::tempdir().unwrap();
        let target_root = tempfile::tempdir().unwrap();
        let tenant_id = TenantId::new("tenant-a").unwrap();
        let artifact_id = ArtifactId::new("artifact-a").unwrap();
        let source = VolumeCasBackend::open_or_create_artifact_scoped(
            source_root.path(),
            tenant_id.clone(),
            artifact_id.clone(),
        )
        .unwrap();
        let target = VolumeCasBackend::open_or_create_artifact_scoped(
            target_root.path(),
            tenant_id.clone(),
            artifact_id,
        )
        .unwrap();
        let payload = b"replication-worker";
        let expected = ObjectSpec::for_bytes(payload);
        let source_transfer = TransferId::new("source-seed").unwrap();
        source
            .stage_write(&source_transfer, &tenant_id, &expected, 0, payload)
            .unwrap();
        assert_eq!(
            source
                .verify_and_publish(&source_transfer, &tenant_id, &expected)
                .unwrap(),
            ObjectPutOutcome::Created
        );

        let object_set = vec![CommitObject::new(
            expected.id,
            expected.size,
            ObjectEncoding::Raw,
            0,
        )];
        let object_set_digest = neoengram_domain::protocol::ObjectSet::new(object_set.clone())
            .unwrap()
            .object_set_digest;
        let ticket = TransferTicket {
            transfer_id: TransferId::new("transfer-copy").unwrap(),
            tenant_id: tenant_id.clone(),
            artifact_id: ArtifactId::new("artifact-a").unwrap(),
            commit_id: CommitId::from_bytes([7; 32]),
            object_set_digest,
            source: endpoint("source"),
            target: endpoint("target"),
            source_session_generation: SessionGeneration::new(1),
            source_mount_generation: MountGeneration::new(1),
            source_route_generation: RouteGeneration::new(1),
            session_generation: SessionGeneration::new(1),
            mount_generation: MountGeneration::new(1),
            route_generation: RouteGeneration::new(1),
            deadline_unix_ms: UnixMillis::new(u64::MAX),
            max_bytes: DecimalU64::new(expected.size),
            allowed_objects: vec![expected.id],
        };
        let progress = Progress::default();
        let replication_id = ReplicationId::new("replication-copy").unwrap();
        let outcomes = ReplicationWorker::default()
            .run_with_backends(
                &replication_id,
                &ticket,
                &object_set,
                &tenant_id,
                &ticket.transfer_id,
                &source,
                &target,
                &progress,
            )
            .unwrap();
        assert_eq!(outcomes.len(), 1);
        assert_eq!(
            progress.states.lock().unwrap().as_slice(),
            &[
                ReplicationState::Planning,
                ReplicationState::Transferring,
                ReplicationState::Verifying,
                ReplicationState::Published,
            ]
        );
        assert_eq!(
            progress.objects.lock().unwrap().as_slice(),
            &[(expected.id, expected.size, ReplicationObjectState::Verified)]
        );
        assert!(*progress.published.lock().unwrap());
        assert_eq!(
            target
                .inspect(&tenant_id, &expected.id)
                .unwrap()
                .unwrap()
                .size,
            expected.size
        );
    }
}

impl ReplicationWorker {
    #[must_use]
    pub const fn new(executor: ObjectSetTransferExecutor) -> Self {
        Self { executor }
    }

    /// Verifies the Central signature and expiry before delegating to the object-copy kernel. No
    /// source read or target staging operation is attempted when the capability is stale,
    /// tampered with, or signed by a revoked/unknown command key.
    #[allow(clippy::too_many_arguments)]
    pub fn run_signed(
        &self,
        replication_id: &ReplicationId,
        signed_ticket: &SignedTransferTicket,
        trust_bundle: &crate::CentralCommandTrustBundle,
        now_unix_ms: UnixMillis,
        object_set: &[CommitObject],
        tenant_id: &TenantId,
        transfer_id: &TransferId,
        source: &dyn TransferSource,
        sink: &dyn TransferSink,
        progress: &dyn ReplicationProgressSink,
    ) -> AgentDaemonResult<Vec<ObjectTransferOutcome>> {
        trust_bundle.verify_transfer_ticket(signed_ticket, now_unix_ms)?;
        self.run(
            replication_id,
            &signed_ticket.ticket,
            object_set,
            tenant_id,
            transfer_id,
            source,
            sink,
            progress,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn run(
        &self,
        replication_id: &ReplicationId,
        ticket: &TransferTicket,
        object_set: &[CommitObject],
        tenant_id: &TenantId,
        transfer_id: &TransferId,
        source: &dyn TransferSource,
        sink: &dyn TransferSink,
        progress: &dyn ReplicationProgressSink,
    ) -> AgentDaemonResult<Vec<ObjectTransferOutcome>> {
        if ticket.source_session_generation.get() == 0
            || ticket.source_mount_generation.get() == 0
            || ticket.source_route_generation.get() == 0
        {
            return Err(AgentDaemonError::Session(
                "replication ticket has invalid source route generations".to_owned(),
            ));
        }
        progress.state(replication_id, ReplicationState::Planning)?;
        progress.state(replication_id, ReplicationState::Transferring)?;
        let outcomes = self
            .executor
            .copy_object_set(ticket, object_set, tenant_id, transfer_id, source, sink)
            .map_err(|error| {
                AgentDaemonError::Session(format!("replication transfer failed: {error}"))
            })?;
        progress.state(replication_id, ReplicationState::Verifying)?;
        for outcome in &outcomes {
            let offset = outcome
                .resumed_from
                .checked_add(outcome.bytes_transferred)
                .ok_or_else(|| {
                    AgentDaemonError::Session("replication progress offset exceeds u64".to_owned())
                })?;
            progress.object(
                replication_id,
                &outcome.object_id,
                offset,
                ReplicationObjectState::Verified,
            )?;
        }
        progress.publish(
            replication_id,
            tenant_id,
            &ticket.commit_id,
            &ticket.object_set_digest,
        )?;
        progress.state(replication_id, ReplicationState::Published)?;
        Ok(outcomes)
    }

    /// Executes a replication against two mounted Volume CAS namespaces.  The network relay can
    /// supply custom [`TransferSource`] and [`TransferSink`] implementations, while local Agent
    /// startup and same-host development use the exact same ticket/checkpoint kernel through this
    /// adapter.  The backends never expose their paths to the ticket or to Central.
    #[allow(clippy::too_many_arguments)]
    pub fn run_with_backends<S, T>(
        &self,
        replication_id: &ReplicationId,
        ticket: &TransferTicket,
        object_set: &[CommitObject],
        tenant_id: &TenantId,
        transfer_id: &TransferId,
        source: &S,
        target: &T,
        progress: &dyn ReplicationProgressSink,
    ) -> AgentDaemonResult<Vec<ObjectTransferOutcome>>
    where
        S: ObjectBackend,
        T: ObjectBackend,
    {
        self.run(
            replication_id,
            ticket,
            object_set,
            tenant_id,
            transfer_id,
            source,
            target,
            progress,
        )
    }
}
