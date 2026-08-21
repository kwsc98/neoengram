//! Agent-side object replication worker.
//!
//! Execution is deliberately separate from Central authority. The supplied progress sink
//! persists checkpoints and publishes the complete PlacementSet only after every object has
//! passed the runtime CAS verification.

use neoengram_domain::core::{ContentDigest, ObjectId};
use neoengram_domain::protocol::{
    CommitObject, ReplicationId, ReplicationObjectState, ReplicationState, TenantId, TransferId,
    TransferTicket,
};
use neoengram_runtime::{
    ObjectSetTransferExecutor, ObjectTransferOutcome, TransferSink, TransferSource,
};

use crate::{AgentDaemonError, AgentDaemonResult};

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

#[derive(Debug, Clone, Copy, Default)]
pub struct ReplicationWorker {
    executor: ObjectSetTransferExecutor,
}

impl ReplicationWorker {
    #[must_use]
    pub const fn new(executor: ObjectSetTransferExecutor) -> Self {
        Self { executor }
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
}
