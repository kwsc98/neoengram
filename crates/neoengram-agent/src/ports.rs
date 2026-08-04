use std::fmt::Debug;

use neoengram_engine::PreparedAdd;
use neoengram_protocol::{AddAssignment, MessageId, UnixMillis};

use crate::{
    AgentError, AgentErrorCode, AgentReport, AgentResult, AssignmentKey, ClaimOutcome, LedgerClaim,
    LedgerRecord, QueuedAgentReport, TransferReceipt,
};

/// Durable idempotency ledger. `claim` must atomically key records by tenant, job, and digest.
pub trait Ledger: Debug + Send + Sync {
    fn claim(&self, claim: LedgerClaim) -> AgentResult<ClaimOutcome>;
    fn load(&self, key: &AssignmentKey) -> AgentResult<Option<LedgerRecord>>;
    /// Returns every assignment that still requires execution, recovery, or central acknowledgement.
    fn list_active(&self) -> AgentResult<Vec<LedgerRecord>>;

    /// Replaces a record only when its persisted revision equals `expected_revision`.
    fn compare_exchange(
        &self,
        expected_revision: u64,
        next: LedgerRecord,
    ) -> AgentResult<LedgerRecord>;
}

/// Performs node-local policy, generation, lease, and deadline checks.
///
/// The Agent invokes this port before the durable claim and again before every resumable,
/// pre-decision execution boundary. Implementations may therefore reject a generation or lease
/// that became stale while an accepted assignment was dormant.
pub trait AssignmentValidator: Debug + Send + Sync {
    fn validate(&self, assignment: &AddAssignment, now_unix_ms: u64) -> AgentResult<()>;
}

/// Executes the non-authoritative Add preparation in the execution engine.
///
/// Recovery may invoke this port again for the same request digest, so implementations must
/// produce an equivalent candidate or reject the replay explicitly.
pub trait AddExecutor: Debug + Send + Sync {
    fn prepare_add(&self, assignment: &AddAssignment) -> AgentResult<PreparedAdd>;
}

/// Negotiates and uploads missing objects, returning durable metadata staging material.
///
/// Every operation may be repeated after an Agent crash. Transfer and staging are idempotent for
/// the assignment/request digest; finalization additionally binds the central decision generation.
pub trait ObjectTransfer: Debug + Send + Sync {
    fn transfer(
        &self,
        assignment: &AddAssignment,
        prepared: &PreparedAdd,
    ) -> AgentResult<TransferReceipt>;

    /// Idempotently stages the exact descriptors and pages retained in the durable receipt.
    ///
    /// A crash can occur after any individual submission, so replaying the complete receipt must
    /// either succeed or reject material that differs from an earlier submission.
    fn stage_metadata(
        &self,
        assignment: &AddAssignment,
        receipt: &TransferReceipt,
    ) -> AgentResult<()>;

    /// Applies durable local cleanup required by a central publish decision.
    fn finalize(
        &self,
        assignment: &AddAssignment,
        prepared: &PreparedAdd,
        decision: &neoengram_protocol::JobDecision,
    ) -> AgentResult<()>;
}

/// Durable or idempotent outbound report channel.
pub trait ReportSink: Debug + Send + Sync {
    fn send(&self, report: AgentReport) -> AgentResult<()>;
}

/// Durable ordered channel used by a session transport to survive process and network failure.
pub trait OutboundReportQueue: Debug + Send + Sync {
    /// Enqueues an exact report idempotently and returns its stable message identity.
    fn enqueue(
        &self,
        report: AgentReport,
        enqueued_at_unix_ms: UnixMillis,
    ) -> AgentResult<QueuedAgentReport>;
    /// Lists the oldest unacknowledged reports in insertion order.
    fn list(&self, limit: usize) -> AgentResult<Vec<QueuedAgentReport>>;
    /// Removes only the exact message that the center acknowledged.
    fn acknowledge(&self, message_id: &MessageId) -> AgentResult<bool>;
}

/// Injectable wall clock used for persisted protocol timestamps and deadline validation.
pub trait Clock: Debug + Send + Sync {
    fn now_unix_ms(&self) -> AgentResult<u64>;
}

/// Baseline protocol-independent checks suitable for production composition.
#[derive(Debug, Default, Clone, Copy)]
pub struct BasicAssignmentValidator;

impl AssignmentValidator for BasicAssignmentValidator {
    fn validate(&self, assignment: &AddAssignment, now_unix_ms: u64) -> AgentResult<()> {
        assignment.validate().map_err(|error| {
            AgentError::new(AgentErrorCode::InvalidAssignment, error.to_string())
        })?;
        if assignment.deadline_unix_ms.get() <= now_unix_ms {
            return Err(AgentError::new(
                AgentErrorCode::DeadlineExceeded,
                format!("assignment {} deadline has elapsed", assignment.job_id),
            ));
        }
        if assignment
            .lease
            .as_ref()
            .is_some_and(|lease| lease.expires_at_unix_ms.get() <= now_unix_ms)
        {
            return Err(AgentError::new(
                AgentErrorCode::DeadlineExceeded,
                format!("assignment {} lease has expired", assignment.job_id),
            ));
        }
        Ok(())
    }
}
