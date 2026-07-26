use std::fmt;

use neoengram_core::ContentDigest;
use neoengram_engine::PreparedAdd;
use neoengram_protocol::{
    AddAssignment, ControlMessage, JobAccepted, JobDecision, JobFailed, JobFinalized, JobId,
    JobPrepared, JobProgress, MetadataBatchDescriptor, MetadataBatchId, MetadataBatchKind,
    MetadataBatchPage, MetadataBatchRecords, MetadataBatchScope, MetadataPublication, TenantId,
};
use serde::{Deserialize, Serialize};

use crate::{AgentError, AgentErrorCode, AgentResult};

/// Tenant-scoped durable identity of one job in an Agent ledger.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct AssignmentKey {
    pub tenant_id: TenantId,
    pub job_id: JobId,
}

impl AssignmentKey {
    #[must_use]
    pub fn from_assignment(assignment: &AddAssignment) -> Self {
        Self {
            tenant_id: assignment.tenant_id.clone(),
            job_id: assignment.job_id.clone(),
        }
    }
}

impl fmt::Display for AssignmentKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}/{}", self.tenant_id, self.job_id)
    }
}

/// Durable Agent-side lifecycle. Every transition is persisted through [`crate::Ledger`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentAssignmentState {
    Claimed,
    Accepted,
    Running,
    Prepared,
    AwaitingDecision,
    Finalizing,
    Completed,
    FailedReported,
    Recovering,
}

/// Result of immutable-object transfer and metadata staging preparation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TransferReceipt {
    #[serde(default)]
    pub metadata_batches: Vec<MetadataBatchDescriptor>,
    #[serde(default)]
    pub metadata_pages: Vec<MetadataBatchPage>,
}

impl TransferReceipt {
    #[must_use]
    pub const fn new(
        metadata_batches: Vec<MetadataBatchDescriptor>,
        metadata_pages: Vec<MetadataBatchPage>,
    ) -> Self {
        Self {
            metadata_batches,
            metadata_pages,
        }
    }

    /// Verifies that the durable material is complete, internally consistent, and assignment-bound.
    pub fn validate(&self, assignment: &AddAssignment) -> AgentResult<()> {
        let mut descriptors =
            std::collections::BTreeMap::<MetadataBatchId, &MetadataBatchDescriptor>::new();
        let mut batch_kinds = std::collections::BTreeMap::<&'static str, usize>::new();
        for descriptor in &self.metadata_batches {
            descriptor.validate()?;
            let kind = match descriptor.kind {
                MetadataBatchKind::Manifest => "manifest",
                MetadataBatchKind::IndexDelta => "index_delta",
                MetadataBatchKind::ObjectReceipt => "object_receipt",
            };
            *batch_kinds.entry(kind).or_default() += 1;
            let scope = &descriptor.scope;
            if scope.tenant_id != assignment.tenant_id
                || scope.project_id != assignment.project_id
                || scope.artifact_id != assignment.artifact_id
                || scope.playground_id != assignment.playground_id
                || scope.job_id != assignment.job_id
                || scope.base_index_version.revision != assignment.expected_index_version.revision
                || scope.base_index_version.digest != assignment.expected_index_version.digest
            {
                return Err(invalid_transfer_receipt(format!(
                    "metadata batch {} does not match assignment scope or base version",
                    descriptor.batch_id
                )));
            }
            if descriptors
                .insert(descriptor.batch_id.clone(), descriptor)
                .is_some()
            {
                return Err(invalid_transfer_receipt(format!(
                    "metadata batch {} is declared more than once",
                    descriptor.batch_id
                )));
            }
        }

        for required in ["manifest", "index_delta", "object_receipt"] {
            let actual = batch_kinds.get(required).copied().unwrap_or_default();
            if actual != 1 {
                return Err(invalid_transfer_receipt(format!(
                    "prepared Add requires exactly one {required} metadata batch, found {actual}"
                )));
            }
        }

        let mut pages_per_batch = std::collections::BTreeMap::<MetadataBatchId, usize>::new();
        let mut pages = std::collections::BTreeSet::<(MetadataBatchId, u32)>::new();
        for page in &self.metadata_pages {
            let descriptor = descriptors.get(&page.batch_id).ok_or_else(|| {
                invalid_transfer_receipt(format!(
                    "metadata page belongs to undeclared batch {}",
                    page.batch_id
                ))
            })?;
            descriptor.validate_page(page)?;
            if let MetadataBatchRecords::ObjectReceipt(records) = &page.records {
                for record in records {
                    if record.tenant_id != assignment.tenant_id
                        || record.artifact_id != assignment.artifact_id
                        || record.job_id != assignment.job_id
                        || record.storage_volume_id != assignment.storage_volume_id
                        || record.artifact_placement_id != assignment.artifact_placement_id
                    {
                        return Err(invalid_transfer_receipt(
                            "object receipt scope or placement differs from assignment",
                        ));
                    }
                }
            }
            if !pages.insert((page.batch_id.clone(), page.page_number)) {
                return Err(invalid_transfer_receipt(format!(
                    "metadata batch {} page {} is present more than once",
                    page.batch_id, page.page_number
                )));
            }
            *pages_per_batch.entry(page.batch_id.clone()).or_default() += 1;
        }

        for descriptor in &self.metadata_batches {
            let actual = pages_per_batch
                .get(&descriptor.batch_id)
                .copied()
                .unwrap_or_default();
            if actual != descriptor.page_count as usize {
                return Err(invalid_transfer_receipt(format!(
                    "metadata batch {} declares {} pages but receipt contains {actual}",
                    descriptor.batch_id, descriptor.page_count
                )));
            }
        }
        Ok(())
    }

    /// Verifies that the wire material is the exact logical publication produced by the Engine.
    pub fn validate_for(
        &self,
        assignment: &AddAssignment,
        prepared: &PreparedAdd,
    ) -> AgentResult<()> {
        self.validate(assignment)?;
        let material = MetadataPublication::from_pages(&self.metadata_pages)
            .map_err(|error| invalid_transfer_receipt(error.to_string()))?;
        let scope = MetadataBatchScope {
            tenant_id: assignment.tenant_id.clone(),
            project_id: assignment.project_id.clone(),
            artifact_id: assignment.artifact_id.clone(),
            playground_id: assignment.playground_id.clone(),
            job_id: assignment.job_id.clone(),
            base_index_version: assignment.expected_index_version.clone(),
            extensions: neoengram_protocol::Extensions::new(),
        };
        let observed = material
            .publication_digest(&scope, prepared.index_delta.result_digest)
            .map_err(|error| invalid_transfer_receipt(error.to_string()))?;
        let expected = prepared
            .publication_digest()
            .map_err(|error| invalid_transfer_receipt(error.to_string()))?;
        if observed != expected {
            return Err(invalid_transfer_receipt(format!(
                "metadata publication digest {observed} differs from Engine candidate {expected}"
            )));
        }
        Ok(())
    }
}

impl Default for TransferReceipt {
    fn default() -> Self {
        Self::new(Vec::new(), Vec::new())
    }
}

fn invalid_transfer_receipt(message: impl Into<String>) -> AgentError {
    AgentError::new(AgentErrorCode::ObjectTransferFailed, message)
}

/// Candidate and transfer evidence retained while waiting for the central publish decision.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PreparedExecution {
    pub candidate: PreparedAdd,
    pub transfer: TransferReceipt,
}

/// Durable state for one claimed Add assignment.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LedgerRecord {
    pub key: AssignmentKey,
    pub request_digest: ContentDigest,
    pub assignment: AddAssignment,
    pub state: AgentAssignmentState,
    pub revision: u64,
    pub claimed_at_unix_ms: u64,
    pub updated_at_unix_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub accepted_at_unix_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prepared: Option<PreparedExecution>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub decision: Option<JobDecision>,
    /// Exact acknowledgement payload persisted before it is sent to the control plane.
    ///
    /// Keeping the timestamp durable makes a retry byte-for-byte stable when the first send
    /// succeeds but the subsequent `Completed` ledger transition does not.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finalized: Option<JobFinalized>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure: Option<JobFailed>,
}

impl LedgerRecord {
    #[must_use]
    pub fn claimed(assignment: AddAssignment, claimed_at_unix_ms: u64) -> Self {
        let key = AssignmentKey::from_assignment(&assignment);
        let request_digest = assignment.request_digest;
        Self {
            key,
            request_digest,
            assignment,
            state: AgentAssignmentState::Claimed,
            revision: 1,
            claimed_at_unix_ms,
            updated_at_unix_ms: claimed_at_unix_ms,
            accepted_at_unix_ms: None,
            prepared: None,
            decision: None,
            finalized: None,
            failure: None,
        }
    }
}

/// Atomic input to [`crate::Ledger::claim`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LedgerClaim {
    pub assignment: AddAssignment,
    pub claimed_at_unix_ms: u64,
}

/// Outcome of an atomic tenant/job/digest claim.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ClaimOutcome {
    Claimed(LedgerRecord),
    Existing(LedgerRecord),
}

/// Whether assignment acceptance advanced a new claim or replayed durable state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ClaimDisposition {
    New,
    Replayed,
}

/// Result returned to an assignment transport adapter.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AssignmentAcceptance {
    pub disposition: ClaimDisposition,
    pub record: LedgerRecord,
}

/// Reports emitted by the state machine. A transport adapter may wrap them in control envelopes.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "report", content = "payload", rename_all = "snake_case")]
pub enum AgentReport {
    Accepted(JobAccepted),
    Progress(JobProgress),
    Prepared(JobPrepared),
    Finalized(JobFinalized),
    Failed(JobFailed),
}

impl AgentReport {
    #[must_use]
    pub fn into_control_message(self) -> ControlMessage {
        match self {
            Self::Accepted(report) => ControlMessage::Accepted(report),
            Self::Progress(report) => ControlMessage::Progress(report),
            Self::Prepared(report) => ControlMessage::Prepared(report),
            Self::Finalized(report) => ControlMessage::Finalized(report),
            Self::Failed(report) => ControlMessage::Failed(report),
        }
    }
}
