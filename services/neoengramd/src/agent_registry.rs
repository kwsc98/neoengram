use std::{collections::BTreeSet, fmt, sync::Arc};

use neoengram_core::ContentDigest;
use neoengram_protocol::{
    AgentBootId, AgentBootstrapAccepted, AgentBootstrapProbe, AgentBootstrapRequest,
    AgentEnrollmentApprovalRequest, AgentEnrollmentDecision, AgentEnrollmentId,
    AgentEnrollmentState, AgentEnrollmentTokenCreateRequest, AgentEnrollmentTokenId, AgentId,
    AgentInstallationId, AgentMountId, AgentMountIdentityDigest, AgentMountStatusReport,
    EdgeClusterId, Extensions, MountAccessMode, MountGeneration, OwnerGeneration, PrincipalRef,
    ProtocolVersion, PvcIdentityDigest, RequestId, ResourceHealth, ResourceVersion, SequenceNumber,
    SessionGeneration, StorageVolumeId, TenantId, UnixMillis, VolumeMarkerId,
};
use serde::{Deserialize, Serialize};

use crate::{
    AgentEnrollmentAuditEvent, AgentEnrollmentAuditKind, AgentRegistryInsertOutcome,
    AgentRegistryRepository, AuthorityStore, CentralError, CentralErrorCode, CentralResult, Clock,
};

pub const AGENT_ENROLLMENT_REVIEW_WINDOW_MS: u64 = 24 * 60 * 60 * 1_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AgentInstanceState {
    Active,
    Revoked,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum VolumeOwnerState {
    Inactive,
    Active,
    RecoveryRequired,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DerivedVolumeState {
    Ready,
    Degraded,
    Unavailable,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PvcVolumeBinding {
    pub pvc_identity_digest: PvcIdentityDigest,
    pub tenant_id: TenantId,
    pub edge_cluster_id: EdgeClusterId,
    pub storage_volume_id: StorageVolumeId,
}

#[derive(Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentEnrollmentRecord {
    pub token_id: AgentEnrollmentTokenId,
    pub token_request_id: RequestId,
    pub enrollment_id: AgentEnrollmentId,
    pub tenant_id: TenantId,
    pub edge_cluster_id: EdgeClusterId,
    pub storage_volume_id: StorageVolumeId,
    pub volume_descriptor_digest: ContentDigest,
    pub pvc_identity_digest: PvcIdentityDigest,
    pub reserved_agent_id: AgentId,
    pub reserved_agent_mount_id: AgentMountId,
    pub bootstrap_token_digest: ContentDigest,
    pub state: AgentEnrollmentState,
    pub created_at_unix_ms: UnixMillis,
    pub expires_at_unix_ms: UnixMillis,
    pub bootstrapped_at_unix_ms: Option<UnixMillis>,
    pub bootstrap_request_id: Option<RequestId>,
    pub review_expires_at_unix_ms: Option<UnixMillis>,
    pub decided_at_unix_ms: Option<UnixMillis>,
    pub decided_by: Option<PrincipalRef>,
    pub decision_request: Option<AgentEnrollmentApprovalRequest>,
    pub replaces_enrollment_id: Option<AgentEnrollmentId>,
    pub replaced_by_enrollment_id: Option<AgentEnrollmentId>,
    pub extensions: Extensions,
}

impl fmt::Debug for AgentEnrollmentRecord {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AgentEnrollmentRecord")
            .field("token_id", &self.token_id)
            .field("token_request_id", &self.token_request_id)
            .field("enrollment_id", &self.enrollment_id)
            .field("tenant_id", &self.tenant_id)
            .field("edge_cluster_id", &self.edge_cluster_id)
            .field("storage_volume_id", &self.storage_volume_id)
            .field("volume_descriptor_digest", &self.volume_descriptor_digest)
            .field("pvc_identity_digest", &self.pvc_identity_digest)
            .field("reserved_agent_id", &self.reserved_agent_id)
            .field("reserved_agent_mount_id", &self.reserved_agent_mount_id)
            .field("bootstrap_token_digest", &"[REDACTED]")
            .field("state", &self.state)
            .field("created_at_unix_ms", &self.created_at_unix_ms)
            .field("expires_at_unix_ms", &self.expires_at_unix_ms)
            .field("bootstrapped_at_unix_ms", &self.bootstrapped_at_unix_ms)
            .field("bootstrap_request_id", &self.bootstrap_request_id)
            .field("review_expires_at_unix_ms", &self.review_expires_at_unix_ms)
            .field("decided_at_unix_ms", &self.decided_at_unix_ms)
            .field("decided_by", &self.decided_by)
            .field("decision_request", &self.decision_request)
            .field("replaces_enrollment_id", &self.replaces_enrollment_id)
            .field("replaced_by_enrollment_id", &self.replaced_by_enrollment_id)
            .field("extensions", &self.extensions)
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentCandidateRecord {
    pub bootstrap_request_id: RequestId,
    pub installation_id: AgentInstallationId,
    pub public_key_fingerprint: ContentDigest,
    pub mount_identity_digest: AgentMountIdentityDigest,
    pub agent_version: String,
    pub supported_protocol_versions: Vec<ProtocolVersion>,
    pub capabilities: BTreeSet<String>,
    pub probe: AgentBootstrapProbe,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentInstanceRecord {
    pub agent_id: AgentId,
    pub installation_id: AgentInstallationId,
    pub public_key_fingerprint: ContentDigest,
    pub agent_version: String,
    pub supported_protocol_versions: Vec<ProtocolVersion>,
    pub capabilities: BTreeSet<String>,
    pub state: AgentInstanceState,
    pub session_generation: Option<SessionGeneration>,
    pub active_boot_id: Option<AgentBootId>,
    #[serde(default)]
    pub session_open_expected_resource_version: Option<ResourceVersion>,
    pub session_opened_at_unix_ms: Option<UnixMillis>,
    pub last_heartbeat_at_unix_ms: Option<UnixMillis>,
    pub last_sequence: Option<SequenceNumber>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentMountRecord {
    pub agent_mount_id: AgentMountId,
    pub storage_volume_id: StorageVolumeId,
    pub mount_generation: MountGeneration,
    pub expected_volume_marker: VolumeMarkerId,
    pub desired_access_mode: MountAccessMode,
    pub mount_identity_digest: Option<AgentMountIdentityDigest>,
    pub observed_volume_marker: Option<VolumeMarkerId>,
    pub observed_access_mode: Option<MountAccessMode>,
    pub reported_health: Option<ResourceHealth>,
    pub health: ResourceHealth,
    pub observed_at_unix_ms: Option<UnixMillis>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct VolumeOwnerRecord {
    pub tenant_id: TenantId,
    pub storage_volume_id: StorageVolumeId,
    pub active_agent_id: Option<AgentId>,
    pub active_agent_mount_id: Option<AgentMountId>,
    pub owner_generation: OwnerGeneration,
    pub state: VolumeOwnerState,
}

/// One CAS aggregate keeps enrollment, identity, mount and ownership changes atomic.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentRegistryRecord {
    pub resource_version: ResourceVersion,
    pub enrollment: AgentEnrollmentRecord,
    pub candidate: Option<AgentCandidateRecord>,
    pub instance: Option<AgentInstanceRecord>,
    pub mount: AgentMountRecord,
    pub owner: VolumeOwnerRecord,
    /// Immutable and atomically persisted with the enrollment decision CAS.
    #[serde(default)]
    pub decision_audit_event: Option<AgentEnrollmentAuditEvent>,
}

impl AgentRegistryRecord {
    #[must_use]
    pub fn derived_volume_state(
        &self,
        now_unix_ms: UnixMillis,
        heartbeat_timeout_ms: u64,
    ) -> DerivedVolumeState {
        if self.enrollment.state != AgentEnrollmentState::Approved
            || self.owner.state != VolumeOwnerState::Active
        {
            return DerivedVolumeState::Unavailable;
        }
        let Some(instance) = &self.instance else {
            return DerivedVolumeState::Unavailable;
        };
        if instance.state != AgentInstanceState::Active
            || instance.active_boot_id.is_none()
            || instance.session_generation.is_none()
            || self.owner.active_agent_id.as_ref() != Some(&instance.agent_id)
        {
            return DerivedVolumeState::Unavailable;
        }
        let Some(last_heartbeat) = instance.last_heartbeat_at_unix_ms else {
            return DerivedVolumeState::Unavailable;
        };
        if now_unix_ms.get().saturating_sub(last_heartbeat.get()) > heartbeat_timeout_ms {
            return DerivedVolumeState::Unavailable;
        }
        if self.mount.mount_identity_digest.is_none()
            || self.mount.observed_volume_marker.as_ref()
                != Some(&self.mount.expected_volume_marker)
        {
            return DerivedVolumeState::Unavailable;
        }
        match self.mount.health {
            ResourceHealth::Ready
                if self.mount.desired_access_mode != MountAccessMode::ReadWrite
                    || self.mount.observed_access_mode == Some(MountAccessMode::ReadWrite) =>
            {
                DerivedVolumeState::Ready
            }
            ResourceHealth::Ready | ResourceHealth::Degraded => DerivedVolumeState::Degraded,
            ResourceHealth::Unavailable | ResourceHealth::Unknown => {
                DerivedVolumeState::Unavailable
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateAgentEnrollmentTokenResult {
    pub record: AgentRegistryRecord,
    pub replayed: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BootstrapAgentResult {
    pub accepted: AgentBootstrapAccepted,
    pub record: AgentRegistryRecord,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DecideAgentEnrollmentResult {
    pub record: AgentRegistryRecord,
    pub revoked_record: Option<AgentRegistryRecord>,
    pub replayed: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AgentRegistryReplacementRecords {
    pub revoked: AgentRegistryRecord,
    pub replacement: AgentRegistryRecord,
}

#[derive(Debug, Clone, PartialEq)]
pub struct OpenAgentSessionRequest {
    pub agent_id: AgentId,
    pub installation_id: AgentInstallationId,
    pub boot_id: AgentBootId,
    pub mount_identity_digest: AgentMountIdentityDigest,
    pub expected_resource_version: ResourceVersion,
}

#[derive(Debug, Clone, PartialEq)]
pub struct OpenAgentSessionResult {
    pub record: AgentRegistryRecord,
    pub session_generation: SessionGeneration,
    pub replayed: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CloseAgentSessionRequest {
    pub agent_id: AgentId,
    pub boot_id: AgentBootId,
    pub session_generation: SessionGeneration,
    pub expected_resource_version: ResourceVersion,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ReportAgentMountResult {
    pub record: AgentRegistryRecord,
    pub replayed: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CompleteVolumeRecoveryRequest {
    pub enrollment_id: AgentEnrollmentId,
    pub expected_resource_version: ResourceVersion,
    pub owner_generation: OwnerGeneration,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ExpireAgentEnrollmentRequest {
    pub enrollment_id: AgentEnrollmentId,
    pub expected_resource_version: ResourceVersion,
}

/// Transport-independent enrollment and one-Volume ownership application service.
pub struct AgentRegistryService {
    repository: Arc<dyn AgentRegistryRepository>,
    clock: Arc<dyn Clock>,
    heartbeat_timeout_ms: u64,
}

impl AgentRegistryService {
    #[must_use]
    pub fn new(
        repository: Arc<dyn AgentRegistryRepository>,
        clock: Arc<dyn Clock>,
        heartbeat_timeout_ms: u64,
    ) -> Self {
        Self {
            repository,
            clock,
            heartbeat_timeout_ms,
        }
    }

    pub fn from_authority(
        authority: &AuthorityStore,
        clock: Arc<dyn Clock>,
        heartbeat_timeout_ms: u64,
    ) -> CentralResult<Self> {
        let repository = authority.agent_registry().ok_or_else(|| {
            error(
                CentralErrorCode::InvalidState,
                "AuthorityStore has no Agent registry composition",
            )
        })?;
        Ok(Self {
            repository,
            clock,
            heartbeat_timeout_ms,
        })
    }

    pub async fn create_token_intent(
        &self,
        request: AgentEnrollmentTokenCreateRequest,
    ) -> CentralResult<CreateAgentEnrollmentTokenResult> {
        request.validate()?;
        if let Some(existing) = self
            .repository
            .get_by_token_request_id(&request.tenant_id, &request.token_request_id)
            .await?
        {
            if token_intent_matches_request(&existing, &request) {
                return Ok(CreateAgentEnrollmentTokenResult {
                    record: existing,
                    replayed: true,
                });
            }
            return Err(error(
                CentralErrorCode::EnrollmentIdReused,
                "token request identity was reused with another enrollment intent",
            ));
        }
        self.repository
            .expire_stale_token_intents(
                &request.tenant_id,
                &request.storage_volume_id,
                &request.edge_cluster_id,
                &request.pvc_identity_digest,
                self.clock.now(),
            )
            .await?;
        if let Some(binding) = self
            .repository
            .get_pvc_binding(&request.edge_cluster_id, &request.pvc_identity_digest)
            .await?
        {
            if binding.tenant_id != request.tenant_id
                || binding.edge_cluster_id != request.edge_cluster_id
                || binding.storage_volume_id != request.storage_volume_id
            {
                return Err(error(
                    CentralErrorCode::VolumeOwnerConflict,
                    "PVC identity is already bound to another StorageVolume",
                ));
            }
        }
        let current = self
            .repository
            .get_current_by_volume(&request.tenant_id, &request.storage_volume_id)
            .await?;
        if current.as_ref().is_some_and(|record| {
            record.enrollment.edge_cluster_id != request.edge_cluster_id
                || record.enrollment.volume_descriptor_digest != request.volume_descriptor_digest
                || record.enrollment.pvc_identity_digest != request.pvc_identity_digest
                || record.mount.expected_volume_marker != request.expected_volume_marker
                || record.mount.desired_access_mode != request.desired_access_mode
        }) {
            return Err(error(
                CentralErrorCode::AgentIdentityMismatch,
                "replacement token intent differs from the active Volume descriptor",
            ));
        }
        if current.as_ref().is_some_and(|record| {
            record.enrollment.reserved_agent_id == request.agent_id
                || record.enrollment.reserved_agent_mount_id == request.agent_mount_id
        }) {
            return Err(error(
                CentralErrorCode::AgentIdentityMismatch,
                "replacement token intent must reserve new Agent and mount identities",
            ));
        }
        let replaces_enrollment_id = current
            .as_ref()
            .map(|record| record.enrollment.enrollment_id.clone());
        let mount_generation = current.as_ref().map_or(MountGeneration::new(1), |record| {
            record.mount.mount_generation
        });
        let owner_generation = current.as_ref().map_or(OwnerGeneration::new(1), |record| {
            record.owner.owner_generation
        });
        let record = token_intent_record(
            request,
            replaces_enrollment_id,
            mount_generation,
            owner_generation,
        );
        match self.repository.insert_or_load(record.clone()).await? {
            AgentRegistryInsertOutcome::Inserted(record) => Ok(CreateAgentEnrollmentTokenResult {
                record,
                replayed: false,
            }),
            AgentRegistryInsertOutcome::Existing(existing) if existing == record => {
                Ok(CreateAgentEnrollmentTokenResult {
                    record: existing,
                    replayed: true,
                })
            }
            AgentRegistryInsertOutcome::Existing(_) => Err(error(
                CentralErrorCode::EnrollmentIdReused,
                "enrollment ID already belongs to another one-Volume request",
            )),
        }
    }

    pub async fn bootstrap_agent(
        &self,
        request: AgentBootstrapRequest,
    ) -> CentralResult<BootstrapAgentResult> {
        request.validate()?;
        let token_digest = ContentDigest::hash(request.bootstrap_token.as_bytes());
        let mut record = self
            .repository
            .get_by_token_digest(&token_digest)
            .await?
            .ok_or_else(|| {
                error(
                    CentralErrorCode::BootstrapDenied,
                    "bootstrap token is invalid or expired",
                )
            })?;
        if request.tenant_id != record.enrollment.tenant_id
            || request.edge_cluster_id != record.enrollment.edge_cluster_id
            || request.storage_volume_id != record.enrollment.storage_volume_id
            || request.volume_descriptor_digest != record.enrollment.volume_descriptor_digest
        {
            return Err(error(
                CentralErrorCode::AgentIdentityMismatch,
                "declared bootstrap scope differs from the token intent",
            ));
        }
        let now = self.clock.now();
        if record.enrollment.state == AgentEnrollmentState::TokenIssued
            && now.get() >= record.enrollment.expires_at_unix_ms.get()
        {
            let previous = record.resource_version.get();
            record.enrollment.state = AgentEnrollmentState::Expired;
            advance_resource_version(&mut record)?;
            match self.repository.replace(previous, record).await {
                Ok(_) => {}
                Err(concurrent) if concurrent.code() == CentralErrorCode::ConcurrentUpdate => {
                    let current = self
                        .repository
                        .get_by_token_digest(&token_digest)
                        .await?
                        .ok_or_else(|| {
                            error(
                                CentralErrorCode::BootstrapDenied,
                                "bootstrap token is invalid or expired",
                            )
                        })?;
                    if current.enrollment.state != AgentEnrollmentState::Expired {
                        return Err(concurrent);
                    }
                }
                Err(repository_error) => return Err(repository_error),
            }
            return Err(error(
                CentralErrorCode::BootstrapDenied,
                "bootstrap token is invalid or expired",
            ));
        }
        let candidate = AgentCandidateRecord {
            bootstrap_request_id: request.bootstrap_request_id,
            installation_id: request.installation_id,
            public_key_fingerprint: request.public_key_fingerprint,
            mount_identity_digest: request.probe.mount_identity_digest,
            agent_version: request.agent_version,
            supported_protocol_versions: request.supported_protocol_versions,
            capabilities: request.capabilities,
            probe: request.probe,
        };
        if let Some(existing) = self
            .repository
            .get_by_bootstrap_request_id(
                &record.enrollment.tenant_id,
                &candidate.bootstrap_request_id,
            )
            .await?
        {
            if existing.enrollment.enrollment_id != record.enrollment.enrollment_id {
                return Err(error(
                    CentralErrorCode::EnrollmentIdReused,
                    "bootstrap request identity is already bound to another enrollment",
                ));
            }
        }
        if let Some(existing) = self
            .repository
            .get_by_installation_id(&candidate.installation_id)
            .await?
        {
            if existing.enrollment.enrollment_id != record.enrollment.enrollment_id {
                return Err(error(
                    CentralErrorCode::AgentIdentityMismatch,
                    "Agent installation identity is already bound to another enrollment",
                ));
            }
        }
        if let Some(existing) = self
            .repository
            .get_by_public_key_fingerprint(&candidate.public_key_fingerprint)
            .await?
        {
            if existing.enrollment.enrollment_id != record.enrollment.enrollment_id {
                return Err(error(
                    CentralErrorCode::AgentIdentityMismatch,
                    "Agent credential identity is already bound to another enrollment",
                ));
            }
        }
        if let Some(existing) = &record.candidate {
            if existing != &candidate
                || record.enrollment.bootstrap_request_id.as_ref()
                    != Some(&candidate.bootstrap_request_id)
            {
                return Err(error(
                    CentralErrorCode::AgentIdentityMismatch,
                    "bootstrap request identity was reused by another Agent candidate",
                ));
            }
            if matches!(
                record.enrollment.state,
                AgentEnrollmentState::PendingApproval | AgentEnrollmentState::Approved
            ) {
                return Ok(BootstrapAgentResult {
                    accepted: bootstrap_accepted(&record, true),
                    record,
                });
            }
            return Err(error(
                CentralErrorCode::BootstrapDenied,
                "bootstrap identity is no longer eligible for approval",
            ));
        }
        if record.enrollment.state != AgentEnrollmentState::TokenIssued
            || now.get() >= record.enrollment.expires_at_unix_ms.get()
        {
            return Err(error(
                CentralErrorCode::BootstrapDenied,
                "bootstrap token is invalid or expired",
            ));
        }
        if let Some(previous_enrollment_id) = &record.enrollment.replaces_enrollment_id {
            let previous = self.load(previous_enrollment_id).await?;
            if let Some(previous_instance) = previous.instance {
                if candidate.installation_id == previous_instance.installation_id
                    || candidate.public_key_fingerprint == previous_instance.public_key_fingerprint
                {
                    return Err(error(
                        CentralErrorCode::AgentIdentityMismatch,
                        "replacement must use a new installation and credential identity",
                    ));
                }
            } else {
                return Err(error(
                    CentralErrorCode::InvalidState,
                    "replacement target has no approved Agent identity",
                ));
            }
        }
        let previous = record.resource_version.get();
        let review_expires_at = now
            .get()
            .checked_add(AGENT_ENROLLMENT_REVIEW_WINDOW_MS)
            .ok_or_else(|| {
                error(
                    CentralErrorCode::Internal,
                    "Agent enrollment review expiry exceeds the wire clock range",
                )
            })?;
        record.enrollment.state = AgentEnrollmentState::PendingApproval;
        record.enrollment.bootstrap_request_id = Some(candidate.bootstrap_request_id.clone());
        record.enrollment.bootstrapped_at_unix_ms = Some(now);
        record.enrollment.review_expires_at_unix_ms = Some(UnixMillis::new(review_expires_at));
        record.mount.mount_identity_digest = Some(candidate.mount_identity_digest);
        record.candidate = Some(candidate);
        advance_resource_version(&mut record)?;
        let record = self.repository.replace(previous, record).await?;
        Ok(BootstrapAgentResult {
            accepted: bootstrap_accepted(&record, false),
            record,
        })
    }

    pub async fn decide_enrollment(
        &self,
        request: AgentEnrollmentApprovalRequest,
        actor: PrincipalRef,
    ) -> CentralResult<DecideAgentEnrollmentResult> {
        request.validate()?;
        let mut record = self.load(&request.enrollment_id).await?;
        if let Some(existing) = self
            .repository
            .get_by_decision_request_id(&record.enrollment.tenant_id, &request.decision_request_id)
            .await?
        {
            if existing.enrollment.enrollment_id != record.enrollment.enrollment_id {
                return Err(error(
                    CentralErrorCode::EnrollmentDecisionConflict,
                    "decision request identity is already bound to another enrollment",
                ));
            }
        }
        if let Some(stored_request) = &record.enrollment.decision_request {
            if stored_request != &request {
                return Err(error(
                    CentralErrorCode::EnrollmentDecisionConflict,
                    "decision request identity or payload differs from the persisted decision",
                ));
            }
            let revoked_record = if request.decision == AgentEnrollmentDecision::Approve {
                match &record.enrollment.replaces_enrollment_id {
                    Some(enrollment_id) => Some(self.load(enrollment_id).await?),
                    None => None,
                }
            } else {
                None
            };
            require_decision_audit(&record)?;
            return Ok(DecideAgentEnrollmentResult {
                record,
                revoked_record,
                replayed: true,
            });
        }
        require_resource_version(&record, request.expected_resource_version)?;
        let now = self.clock.now();
        if record.enrollment.state == AgentEnrollmentState::PendingApproval
            && review_window_elapsed(&record, now)?
        {
            let previous = record.resource_version.get();
            record.enrollment.state = AgentEnrollmentState::Expired;
            advance_resource_version(&mut record)?;
            self.repository.replace(previous, record).await?;
            return Err(error(
                CentralErrorCode::EnrollmentExpired,
                "Agent enrollment review window expired",
            ));
        }
        if record.enrollment.state != AgentEnrollmentState::PendingApproval {
            return Err(error(
                CentralErrorCode::InvalidState,
                "enrollment decision requires pending approval",
            ));
        }
        if request.confirm_replacement {
            let confirms_replacement = request.decision == AgentEnrollmentDecision::Approve
                && record.enrollment.replaces_enrollment_id.is_some();
            if !confirms_replacement {
                return Err(error(
                    CentralErrorCode::ProtocolInvalid,
                    "replacement confirmation is only valid when approving a replacement enrollment",
                ));
            }
        }
        let candidate = record.candidate.clone().ok_or_else(|| {
            error(
                CentralErrorCode::ApprovalRequired,
                "an Agent candidate must bootstrap before tenant approval",
            )
        })?;
        if request.decision == AgentEnrollmentDecision::Reject {
            let previous = record.resource_version.get();
            record.enrollment.state = AgentEnrollmentState::Rejected;
            record.enrollment.decided_at_unix_ms = Some(now);
            record.enrollment.decided_by = Some(actor);
            record.enrollment.decision_request = Some(request);
            advance_resource_version(&mut record)?;
            record.decision_audit_event = Some(decision_audit_event(&record)?);
            let record = self.repository.replace(previous, record).await?;
            return Ok(DecideAgentEnrollmentResult {
                record,
                revoked_record: None,
                replayed: false,
            });
        }
        validate_bootstrap_probe(&record, &candidate.probe)?;
        let instance = active_instance(&record.enrollment.reserved_agent_id, candidate);
        if let Some(previous_enrollment_id) = record.enrollment.replaces_enrollment_id.clone() {
            if !request.confirm_replacement {
                return Err(error(
                    CentralErrorCode::VolumeOwnerConflict,
                    "replacement approval requires explicit confirmation",
                ));
            }
            let mut previous_record = self.load(&previous_enrollment_id).await?;
            require_approved(&previous_record)?;
            validate_replacement_scope(&previous_record, &record, &instance)?;
            let previous_version = previous_record.resource_version.get();
            let replacement_version = record.resource_version.get();
            let next_mount_generation = checked_next_generation(
                previous_record.mount.mount_generation.get(),
                "mount generation",
            )?;
            let next_owner_generation = checked_next_generation(
                previous_record.owner.owner_generation.get(),
                "owner generation",
            )?;

            revoke_for_replacement(
                &mut previous_record,
                &record.enrollment.enrollment_id,
                next_mount_generation,
                next_owner_generation,
            )?;
            record.enrollment.state = AgentEnrollmentState::Approved;
            record.enrollment.decided_at_unix_ms = Some(now);
            record.enrollment.decided_by = Some(actor);
            record.enrollment.decision_request = Some(request);
            record.mount.mount_generation = MountGeneration::new(next_mount_generation);
            record.owner.owner_generation = OwnerGeneration::new(next_owner_generation);
            record.owner.active_agent_id = Some(instance.agent_id.clone());
            record.owner.active_agent_mount_id = Some(record.mount.agent_mount_id.clone());
            record.owner.state = VolumeOwnerState::RecoveryRequired;
            record.instance = Some(instance);
            advance_resource_version(&mut record)?;
            record.decision_audit_event = Some(decision_audit_event(&record)?);
            let records = self
                .repository
                .activate_replacement(
                    previous_version,
                    previous_record,
                    replacement_version,
                    record,
                )
                .await?;
            return Ok(DecideAgentEnrollmentResult {
                record: records.replacement,
                revoked_record: Some(records.revoked),
                replayed: false,
            });
        }

        record.enrollment.state = AgentEnrollmentState::Approved;
        record.enrollment.decided_at_unix_ms = Some(now);
        record.enrollment.decided_by = Some(actor);
        record.enrollment.decision_request = Some(request);
        record.owner.active_agent_id = Some(instance.agent_id.clone());
        record.owner.active_agent_mount_id = Some(record.mount.agent_mount_id.clone());
        record.owner.state = VolumeOwnerState::Active;
        record.instance = Some(instance);
        let previous = record.resource_version.get();
        advance_resource_version(&mut record)?;
        record.decision_audit_event = Some(decision_audit_event(&record)?);
        let record = self.repository.replace(previous, record).await?;
        Ok(DecideAgentEnrollmentResult {
            record,
            revoked_record: None,
            replayed: false,
        })
    }

    pub async fn open_session(
        &self,
        request: OpenAgentSessionRequest,
    ) -> CentralResult<OpenAgentSessionResult> {
        let mut record = self.load_by_agent(&request.agent_id).await?;
        require_approved(&record)?;
        let instance = record.instance.as_ref().ok_or_else(|| {
            error(
                CentralErrorCode::AgentIdentityMismatch,
                "approved enrollment has no Agent instance",
            )
        })?;
        if instance.installation_id != request.installation_id {
            return Err(error(
                CentralErrorCode::AgentIdentityMismatch,
                "session installation differs from bootstrap identity",
            ));
        }
        if record.mount.mount_identity_digest.as_ref() != Some(&request.mount_identity_digest) {
            return Err(error(
                CentralErrorCode::AgentIdentityMismatch,
                "session mount identity differs from the approved bootstrap",
            ));
        }
        if instance.active_boot_id.as_ref() == Some(&request.boot_id) {
            if instance.session_open_expected_resource_version
                != Some(request.expected_resource_version)
            {
                return Err(error(
                    CentralErrorCode::ConcurrentUpdate,
                    "session open replay payload differs from the persisted request",
                ));
            }
            let session_generation = instance.session_generation.ok_or_else(|| {
                error(
                    CentralErrorCode::Internal,
                    "active session is missing its generation",
                )
            })?;
            return Ok(OpenAgentSessionResult {
                session_generation,
                record,
                replayed: true,
            });
        }
        require_resource_version(&record, request.expected_resource_version)?;
        let now = self.clock.now();
        let stale_active_session = instance.active_boot_id.is_some()
            && session_is_stale(instance, now, self.heartbeat_timeout_ms);
        if instance.active_boot_id.is_some() && !stale_active_session {
            return Err(error(
                CentralErrorCode::AgentSessionActive,
                "another Agent boot already owns the active session",
            ));
        }
        let instance = record
            .instance
            .as_mut()
            .expect("instance was checked above");
        let next_generation = instance
            .session_generation
            .map_or(Some(1), |generation| generation.get().checked_add(1))
            .ok_or_else(|| error(CentralErrorCode::Internal, "session generation exhausted"))?;
        instance.session_generation = Some(SessionGeneration::new(next_generation));
        instance.active_boot_id = Some(request.boot_id);
        instance.session_open_expected_resource_version = Some(request.expected_resource_version);
        instance.session_opened_at_unix_ms = Some(now);
        instance.last_heartbeat_at_unix_ms = None;
        instance.last_sequence = None;
        let generation = instance
            .session_generation
            .expect("generation was assigned");
        if stale_active_session {
            record.mount.observed_volume_marker = None;
            record.mount.observed_access_mode = None;
            record.mount.reported_health = None;
            record.mount.health = ResourceHealth::Unknown;
            record.mount.observed_at_unix_ms = None;
        }
        let previous = record.resource_version.get();
        advance_resource_version(&mut record)?;
        let record = self.repository.replace(previous, record).await?;
        Ok(OpenAgentSessionResult {
            record,
            session_generation: generation,
            replayed: false,
        })
    }

    pub async fn close_session(
        &self,
        request: CloseAgentSessionRequest,
    ) -> CentralResult<AgentRegistryRecord> {
        let mut record = self.load_by_agent(&request.agent_id).await?;
        require_resource_version(&record, request.expected_resource_version)?;
        let instance = require_session(&mut record, &request.boot_id, request.session_generation)?;
        instance.active_boot_id = None;
        instance.session_open_expected_resource_version = None;
        instance.session_opened_at_unix_ms = None;
        instance.last_heartbeat_at_unix_ms = None;
        instance.last_sequence = None;
        record.mount.health = ResourceHealth::Unknown;
        record.mount.observed_at_unix_ms = None;
        let previous = record.resource_version.get();
        advance_resource_version(&mut record)?;
        self.repository.replace(previous, record).await
    }

    pub async fn report_mount(
        &self,
        report: AgentMountStatusReport,
    ) -> CentralResult<ReportAgentMountResult> {
        report.validate()?;
        let received_at_unix_ms = self.clock.now();
        let mut record = self.load_by_agent(&report.agent_id).await?;
        require_approved(&record)?;
        validate_report_scope(&record, &report)?;
        let instance = require_session(&mut record, &report.boot_id, report.session_generation)?;
        if let Some(last) = instance.last_sequence {
            if report.sequence.get() < last.get() {
                return Err(error(
                    CentralErrorCode::GenerationMismatch,
                    "mount report sequence moved backwards",
                ));
            }
            if report.sequence == last {
                let replayed = record.mount.observed_at_unix_ms == Some(report.observed_at_unix_ms)
                    && record.mount.observed_volume_marker
                        == Some(report.observed_volume_marker.clone())
                    && record.mount.mount_identity_digest == Some(report.mount_identity_digest)
                    && record.mount.observed_access_mode == Some(report.access_mode)
                    && record.mount.reported_health == Some(report.health);
                if replayed {
                    return Ok(ReportAgentMountResult {
                        record,
                        replayed: true,
                    });
                }
                return Err(error(
                    CentralErrorCode::GenerationMismatch,
                    "mount report sequence was reused with another observation",
                ));
            }
        }
        instance.last_sequence = Some(report.sequence);
        instance.last_heartbeat_at_unix_ms = Some(received_at_unix_ms);
        record.mount.observed_volume_marker = Some(report.observed_volume_marker.clone());
        record.mount.observed_access_mode = Some(report.access_mode);
        record.mount.reported_health = Some(report.health);
        record.mount.observed_at_unix_ms = Some(report.observed_at_unix_ms);
        record.mount.health =
            if report.observed_volume_marker != record.mount.expected_volume_marker {
                ResourceHealth::Unavailable
            } else if record.mount.desired_access_mode == MountAccessMode::ReadWrite
                && report.access_mode != MountAccessMode::ReadWrite
            {
                ResourceHealth::Degraded
            } else {
                report.health
            };
        let previous = record.resource_version.get();
        advance_resource_version(&mut record)?;
        let record = self.repository.replace(previous, record).await?;
        Ok(ReportAgentMountResult {
            record,
            replayed: false,
        })
    }

    pub async fn complete_recovery(
        &self,
        request: CompleteVolumeRecoveryRequest,
    ) -> CentralResult<AgentRegistryRecord> {
        let mut record = self.load(&request.enrollment_id).await?;
        require_resource_version(&record, request.expected_resource_version)?;
        if record.owner.owner_generation != request.owner_generation
            || record.owner.state != VolumeOwnerState::RecoveryRequired
        {
            return Err(error(
                CentralErrorCode::VolumeOwnerConflict,
                "Volume is not awaiting recovery for this owner generation",
            ));
        }
        if record.derived_volume_state_without_owner(self.clock.now(), self.heartbeat_timeout_ms)
            != DerivedVolumeState::Ready
        {
            return Err(error(
                CentralErrorCode::InvalidState,
                "Volume mount must report Ready before recovery completes",
            ));
        }
        record.owner.state = VolumeOwnerState::Active;
        let previous = record.resource_version.get();
        advance_resource_version(&mut record)?;
        self.repository.replace(previous, record).await
    }

    pub async fn expire_enrollment(
        &self,
        request: ExpireAgentEnrollmentRequest,
    ) -> CentralResult<AgentRegistryRecord> {
        let mut record = self.load(&request.enrollment_id).await?;
        require_resource_version(&record, request.expected_resource_version)?;
        let elapsed = match record.enrollment.state {
            AgentEnrollmentState::TokenIssued => {
                self.clock.now().get() >= record.enrollment.expires_at_unix_ms.get()
            }
            AgentEnrollmentState::PendingApproval => {
                review_window_elapsed(&record, self.clock.now())?
            }
            _ => {
                return Err(error(
                    CentralErrorCode::InvalidState,
                    "only an issued token or pending Agent enrollment can expire",
                ));
            }
        };
        if !elapsed {
            return Err(error(
                CentralErrorCode::InvalidState,
                "Agent enrollment expiry window has not elapsed",
            ));
        }
        let previous = record.resource_version.get();
        record.enrollment.state = AgentEnrollmentState::Expired;
        advance_resource_version(&mut record)?;
        self.repository.replace(previous, record).await
    }

    pub async fn volume_state(
        &self,
        enrollment_id: &AgentEnrollmentId,
    ) -> CentralResult<DerivedVolumeState> {
        let record = self.load(enrollment_id).await?;
        Ok(record.derived_volume_state(self.clock.now(), self.heartbeat_timeout_ms))
    }

    async fn load(&self, enrollment_id: &AgentEnrollmentId) -> CentralResult<AgentRegistryRecord> {
        self.repository.get(enrollment_id).await?.ok_or_else(|| {
            error(
                CentralErrorCode::EnrollmentNotFound,
                "enrollment was not found",
            )
        })
    }

    async fn load_by_agent(&self, agent_id: &AgentId) -> CentralResult<AgentRegistryRecord> {
        self.repository
            .get_by_agent(agent_id)
            .await?
            .ok_or_else(|| {
                error(
                    CentralErrorCode::EnrollmentNotFound,
                    "Agent was not enrolled",
                )
            })
    }
}

pub(crate) fn decision_audit_event(
    record: &AgentRegistryRecord,
) -> CentralResult<AgentEnrollmentAuditEvent> {
    let request = record.enrollment.decision_request.as_ref().ok_or_else(|| {
        error(
            CentralErrorCode::Internal,
            "decided enrollment has no stable decision request",
        )
    })?;
    let kind = match (
        request.decision,
        record.enrollment.replaces_enrollment_id.is_some(),
    ) {
        (AgentEnrollmentDecision::Approve, true) => AgentEnrollmentAuditKind::ReplacementApproved,
        (AgentEnrollmentDecision::Approve, false) => AgentEnrollmentAuditKind::Approved,
        (AgentEnrollmentDecision::Reject, _) => AgentEnrollmentAuditKind::Rejected,
    };
    let actor = record.enrollment.decided_by.clone().ok_or_else(|| {
        error(
            CentralErrorCode::Internal,
            "decided enrollment has no audit actor",
        )
    })?;
    let occurred_at_unix_ms = record.enrollment.decided_at_unix_ms.ok_or_else(|| {
        error(
            CentralErrorCode::Internal,
            "decided enrollment has no audit timestamp",
        )
    })?;
    let decision_resource_version = request
        .expected_resource_version
        .get()
        .checked_add(1)
        .map(ResourceVersion::new)
        .ok_or_else(|| {
            error(
                CentralErrorCode::Internal,
                "decision ResourceVersion exceeds the registry range",
            )
        })?;
    Ok(AgentEnrollmentAuditEvent {
        event_id: format!(
            "agent-enrollment:{}:{}",
            record.enrollment.enrollment_id, request.decision_request_id
        ),
        kind,
        tenant_id: record.enrollment.tenant_id.clone(),
        enrollment_id: record.enrollment.enrollment_id.clone(),
        storage_volume_id: record.enrollment.storage_volume_id.clone(),
        decision_request_id: request.decision_request_id.clone(),
        resource_version: decision_resource_version,
        actor,
        occurred_at_unix_ms,
    })
}

fn require_decision_audit(record: &AgentRegistryRecord) -> CentralResult<()> {
    let expected = decision_audit_event(record)?;
    if record.decision_audit_event.as_ref() == Some(&expected) {
        Ok(())
    } else {
        Err(error(
            CentralErrorCode::Internal,
            "decided enrollment is missing its immutable audit event",
        ))
    }
}

impl AgentRegistryRecord {
    fn derived_volume_state_without_owner(
        &self,
        now_unix_ms: UnixMillis,
        heartbeat_timeout_ms: u64,
    ) -> DerivedVolumeState {
        let mut candidate = self.clone();
        candidate.owner.state = VolumeOwnerState::Active;
        candidate.derived_volume_state(now_unix_ms, heartbeat_timeout_ms)
    }
}

fn token_intent_record(
    request: AgentEnrollmentTokenCreateRequest,
    replaces_enrollment_id: Option<AgentEnrollmentId>,
    mount_generation: MountGeneration,
    owner_generation: OwnerGeneration,
) -> AgentRegistryRecord {
    AgentRegistryRecord {
        resource_version: ResourceVersion::new(1),
        enrollment: AgentEnrollmentRecord {
            token_id: request.token_id,
            token_request_id: request.token_request_id,
            enrollment_id: request.enrollment_id,
            tenant_id: request.tenant_id.clone(),
            edge_cluster_id: request.edge_cluster_id,
            storage_volume_id: request.storage_volume_id.clone(),
            volume_descriptor_digest: request.volume_descriptor_digest,
            pvc_identity_digest: request.pvc_identity_digest,
            reserved_agent_id: request.agent_id,
            reserved_agent_mount_id: request.agent_mount_id.clone(),
            bootstrap_token_digest: ContentDigest::hash(request.bootstrap_token.as_bytes()),
            state: AgentEnrollmentState::TokenIssued,
            created_at_unix_ms: request.created_at_unix_ms,
            expires_at_unix_ms: request.expires_at_unix_ms,
            bootstrapped_at_unix_ms: None,
            bootstrap_request_id: None,
            review_expires_at_unix_ms: None,
            decided_at_unix_ms: None,
            decided_by: None,
            decision_request: None,
            replaces_enrollment_id,
            replaced_by_enrollment_id: None,
            extensions: request.extensions,
        },
        candidate: None,
        instance: None,
        mount: AgentMountRecord {
            agent_mount_id: request.agent_mount_id,
            storage_volume_id: request.storage_volume_id.clone(),
            mount_generation,
            expected_volume_marker: request.expected_volume_marker,
            desired_access_mode: request.desired_access_mode,
            mount_identity_digest: None,
            observed_volume_marker: None,
            observed_access_mode: None,
            reported_health: None,
            health: ResourceHealth::Unknown,
            observed_at_unix_ms: None,
        },
        owner: VolumeOwnerRecord {
            tenant_id: request.tenant_id,
            storage_volume_id: request.storage_volume_id,
            active_agent_id: None,
            active_agent_mount_id: None,
            owner_generation,
            state: VolumeOwnerState::Inactive,
        },
        decision_audit_event: None,
    }
}

fn token_intent_matches_request(
    record: &AgentRegistryRecord,
    request: &AgentEnrollmentTokenCreateRequest,
) -> bool {
    record.enrollment.token_id == request.token_id
        && record.enrollment.token_request_id == request.token_request_id
        && record.enrollment.enrollment_id == request.enrollment_id
        && record.enrollment.tenant_id == request.tenant_id
        && record.enrollment.edge_cluster_id == request.edge_cluster_id
        && record.enrollment.storage_volume_id == request.storage_volume_id
        && record.enrollment.volume_descriptor_digest == request.volume_descriptor_digest
        && record.enrollment.pvc_identity_digest == request.pvc_identity_digest
        && record.enrollment.reserved_agent_id == request.agent_id
        && record.enrollment.reserved_agent_mount_id == request.agent_mount_id
        && record.enrollment.bootstrap_token_digest
            == ContentDigest::hash(request.bootstrap_token.as_bytes())
        && record.enrollment.created_at_unix_ms == request.created_at_unix_ms
        && record.enrollment.expires_at_unix_ms == request.expires_at_unix_ms
        && record.enrollment.extensions == request.extensions
        && record.mount.expected_volume_marker == request.expected_volume_marker
        && record.mount.desired_access_mode == request.desired_access_mode
}

fn active_instance(agent_id: &AgentId, candidate: AgentCandidateRecord) -> AgentInstanceRecord {
    AgentInstanceRecord {
        agent_id: agent_id.clone(),
        installation_id: candidate.installation_id,
        public_key_fingerprint: candidate.public_key_fingerprint,
        agent_version: candidate.agent_version,
        supported_protocol_versions: candidate.supported_protocol_versions,
        capabilities: candidate.capabilities,
        state: AgentInstanceState::Active,
        session_generation: None,
        active_boot_id: None,
        session_open_expected_resource_version: None,
        session_opened_at_unix_ms: None,
        last_heartbeat_at_unix_ms: None,
        last_sequence: None,
    }
}

fn validate_bootstrap_probe(
    record: &AgentRegistryRecord,
    probe: &AgentBootstrapProbe,
) -> CentralResult<()> {
    let marker_matches =
        probe.observed_volume_marker.as_ref() == Some(&record.mount.expected_volume_marker);
    let access_mode_matches = probe.access_mode == Some(record.mount.desired_access_mode);
    let read_write_capabilities = record.mount.desired_access_mode != MountAccessMode::ReadWrite
        || (probe.rename_supported && probe.fsync_supported);
    if !probe.marker_matches
        || !marker_matches
        || !probe.mount_boundary_detected
        || !access_mode_matches
        || !read_write_capabilities
        || matches!(
            probe.health,
            ResourceHealth::Unavailable | ResourceHealth::Unknown
        )
    {
        return Err(error(
            CentralErrorCode::EnrollmentProbeFailed,
            "bootstrap probe does not prove the frozen Volume marker, mount boundary, access mode, and required filesystem semantics",
        ));
    }
    Ok(())
}

fn validate_replacement_scope(
    previous: &AgentRegistryRecord,
    replacement: &AgentRegistryRecord,
    replacement_instance: &AgentInstanceRecord,
) -> CentralResult<()> {
    let previous_instance = previous.instance.as_ref().ok_or_else(|| {
        error(
            CentralErrorCode::AgentIdentityMismatch,
            "replacement target has no approved Agent identity",
        )
    })?;
    if previous.enrollment.tenant_id != replacement.enrollment.tenant_id
        || previous.enrollment.edge_cluster_id != replacement.enrollment.edge_cluster_id
        || previous.enrollment.storage_volume_id != replacement.enrollment.storage_volume_id
        || previous.enrollment.volume_descriptor_digest
            != replacement.enrollment.volume_descriptor_digest
        || previous.enrollment.pvc_identity_digest != replacement.enrollment.pvc_identity_digest
        || previous.mount.expected_volume_marker != replacement.mount.expected_volume_marker
        || previous.mount.desired_access_mode != replacement.mount.desired_access_mode
        || previous.mount.mount_identity_digest != replacement.mount.mount_identity_digest
    {
        return Err(error(
            CentralErrorCode::AgentIdentityMismatch,
            "replacement scope differs from the active Volume binding",
        ));
    }
    if previous_instance.agent_id == replacement_instance.agent_id
        || previous_instance.installation_id == replacement_instance.installation_id
        || previous_instance.public_key_fingerprint == replacement_instance.public_key_fingerprint
        || previous.mount.agent_mount_id == replacement.mount.agent_mount_id
    {
        return Err(error(
            CentralErrorCode::AgentIdentityMismatch,
            "replacement must use new Agent, installation, credential, and mount identities",
        ));
    }
    Ok(())
}

fn revoke_for_replacement(
    record: &mut AgentRegistryRecord,
    replaced_by_enrollment_id: &AgentEnrollmentId,
    mount_generation: u64,
    owner_generation: u64,
) -> CentralResult<()> {
    record.enrollment.state = AgentEnrollmentState::Revoked;
    record.enrollment.replaced_by_enrollment_id = Some(replaced_by_enrollment_id.clone());
    let instance = record.instance.as_mut().ok_or_else(|| {
        error(
            CentralErrorCode::AgentIdentityMismatch,
            "replacement target has no approved Agent identity",
        )
    })?;
    instance.state = AgentInstanceState::Revoked;
    instance.active_boot_id = None;
    instance.session_open_expected_resource_version = None;
    instance.session_opened_at_unix_ms = None;
    instance.last_heartbeat_at_unix_ms = None;
    instance.last_sequence = None;
    record.mount.mount_generation = MountGeneration::new(mount_generation);
    record.mount.health = ResourceHealth::Unknown;
    record.mount.observed_volume_marker = None;
    record.mount.observed_access_mode = None;
    record.mount.reported_health = None;
    record.mount.observed_at_unix_ms = None;
    record.owner.owner_generation = OwnerGeneration::new(owner_generation);
    record.owner.active_agent_id = None;
    record.owner.active_agent_mount_id = None;
    record.owner.state = VolumeOwnerState::Inactive;
    advance_resource_version(record)
}

fn checked_next_generation(current: u64, name: &'static str) -> CentralResult<u64> {
    current.checked_add(1).ok_or_else(|| {
        error(
            CentralErrorCode::Internal,
            format!("{name} exhausted during Agent replacement"),
        )
    })
}

fn review_window_elapsed(record: &AgentRegistryRecord, now: UnixMillis) -> CentralResult<bool> {
    let expires_at = record.enrollment.review_expires_at_unix_ms.ok_or_else(|| {
        error(
            CentralErrorCode::Internal,
            "pending Agent enrollment has no review expiry",
        )
    })?;
    Ok(now.get() >= expires_at.get())
}

fn bootstrap_accepted(record: &AgentRegistryRecord, replayed: bool) -> AgentBootstrapAccepted {
    AgentBootstrapAccepted {
        bootstrap_request_id: record
            .enrollment
            .bootstrap_request_id
            .clone()
            .expect("accepted bootstrap has a request identity"),
        enrollment_id: record.enrollment.enrollment_id.clone(),
        agent_id: record.enrollment.reserved_agent_id.clone(),
        agent_mount_id: record.enrollment.reserved_agent_mount_id.clone(),
        state: record.enrollment.state,
        mount_generation: record.mount.mount_generation,
        resource_version: record.resource_version,
        review_expires_at_unix_ms: record
            .enrollment
            .review_expires_at_unix_ms
            .expect("accepted bootstrap has a review expiry"),
        replayed,
        extensions: Extensions::new(),
    }
}

fn require_resource_version(
    record: &AgentRegistryRecord,
    expected: ResourceVersion,
) -> CentralResult<()> {
    if record.resource_version != expected {
        Err(error(
            CentralErrorCode::ConcurrentUpdate,
            "Agent registry ResourceVersion changed",
        ))
    } else {
        Ok(())
    }
}

fn require_approved(record: &AgentRegistryRecord) -> CentralResult<()> {
    if record.enrollment.state == AgentEnrollmentState::Approved
        && record
            .instance
            .as_ref()
            .is_some_and(|instance| instance.state == AgentInstanceState::Active)
    {
        Ok(())
    } else {
        Err(error(
            CentralErrorCode::ApprovalRequired,
            "Agent enrollment is not approved",
        ))
    }
}

fn require_session<'a>(
    record: &'a mut AgentRegistryRecord,
    boot_id: &AgentBootId,
    generation: SessionGeneration,
) -> CentralResult<&'a mut AgentInstanceRecord> {
    let instance = record.instance.as_mut().ok_or_else(|| {
        error(
            CentralErrorCode::AgentIdentityMismatch,
            "enrollment has no Agent instance",
        )
    })?;
    if instance.active_boot_id.as_ref() != Some(boot_id)
        || instance.session_generation != Some(generation)
    {
        return Err(error(
            CentralErrorCode::GenerationMismatch,
            "Agent report belongs to a stale session",
        ));
    }
    Ok(instance)
}

fn validate_report_scope(
    record: &AgentRegistryRecord,
    report: &AgentMountStatusReport,
) -> CentralResult<()> {
    let instance = record.instance.as_ref().ok_or_else(|| {
        error(
            CentralErrorCode::AgentIdentityMismatch,
            "enrollment has no Agent instance",
        )
    })?;
    if report.installation_id != instance.installation_id
        || report.agent_mount_id != record.mount.agent_mount_id
        || report.storage_volume_id != record.mount.storage_volume_id
        || record.mount.mount_identity_digest.as_ref() != Some(&report.mount_identity_digest)
    {
        return Err(error(
            CentralErrorCode::AgentIdentityMismatch,
            "mount report identity differs from the approved enrollment",
        ));
    }
    if report.mount_generation != record.mount.mount_generation
        || report.owner_generation != record.owner.owner_generation
    {
        return Err(error(
            CentralErrorCode::GenerationMismatch,
            "mount report carries a stale mount or owner generation",
        ));
    }
    Ok(())
}

fn session_is_stale(
    instance: &AgentInstanceRecord,
    now_unix_ms: UnixMillis,
    heartbeat_timeout_ms: u64,
) -> bool {
    instance
        .last_heartbeat_at_unix_ms
        .or(instance.session_opened_at_unix_ms)
        .is_some_and(|last_seen| {
            now_unix_ms.get().saturating_sub(last_seen.get()) > heartbeat_timeout_ms
        })
}

fn advance_resource_version(record: &mut AgentRegistryRecord) -> CentralResult<()> {
    let next = record
        .resource_version
        .get()
        .checked_add(1)
        .ok_or_else(|| error(CentralErrorCode::Internal, "ResourceVersion exhausted"))?;
    record.resource_version = ResourceVersion::new(next);
    Ok(())
}

pub(crate) fn ensure_immutable_registry_scope(
    stored: &AgentRegistryRecord,
    updated: &AgentRegistryRecord,
) -> CentralResult<()> {
    let enrollment_scope_matches = stored.enrollment.token_id == updated.enrollment.token_id
        && stored.enrollment.token_request_id == updated.enrollment.token_request_id
        && stored.enrollment.enrollment_id == updated.enrollment.enrollment_id
        && stored.enrollment.tenant_id == updated.enrollment.tenant_id
        && stored.enrollment.edge_cluster_id == updated.enrollment.edge_cluster_id
        && stored.enrollment.storage_volume_id == updated.enrollment.storage_volume_id
        && stored.enrollment.volume_descriptor_digest
            == updated.enrollment.volume_descriptor_digest
        && stored.enrollment.pvc_identity_digest == updated.enrollment.pvc_identity_digest
        && stored.enrollment.reserved_agent_id == updated.enrollment.reserved_agent_id
        && stored.enrollment.reserved_agent_mount_id == updated.enrollment.reserved_agent_mount_id
        && stored.enrollment.bootstrap_token_digest == updated.enrollment.bootstrap_token_digest
        && stored.enrollment.created_at_unix_ms == updated.enrollment.created_at_unix_ms
        && stored.enrollment.expires_at_unix_ms == updated.enrollment.expires_at_unix_ms
        && stored.enrollment.replaces_enrollment_id == updated.enrollment.replaces_enrollment_id
        && stored.enrollment.extensions == updated.enrollment.extensions;
    let candidate_transition_valid = match (&stored.candidate, &updated.candidate) {
        (None, None) => updated.mount.mount_identity_digest.is_none(),
        (None, Some(candidate)) => {
            candidate.mount_identity_digest == candidate.probe.mount_identity_digest
                && updated.mount.mount_identity_digest == Some(candidate.mount_identity_digest)
        }
        (Some(stored), Some(updated)) => stored == updated,
        (Some(_), None) => false,
    };
    let mount_scope_matches = stored.mount.agent_mount_id == updated.mount.agent_mount_id
        && stored.mount.storage_volume_id == updated.mount.storage_volume_id
        && stored.mount.expected_volume_marker == updated.mount.expected_volume_marker
        && stored.mount.desired_access_mode == updated.mount.desired_access_mode
        && candidate_transition_valid;
    let owner_scope_matches = stored.owner.tenant_id == updated.owner.tenant_id
        && stored.owner.storage_volume_id == updated.owner.storage_volume_id;
    let decision_audit_transition_valid =
        match (&stored.decision_audit_event, &updated.decision_audit_event) {
            (None, None | Some(_)) => true,
            (Some(stored), Some(updated)) => stored == updated,
            (Some(_), None) => false,
        };
    let decision_request_transition_valid = match (
        &stored.enrollment.decision_request,
        &updated.enrollment.decision_request,
    ) {
        (None, _) => true,
        (Some(stored), Some(updated)) => stored == updated,
        (Some(_), None) => false,
    };
    let decision_actor_transition_valid = match (
        &stored.enrollment.decided_by,
        &updated.enrollment.decided_by,
    ) {
        (None, _) => true,
        (Some(stored), Some(updated)) => stored == updated,
        (Some(_), None) => false,
    };
    let decision_time_transition_valid = stored.enrollment.decided_at_unix_ms.is_none()
        || stored.enrollment.decided_at_unix_ms == updated.enrollment.decided_at_unix_ms;
    let bootstrap_time_transition_valid = stored.enrollment.bootstrapped_at_unix_ms.is_none()
        || stored.enrollment.bootstrapped_at_unix_ms == updated.enrollment.bootstrapped_at_unix_ms;
    let review_expiry_transition_valid = stored.enrollment.review_expires_at_unix_ms.is_none()
        || stored.enrollment.review_expires_at_unix_ms
            == updated.enrollment.review_expires_at_unix_ms;
    let replacement_link_transition_valid = stored
        .enrollment
        .replaced_by_enrollment_id
        .as_ref()
        .is_none()
        || stored.enrollment.replaced_by_enrollment_id
            == updated.enrollment.replaced_by_enrollment_id;
    if !enrollment_scope_matches
        || !mount_scope_matches
        || !owner_scope_matches
        || !decision_audit_transition_valid
        || !decision_request_transition_valid
        || !decision_actor_transition_valid
        || !decision_time_transition_valid
        || !bootstrap_time_transition_valid
        || !review_expiry_transition_valid
        || !replacement_link_transition_valid
    {
        return Err(error(
            CentralErrorCode::AgentIdentityMismatch,
            "Agent registry update attempted to change immutable identity or Volume scope",
        ));
    }
    Ok(())
}

pub(crate) fn validate_registry_replace_transition(
    stored: &AgentRegistryRecord,
    updated: &AgentRegistryRecord,
) -> CentralResult<()> {
    if stored.mount.mount_generation != updated.mount.mount_generation
        || stored.owner.owner_generation != updated.owner.owner_generation
    {
        return Err(registry_corruption(
            "Agent registry replace attempted to change a fenced generation",
        ));
    }
    let valid = (stored.enrollment.state == AgentEnrollmentState::Approved
        && updated.enrollment.state == AgentEnrollmentState::Approved)
        || matches!(
            (stored.enrollment.state, updated.enrollment.state),
            (
                AgentEnrollmentState::TokenIssued,
                AgentEnrollmentState::PendingApproval | AgentEnrollmentState::Expired
            ) | (
                AgentEnrollmentState::PendingApproval,
                AgentEnrollmentState::Rejected | AgentEnrollmentState::Expired
            )
        )
        || (stored.enrollment.state == AgentEnrollmentState::PendingApproval
            && updated.enrollment.state == AgentEnrollmentState::Approved
            && stored.enrollment.replaces_enrollment_id.is_none());
    if !valid {
        return Err(registry_corruption(
            "Agent registry replace attempted an invalid enrollment state transition",
        ));
    }
    if stored.enrollment.state == AgentEnrollmentState::PendingApproval
        && matches!(
            updated.enrollment.state,
            AgentEnrollmentState::Approved | AgentEnrollmentState::Rejected
        )
    {
        let decision_matches_cas =
            updated
                .enrollment
                .decision_request
                .as_ref()
                .is_some_and(|request| {
                    request.expected_resource_version == stored.resource_version
                        && !request.confirm_replacement
                });
        if !decision_matches_cas {
            return Err(registry_corruption(
                "Agent enrollment decision is not bound to its repository CAS",
            ));
        }
    }
    if stored.enrollment.state == AgentEnrollmentState::Approved
        && updated.enrollment.state == AgentEnrollmentState::Approved
    {
        let owner_state_valid = stored.owner.state == updated.owner.state
            || (stored.owner.state == VolumeOwnerState::RecoveryRequired
                && updated.owner.state == VolumeOwnerState::Active);
        let runtime_scope_frozen = stored.owner.active_agent_id == updated.owner.active_agent_id
            && stored.owner.active_agent_mount_id == updated.owner.active_agent_mount_id
            && stored
                .instance
                .as_ref()
                .zip(updated.instance.as_ref())
                .is_some_and(|(stored, updated)| {
                    stored.agent_id == updated.agent_id
                        && stored.installation_id == updated.installation_id
                        && stored.public_key_fingerprint == updated.public_key_fingerprint
                        && stored.agent_version == updated.agent_version
                        && stored.supported_protocol_versions == updated.supported_protocol_versions
                        && stored.capabilities == updated.capabilities
                        && stored.state == updated.state
                });
        if !owner_state_valid || !runtime_scope_frozen {
            return Err(registry_corruption(
                "approved Agent runtime update changed fenced owner or static identity state",
            ));
        }
    }
    Ok(())
}

pub(crate) fn validate_registry_insert(record: &AgentRegistryRecord) -> CentralResult<()> {
    validate_registry_record(record)?;
    if record.enrollment.state != AgentEnrollmentState::TokenIssued {
        return Err(registry_corruption(
            "Agent registry insertion requires a fresh token intent",
        ));
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn validate_registry_replacement_transition(
    stored_previous: &AgentRegistryRecord,
    expected_previous: u64,
    revoked: &AgentRegistryRecord,
    stored_replacement: &AgentRegistryRecord,
    expected_replacement: u64,
    replacement: &AgentRegistryRecord,
) -> CentralResult<()> {
    if stored_previous.resource_version.get() != expected_previous
        || stored_replacement.resource_version.get() != expected_replacement
        || stored_previous.enrollment.enrollment_id != revoked.enrollment.enrollment_id
        || stored_replacement.enrollment.enrollment_id != replacement.enrollment.enrollment_id
    {
        return Err(CentralError::new(
            CentralErrorCode::ConcurrentUpdate,
            "Agent replacement records changed before atomic approval",
        ));
    }
    let next_previous = checked_next_registry_resource_version(expected_previous)?;
    let next_replacement = checked_next_registry_resource_version(expected_replacement)?;
    if revoked.resource_version.get() != next_previous
        || replacement.resource_version.get() != next_replacement
    {
        return Err(CentralError::new(
            CentralErrorCode::ConcurrentUpdate,
            "Agent replacement records did not advance ResourceVersion by one",
        ));
    }

    let previous_id = &stored_previous.enrollment.enrollment_id;
    let replacement_id = &stored_replacement.enrollment.enrollment_id;
    let linked = previous_id != replacement_id
        && stored_previous.enrollment.state == AgentEnrollmentState::Approved
        && revoked.enrollment.state == AgentEnrollmentState::Revoked
        && stored_replacement.enrollment.state == AgentEnrollmentState::PendingApproval
        && replacement.enrollment.state == AgentEnrollmentState::Approved
        && stored_replacement
            .enrollment
            .replaces_enrollment_id
            .as_ref()
            == Some(previous_id)
        && replacement.enrollment.replaces_enrollment_id.as_ref() == Some(previous_id)
        && revoked.enrollment.replaced_by_enrollment_id.as_ref() == Some(replacement_id)
        && replacement.enrollment.replaced_by_enrollment_id.is_none()
        && replacement
            .enrollment
            .decision_request
            .as_ref()
            .is_some_and(|request| {
                request.decision == AgentEnrollmentDecision::Approve
                    && request.confirm_replacement
                    && request.expected_resource_version == stored_replacement.resource_version
            });
    if !linked {
        return Err(registry_corruption(
            "Agent replacement records do not form one bidirectional state transition",
        ));
    }

    let scope_matches = stored_previous.enrollment.tenant_id
        == stored_replacement.enrollment.tenant_id
        && stored_previous.enrollment.edge_cluster_id
            == stored_replacement.enrollment.edge_cluster_id
        && stored_previous.enrollment.storage_volume_id
            == stored_replacement.enrollment.storage_volume_id
        && stored_previous.enrollment.volume_descriptor_digest
            == stored_replacement.enrollment.volume_descriptor_digest
        && stored_previous.enrollment.pvc_identity_digest
            == stored_replacement.enrollment.pvc_identity_digest
        && stored_previous.mount.expected_volume_marker
            == stored_replacement.mount.expected_volume_marker
        && stored_previous.mount.desired_access_mode
            == stored_replacement.mount.desired_access_mode
        && stored_previous.mount.mount_identity_digest
            == stored_replacement.mount.mount_identity_digest;
    if !scope_matches {
        return Err(registry_corruption(
            "Agent replacement records disagree on their frozen Volume scope",
        ));
    }

    let Some(next_mount_generation) = stored_previous.mount.mount_generation.get().checked_add(1)
    else {
        return Err(registry_corruption(
            "Agent replacement mount generation is exhausted",
        ));
    };
    let Some(next_owner_generation) = stored_previous.owner.owner_generation.get().checked_add(1)
    else {
        return Err(registry_corruption(
            "Agent replacement owner generation is exhausted",
        ));
    };
    if revoked.mount.mount_generation.get() != next_mount_generation
        || replacement.mount.mount_generation.get() != next_mount_generation
        || revoked.owner.owner_generation.get() != next_owner_generation
        || replacement.owner.owner_generation.get() != next_owner_generation
        || replacement.owner.state != VolumeOwnerState::RecoveryRequired
    {
        return Err(registry_corruption(
            "Agent replacement records do not advance matching mount and owner generations",
        ));
    }

    let identities_are_distinct = stored_previous
        .instance
        .as_ref()
        .zip(replacement.instance.as_ref())
        .is_some_and(|(previous, next)| {
            previous.agent_id != next.agent_id
                && previous.installation_id != next.installation_id
                && previous.public_key_fingerprint != next.public_key_fingerprint
                && stored_previous.mount.agent_mount_id != replacement.mount.agent_mount_id
        });
    if !identities_are_distinct {
        return Err(registry_corruption(
            "Agent replacement must activate a distinct Agent identity",
        ));
    }
    Ok(())
}

pub(crate) fn checked_next_registry_resource_version(current: u64) -> CentralResult<u64> {
    current
        .checked_add(1)
        .ok_or_else(|| registry_corruption("Agent registry ResourceVersion is exhausted"))
}

pub(crate) fn ensure_registry_marker_invariant(record: &AgentRegistryRecord) -> CentralResult<()> {
    if record.mount.expected_volume_marker.as_str() != record.enrollment.storage_volume_id.as_str()
    {
        return Err(error(
            CentralErrorCode::AgentIdentityMismatch,
            "Volume marker identity must equal the StorageVolume ID",
        ));
    }
    let mount_identity_is_bound = match &record.candidate {
        None => record.mount.mount_identity_digest.is_none(),
        Some(candidate) => {
            candidate.mount_identity_digest == candidate.probe.mount_identity_digest
                && record.mount.mount_identity_digest == Some(candidate.mount_identity_digest)
        }
    };
    if !mount_identity_is_bound {
        return Err(error(
            CentralErrorCode::AgentIdentityMismatch,
            "mount identity must remain bound to the accepted bootstrap probe",
        ));
    }
    Ok(())
}

pub(crate) fn validate_registry_record(record: &AgentRegistryRecord) -> CentralResult<()> {
    ensure_registry_marker_invariant(record)?;
    if record.resource_version.get() == 0
        || record.mount.mount_generation.get() == 0
        || record.owner.owner_generation.get() == 0
        || record.enrollment.tenant_id != record.owner.tenant_id
        || record.enrollment.storage_volume_id != record.mount.storage_volume_id
        || record.enrollment.storage_volume_id != record.owner.storage_volume_id
        || record.enrollment.reserved_agent_mount_id != record.mount.agent_mount_id
        || record.enrollment.replaces_enrollment_id.as_ref()
            == Some(&record.enrollment.enrollment_id)
        || record.enrollment.replaced_by_enrollment_id.as_ref()
            == Some(&record.enrollment.enrollment_id)
    {
        return Err(registry_corruption(
            "Agent registry aggregate contains inconsistent scope or generation",
        ));
    }
    if let Some(instance) = &record.instance {
        let static_identity_matches = record.candidate.as_ref().is_some_and(|candidate| {
            instance.installation_id == candidate.installation_id
                && instance.public_key_fingerprint == candidate.public_key_fingerprint
                && instance.agent_version == candidate.agent_version
                && instance.supported_protocol_versions == candidate.supported_protocol_versions
                && instance.capabilities == candidate.capabilities
        });
        let session_metadata_complete = match &instance.active_boot_id {
            Some(_) => {
                instance.session_generation.is_some()
                    && instance.session_open_expected_resource_version.is_some()
                    && instance.session_opened_at_unix_ms.is_some()
                    && (instance.last_heartbeat_at_unix_ms.is_some()
                        == instance.last_sequence.is_some())
            }
            None => {
                instance.session_open_expected_resource_version.is_none()
                    && instance.session_opened_at_unix_ms.is_none()
                    && instance.last_heartbeat_at_unix_ms.is_none()
                    && instance.last_sequence.is_none()
            }
        };
        if instance.agent_id != record.enrollment.reserved_agent_id
            || instance
                .session_generation
                .is_some_and(|generation| generation.get() == 0)
            || !static_identity_matches
            || !session_metadata_complete
        {
            return Err(registry_corruption(
                "Agent instance differs from its reserved candidate or session identity",
            ));
        }
    }
    if record
        .candidate
        .as_ref()
        .map(|candidate| &candidate.bootstrap_request_id)
        != record.enrollment.bootstrap_request_id.as_ref()
    {
        return Err(registry_corruption(
            "Agent candidate differs from its bootstrap request identity",
        ));
    }
    if record
        .enrollment
        .decision_request
        .as_ref()
        .is_some_and(|request| request.enrollment_id != record.enrollment.enrollment_id)
    {
        return Err(registry_corruption(
            "persisted decision request targets another enrollment",
        ));
    }
    let has_decision = record.enrollment.decision_request.is_some()
        && record.enrollment.decided_at_unix_ms.is_some()
        && record.enrollment.decided_by.is_some();
    let decision_state_valid = match record.enrollment.state {
        AgentEnrollmentState::Approved
        | AgentEnrollmentState::Rejected
        | AgentEnrollmentState::Revoked => has_decision,
        AgentEnrollmentState::TokenIssued
        | AgentEnrollmentState::PendingApproval
        | AgentEnrollmentState::Expired => {
            record.enrollment.decision_request.is_none()
                && record.enrollment.decided_at_unix_ms.is_none()
                && record.enrollment.decided_by.is_none()
        }
    };
    if !decision_state_valid {
        return Err(registry_corruption(
            "Agent enrollment decision state lacks its stable request identity or actor",
        ));
    }
    let decision_value_valid =
        match record.enrollment.state {
            AgentEnrollmentState::Approved | AgentEnrollmentState::Revoked => record
                .enrollment
                .decision_request
                .as_ref()
                .is_some_and(|request| {
                    request.decision == AgentEnrollmentDecision::Approve
                        && request.confirm_replacement
                            == record.enrollment.replaces_enrollment_id.is_some()
                }),
            AgentEnrollmentState::Rejected => record
                .enrollment
                .decision_request
                .as_ref()
                .is_some_and(|request| {
                    request.decision == AgentEnrollmentDecision::Reject
                        && !request.confirm_replacement
                }),
            AgentEnrollmentState::TokenIssued
            | AgentEnrollmentState::PendingApproval
            | AgentEnrollmentState::Expired => true,
        };
    if !decision_value_valid {
        return Err(registry_corruption(
            "Agent enrollment state disagrees with its persisted decision value",
        ));
    }
    let audit_state_valid = match record.enrollment.state {
        AgentEnrollmentState::Approved
        | AgentEnrollmentState::Rejected
        | AgentEnrollmentState::Revoked => {
            decision_audit_event(record).ok().as_ref() == record.decision_audit_event.as_ref()
        }
        AgentEnrollmentState::TokenIssued
        | AgentEnrollmentState::PendingApproval
        | AgentEnrollmentState::Expired => record.decision_audit_event.is_none(),
    };
    if !audit_state_valid {
        return Err(registry_corruption(
            "Agent enrollment decision and immutable audit event disagree",
        ));
    }
    let state_valid = match record.enrollment.state {
        AgentEnrollmentState::TokenIssued => {
            record.candidate.is_none()
                && record.instance.is_none()
                && record.enrollment.bootstrapped_at_unix_ms.is_none()
                && record.enrollment.review_expires_at_unix_ms.is_none()
                && record.owner.state == VolumeOwnerState::Inactive
        }
        AgentEnrollmentState::PendingApproval => {
            record.candidate.is_some()
                && record.instance.is_none()
                && record.enrollment.bootstrapped_at_unix_ms.is_some()
                && record.enrollment.review_expires_at_unix_ms.is_some()
                && record.owner.state == VolumeOwnerState::Inactive
        }
        AgentEnrollmentState::Approved => {
            record.candidate.is_some()
                && record
                    .instance
                    .as_ref()
                    .is_some_and(|instance| instance.state == AgentInstanceState::Active)
                && record.enrollment.bootstrapped_at_unix_ms.is_some()
                && record.enrollment.review_expires_at_unix_ms.is_some()
                && matches!(
                    record.owner.state,
                    VolumeOwnerState::Active | VolumeOwnerState::RecoveryRequired
                )
        }
        AgentEnrollmentState::Rejected => {
            record.candidate.is_some()
                && record.instance.is_none()
                && record.enrollment.bootstrapped_at_unix_ms.is_some()
                && record.enrollment.review_expires_at_unix_ms.is_some()
                && record.owner.state == VolumeOwnerState::Inactive
        }
        AgentEnrollmentState::Expired => {
            record.instance.is_none()
                && record.owner.state == VolumeOwnerState::Inactive
                && ((record.candidate.is_none()
                    && record.enrollment.bootstrap_request_id.is_none()
                    && record.enrollment.bootstrapped_at_unix_ms.is_none()
                    && record.enrollment.review_expires_at_unix_ms.is_none())
                    || (record.candidate.is_some()
                        && record.enrollment.bootstrap_request_id.is_some()
                        && record.enrollment.bootstrapped_at_unix_ms.is_some()
                        && record.enrollment.review_expires_at_unix_ms.is_some()))
        }
        AgentEnrollmentState::Revoked => {
            record
                .instance
                .as_ref()
                .is_some_and(|instance| instance.state == AgentInstanceState::Revoked)
                && record.enrollment.bootstrapped_at_unix_ms.is_some()
                && record.enrollment.review_expires_at_unix_ms.is_some()
                && record.owner.state == VolumeOwnerState::Inactive
        }
    };
    if !state_valid {
        return Err(registry_corruption(
            "Agent enrollment state disagrees with its candidate, instance, or owner",
        ));
    }
    match record.owner.state {
        VolumeOwnerState::Inactive => {
            if record.owner.active_agent_id.is_some()
                || record.owner.active_agent_mount_id.is_some()
            {
                return Err(registry_corruption(
                    "inactive Volume owner retains an active Agent identity",
                ));
            }
        }
        VolumeOwnerState::Active | VolumeOwnerState::RecoveryRequired => {
            let Some(instance) = &record.instance else {
                return Err(registry_corruption(
                    "active Volume owner has no Agent instance",
                ));
            };
            if instance.state != AgentInstanceState::Active
                || record.owner.active_agent_id.as_ref() != Some(&instance.agent_id)
                || record.owner.active_agent_mount_id.as_ref() != Some(&record.mount.agent_mount_id)
            {
                return Err(registry_corruption(
                    "Volume owner identity differs from its Agent and mount",
                ));
            }
        }
    }
    if record.enrollment.replaced_by_enrollment_id.is_some()
        && record.enrollment.state != AgentEnrollmentState::Revoked
    {
        return Err(registry_corruption(
            "only a revoked enrollment can point at its replacement",
        ));
    }
    if record.enrollment.state == AgentEnrollmentState::Revoked
        && record.enrollment.replaced_by_enrollment_id.is_none()
    {
        return Err(registry_corruption(
            "a revoked enrollment must point at its replacement",
        ));
    }
    Ok(())
}

fn registry_corruption(message: impl Into<String>) -> CentralError {
    CentralError::new(CentralErrorCode::StorageFailure, message).with_retryable(false)
}

fn error(code: CentralErrorCode, message: impl Into<String>) -> CentralError {
    CentralError::new(code, message).with_retryable(false)
}
