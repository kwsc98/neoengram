use std::collections::BTreeSet;

use neoengram_core::{ContentDigest, LogicalPath};
use schemars::JsonSchema;
use serde::{de, Deserialize, Deserializer, Serialize};

use crate::validation::{
    parse_unique_json, validate_collection_limit, validate_extension_keys,
    validate_nonempty_limited, validate_positive, CONTENT_DIGEST_PATTERN,
};
use crate::{
    AgentId, AgentMountId, ArtifactId, ArtifactPlacementId, AssignmentGeneration, AssignmentId,
    ComputeNodeId, DecimalU64, DecisionGeneration, EdgeClusterId, Extensions, FencingToken, JobId,
    LeaseId, MessageId, MetadataBatchDescriptor, MountGeneration, OwnerGeneration,
    PlacementGeneration, PrincipalId, ProjectId, ProtocolError, ProtocolResult, ProtocolVersion,
    RequestId, ResourceVersion, SessionGeneration, SnapshotId, StorageVolumeId, TenantId, TraceId,
    UnixMillis, WireIndexVersion, MAX_CONTROL_MESSAGE_BYTES, MAX_RECORDS_PER_PAGE,
    PROTOCOL_VERSION_V1,
};

/// One control-stream frame. `message.type` is flattened to the top-level JSON object.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ControlEnvelope {
    #[schemars(transform = crate::schema::require_protocol_v1)]
    pub protocol_version: ProtocolVersion,
    pub message_id: MessageId,
    pub session_generation: SessionGeneration,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resource_version: Option<ResourceVersion>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_id: Option<RequestId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trace_id: Option<TraceId>,
    pub sent_at_unix_ms: UnixMillis,
    #[serde(flatten)]
    pub message: ControlMessage,
    #[serde(default, flatten)]
    pub extensions: Extensions,
}

impl ControlEnvelope {
    /// Decodes and validates one I-JSON control frame, rejecting duplicate object members and
    /// mapping unknown message types to the stable `PROTOCOL_UNSUPPORTED` error class.
    pub fn decode_json(bytes: &[u8]) -> ProtocolResult<Self> {
        if bytes.len() > MAX_CONTROL_MESSAGE_BYTES {
            return Err(ProtocolError::LimitExceeded {
                limit_name: "control message bytes",
                limit: MAX_CONTROL_MESSAGE_BYTES,
                actual: bytes.len(),
            });
        }
        let value = parse_unique_json(bytes)?;
        let message_type = value
            .get("type")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| ProtocolError::InvalidField {
                field: "type",
                reason: "missing or non-string control message type".to_owned(),
            })?;
        if !ControlMessage::supports_type(message_type) {
            return Err(ProtocolError::UnsupportedMessageType(
                message_type.to_owned(),
            ));
        }
        let envelope: Self = serde_json::from_value(value)?;
        envelope.validate()?;
        Ok(envelope)
    }

    pub fn validate(&self) -> ProtocolResult<()> {
        if self.protocol_version != PROTOCOL_VERSION_V1 {
            return Err(ProtocolError::UnsupportedProtocolVersion(
                self.protocol_version.get(),
            ));
        }
        validate_positive("session_generation", self.session_generation.get())?;
        validate_extension_keys(
            &self.extensions,
            &[
                "protocol_version",
                "message_id",
                "session_generation",
                "resource_version",
                "request_id",
                "trace_id",
                "sent_at_unix_ms",
                "type",
                "payload",
            ],
        )?;
        self.message.validate()?;
        let encoded = serde_json::to_vec(self)?;
        if encoded.len() > MAX_CONTROL_MESSAGE_BYTES {
            return Err(ProtocolError::LimitExceeded {
                limit_name: "control message bytes",
                limit: MAX_CONTROL_MESSAGE_BYTES,
                actual: encoded.len(),
            });
        }
        Ok(())
    }
}

/// All v1 messages carried by the bidirectional control stream.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", content = "payload")]
pub enum ControlMessage {
    #[serde(rename = "agent.hello")]
    Hello(AgentHello),
    #[serde(rename = "agent.heartbeat")]
    Heartbeat(AgentHeartbeat),
    #[serde(rename = "job.assignment")]
    Assignment(Box<JobAssignment>),
    #[serde(rename = "job.accepted")]
    Accepted(JobAccepted),
    #[serde(rename = "job.progress")]
    Progress(JobProgress),
    #[serde(rename = "job.prepared")]
    Prepared(JobPrepared),
    #[serde(rename = "job.failed")]
    Failed(JobFailed),
    #[serde(rename = "job.decision")]
    Decision(JobDecision),
    #[serde(rename = "job.finalized")]
    Finalized(JobFinalized),
    #[serde(rename = "protocol.error")]
    Error(ControlError),
}

impl ControlMessage {
    #[must_use]
    pub fn supports_type(message_type: &str) -> bool {
        matches!(
            message_type,
            "agent.hello"
                | "agent.heartbeat"
                | "job.assignment"
                | "job.accepted"
                | "job.progress"
                | "job.prepared"
                | "job.failed"
                | "job.decision"
                | "job.finalized"
                | "protocol.error"
        )
    }

    fn validate(&self) -> ProtocolResult<()> {
        match self {
            Self::Hello(message) => message.validate(),
            Self::Heartbeat(message) => message.validate(),
            Self::Assignment(message) => message.validate(),
            Self::Accepted(message) => message.validate(),
            Self::Progress(message) => message.validate(),
            Self::Prepared(message) => message.validate(),
            Self::Failed(message) => message.validate(),
            Self::Decision(message) => message.validate(),
            Self::Finalized(message) => message.validate(),
            Self::Error(message) => message.validate(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct AgentHello {
    pub agent_id: AgentId,
    pub edge_cluster_id: EdgeClusterId,
    pub compute_node_id: ComputeNodeId,
    #[schemars(length(min = 1, max = 128))]
    pub agent_version: String,
    #[schemars(length(max = 256))]
    pub supported_protocol_versions: Vec<ProtocolVersion>,
    #[serde(default)]
    #[schemars(length(max = 256))]
    pub capabilities: BTreeSet<String>,
    #[serde(default, flatten)]
    pub extensions: Extensions,
}

impl AgentHello {
    fn validate(&self) -> ProtocolResult<()> {
        if !self
            .supported_protocol_versions
            .contains(&PROTOCOL_VERSION_V1)
        {
            return Err(ProtocolError::InvalidField {
                field: "supported_protocol_versions",
                reason: "v1 support is required".to_owned(),
            });
        }
        validate_nonempty_limited("agent_version", &self.agent_version, 128)?;
        validate_collection_limit(
            "supported_protocol_versions",
            self.supported_protocol_versions.len(),
            256,
        )?;
        validate_collection_limit("capabilities", self.capabilities.len(), 256)?;
        validate_extension_keys(
            &self.extensions,
            &[
                "agent_id",
                "edge_cluster_id",
                "compute_node_id",
                "agent_version",
                "supported_protocol_versions",
                "capabilities",
            ],
        )
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct AgentHeartbeat {
    pub agent_id: AgentId,
    pub observed_at_unix_ms: UnixMillis,
    pub acknowledged_resource_version: ResourceVersion,
    pub sequence: crate::SequenceNumber,
    #[serde(default)]
    #[schemars(length(max = 4096))]
    pub running_jobs: Vec<RunningJobObservation>,
    #[serde(default)]
    #[schemars(length(max = 4096))]
    pub mount_observations: Vec<MountObservation>,
    #[serde(default, flatten)]
    pub extensions: Extensions,
}

impl AgentHeartbeat {
    pub fn validate(&self) -> ProtocolResult<()> {
        validate_collection_limit(
            "running_jobs",
            self.running_jobs.len(),
            MAX_RECORDS_PER_PAGE,
        )?;
        validate_collection_limit(
            "mount_observations",
            self.mount_observations.len(),
            MAX_RECORDS_PER_PAGE,
        )?;
        self.running_jobs
            .iter()
            .try_for_each(RunningJobObservation::validate)?;
        self.mount_observations
            .iter()
            .try_for_each(MountObservation::validate)?;
        validate_extension_keys(
            &self.extensions,
            &[
                "agent_id",
                "observed_at_unix_ms",
                "acknowledged_resource_version",
                "sequence",
                "running_jobs",
                "mount_observations",
            ],
        )
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct RunningJobObservation {
    pub job_id: JobId,
    pub assignment_id: AssignmentId,
    pub assignment_generation: AssignmentGeneration,
    pub state: JobState,
    #[serde(default, flatten)]
    pub extensions: Extensions,
}

impl RunningJobObservation {
    fn validate(&self) -> ProtocolResult<()> {
        validate_positive("assignment_generation", self.assignment_generation.get())?;
        validate_extension_keys(
            &self.extensions,
            &["job_id", "assignment_id", "assignment_generation", "state"],
        )
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct MountObservation {
    pub agent_mount_id: AgentMountId,
    pub storage_volume_id: StorageVolumeId,
    pub mount_generation: MountGeneration,
    pub access_mode: MountAccessMode,
    pub health: ResourceHealth,
    #[serde(default, flatten)]
    pub extensions: Extensions,
}

impl MountObservation {
    fn validate(&self) -> ProtocolResult<()> {
        validate_positive("mount_generation", self.mount_generation.get())?;
        validate_extension_keys(
            &self.extensions,
            &[
                "agent_mount_id",
                "storage_volume_id",
                "mount_generation",
                "access_mode",
                "health",
            ],
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum MountAccessMode {
    ReadOnly,
    ReadWrite,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ResourceHealth {
    Ready,
    Degraded,
    Unavailable,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct JobAssignment {
    pub assignment: AssignmentOperation,
    #[serde(default, flatten)]
    pub extensions: Extensions,
}

impl JobAssignment {
    pub fn validate(&self) -> ProtocolResult<()> {
        self.assignment.validate()?;
        validate_extension_keys(&self.extensions, &["assignment"])
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "operation", rename_all = "snake_case")]
pub enum AssignmentOperation {
    Add {
        input: AddAssignment,
        #[serde(default, flatten)]
        extensions: Extensions,
    },
    /// Creates the server-derived Playground directory below the approved Agent mount.
    ///
    /// This operation deliberately reuses the normal job accepted/progress/failed reports. It
    /// does not enter the managed-Add publication state machine: successful materialization is
    /// terminal after the Agent reports `job.progress(state=succeeded)`.
    WorkspaceMaterialize {
        input: WorkspaceMaterializeAssignment,
        #[serde(default, flatten)]
        extensions: Extensions,
    },
    /// Mounts one immutable Artifact Commit as a server-derived read-only Snapshot path.
    SnapshotMount {
        input: SnapshotMountAssignment,
        #[serde(default, flatten)]
        extensions: Extensions,
    },
}

impl AssignmentOperation {
    fn validate(&self) -> ProtocolResult<()> {
        match self {
            Self::Add { input, extensions } => {
                input.validate()?;
                validate_extension_keys(extensions, &["operation", "input"])
            }
            Self::WorkspaceMaterialize { input, extensions } => {
                input.validate()?;
                validate_extension_keys(extensions, &["operation", "input"])
            }
            Self::SnapshotMount { input, extensions } => {
                input.validate()?;
                validate_extension_keys(extensions, &["operation", "input"])
            }
        }
    }
}

/// Canonical user-operation fields bound by [`AddAssignment::request_digest`].
///
/// Placement, lease, and assignment generations are intentionally excluded: the control plane
/// chooses them after accepting the immutable Add request. The digest still binds the complete
/// tenant/resource scope, principal, base IndexVersion, deadline, and normalized path set.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct AddOperation {
    pub job_id: JobId,
    pub principal: PrincipalRef,
    pub tenant_id: TenantId,
    pub project_id: ProjectId,
    pub artifact_id: ArtifactId,
    pub playground_id: crate::PlaygroundId,
    pub expected_index_version: WireIndexVersion,
    pub deadline_unix_ms: UnixMillis,
    #[serde(default)]
    #[schemars(with = "Vec<String>", length(max = 4096))]
    pub paths: Vec<LogicalPath>,
    pub all: bool,
    #[serde(default, flatten)]
    pub extensions: Extensions,
}

impl AddOperation {
    pub fn validate(&self) -> ProtocolResult<()> {
        if !self.all && self.paths.is_empty() {
            return Err(ProtocolError::InvalidField {
                field: "paths",
                reason: "paths must be non-empty unless all is true".to_owned(),
            });
        }
        validate_collection_limit("paths", self.paths.len(), MAX_RECORDS_PER_PAGE)?;
        neoengram_core::validate_path_set(self.paths.iter()).map_err(|error| {
            ProtocolError::InvalidField {
                field: "paths",
                reason: error.to_string(),
            }
        })?;
        self.expected_index_version.validate()?;
        self.principal.validate()?;
        validate_extension_keys(
            &self.extensions,
            &[
                "job_id",
                "principal",
                "tenant_id",
                "project_id",
                "artifact_id",
                "playground_id",
                "expected_index_version",
                "deadline_unix_ms",
                "paths",
                "all",
            ],
        )
    }

    /// Returns BLAKE3 over the RFC 8785 canonical JSON form of this operation.
    pub fn request_digest(&self) -> ProtocolResult<ContentDigest> {
        self.validate()?;
        crate::jcs_blake3(self)
    }
}

/// Complete immutable scope required to execute one managed `add` assignment.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct AddAssignment {
    pub job_id: JobId,
    pub assignment_id: AssignmentId,
    pub assignment_generation: AssignmentGeneration,
    pub agent_id: AgentId,
    pub principal: PrincipalRef,
    pub tenant_id: TenantId,
    pub project_id: ProjectId,
    pub artifact_id: ArtifactId,
    pub playground_id: crate::PlaygroundId,
    pub edge_cluster_id: EdgeClusterId,
    pub storage_volume_id: StorageVolumeId,
    pub artifact_placement_id: ArtifactPlacementId,
    pub placement_generation: PlacementGeneration,
    pub agent_mount_id: AgentMountId,
    pub mount_generation: MountGeneration,
    pub owner_generation: OwnerGeneration,
    pub expected_index_version: WireIndexVersion,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lease: Option<LeaseGrant>,
    #[schemars(
        with = "String",
        length(equal = 64),
        regex(pattern = CONTENT_DIGEST_PATTERN)
    )]
    pub request_digest: ContentDigest,
    pub deadline_unix_ms: UnixMillis,
    #[serde(default)]
    #[schemars(with = "Vec<String>", length(max = 4096))]
    pub paths: Vec<LogicalPath>,
    pub all: bool,
    #[serde(default, flatten)]
    pub extensions: Extensions,
}

impl AddAssignment {
    #[must_use]
    pub fn operation(&self) -> AddOperation {
        AddOperation {
            job_id: self.job_id.clone(),
            principal: self.principal.clone(),
            tenant_id: self.tenant_id.clone(),
            project_id: self.project_id.clone(),
            artifact_id: self.artifact_id.clone(),
            playground_id: self.playground_id.clone(),
            expected_index_version: self.expected_index_version.clone(),
            deadline_unix_ms: self.deadline_unix_ms,
            paths: self.paths.clone(),
            all: self.all,
            extensions: self.extensions.clone(),
        }
    }

    pub fn computed_request_digest(&self) -> ProtocolResult<ContentDigest> {
        self.operation().request_digest()
    }

    pub fn validate(&self) -> ProtocolResult<()> {
        for (field, generation) in [
            ("assignment_generation", self.assignment_generation.get()),
            ("placement_generation", self.placement_generation.get()),
            ("mount_generation", self.mount_generation.get()),
            ("owner_generation", self.owner_generation.get()),
        ] {
            validate_positive(field, generation)?;
        }
        let computed_request_digest = self.computed_request_digest()?;
        if self.request_digest != computed_request_digest {
            return Err(ProtocolError::InvalidDigest(format!(
                "Add request digest mismatch: expected {}, observed {}",
                self.request_digest, computed_request_digest
            )));
        }
        if let Some(lease) = &self.lease {
            lease.validate()?;
        }
        validate_extension_keys(
            &self.extensions,
            &[
                "job_id",
                "assignment_id",
                "assignment_generation",
                "agent_id",
                "principal",
                "tenant_id",
                "project_id",
                "artifact_id",
                "playground_id",
                "edge_cluster_id",
                "storage_volume_id",
                "artifact_placement_id",
                "placement_generation",
                "agent_mount_id",
                "mount_generation",
                "owner_generation",
                "expected_index_version",
                "lease",
                "request_digest",
                "deadline_unix_ms",
                "paths",
                "all",
            ],
        )
    }
}

/// Canonical user-operation fields bound by
/// [`WorkspaceMaterializeAssignment::request_digest`].
///
/// Agent identity, mount identity, and assignment generations are intentionally excluded because
/// the scheduler chooses them after accepting this immutable operation. The selected Volume and
/// canonical Playground path remain bound because they are part of the create request itself.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct WorkspaceMaterializeOperation {
    pub job_id: JobId,
    pub principal: PrincipalRef,
    pub tenant_id: TenantId,
    pub project_id: ProjectId,
    pub artifact_id: ArtifactId,
    pub playground_id: crate::PlaygroundId,
    pub storage_volume_id: StorageVolumeId,
    /// Server-derived path relative to the approved Volume mount.
    #[schemars(with = "String")]
    pub relative_root: LogicalPath,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(with = "Option<String>")]
    pub base_commit_id: Option<ContentDigest>,
    /// Immutable Index snapshot carried by `base_commit_id`. Both fields are present for a
    /// non-empty baseline and absent for an empty Artifact.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_index_version: Option<WireIndexVersion>,
    pub deadline_unix_ms: UnixMillis,
    #[serde(default, flatten)]
    pub extensions: Extensions,
}

impl WorkspaceMaterializeOperation {
    /// Returns the canonical relative directory derived from the resource identity.
    pub fn canonical_relative_root(
        project_id: &ProjectId,
        artifact_id: &ArtifactId,
        playground_id: &crate::PlaygroundId,
    ) -> ProtocolResult<LogicalPath> {
        LogicalPath::parse(format!(
            "playgrounds/{project_id}/{artifact_id}/{playground_id}"
        ))
        .map_err(|error| ProtocolError::InvalidField {
            field: "relative_root",
            reason: error.to_string(),
        })
    }

    pub fn validate(&self) -> ProtocolResult<()> {
        self.principal.validate()?;
        if self.base_commit_id.is_some() != self.base_index_version.is_some() {
            return Err(ProtocolError::InvalidField {
                field: "base_index_version",
                reason: "must be present exactly when base_commit_id is present".to_owned(),
            });
        }
        if let Some(version) = &self.base_index_version {
            version.validate()?;
        }
        let canonical = Self::canonical_relative_root(
            &self.project_id,
            &self.artifact_id,
            &self.playground_id,
        )?;
        if self.relative_root != canonical {
            return Err(ProtocolError::InvalidField {
                field: "relative_root",
                reason: format!("must equal the server-derived Playground path {canonical}"),
            });
        }
        validate_extension_keys(
            &self.extensions,
            &[
                "job_id",
                "principal",
                "tenant_id",
                "project_id",
                "artifact_id",
                "playground_id",
                "storage_volume_id",
                "relative_root",
                "base_commit_id",
                "base_index_version",
                "deadline_unix_ms",
            ],
        )
    }

    /// Returns BLAKE3 over the RFC 8785 canonical JSON form of this operation.
    pub fn request_digest(&self) -> ProtocolResult<ContentDigest> {
        self.validate()?;
        crate::jcs_blake3(self)
    }
}

/// Complete immutable scope required to create one server-derived Playground directory.
///
/// `relative_root` is carried for explicit protocol observability, but is not trusted by the
/// Agent. Validation requires it to equal the canonical path derived from the typed resource
/// identifiers. `base_commit_id` and `base_index_version` are absent only for an empty baseline.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct WorkspaceMaterializeAssignment {
    pub job_id: JobId,
    pub assignment_id: AssignmentId,
    pub assignment_generation: AssignmentGeneration,
    pub agent_id: AgentId,
    pub principal: PrincipalRef,
    pub tenant_id: TenantId,
    pub project_id: ProjectId,
    pub artifact_id: ArtifactId,
    pub playground_id: crate::PlaygroundId,
    pub storage_volume_id: StorageVolumeId,
    pub agent_mount_id: AgentMountId,
    pub mount_generation: MountGeneration,
    pub owner_generation: OwnerGeneration,
    /// Server-derived path relative to the approved Volume mount.
    #[schemars(with = "String")]
    pub relative_root: LogicalPath,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(with = "Option<String>")]
    pub base_commit_id: Option<ContentDigest>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_index_version: Option<WireIndexVersion>,
    #[schemars(
        with = "String",
        length(equal = 64),
        regex(pattern = CONTENT_DIGEST_PATTERN)
    )]
    pub request_digest: ContentDigest,
    pub deadline_unix_ms: UnixMillis,
    #[serde(default, flatten)]
    pub extensions: Extensions,
}

impl WorkspaceMaterializeAssignment {
    /// Returns the canonical relative directory derived from the resource identity.
    pub fn canonical_relative_root(
        project_id: &ProjectId,
        artifact_id: &ArtifactId,
        playground_id: &crate::PlaygroundId,
    ) -> ProtocolResult<LogicalPath> {
        WorkspaceMaterializeOperation::canonical_relative_root(
            project_id,
            artifact_id,
            playground_id,
        )
    }

    #[must_use]
    pub fn operation(&self) -> WorkspaceMaterializeOperation {
        WorkspaceMaterializeOperation {
            job_id: self.job_id.clone(),
            principal: self.principal.clone(),
            tenant_id: self.tenant_id.clone(),
            project_id: self.project_id.clone(),
            artifact_id: self.artifact_id.clone(),
            playground_id: self.playground_id.clone(),
            storage_volume_id: self.storage_volume_id.clone(),
            relative_root: self.relative_root.clone(),
            base_commit_id: self.base_commit_id,
            base_index_version: self.base_index_version.clone(),
            deadline_unix_ms: self.deadline_unix_ms,
            extensions: self.extensions.clone(),
        }
    }

    /// Computes BLAKE3 over the canonical JSON form of the immutable materialize request.
    pub fn computed_request_digest(&self) -> ProtocolResult<ContentDigest> {
        self.operation().request_digest()
    }

    pub fn validate(&self) -> ProtocolResult<()> {
        for (field, generation) in [
            ("assignment_generation", self.assignment_generation.get()),
            ("mount_generation", self.mount_generation.get()),
            ("owner_generation", self.owner_generation.get()),
        ] {
            validate_positive(field, generation)?;
        }
        self.operation().validate()?;
        let computed = self.computed_request_digest()?;
        if self.request_digest != computed {
            return Err(ProtocolError::InvalidDigest(format!(
                "Workspace materialize request digest mismatch: expected {}, observed {}",
                self.request_digest, computed
            )));
        }
        validate_extension_keys(
            &self.extensions,
            &[
                "job_id",
                "assignment_id",
                "assignment_generation",
                "agent_id",
                "principal",
                "tenant_id",
                "project_id",
                "artifact_id",
                "playground_id",
                "storage_volume_id",
                "agent_mount_id",
                "mount_generation",
                "owner_generation",
                "relative_root",
                "base_commit_id",
                "base_index_version",
                "request_digest",
                "deadline_unix_ms",
            ],
        )
    }
}

/// Canonical user-operation fields bound by [`SnapshotMountAssignment::request_digest`].
///
/// The immutable Commit and Index fence are part of the operation. Agent and mount generations
/// are placement details added later by the scheduler and are deliberately excluded.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct SnapshotMountOperation {
    pub job_id: JobId,
    pub principal: PrincipalRef,
    pub tenant_id: TenantId,
    pub project_id: ProjectId,
    pub artifact_id: ArtifactId,
    pub snapshot_id: SnapshotId,
    pub storage_volume_id: StorageVolumeId,
    /// Server-derived path relative to the approved Volume mount.
    #[schemars(with = "String")]
    pub relative_root: LogicalPath,
    #[schemars(
        with = "String",
        length(equal = 64),
        regex(pattern = CONTENT_DIGEST_PATTERN)
    )]
    pub commit_id: ContentDigest,
    pub index_version: WireIndexVersion,
    pub deadline_unix_ms: UnixMillis,
    #[serde(default, flatten)]
    pub extensions: Extensions,
}

impl SnapshotMountOperation {
    /// Returns the only Snapshot path accepted by the Agent for this resource identity.
    pub fn canonical_relative_root(
        project_id: &ProjectId,
        artifact_id: &ArtifactId,
        snapshot_id: &SnapshotId,
    ) -> ProtocolResult<LogicalPath> {
        LogicalPath::parse(format!(
            "snapshots/{project_id}/{artifact_id}/{snapshot_id}"
        ))
        .map_err(|error| ProtocolError::InvalidField {
            field: "relative_root",
            reason: error.to_string(),
        })
    }

    pub fn validate(&self) -> ProtocolResult<()> {
        self.principal.validate()?;
        self.index_version.validate()?;
        let canonical =
            Self::canonical_relative_root(&self.project_id, &self.artifact_id, &self.snapshot_id)?;
        if self.relative_root != canonical {
            return Err(ProtocolError::InvalidField {
                field: "relative_root",
                reason: format!("must equal the server-derived Snapshot path {canonical}"),
            });
        }
        validate_extension_keys(
            &self.extensions,
            &[
                "job_id",
                "principal",
                "tenant_id",
                "project_id",
                "artifact_id",
                "snapshot_id",
                "storage_volume_id",
                "relative_root",
                "commit_id",
                "index_version",
                "deadline_unix_ms",
            ],
        )
    }

    /// Returns BLAKE3 over the RFC 8785 canonical JSON form of this operation.
    pub fn request_digest(&self) -> ProtocolResult<ContentDigest> {
        self.validate()?;
        crate::jcs_blake3(self)
    }
}

/// Complete immutable placement required to mount one read-only Snapshot.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct SnapshotMountAssignment {
    pub job_id: JobId,
    pub assignment_id: AssignmentId,
    pub assignment_generation: AssignmentGeneration,
    pub agent_id: AgentId,
    pub principal: PrincipalRef,
    pub tenant_id: TenantId,
    pub project_id: ProjectId,
    pub artifact_id: ArtifactId,
    pub snapshot_id: SnapshotId,
    pub storage_volume_id: StorageVolumeId,
    pub agent_mount_id: AgentMountId,
    pub mount_generation: MountGeneration,
    pub owner_generation: OwnerGeneration,
    /// Server-derived path relative to the approved Volume mount.
    #[schemars(with = "String")]
    pub relative_root: LogicalPath,
    #[schemars(
        with = "String",
        length(equal = 64),
        regex(pattern = CONTENT_DIGEST_PATTERN)
    )]
    pub commit_id: ContentDigest,
    pub index_version: WireIndexVersion,
    #[schemars(
        with = "String",
        length(equal = 64),
        regex(pattern = CONTENT_DIGEST_PATTERN)
    )]
    pub request_digest: ContentDigest,
    pub deadline_unix_ms: UnixMillis,
    #[serde(default, flatten)]
    pub extensions: Extensions,
}

impl SnapshotMountAssignment {
    /// Returns the canonical relative directory derived from the resource identity.
    pub fn canonical_relative_root(
        project_id: &ProjectId,
        artifact_id: &ArtifactId,
        snapshot_id: &SnapshotId,
    ) -> ProtocolResult<LogicalPath> {
        SnapshotMountOperation::canonical_relative_root(project_id, artifact_id, snapshot_id)
    }

    #[must_use]
    pub fn operation(&self) -> SnapshotMountOperation {
        SnapshotMountOperation {
            job_id: self.job_id.clone(),
            principal: self.principal.clone(),
            tenant_id: self.tenant_id.clone(),
            project_id: self.project_id.clone(),
            artifact_id: self.artifact_id.clone(),
            snapshot_id: self.snapshot_id.clone(),
            storage_volume_id: self.storage_volume_id.clone(),
            relative_root: self.relative_root.clone(),
            commit_id: self.commit_id,
            index_version: self.index_version.clone(),
            deadline_unix_ms: self.deadline_unix_ms,
            extensions: self.extensions.clone(),
        }
    }

    /// Computes BLAKE3 over the canonical JSON form of the immutable Snapshot request.
    pub fn computed_request_digest(&self) -> ProtocolResult<ContentDigest> {
        self.operation().request_digest()
    }

    pub fn validate(&self) -> ProtocolResult<()> {
        for (field, generation) in [
            ("assignment_generation", self.assignment_generation.get()),
            ("mount_generation", self.mount_generation.get()),
            ("owner_generation", self.owner_generation.get()),
        ] {
            validate_positive(field, generation)?;
        }
        self.operation().validate()?;
        let computed = self.computed_request_digest()?;
        if self.request_digest != computed {
            return Err(ProtocolError::InvalidDigest(format!(
                "Snapshot mount request digest mismatch: expected {}, observed {}",
                self.request_digest, computed
            )));
        }
        validate_extension_keys(
            &self.extensions,
            &[
                "job_id",
                "assignment_id",
                "assignment_generation",
                "agent_id",
                "principal",
                "tenant_id",
                "project_id",
                "artifact_id",
                "snapshot_id",
                "storage_volume_id",
                "agent_mount_id",
                "mount_generation",
                "owner_generation",
                "relative_root",
                "commit_id",
                "index_version",
                "request_digest",
                "deadline_unix_ms",
            ],
        )
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct PrincipalRef {
    pub kind: PrincipalKind,
    pub id: PrincipalId,
    #[serde(default, flatten)]
    pub extensions: Extensions,
}

impl PrincipalRef {
    fn validate(&self) -> ProtocolResult<()> {
        validate_extension_keys(&self.extensions, &["kind", "id"])
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum PrincipalKind {
    User,
    Service,
    System,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct LeaseGrant {
    pub lease_id: LeaseId,
    pub mode: LeaseMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fencing_token: Option<FencingToken>,
    pub expires_at_unix_ms: UnixMillis,
    #[serde(default, flatten)]
    pub extensions: Extensions,
}

impl LeaseGrant {
    fn validate(&self) -> ProtocolResult<()> {
        match (self.mode, self.fencing_token) {
            (LeaseMode::SharedRead, None) => Ok(()),
            (LeaseMode::ExclusiveWrite, Some(token)) => {
                validate_positive("fencing_token", token.get())
            }
            (LeaseMode::SharedRead, Some(_)) => Err(ProtocolError::InvalidField {
                field: "fencing_token",
                reason: "shared read leases must not carry a writer fence".to_owned(),
            }),
            (LeaseMode::ExclusiveWrite, None) => Err(ProtocolError::InvalidField {
                field: "fencing_token",
                reason: "exclusive write leases require a writer fence".to_owned(),
            }),
        }?;
        validate_extension_keys(
            &self.extensions,
            &["lease_id", "mode", "fencing_token", "expires_at_unix_ms"],
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum LeaseMode {
    SharedRead,
    ExclusiveWrite,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct JobAccepted {
    pub job_id: JobId,
    pub assignment_id: AssignmentId,
    pub assignment_generation: AssignmentGeneration,
    pub accepted_at_unix_ms: UnixMillis,
    #[schemars(
        with = "String",
        length(equal = 64),
        regex(pattern = CONTENT_DIGEST_PATTERN)
    )]
    pub request_digest: ContentDigest,
    #[serde(default, flatten)]
    pub extensions: Extensions,
}

impl JobAccepted {
    pub fn validate(&self) -> ProtocolResult<()> {
        validate_positive("assignment_generation", self.assignment_generation.get())?;
        validate_extension_keys(
            &self.extensions,
            &[
                "job_id",
                "assignment_id",
                "assignment_generation",
                "accepted_at_unix_ms",
                "request_digest",
            ],
        )
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct JobProgress {
    pub job_id: JobId,
    pub assignment_id: AssignmentId,
    pub assignment_generation: AssignmentGeneration,
    pub state: JobState,
    #[schemars(length(min = 1, max = 128))]
    pub phase: String,
    pub files_completed: DecimalU64,
    pub bytes_completed: DecimalU64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_after_ms: Option<DecimalU64>,
    #[serde(default, flatten)]
    pub extensions: Extensions,
}

impl JobProgress {
    pub fn validate(&self) -> ProtocolResult<()> {
        validate_positive("assignment_generation", self.assignment_generation.get())?;
        validate_nonempty_limited("phase", &self.phase, 128)?;
        validate_extension_keys(
            &self.extensions,
            &[
                "job_id",
                "assignment_id",
                "assignment_generation",
                "state",
                "phase",
                "files_completed",
                "bytes_completed",
                "retry_after_ms",
            ],
        )
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct JobPrepared {
    pub job_id: JobId,
    pub assignment_id: AssignmentId,
    pub assignment_generation: AssignmentGeneration,
    pub base_index_version: WireIndexVersion,
    /// Canonical digest expected for the complete Index snapshot after this delta is applied.
    #[schemars(
        with = "String",
        length(equal = 64),
        regex(pattern = CONTENT_DIGEST_PATTERN)
    )]
    pub result_index_digest: ContentDigest,
    /// Backend-independent digest of scope, IndexDelta, Manifests, and ObjectSpecs.
    #[schemars(
        with = "String",
        length(equal = 64),
        regex(pattern = CONTENT_DIGEST_PATTERN)
    )]
    pub publication_digest: ContentDigest,
    /// Digest of this exact report, including publication identity and ordered descriptors.
    #[schemars(
        with = "String",
        length(equal = 64),
        regex(pattern = CONTENT_DIGEST_PATTERN)
    )]
    pub candidate_digest: ContentDigest,
    #[serde(default)]
    #[schemars(length(max = 4096))]
    pub metadata_batches: Vec<MetadataBatchDescriptor>,
    #[serde(default, flatten)]
    pub extensions: Extensions,
}

impl JobPrepared {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        job_id: JobId,
        assignment_id: AssignmentId,
        assignment_generation: AssignmentGeneration,
        base_index_version: WireIndexVersion,
        result_index_digest: ContentDigest,
        publication_digest: ContentDigest,
        metadata_batches: Vec<MetadataBatchDescriptor>,
        extensions: Extensions,
    ) -> ProtocolResult<Self> {
        let candidate_digest = prepared_candidate_digest(
            &job_id,
            &assignment_id,
            assignment_generation,
            &base_index_version,
            result_index_digest,
            publication_digest,
            &metadata_batches,
            &extensions,
        )?;
        let prepared = Self {
            job_id,
            assignment_id,
            assignment_generation,
            base_index_version,
            result_index_digest,
            publication_digest,
            candidate_digest,
            metadata_batches,
            extensions,
        };
        prepared.validate()?;
        Ok(prepared)
    }

    pub fn computed_candidate_digest(&self) -> ProtocolResult<ContentDigest> {
        prepared_candidate_digest(
            &self.job_id,
            &self.assignment_id,
            self.assignment_generation,
            &self.base_index_version,
            self.result_index_digest,
            self.publication_digest,
            &self.metadata_batches,
            &self.extensions,
        )
    }

    pub fn validate(&self) -> ProtocolResult<()> {
        validate_positive("assignment_generation", self.assignment_generation.get())?;
        self.base_index_version.validate()?;
        validate_collection_limit(
            "metadata_batches",
            self.metadata_batches.len(),
            MAX_RECORDS_PER_PAGE,
        )?;
        self.metadata_batches
            .iter()
            .try_for_each(MetadataBatchDescriptor::validate)?;
        validate_extension_keys(
            &self.extensions,
            &[
                "job_id",
                "assignment_id",
                "assignment_generation",
                "base_index_version",
                "result_index_digest",
                "publication_digest",
                "candidate_digest",
                "metadata_batches",
            ],
        )?;
        let computed = self.computed_candidate_digest()?;
        if computed != self.candidate_digest {
            return Err(ProtocolError::InvalidDigest(format!(
                "prepared candidate digest mismatch: expected {}, observed {}",
                self.candidate_digest, computed
            )));
        }
        Ok(())
    }
}

#[allow(clippy::too_many_arguments)]
fn prepared_candidate_digest(
    job_id: &JobId,
    assignment_id: &AssignmentId,
    assignment_generation: AssignmentGeneration,
    base_index_version: &WireIndexVersion,
    result_index_digest: ContentDigest,
    publication_digest: ContentDigest,
    metadata_batches: &[MetadataBatchDescriptor],
    extensions: &Extensions,
) -> ProtocolResult<ContentDigest> {
    crate::jcs_blake3(&JobPreparedDigestInput {
        job_id,
        assignment_id,
        assignment_generation,
        base_index_version,
        result_index_digest,
        publication_digest,
        metadata_batches,
        extensions,
    })
}

#[derive(Serialize)]
struct JobPreparedDigestInput<'a> {
    job_id: &'a JobId,
    assignment_id: &'a AssignmentId,
    assignment_generation: AssignmentGeneration,
    base_index_version: &'a WireIndexVersion,
    result_index_digest: ContentDigest,
    publication_digest: ContentDigest,
    metadata_batches: &'a [MetadataBatchDescriptor],
    #[serde(flatten)]
    extensions: &'a Extensions,
}

/// Job-bound execution failure reported by an Agent.
///
/// This is distinct from [`ControlError`], which remains the payload of the generic
/// `protocol.error` message for failures that are not bound to an assigned job.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct JobFailed {
    pub tenant_id: TenantId,
    pub job_id: JobId,
    pub assignment_id: AssignmentId,
    pub assignment_generation: AssignmentGeneration,
    pub final_state: JobState,
    pub failed_at_unix_ms: UnixMillis,
    pub stage: JobFailureStage,
    pub error: ControlError,
    #[serde(default, flatten)]
    pub extensions: Extensions,
}

impl JobFailed {
    pub fn validate(&self) -> ProtocolResult<()> {
        validate_positive("assignment_generation", self.assignment_generation.get())?;
        if !self.final_state.is_failure_terminal() {
            return Err(ProtocolError::InvalidField {
                field: "final_state",
                reason: "job.failed requires a failure terminal state".to_owned(),
            });
        }
        self.error.validate()?;
        validate_extension_keys(
            &self.extensions,
            &[
                "tenant_id",
                "job_id",
                "assignment_id",
                "assignment_generation",
                "final_state",
                "failed_at_unix_ms",
                "stage",
                "error",
            ],
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum JobFailureStage {
    Execution,
    ObjectTransfer,
    Reporting,
    Finalization,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct JobDecision {
    pub job_id: JobId,
    pub assignment_id: AssignmentId,
    pub assignment_generation: AssignmentGeneration,
    pub decision_generation: DecisionGeneration,
    pub decision: PublishDecision,
    pub final_state: JobState,
    #[serde(default, flatten)]
    pub extensions: Extensions,
}

impl JobDecision {
    pub fn validate(&self) -> ProtocolResult<()> {
        validate_positive("assignment_generation", self.assignment_generation.get())?;
        validate_positive("decision_generation", self.decision_generation.get())?;
        self.decision.validate()?;
        let final_state_matches = matches!(
            (&self.decision, self.final_state),
            (PublishDecision::Publish { .. }, JobState::Succeeded)
                | (PublishDecision::Conflict { .. }, JobState::Conflicted)
                | (
                    PublishDecision::Reject { .. },
                    JobState::Rejected
                        | JobState::Failed
                        | JobState::Cancelled
                        | JobState::TimedOut
                        | JobState::RecoveryRequired,
                )
        );
        if !final_state_matches {
            return Err(ProtocolError::InvalidField {
                field: "final_state",
                reason: format!(
                    "job decision outcome is incompatible with final state {:?}",
                    self.final_state
                ),
            });
        }
        validate_extension_keys(
            &self.extensions,
            &[
                "job_id",
                "assignment_id",
                "assignment_generation",
                "decision_generation",
                "decision",
                "final_state",
            ],
        )
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum PublishDecision {
    Publish {
        published_index_version: WireIndexVersion,
        #[serde(default, flatten)]
        extensions: Extensions,
    },
    Conflict {
        current_index_version: WireIndexVersion,
        #[serde(default, flatten)]
        extensions: Extensions,
    },
    Reject {
        error: ControlError,
        #[serde(default, flatten)]
        extensions: Extensions,
    },
}

impl PublishDecision {
    pub fn validate(&self) -> ProtocolResult<()> {
        match self {
            Self::Publish {
                published_index_version,
                extensions,
            } => {
                published_index_version.validate()?;
                validate_extension_keys(extensions, &["outcome", "published_index_version"])
            }
            Self::Conflict {
                current_index_version,
                extensions,
            } => {
                current_index_version.validate()?;
                validate_extension_keys(extensions, &["outcome", "current_index_version"])
            }
            Self::Reject { error, extensions } => {
                error.validate()?;
                validate_extension_keys(extensions, &["outcome", "error"])
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct JobFinalized {
    pub job_id: JobId,
    pub assignment_id: AssignmentId,
    pub assignment_generation: AssignmentGeneration,
    pub decision_generation: DecisionGeneration,
    pub final_state: JobState,
    pub finalized_at_unix_ms: UnixMillis,
    #[serde(default, flatten)]
    pub extensions: Extensions,
}

impl JobFinalized {
    pub fn validate(&self) -> ProtocolResult<()> {
        validate_positive("assignment_generation", self.assignment_generation.get())?;
        validate_positive("decision_generation", self.decision_generation.get())?;
        if !self.final_state.is_terminal() {
            return Err(ProtocolError::InvalidField {
                field: "final_state",
                reason: "finalized messages require a terminal job state".to_owned(),
            });
        }
        validate_extension_keys(
            &self.extensions,
            &[
                "job_id",
                "assignment_id",
                "assignment_generation",
                "decision_generation",
                "final_state",
                "finalized_at_unix_ms",
            ],
        )
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ControlError {
    pub code: ErrorCode,
    #[schemars(length(min = 1, max = 4096))]
    pub message: String,
    pub retryable: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_after_ms: Option<DecimalU64>,
    #[serde(default, flatten)]
    pub extensions: Extensions,
}

impl ControlError {
    pub fn validate(&self) -> ProtocolResult<()> {
        validate_nonempty_limited("message", &self.message, 4096)?;
        validate_extension_keys(
            &self.extensions,
            &["code", "message", "retryable", "retry_after_ms"],
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, JsonSchema)]
#[serde(transparent)]
pub struct ErrorCode(
    #[schemars(length(min = 1, max = 96), regex(pattern = r"^[A-Z][A-Z0-9_]{0,95}$"))] String,
);

impl ErrorCode {
    pub fn new(value: impl Into<String>) -> ProtocolResult<Self> {
        let value = value.into();
        let valid = !value.is_empty()
            && value.len() <= 96
            && value
                .bytes()
                .next()
                .is_some_and(|byte| byte.is_ascii_uppercase())
            && value
                .bytes()
                .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_');
        if valid {
            Ok(Self(value))
        } else {
            Err(ProtocolError::InvalidField {
                field: "error.code",
                reason: format!("invalid stable error code {value:?}"),
            })
        }
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl<'de> Deserialize<'de> for ErrorCode {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::new(value).map_err(de::Error::custom)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum JobState {
    Queued,
    Assigned,
    Accepted,
    Running,
    Prepared,
    Publishing,
    CancelRequested,
    Succeeded,
    Conflicted,
    Rejected,
    Failed,
    Cancelled,
    TimedOut,
    RecoveryRequired,
    Unknown,
}

impl JobState {
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Succeeded
                | Self::Conflicted
                | Self::Rejected
                | Self::Failed
                | Self::Cancelled
                | Self::TimedOut
                | Self::RecoveryRequired
        )
    }

    #[must_use]
    pub const fn is_failure_terminal(self) -> bool {
        matches!(
            self,
            Self::Rejected
                | Self::Failed
                | Self::Cancelled
                | Self::TimedOut
                | Self::RecoveryRequired
        )
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::{
        MetadataBatchId, MetadataBatchPage, MetadataBatchRecords, MetadataBatchScope, PlaygroundId,
    };

    fn canonical_add_operation() -> AddOperation {
        AddOperation {
            job_id: JobId::new("job-digest-1").unwrap(),
            principal: PrincipalRef {
                kind: PrincipalKind::Service,
                id: PrincipalId::new("principal-digest-1").unwrap(),
                extensions: Extensions::new(),
            },
            tenant_id: TenantId::new("tenant-digest-1").unwrap(),
            project_id: ProjectId::new("project-digest-1").unwrap(),
            artifact_id: ArtifactId::new("artifact-digest-1").unwrap(),
            playground_id: crate::PlaygroundId::new("playground-digest-1").unwrap(),
            expected_index_version: WireIndexVersion {
                revision: crate::IndexRevision::new(7),
                digest: ContentDigest::from_bytes([0x42; 32]),
                extensions: Extensions::new(),
            },
            deadline_unix_ms: UnixMillis::new(1_234_567),
            paths: vec![LogicalPath::parse("dataset/train.bin").unwrap()],
            all: false,
            extensions: Extensions::new(),
        }
    }

    fn assignment_for(operation: &AddOperation, request_digest: ContentDigest) -> AddAssignment {
        AddAssignment {
            job_id: operation.job_id.clone(),
            assignment_id: AssignmentId::new("assignment-digest-1").unwrap(),
            assignment_generation: AssignmentGeneration::new(1),
            agent_id: AgentId::new("agent-digest-1").unwrap(),
            principal: operation.principal.clone(),
            tenant_id: operation.tenant_id.clone(),
            project_id: operation.project_id.clone(),
            artifact_id: operation.artifact_id.clone(),
            playground_id: operation.playground_id.clone(),
            edge_cluster_id: EdgeClusterId::new("cluster-digest-1").unwrap(),
            storage_volume_id: StorageVolumeId::new("volume-digest-1").unwrap(),
            artifact_placement_id: ArtifactPlacementId::new("placement-digest-1").unwrap(),
            placement_generation: PlacementGeneration::new(1),
            agent_mount_id: AgentMountId::new("mount-digest-1").unwrap(),
            mount_generation: MountGeneration::new(1),
            owner_generation: OwnerGeneration::new(1),
            expected_index_version: operation.expected_index_version.clone(),
            lease: None,
            request_digest,
            deadline_unix_ms: operation.deadline_unix_ms,
            paths: operation.paths.clone(),
            all: operation.all,
            extensions: operation.extensions.clone(),
        }
    }

    fn workspace_assignment() -> WorkspaceMaterializeAssignment {
        let project_id = ProjectId::new("project-materialize-1").unwrap();
        let artifact_id = ArtifactId::new("artifact-materialize-1").unwrap();
        let playground_id = PlaygroundId::new("playground-materialize-1").unwrap();
        let relative_root = WorkspaceMaterializeAssignment::canonical_relative_root(
            &project_id,
            &artifact_id,
            &playground_id,
        )
        .unwrap();
        let mut assignment = WorkspaceMaterializeAssignment {
            job_id: JobId::new("job-materialize-1").unwrap(),
            assignment_id: AssignmentId::new("assignment-materialize-1").unwrap(),
            assignment_generation: AssignmentGeneration::new(1),
            agent_id: AgentId::new("agent-materialize-1").unwrap(),
            principal: PrincipalRef {
                kind: PrincipalKind::Service,
                id: PrincipalId::new("principal-materialize-1").unwrap(),
                extensions: Extensions::new(),
            },
            tenant_id: TenantId::new("tenant-materialize-1").unwrap(),
            project_id,
            artifact_id,
            playground_id,
            storage_volume_id: StorageVolumeId::new("volume-materialize-1").unwrap(),
            agent_mount_id: AgentMountId::new("mount-materialize-1").unwrap(),
            mount_generation: MountGeneration::new(1),
            owner_generation: OwnerGeneration::new(1),
            relative_root,
            base_commit_id: None,
            base_index_version: None,
            request_digest: ContentDigest::from_bytes([0; 32]),
            deadline_unix_ms: UnixMillis::new(1_234_567),
            extensions: Extensions::new(),
        };
        assignment.request_digest = assignment.computed_request_digest().unwrap();
        assignment
    }

    fn snapshot_assignment() -> SnapshotMountAssignment {
        let project_id = ProjectId::new("project-snapshot-1").unwrap();
        let artifact_id = ArtifactId::new("artifact-snapshot-1").unwrap();
        let snapshot_id = SnapshotId::new("snapshot-1").unwrap();
        let relative_root = SnapshotMountAssignment::canonical_relative_root(
            &project_id,
            &artifact_id,
            &snapshot_id,
        )
        .unwrap();
        let mut assignment = SnapshotMountAssignment {
            job_id: JobId::new("job-snapshot-1").unwrap(),
            assignment_id: AssignmentId::new("assignment-snapshot-1").unwrap(),
            assignment_generation: AssignmentGeneration::new(1),
            agent_id: AgentId::new("agent-snapshot-1").unwrap(),
            principal: PrincipalRef {
                kind: PrincipalKind::Service,
                id: PrincipalId::new("principal-snapshot-1").unwrap(),
                extensions: Extensions::new(),
            },
            tenant_id: TenantId::new("tenant-snapshot-1").unwrap(),
            project_id,
            artifact_id,
            snapshot_id,
            storage_volume_id: StorageVolumeId::new("volume-snapshot-1").unwrap(),
            agent_mount_id: AgentMountId::new("mount-snapshot-1").unwrap(),
            mount_generation: MountGeneration::new(1),
            owner_generation: OwnerGeneration::new(1),
            relative_root,
            commit_id: ContentDigest::from_bytes([0x24; 32]),
            index_version: WireIndexVersion {
                revision: crate::IndexRevision::new(4),
                digest: ContentDigest::from_bytes([0x42; 32]),
                extensions: Extensions::new(),
            },
            request_digest: ContentDigest::from_bytes([0; 32]),
            deadline_unix_ms: UnixMillis::new(1_234_567),
            extensions: Extensions::new(),
        };
        assignment.request_digest = assignment.computed_request_digest().unwrap();
        assignment
    }

    #[test]
    fn add_operation_digest_has_a_stable_jcs_golden_vector() {
        assert_eq!(
            canonical_add_operation()
                .request_digest()
                .unwrap()
                .to_string(),
            "9afc191146651a38b3162d79f95699b5060ef4f1df53996cae3f19bc5f7b8866"
        );
    }

    #[test]
    fn assignment_validation_rejects_an_operation_digest_mismatch() {
        let operation = canonical_add_operation();
        let digest = operation.request_digest().unwrap();
        let valid = assignment_for(&operation, digest);
        valid.validate().unwrap();

        let mut tampered = valid;
        tampered.paths = vec![LogicalPath::parse("dataset/test.bin").unwrap()];
        assert!(matches!(
            tampered.validate(),
            Err(ProtocolError::InvalidDigest(_))
        ));
    }

    #[test]
    fn workspace_materialize_assignment_requires_server_derived_relative_root() {
        let valid = workspace_assignment();
        valid.validate().unwrap();

        let mut absolute = serde_json::to_value(&valid).unwrap();
        absolute["relative_root"] = json!("/tmp/playground");
        assert!(serde_json::from_value::<WorkspaceMaterializeAssignment>(absolute).is_err());

        let mut mismatched = valid.clone();
        mismatched.relative_root =
            LogicalPath::parse("playgrounds/other/artifact/playground").unwrap();
        assert!(matches!(
            mismatched.validate(),
            Err(ProtocolError::InvalidField {
                field: "relative_root",
                ..
            })
        ));

        let mut tampered = valid;
        tampered.base_commit_id = Some(ContentDigest::from_bytes([0x42; 32]));
        tampered.base_index_version = Some(canonical_add_operation().expected_index_version);
        assert!(matches!(
            tampered.validate(),
            Err(ProtocolError::InvalidDigest(_))
        ));
    }

    #[test]
    fn workspace_materialize_operation_round_trips_as_a_distinct_assignment_kind() {
        let input = workspace_assignment();
        let envelope = ControlEnvelope {
            protocol_version: PROTOCOL_VERSION_V1,
            message_id: MessageId::new("msg-materialize-1").unwrap(),
            session_generation: SessionGeneration::new(1),
            resource_version: None,
            request_id: None,
            trace_id: None,
            sent_at_unix_ms: UnixMillis::new(10),
            message: ControlMessage::Assignment(Box::new(JobAssignment {
                assignment: AssignmentOperation::WorkspaceMaterialize {
                    input: input.clone(),
                    extensions: Extensions::new(),
                },
                extensions: Extensions::new(),
            })),
            extensions: Extensions::new(),
        };
        let encoded = serde_json::to_vec(&envelope).unwrap();
        let decoded = ControlEnvelope::decode_json(&encoded).unwrap();
        assert_eq!(decoded, envelope);
    }

    #[test]
    fn snapshot_mount_assignment_binds_commit_index_and_canonical_root() {
        let valid = snapshot_assignment();
        valid.validate().unwrap();

        let mut mismatched = valid.clone();
        mismatched.relative_root = LogicalPath::parse("snapshots/other/artifact/snapshot").unwrap();
        assert!(matches!(
            mismatched.validate(),
            Err(ProtocolError::InvalidField {
                field: "relative_root",
                ..
            })
        ));

        let mut commit_tampered = valid.clone();
        commit_tampered.commit_id = ContentDigest::from_bytes([0x25; 32]);
        assert!(matches!(
            commit_tampered.validate(),
            Err(ProtocolError::InvalidDigest(_))
        ));

        let mut index_tampered = valid;
        index_tampered.index_version.revision = crate::IndexRevision::new(5);
        assert!(matches!(
            index_tampered.validate(),
            Err(ProtocolError::InvalidDigest(_))
        ));
    }

    #[test]
    fn snapshot_mount_round_trips_as_a_distinct_assignment_kind() {
        let input = snapshot_assignment();
        let envelope = ControlEnvelope {
            protocol_version: PROTOCOL_VERSION_V1,
            message_id: MessageId::new("msg-snapshot-1").unwrap(),
            session_generation: SessionGeneration::new(1),
            resource_version: None,
            request_id: None,
            trace_id: None,
            sent_at_unix_ms: UnixMillis::new(10),
            message: ControlMessage::Assignment(Box::new(JobAssignment {
                assignment: AssignmentOperation::SnapshotMount {
                    input: input.clone(),
                    extensions: Extensions::new(),
                },
                extensions: Extensions::new(),
            })),
            extensions: Extensions::new(),
        };
        let encoded = serde_json::to_vec(&envelope).unwrap();
        let decoded = ControlEnvelope::decode_json(&encoded).unwrap();
        assert_eq!(decoded, envelope);
    }

    #[test]
    fn prepared_candidate_digest_has_a_stable_jcs_golden_vector() {
        let prepared = test_job_prepared("batch-manifest-1");
        assert_eq!(
            prepared.candidate_digest.to_string(),
            "dd7f5c35750f9bd2b8b419fb037a044fdff8db0b8c2dc84456281ba3c032382c"
        );
        prepared.validate().unwrap();
    }

    #[test]
    fn prepared_validation_rejects_a_valid_replacement_descriptor() {
        let mut prepared = test_job_prepared("batch-manifest-1");
        let replacement = test_job_prepared("batch-manifest-2")
            .metadata_batches
            .remove(0);
        replacement.validate().unwrap();
        prepared.metadata_batches[0] = replacement;

        assert!(matches!(
            prepared.validate(),
            Err(ProtocolError::InvalidDigest(_))
        ));
    }

    #[test]
    fn job_failed_wire_round_trip_preserves_extensions() {
        let envelope = ControlEnvelope {
            protocol_version: PROTOCOL_VERSION_V1,
            message_id: MessageId::new("msg-failure-1").unwrap(),
            session_generation: SessionGeneration::new(2),
            resource_version: None,
            request_id: None,
            trace_id: None,
            sent_at_unix_ms: UnixMillis::new(1235),
            message: ControlMessage::Failed(test_job_failed()),
            extensions: Extensions::new(),
        };
        let encoded = serde_json::to_vec(&envelope).unwrap();
        let decoded = ControlEnvelope::decode_json(&encoded).unwrap();
        let ControlMessage::Failed(failed) = &decoded.message else {
            panic!("expected job.failed message");
        };
        assert_eq!(failed.extensions["future_failure"], json!({"attempt": 2}));
        assert_eq!(decoded, envelope);
        assert_eq!(
            serde_json::to_value(decoded).unwrap()["type"],
            json!("job.failed")
        );
    }

    #[test]
    fn job_failed_rejects_zero_generation_and_non_failure_terminal_states() {
        let mut failed = test_job_failed();
        failed.assignment_generation = AssignmentGeneration::new(0);
        assert!(matches!(
            failed.validate(),
            Err(ProtocolError::InvalidField {
                field: "assignment_generation",
                ..
            })
        ));

        failed.assignment_generation = AssignmentGeneration::new(1);
        for state in [JobState::Running, JobState::Succeeded, JobState::Conflicted] {
            failed.final_state = state;
            assert!(matches!(
                failed.validate(),
                Err(ProtocolError::InvalidField {
                    field: "final_state",
                    ..
                })
            ));
        }
    }

    #[test]
    fn unknown_envelope_and_payload_fields_round_trip() {
        let encoded = r#"{
            "protocol_version":1,
            "message_id":"msg-1",
            "session_generation":"1",
            "sent_at_unix_ms":"2",
            "type":"protocol.error",
            "payload":{
                "code":"FUTURE_ERROR",
                "message":"future",
                "retryable":false,
                "future_payload":{"enabled":true}
            },
            "future_envelope":"retained"
        }"#;
        let envelope: ControlEnvelope = serde_json::from_str(encoded).unwrap();
        assert_eq!(envelope.extensions["future_envelope"], json!("retained"));
        let ControlMessage::Error(error) = &envelope.message else {
            panic!("expected error message");
        };
        assert_eq!(error.extensions["future_payload"], json!({"enabled": true}));

        let round_trip = serde_json::to_value(envelope).unwrap();
        assert_eq!(round_trip["future_envelope"], json!("retained"));
        assert_eq!(
            round_trip["payload"]["future_payload"],
            json!({"enabled": true})
        );
    }

    #[test]
    fn unknown_assignment_and_decision_fields_round_trip() {
        let operation: AssignmentOperation = serde_json::from_value(json!({
            "operation": "add",
            "input": {
                "job_id": "job-1",
                "assignment_id": "assignment-1",
                "assignment_generation": "1",
                "agent_id": "agent-1",
                "principal": {"kind": "service", "id": "principal-1"},
                "tenant_id": "tenant-1",
                "project_id": "project-1",
                "artifact_id": "artifact-1",
                "playground_id": "playground-1",
                "edge_cluster_id": "cluster-1",
                "storage_volume_id": "volume-1",
                "artifact_placement_id": "placement-1",
                "placement_generation": "1",
                "agent_mount_id": "mount-1",
                "mount_generation": "1",
                "owner_generation": "1",
                "expected_index_version": {
                    "revision": "1",
                    "digest": "00".repeat(32)
                },
                "request_digest": "11".repeat(32),
                "deadline_unix_ms": "100",
                "paths": [],
                "all": true
            },
            "future_operation": {"mode": "v2"}
        }))
        .unwrap();
        let AssignmentOperation::Add { extensions, .. } = &operation else {
            panic!("the Add wire fixture must decode as an Add operation");
        };
        assert_eq!(extensions["future_operation"], json!({"mode": "v2"}));
        assert_eq!(
            serde_json::to_value(operation).unwrap()["future_operation"],
            json!({"mode": "v2"})
        );

        let decision: PublishDecision = serde_json::from_value(json!({
            "outcome": "publish",
            "published_index_version": {
                "revision": "2",
                "digest": "22".repeat(32)
            },
            "future_decision": true
        }))
        .unwrap();
        let PublishDecision::Publish { extensions, .. } = &decision else {
            panic!("expected publish decision");
        };
        assert_eq!(extensions["future_decision"], json!(true));
        assert_eq!(
            serde_json::to_value(decision).unwrap()["future_decision"],
            json!(true)
        );
    }

    #[test]
    fn recursive_validation_rejects_nested_reserved_extension_keys() {
        let mut hello_extensions = Extensions::new();
        hello_extensions.insert("agent_id".to_owned(), json!("shadow-agent"));
        let hello = AgentHello {
            agent_id: AgentId::new("agent-1").unwrap(),
            edge_cluster_id: EdgeClusterId::new("cluster-1").unwrap(),
            compute_node_id: ComputeNodeId::new("node-1").unwrap(),
            agent_version: "0.2.0".to_owned(),
            supported_protocol_versions: vec![PROTOCOL_VERSION_V1],
            capabilities: BTreeSet::new(),
            extensions: hello_extensions,
        };
        assert!(matches!(
            hello.validate(),
            Err(ProtocolError::InvalidField {
                field: "extensions",
                ..
            })
        ));

        let mut observation_extensions = Extensions::new();
        observation_extensions.insert("job_id".to_owned(), json!("shadow-job"));
        let heartbeat = AgentHeartbeat {
            agent_id: AgentId::new("agent-1").unwrap(),
            observed_at_unix_ms: UnixMillis::new(1),
            acknowledged_resource_version: ResourceVersion::new(1),
            sequence: crate::SequenceNumber::new(1),
            running_jobs: vec![RunningJobObservation {
                job_id: JobId::new("job-1").unwrap(),
                assignment_id: AssignmentId::new("assignment-1").unwrap(),
                assignment_generation: AssignmentGeneration::new(1),
                state: JobState::Running,
                extensions: observation_extensions,
            }],
            mount_observations: Vec::new(),
            extensions: Extensions::new(),
        };
        assert!(matches!(
            heartbeat.validate(),
            Err(ProtocolError::InvalidField {
                field: "extensions",
                ..
            })
        ));
    }

    #[test]
    fn terminal_state_classification_is_explicit() {
        assert!(JobState::Succeeded.is_terminal());
        assert!(JobState::RecoveryRequired.is_terminal());
        assert!(!JobState::Publishing.is_terminal());
        assert!(!JobState::Unknown.is_terminal());
    }

    #[test]
    fn runtime_string_limits_count_unicode_scalars_like_json_schema() {
        let mut error = ControlError {
            code: ErrorCode::new("UNICODE_TEST").unwrap(),
            message: "界".repeat(4096),
            retryable: false,
            retry_after_ms: None,
            extensions: Extensions::new(),
        };
        error.validate().unwrap();

        error.message.push('界');
        assert!(matches!(
            error.validate(),
            Err(ProtocolError::LimitExceeded {
                limit_name: "message",
                limit: 4096,
                actual: 4097,
            })
        ));
    }

    #[test]
    fn decision_outcomes_accept_only_their_declared_final_states() {
        const ALL_STATES: [JobState; 15] = [
            JobState::Queued,
            JobState::Assigned,
            JobState::Accepted,
            JobState::Running,
            JobState::Prepared,
            JobState::Publishing,
            JobState::CancelRequested,
            JobState::Succeeded,
            JobState::Conflicted,
            JobState::Rejected,
            JobState::Failed,
            JobState::Cancelled,
            JobState::TimedOut,
            JobState::RecoveryRequired,
            JobState::Unknown,
        ];

        for final_state in ALL_STATES {
            let publish = test_job_decision(test_publish_decision(), final_state);
            assert_eq!(
                publish.validate().is_ok(),
                final_state == JobState::Succeeded,
                "unexpected publish validation result for {final_state:?}"
            );

            let conflict = test_job_decision(test_conflict_decision(), final_state);
            assert_eq!(
                conflict.validate().is_ok(),
                final_state == JobState::Conflicted,
                "unexpected conflict validation result for {final_state:?}"
            );

            let reject = test_job_decision(test_reject_decision(), final_state);
            assert_eq!(
                reject.validate().is_ok(),
                matches!(
                    final_state,
                    JobState::Rejected
                        | JobState::Failed
                        | JobState::Cancelled
                        | JobState::TimedOut
                        | JobState::RecoveryRequired
                ),
                "unexpected reject validation result for {final_state:?}"
            );
        }
    }

    #[test]
    fn unknown_message_type_has_stable_protocol_unsupported_error() {
        let encoded = br#"{
            "protocol_version":1,
            "message_id":"msg-1",
            "session_generation":"1",
            "sent_at_unix_ms":"2",
            "type":"agent.future",
            "payload":{}
        }"#;
        let error = ControlEnvelope::decode_json(encoded).unwrap_err();
        assert!(matches!(error, ProtocolError::UnsupportedMessageType(_)));
        assert_eq!(error.stable_code(), "PROTOCOL_UNSUPPORTED");
    }

    #[test]
    fn control_decode_rejects_duplicate_members_recursively() {
        let frames: [&[u8]; 3] = [
            br#"{
                "protocol_version":1,
                "message_id":"msg-first",
                "message_id":"msg-last",
                "session_generation":"1",
                "sent_at_unix_ms":"2",
                "type":"protocol.error",
                "payload":{"code":"INVALID","message":"invalid","retryable":false}
            }"#,
            br#"{
                "protocol_version":1,
                "message_id":"msg-1",
                "session_generation":"1",
                "sent_at_unix_ms":"2",
                "type":"protocol.error",
                "payload":{
                    "code":"INVALID",
                    "message":"first",
                    "message":"last",
                    "retryable":false
                }
            }"#,
            br#"{
                "protocol_version":1,
                "message_id":"msg-1",
                "session_generation":"1",
                "sent_at_unix_ms":"2",
                "type":"protocol.error",
                "payload":{"code":"INVALID","message":"invalid","retryable":false},
                "future_envelope":{"nested":{"mode":"first","mode":"last"}}
            }"#,
        ];

        for frame in frames {
            let error = ControlEnvelope::decode_json(frame).unwrap_err();
            assert_eq!(error.stable_code(), "PROTOCOL_INVALID");
            assert!(error.to_string().contains("duplicate JSON object member"));
        }
    }

    #[test]
    fn control_frames_over_one_mib_are_rejected_before_decode() {
        let encoded = vec![b' '; MAX_CONTROL_MESSAGE_BYTES + 1];
        assert!(matches!(
            ControlEnvelope::decode_json(&encoded),
            Err(ProtocolError::LimitExceeded {
                limit_name: "control message bytes",
                ..
            })
        ));
    }

    #[test]
    fn protocol_version_is_typed_and_unsupported_values_keep_the_stable_error() {
        let encoded = br#"{
            "protocol_version":2,
            "message_id":"msg-1",
            "session_generation":"1",
            "sent_at_unix_ms":"2",
            "type":"protocol.error",
            "payload":{"code":"FUTURE_ERROR","message":"future","retryable":false}
        }"#;
        let error = ControlEnvelope::decode_json(encoded).unwrap_err();
        assert_eq!(error, ProtocolError::UnsupportedProtocolVersion(2));
        assert_eq!(error.stable_code(), "PROTOCOL_UNSUPPORTED");
    }

    #[test]
    fn zero_generations_and_fencing_tokens_are_rejected_by_wire_validation() {
        let encoded = br#"{
            "protocol_version":1,
            "message_id":"msg-1",
            "session_generation":"0",
            "sent_at_unix_ms":"2",
            "type":"protocol.error",
            "payload":{"code":"INVALID","message":"invalid","retryable":false}
        }"#;
        let error = ControlEnvelope::decode_json(encoded).unwrap_err();
        assert_eq!(error.stable_code(), "PROTOCOL_INVALID");

        let lease = LeaseGrant {
            lease_id: LeaseId::new("lease-1").unwrap(),
            mode: LeaseMode::ExclusiveWrite,
            fencing_token: Some(FencingToken::new(0)),
            expires_at_unix_ms: UnixMillis::new(100),
            extensions: Extensions::new(),
        };
        assert!(matches!(
            lease.validate(),
            Err(ProtocolError::InvalidField {
                field: "fencing_token",
                ..
            })
        ));
    }

    fn test_job_decision(decision: PublishDecision, final_state: JobState) -> JobDecision {
        JobDecision {
            job_id: JobId::new("job-1").unwrap(),
            assignment_id: AssignmentId::new("assignment-1").unwrap(),
            assignment_generation: AssignmentGeneration::new(1),
            decision_generation: DecisionGeneration::new(1),
            decision,
            final_state,
            extensions: Extensions::new(),
        }
    }

    fn test_publish_decision() -> PublishDecision {
        PublishDecision::Publish {
            published_index_version: test_index_version(),
            extensions: Extensions::new(),
        }
    }

    fn test_conflict_decision() -> PublishDecision {
        PublishDecision::Conflict {
            current_index_version: test_index_version(),
            extensions: Extensions::new(),
        }
    }

    fn test_reject_decision() -> PublishDecision {
        PublishDecision::Reject {
            error: ControlError {
                code: ErrorCode::new("TEST_REJECTED").unwrap(),
                message: "test rejection".to_owned(),
                retryable: false,
                retry_after_ms: None,
                extensions: Extensions::new(),
            },
            extensions: Extensions::new(),
        }
    }

    fn test_index_version() -> WireIndexVersion {
        WireIndexVersion {
            revision: crate::IndexRevision::new(1),
            digest: "0000000000000000000000000000000000000000000000000000000000000000"
                .parse()
                .unwrap(),
            extensions: Extensions::new(),
        }
    }

    fn test_job_prepared(batch_id: &str) -> JobPrepared {
        let base_index_version = WireIndexVersion {
            revision: crate::IndexRevision::new(7),
            digest: ContentDigest::from_bytes([0x42; 32]),
            extensions: Extensions::new(),
        };
        let scope = MetadataBatchScope {
            tenant_id: TenantId::new("tenant-digest-1").unwrap(),
            project_id: ProjectId::new("project-digest-1").unwrap(),
            artifact_id: ArtifactId::new("artifact-digest-1").unwrap(),
            playground_id: PlaygroundId::new("playground-digest-1").unwrap(),
            job_id: JobId::new("job-digest-1").unwrap(),
            base_index_version: base_index_version.clone(),
            extensions: Extensions::new(),
        };
        let page = MetadataBatchPage::new(
            MetadataBatchId::new(batch_id).unwrap(),
            scope.clone(),
            0,
            1,
            MetadataBatchRecords::Manifest(Vec::new()),
            Extensions::new(),
        )
        .unwrap();
        let descriptor = MetadataBatchDescriptor::from_pages(
            page.batch_id.clone(),
            scope,
            std::slice::from_ref(&page),
            Extensions::new(),
        )
        .unwrap();
        JobPrepared::new(
            JobId::new("job-digest-1").unwrap(),
            AssignmentId::new("assignment-digest-1").unwrap(),
            AssignmentGeneration::new(3),
            base_index_version,
            ContentDigest::from_bytes([0x43; 32]),
            ContentDigest::from_bytes([0x44; 32]),
            vec![descriptor],
            Extensions::new(),
        )
        .unwrap()
    }

    fn test_job_failed() -> JobFailed {
        JobFailed {
            tenant_id: TenantId::new("tenant-failure-1").unwrap(),
            job_id: JobId::new("job-failure-1").unwrap(),
            assignment_id: AssignmentId::new("assignment-failure-1").unwrap(),
            assignment_generation: AssignmentGeneration::new(1),
            final_state: JobState::RecoveryRequired,
            failed_at_unix_ms: UnixMillis::new(1234),
            stage: JobFailureStage::Execution,
            error: ControlError {
                code: ErrorCode::new("EXECUTION_FAILED").unwrap(),
                message: "executor failed".to_owned(),
                retryable: true,
                retry_after_ms: None,
                extensions: Extensions::new(),
            },
            extensions: Extensions::from([("future_failure".to_owned(), json!({"attempt": 2}))]),
        }
    }
}
