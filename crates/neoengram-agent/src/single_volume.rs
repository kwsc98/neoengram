use std::path::{Component, Path, PathBuf};

use neoengram_protocol::{
    AddAssignment, AgentBootId, AgentId, AgentInstallationId, AgentMountId,
    AgentMountIdentityDigest, AgentMountStatusReport, EdgeClusterId, Extensions, MountAccessMode,
    MountGeneration, OwnerGeneration, ResourceHealth, SequenceNumber, SessionGeneration,
    StorageVolumeId, TenantId, UnixMillis, VolumeMarkerId,
};

use crate::{
    AgentError, AgentErrorCode, AgentResult, AssignmentValidator, BasicAssignmentValidator,
};

/// Immutable configuration for the 0.0.1 one-Tenant, one-Volume Agent composition.
#[derive(Clone, PartialEq, Eq)]
pub struct SingleVolumeAgentConfig {
    pub agent_id: AgentId,
    pub installation_id: AgentInstallationId,
    pub tenant_id: TenantId,
    pub edge_cluster_id: EdgeClusterId,
    pub storage_volume_id: StorageVolumeId,
    pub agent_mount_id: AgentMountId,
    pub mount_generation: MountGeneration,
    pub owner_generation: OwnerGeneration,
    pub expected_volume_marker: VolumeMarkerId,
    pub desired_access_mode: MountAccessMode,
    pub data_root: PathBuf,
    pub state_root: PathBuf,
}

impl std::fmt::Debug for SingleVolumeAgentConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SingleVolumeAgentConfig")
            .field("agent_id", &self.agent_id)
            .field("installation_id", &self.installation_id)
            .field("tenant_id", &self.tenant_id)
            .field("edge_cluster_id", &self.edge_cluster_id)
            .field("storage_volume_id", &self.storage_volume_id)
            .field("agent_mount_id", &self.agent_mount_id)
            .field(
                "mount_generation_present",
                &(self.mount_generation.get() != 0),
            )
            .field(
                "owner_generation_present",
                &(self.owner_generation.get() != 0),
            )
            .field("expected_volume_marker", &self.expected_volume_marker)
            .field("desired_access_mode", &self.desired_access_mode)
            .field(
                "data_root_configured",
                &!self.data_root.as_os_str().is_empty(),
            )
            .field(
                "state_root_configured",
                &!self.state_root.as_os_str().is_empty(),
            )
            .finish()
    }
}

impl SingleVolumeAgentConfig {
    pub fn validate(&self) -> AgentResult<()> {
        if self.mount_generation.get() == 0 || self.owner_generation.get() == 0 {
            return Err(AgentError::new(
                AgentErrorCode::GenerationMismatch,
                "mount and owner generations must be positive",
            ));
        }
        if self.expected_volume_marker.as_str() != self.storage_volume_id.as_str() {
            return Err(AgentError::new(
                AgentErrorCode::ScopeMismatch,
                "expected Volume marker must equal the complete StorageVolume ID",
            ));
        }
        if self.desired_access_mode != MountAccessMode::ReadWrite {
            return Err(AgentError::new(
                AgentErrorCode::InvalidState,
                "0.0.1 single-Volume Agents require read-write access",
            ));
        }
        validate_managed_root("data root", &self.data_root)?;
        validate_managed_root("state root", &self.state_root)?;
        if self.data_root.starts_with(&self.state_root)
            || self.state_root.starts_with(&self.data_root)
        {
            return Err(AgentError::new(
                AgentErrorCode::ScopeMismatch,
                "data root and Agent state root must be distinct and non-overlapping",
            ));
        }
        Ok(())
    }

    /// Maps an adapter-provided observation into a protocol report without filesystem discovery.
    pub fn mount_status(
        &self,
        probe: SingleVolumeMountProbe,
    ) -> AgentResult<SingleVolumeMountStatus> {
        self.validate()?;
        if probe.session_generation.get() == 0 {
            return Err(AgentError::new(
                AgentErrorCode::GenerationMismatch,
                "session generation must be positive",
            ));
        }

        let (health, condition) = if probe.observed_volume_marker != self.expected_volume_marker {
            (
                ResourceHealth::Unavailable,
                MountStatusCondition::MarkerMismatch,
            )
        } else if self.desired_access_mode == MountAccessMode::ReadWrite
            && probe.access_mode != MountAccessMode::ReadWrite
        {
            (
                ResourceHealth::Degraded,
                MountStatusCondition::AccessModeMismatch,
            )
        } else {
            let condition = match probe.health {
                ResourceHealth::Ready => MountStatusCondition::Ready,
                ResourceHealth::Degraded => MountStatusCondition::ReportedDegraded,
                ResourceHealth::Unavailable => MountStatusCondition::ReportedUnavailable,
                ResourceHealth::Unknown => MountStatusCondition::ReportedUnknown,
            };
            (probe.health, condition)
        };

        Ok(SingleVolumeMountStatus {
            report: AgentMountStatusReport {
                agent_id: self.agent_id.clone(),
                installation_id: self.installation_id.clone(),
                boot_id: probe.boot_id,
                session_generation: probe.session_generation,
                sequence: probe.sequence,
                agent_mount_id: self.agent_mount_id.clone(),
                storage_volume_id: self.storage_volume_id.clone(),
                mount_generation: self.mount_generation,
                owner_generation: self.owner_generation,
                observed_volume_marker: probe.observed_volume_marker,
                mount_identity_digest: probe.mount_identity_digest,
                access_mode: probe.access_mode,
                health,
                observed_at_unix_ms: probe.observed_at_unix_ms,
                extensions: Extensions::new(),
            },
            condition,
        })
    }

    fn validate_assignment_scope(&self, assignment: &AddAssignment) -> AgentResult<()> {
        let scope_matches = assignment.agent_id == self.agent_id
            && assignment.tenant_id == self.tenant_id
            && assignment.edge_cluster_id == self.edge_cluster_id
            && assignment.storage_volume_id == self.storage_volume_id
            && assignment.agent_mount_id == self.agent_mount_id;
        if !scope_matches {
            return Err(AgentError::new(
                AgentErrorCode::ScopeMismatch,
                "assignment does not target this Agent's configured Tenant and Volume",
            ));
        }
        if assignment.mount_generation != self.mount_generation
            || assignment.owner_generation != self.owner_generation
        {
            return Err(AgentError::new(
                AgentErrorCode::GenerationMismatch,
                "assignment carries a stale mount or Volume owner generation",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SingleVolumeMountProbe {
    pub boot_id: AgentBootId,
    pub session_generation: SessionGeneration,
    pub sequence: SequenceNumber,
    pub observed_volume_marker: VolumeMarkerId,
    pub mount_identity_digest: AgentMountIdentityDigest,
    pub access_mode: MountAccessMode,
    pub health: ResourceHealth,
    pub observed_at_unix_ms: UnixMillis,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MountStatusCondition {
    Ready,
    ReportedDegraded,
    ReportedUnavailable,
    ReportedUnknown,
    MarkerMismatch,
    AccessModeMismatch,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SingleVolumeMountStatus {
    pub report: AgentMountStatusReport,
    pub condition: MountStatusCondition,
}

/// Assignment gate composed from the standard deadline checks and one-Volume desired state.
#[derive(Debug, Clone)]
pub struct SingleVolumeAssignmentValidator {
    config: SingleVolumeAgentConfig,
    mount_health: ResourceHealth,
}

impl SingleVolumeAssignmentValidator {
    #[must_use]
    pub const fn new(config: SingleVolumeAgentConfig, mount_health: ResourceHealth) -> Self {
        Self {
            config,
            mount_health,
        }
    }
}

impl AssignmentValidator for SingleVolumeAssignmentValidator {
    fn validate(&self, assignment: &AddAssignment, now_unix_ms: u64) -> AgentResult<()> {
        self.config.validate()?;
        BasicAssignmentValidator.validate(assignment, now_unix_ms)?;
        self.config.validate_assignment_scope(assignment)?;
        if self.mount_health != ResourceHealth::Ready {
            return Err(AgentError::new(
                AgentErrorCode::MountUnavailable,
                "the configured Volume mount is not Ready",
            ));
        }
        Ok(())
    }
}

fn validate_managed_root(name: &'static str, path: &Path) -> AgentResult<()> {
    if !path.is_absolute() || path.components().any(|part| part == Component::ParentDir) {
        return Err(AgentError::new(
            AgentErrorCode::ScopeMismatch,
            format!("{name} must be an absolute normalized path"),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> SingleVolumeAgentConfig {
        let temp = std::env::temp_dir();
        SingleVolumeAgentConfig {
            agent_id: AgentId::new("agent-a").unwrap(),
            installation_id: AgentInstallationId::new("install-a").unwrap(),
            tenant_id: TenantId::new("tenant-a").unwrap(),
            edge_cluster_id: EdgeClusterId::new("cluster-a").unwrap(),
            storage_volume_id: StorageVolumeId::new("volume-a").unwrap(),
            agent_mount_id: AgentMountId::new("mount-a").unwrap(),
            mount_generation: MountGeneration::new(1),
            owner_generation: OwnerGeneration::new(1),
            expected_volume_marker: VolumeMarkerId::new("volume-a").unwrap(),
            desired_access_mode: MountAccessMode::ReadWrite,
            data_root: temp.join("neoengram-agent-data"),
            state_root: temp.join("neoengram-agent-state"),
        }
    }

    fn probe(marker: &str) -> SingleVolumeMountProbe {
        SingleVolumeMountProbe {
            boot_id: AgentBootId::new("boot-a").unwrap(),
            session_generation: SessionGeneration::new(1),
            sequence: SequenceNumber::new(1),
            observed_volume_marker: VolumeMarkerId::new(marker).unwrap(),
            mount_identity_digest: AgentMountIdentityDigest::new(
                neoengram_core::ContentDigest::from_bytes([0x42; 32]),
            ),
            access_mode: MountAccessMode::ReadWrite,
            health: ResourceHealth::Ready,
            observed_at_unix_ms: UnixMillis::new(100),
        }
    }

    #[test]
    fn marker_mismatch_is_never_reported_ready() {
        let status = config().mount_status(probe("another-marker")).unwrap();
        assert_eq!(status.condition, MountStatusCondition::MarkerMismatch);
        assert_eq!(status.report.health, ResourceHealth::Unavailable);
    }

    #[test]
    fn read_only_observation_degrades_a_write_mount() {
        let mut observed = probe("volume-a");
        observed.access_mode = MountAccessMode::ReadOnly;
        let status = config().mount_status(observed).unwrap();
        assert_eq!(status.condition, MountStatusCondition::AccessModeMismatch);
        assert_eq!(status.report.health, ResourceHealth::Degraded);
    }

    #[test]
    fn mount_status_transmits_identity_without_debug_disclosure() {
        let status = config().mount_status(probe("volume-a")).unwrap();
        let digest = status.report.mount_identity_digest;
        assert_eq!(digest.as_digest().as_bytes(), &[0x42; 32]);
        let debug = format!("{status:?}");
        assert!(debug.contains("[REDACTED]"));
        assert!(!debug.contains(&digest.as_digest().to_string()));
    }

    #[test]
    fn expected_marker_must_be_the_complete_storage_volume_id() {
        let mut invalid = config();
        invalid.expected_volume_marker = VolumeMarkerId::new("marker-a").unwrap();
        assert_eq!(
            invalid.validate().unwrap_err().code(),
            AgentErrorCode::ScopeMismatch
        );
    }

    #[test]
    fn p0_single_volume_config_rejects_read_only_intent() {
        let mut invalid = config();
        invalid.desired_access_mode = MountAccessMode::ReadOnly;
        assert_eq!(
            invalid.validate().unwrap_err().code(),
            AgentErrorCode::InvalidState
        );
    }

    #[test]
    fn single_volume_config_debug_redacts_physical_roots() {
        let mut configured = config();
        configured.data_root = PathBuf::from("/sensitive/pvc-data-root");
        configured.state_root = PathBuf::from("/sensitive/agent-state-root");

        let debug = format!("{configured:?}");

        assert!(debug.contains("data_root_configured: true"));
        assert!(debug.contains("state_root_configured: true"));
        assert!(!debug.contains("/sensitive/pvc-data-root"));
        assert!(!debug.contains("/sensitive/agent-state-root"));
    }

    #[test]
    fn data_and_state_roots_cannot_contain_each_other() {
        let mut state_inside_data = config();
        state_inside_data.state_root = state_inside_data.data_root.join("agent-state");
        assert_eq!(
            state_inside_data.validate().unwrap_err().code(),
            AgentErrorCode::ScopeMismatch
        );

        let mut data_inside_state = config();
        data_inside_state.data_root = data_inside_state.state_root.join("data");
        assert_eq!(
            data_inside_state.validate().unwrap_err().code(),
            AgentErrorCode::ScopeMismatch
        );
    }
}
