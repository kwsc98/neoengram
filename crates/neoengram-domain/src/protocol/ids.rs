use std::{fmt, str::FromStr};

use schemars::JsonSchema;
use serde::{de, Deserialize, Deserializer, Serialize};

use crate::{ProtocolError, ProtocolResult};

const MAX_RESOURCE_ID_BYTES: usize = 128;
const RESOURCE_ID_PATTERN: &str = r"^[A-Za-z0-9](?:[A-Za-z0-9._:-]{0,127})$";

fn validate_resource_id(kind: &'static str, value: &str) -> ProtocolResult<()> {
    let valid_length = !value.is_empty() && value.len() <= MAX_RESOURCE_ID_BYTES;
    let mut bytes = value.bytes();
    let valid_first = bytes
        .next()
        .is_some_and(|byte| byte.is_ascii_alphanumeric());
    let valid_rest =
        bytes.all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b':' | b'-'));
    if valid_length && valid_first && valid_rest {
        Ok(())
    } else {
        Err(ProtocolError::InvalidIdentifier {
            kind,
            value: value.to_owned(),
        })
    }
}

macro_rules! resource_id {
    ($name:ident, $kind:literal) => {
        #[doc = concat!("A validated ", $kind, " wire identifier.")]
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, JsonSchema)]
        #[serde(transparent)]
        pub struct $name(
            #[schemars(
                                                                length(min = 1, max = 128),
                                                                regex(pattern = RESOURCE_ID_PATTERN)
                                                            )]
            String,
        );

        impl $name {
            pub fn new(value: impl Into<String>) -> ProtocolResult<Self> {
                let value = value.into();
                validate_resource_id($kind, &value)?;
                Ok(Self(value))
            }

            #[must_use]
            pub fn as_str(&self) -> &str {
                &self.0
            }

            #[must_use]
            pub fn into_string(self) -> String {
                self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(&self.0)
            }
        }

        impl FromStr for $name {
            type Err = ProtocolError;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                Self::new(value)
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: Deserializer<'de>,
            {
                let value = String::deserialize(deserializer)?;
                Self::new(value).map_err(de::Error::custom)
            }
        }
    };
}

resource_id!(TenantId, "tenant ID");
resource_id!(ProjectId, "project ID");
resource_id!(ArtifactId, "artifact ID");
// A first-class namespace for content-addressed objects.  The initial product mapping is one
// namespace per Artifact, but the namespace is deliberately carried independently on every v2
// placement and transfer contract.
resource_id!(ObjectNamespaceId, "object namespace ID");
resource_id!(PlaygroundId, "playground ID");
resource_id!(SnapshotId, "snapshot ID");
resource_id!(EdgeClusterId, "edge cluster ID");
resource_id!(ComputeNodeId, "compute node ID");
resource_id!(AgentId, "agent ID");
resource_id!(AgentEnrollmentId, "agent enrollment ID");
resource_id!(AgentEnrollmentTokenId, "agent enrollment token ID");
resource_id!(AgentInstallationId, "agent installation ID");
resource_id!(AgentBootId, "agent boot ID");
resource_id!(AgentMountId, "agent mount ID");
resource_id!(StorageVolumeId, "storage volume ID");
resource_id!(VolumeMarkerId, "volume marker ID");
resource_id!(ArtifactPlacementId, "artifact placement ID");
resource_id!(GatewayPoolId, "gateway pool ID");
resource_id!(GatewayReplicaId, "gateway replica ID");
resource_id!(GatewayConnectionId, "gateway connection ID");
resource_id!(TransferRouteId, "transfer route ID");
resource_id!(JobId, "job ID");
resource_id!(AssignmentId, "assignment ID");
resource_id!(LeaseId, "lease ID");
resource_id!(SessionId, "session ID");
resource_id!(MetadataBatchId, "metadata batch ID");
resource_id!(ObjectTicketId, "object ticket ID");
resource_id!(ObjectReceiptId, "object receipt ID");
resource_id!(S3AccessPointId, "S3 access point ID");
resource_id!(S3CredentialId, "S3 credential ID");
resource_id!(SnapshotDeliveryId, "Snapshot delivery ID");
resource_id!(DeletionId, "deletion operation ID");
resource_id!(RetentionHoldId, "retention hold ID");
resource_id!(LifecycleEventId, "lifecycle event ID");
resource_id!(DeletionProofId, "deletion proof ID");
resource_id!(LifecycleAssignmentId, "lifecycle assignment ID");
resource_id!(MessageId, "message ID");
resource_id!(RequestId, "request ID");
resource_id!(TraceId, "trace ID");
resource_id!(PrincipalId, "principal ID");
// Placement-first storage and transfer identities.  These are deliberately opaque resource
// identifiers rather than paths or legacy artifact-placement keys; the authority assigns them and
// every wire boundary validates them with the same strict grammar as the existing IDs.
resource_id!(BackendId, "storage backend ID");
resource_id!(ArchiveId, "archive ID");
resource_id!(RegionId, "region ID");
resource_id!(PlacementId, "object placement ID");
resource_id!(PlacementSetId, "commit placement set ID");
resource_id!(ReplicationId, "replication ID");
resource_id!(TransferId, "transfer ID");
resource_id!(MaterializationId, "materialization ID");
resource_id!(MaterializationBatchId, "materialization batch ID");
resource_id!(IntegrityScanId, "integrity scan ID");
resource_id!(WorkspaceId, "workspace ID");
// Unified operation/audit identities. These remain opaque resource identifiers so Central can
// choose an implementation-specific format while every wire boundary applies the same grammar.
resource_id!(TaskId, "operation task ID");
resource_id!(TaskAttemptId, "task attempt ID");
resource_id!(TaskEventId, "task event ID");

/// Explicit spelling used by persistence adapters when a generic task ID would be ambiguous.
pub type OperationTaskId = TaskId;

/// Alias used by placement APIs when the backend is specifically a storage volume or archive.
pub type StorageBackendId = BackendId;

/// Alias emphasizing that a placement set belongs to a Commit.
pub type CommitPlacementSetId = PlacementSetId;

impl From<ArtifactId> for ObjectNamespaceId {
    fn from(value: ArtifactId) -> Self {
        // Resource IDs have the same strict grammar.  ArtifactId can only have been constructed
        // through that grammar, so this conversion is infallible while preserving the explicit
        // namespace on the wire.
        Self::new(value.into_string()).expect("validated ArtifactId is a valid object namespace")
    }
}

impl ObjectNamespaceId {
    /// Returns the initial v2 namespace mapping for an Artifact.
    #[must_use]
    pub fn from_artifact(artifact_id: &ArtifactId) -> Self {
        artifact_id.clone().into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identifiers_reject_unsafe_or_oversized_values() {
        assert!(TenantId::new("tenant-a").is_ok());
        assert!(TenantId::new("").is_err());
        assert!(TenantId::new("/tenant-a").is_err());
        assert!(TenantId::new("tenant/a").is_err());
        assert!(TenantId::new("a".repeat(129)).is_err());
    }

    #[test]
    fn deserialize_always_validates() {
        let result = serde_json::from_str::<TenantId>(r#""../tenant""#);
        assert!(result.is_err());
    }
}
