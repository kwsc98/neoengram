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
resource_id!(PlaygroundId, "playground ID");
resource_id!(SnapshotId, "snapshot ID");
resource_id!(EdgeClusterId, "edge cluster ID");
resource_id!(ComputeNodeId, "compute node ID");
resource_id!(AgentId, "agent ID");
resource_id!(AgentMountId, "agent mount ID");
resource_id!(StorageVolumeId, "storage volume ID");
resource_id!(ArtifactPlacementId, "artifact placement ID");
resource_id!(GatewayId, "gateway ID");
resource_id!(TransferRouteId, "transfer route ID");
resource_id!(JobId, "job ID");
resource_id!(AssignmentId, "assignment ID");
resource_id!(LeaseId, "lease ID");
resource_id!(SessionId, "session ID");
resource_id!(MetadataBatchId, "metadata batch ID");
resource_id!(ObjectTicketId, "object ticket ID");
resource_id!(ObjectReceiptId, "object receipt ID");
resource_id!(MessageId, "message ID");
resource_id!(RequestId, "request ID");
resource_id!(TraceId, "trace ID");
resource_id!(PrincipalId, "principal ID");

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
