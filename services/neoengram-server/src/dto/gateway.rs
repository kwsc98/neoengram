use fusen_rs::SensitiveFields;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct CreateGatewayPoolRequest {
    pub gateway_pool_id: String,
    pub edge_cluster_id: String,
    pub display_name: String,
    pub agent_endpoint: String,
    #[serde(default)]
    pub s3_endpoint: Option<String>,
    pub desired_replicas: u16,
    pub minimum_ready_replicas: u16,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct QueryGatewayPoolRequest {
    pub gateway_pool_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct QueryGatewayPoolListRequest {
    #[serde(default)]
    pub edge_cluster_id: Option<String>,
    #[serde(default)]
    pub state: Option<String>,
    #[serde(default)]
    pub after: Option<String>,
    #[serde(default)]
    pub page_size: Option<u16>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct UpdateGatewayPoolRequest {
    pub gateway_pool_id: String,
    pub expected_resource_version: String,
    #[serde(default)]
    pub display_name: Option<String>,
    #[serde(default)]
    pub agent_endpoint: Option<String>,
    #[serde(default)]
    pub s3_endpoint: Option<String>,
    #[serde(default)]
    pub clear_s3_endpoint: bool,
    #[serde(default)]
    pub desired_replicas: Option<u16>,
    #[serde(default)]
    pub minimum_ready_replicas: Option<u16>,
    #[serde(default)]
    pub state: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct DrainGatewayPoolRequest {
    pub gateway_pool_id: String,
    pub expected_resource_version: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
pub struct GatewayPoolResponse {
    pub gateway_pool: GatewayPoolView,
    #[sensitive(kind = "public")]
    pub replayed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
pub struct GatewayPoolListResponse {
    pub items: Vec<GatewayPoolView>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_after: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct GatewayPoolView {
    pub gateway_pool_id: String,
    pub edge_cluster_id: String,
    pub display_name: String,
    pub agent_endpoint: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub s3_endpoint: Option<String>,
    pub desired_replicas: u16,
    pub minimum_ready_replicas: u16,
    pub state: String,
    pub config_generation: String,
    pub resource_version: String,
    pub created_at_unix_ms: String,
    pub updated_at_unix_ms: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct CreateGatewayReplicaRequest {
    pub gateway_replica_id: String,
    pub gateway_pool_id: String,
    pub control_endpoint: String,
    pub peer_endpoint: String,
    pub bootstrap_endpoint: String,
    pub software_version: String,
    pub supported_protocol_versions: Vec<u16>,
    #[serde(default)]
    pub capabilities: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct QueryGatewayReplicaListRequest {
    pub gateway_pool_id: String,
    #[serde(default)]
    pub state: Option<String>,
    #[serde(default)]
    pub after: Option<String>,
    #[serde(default)]
    pub page_size: Option<u16>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct MutateGatewayReplicaRequest {
    pub gateway_replica_id: String,
    pub expected_resource_version: String,
}

/// Activates a pending Gateway Replica using the one-time token returned by create.
///
/// The bootstrap endpoint is deliberately absent from this request.  Central always resolves
/// it from the authoritative Registry record before opening the bootstrap connection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct ActivateGatewayReplicaRequest {
    pub gateway_replica_id: String,
    pub expected_resource_version: String,
    pub activation_token: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct CreateGatewayReplicaResponse {
    pub gateway_replica: GatewayReplicaView,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub activation_token: Option<String>,
    pub replayed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
pub struct GatewayReplicaResponse {
    pub gateway_replica: GatewayReplicaView,
    #[sensitive(kind = "public")]
    pub replayed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
pub struct GatewayReplicaListResponse {
    pub items: Vec<GatewayReplicaView>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_after: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SensitiveFields)]
#[serde(deny_unknown_fields)]
#[sensitive(opaque)]
pub struct GatewayReplicaView {
    pub gateway_replica_id: String,
    pub gateway_pool_id: String,
    pub edge_cluster_id: String,
    pub control_endpoint: String,
    pub peer_endpoint: String,
    pub bootstrap_endpoint: String,
    pub software_version: String,
    pub supported_protocol_versions: Vec<u16>,
    pub capabilities: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_heartbeat_at_unix_ms: Option<String>,
    pub state: String,
    pub credential_state: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub certificate_generation: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub certificate_not_after_unix_ms: Option<String>,
    pub resource_version: String,
    pub created_at_unix_ms: String,
    pub updated_at_unix_ms: String,
}
