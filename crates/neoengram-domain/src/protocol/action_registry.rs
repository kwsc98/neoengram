//! The canonical action registry shared by protocol and transport adapters.
//!
//! Action paths are protocol data, not an implementation detail of one HTTP server. Keeping
//! descriptors here prevents the Gateway and Central adapters from drifting apart while
//! retaining their transport-specific handler enums.

use crate::{
    AGENT_ENROLLMENT_BOOTSTRAP_PATH, AGENT_ENROLLMENT_STATUS_QUERY_PATH,
    AGENT_JOB_INDEX_PAGE_QUERY_PATH, AGENT_JOB_MANIFEST_PAGE_QUERY_PATH,
    AGENT_JOB_METADATA_BATCH_STAGE_PATH, AGENT_JOB_METADATA_PAGE_STAGE_PATH,
    AGENT_JOB_REPORT_CREATE_PATH, AGENT_SESSION_CHANNEL_OPEN_PATH, AGENT_SESSION_CLOSE_PATH,
    AGENT_SESSION_HEARTBEAT_REPORT_PATH, AGENT_SESSION_OPEN_PATH,
};

/// Action identifiers carried by strict [`crate::Envelope`] headers for Agent data-plane
/// deliveries. The transport path may wrap these bodies in an H2 frame, but the action identity
/// remains stable across Central, Gateway, and Agent adapters.
pub const AGENT_JOB_ASSIGNMENT_ACTION: &str = "agent.job.assignment";
pub const AGENT_JOB_DECISION_ACTION: &str = "agent.job.decision";
pub const AGENT_LIFECYCLE_ASSIGNMENT_ACTION: &str = "agent.lifecycle.assignment";
pub const AGENT_JOB_REPORT_ACTION: &str = "agent.job.report.create";
pub const AGENT_PROTOCOL_ERROR_ACTION: &str = "agent.protocol.error";

const CONTROL_ACTIONS: &[&str] = &[
    AGENT_JOB_ASSIGNMENT_ACTION,
    AGENT_JOB_DECISION_ACTION,
    AGENT_LIFECYCLE_ASSIGNMENT_ACTION,
    AGENT_JOB_REPORT_ACTION,
    AGENT_PROTOCOL_ERROR_ACTION,
    "agent.hello",
    "agent.heartbeat",
];

/// The protocol-level identity of an Agent action.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(usize)]
pub enum AgentActionKind {
    EnrollmentBootstrap,
    EnrollmentStatusQuery,
    SessionOpen,
    SessionChannelOpen,
    SessionHeartbeatReport,
    JobReportCreate,
    JobMetadataBatchStage,
    JobMetadataPageStage,
    JobIndexPageQuery,
    JobManifestPageQuery,
    SessionClose,
}

/// Transport used by an Agent action.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AgentActionTransport {
    /// Unary action-style HTTP request.
    HttpPost,
    /// Full-duplex HTTP/2 NDJSON control channel.
    Http2Ndjson,
}

/// One canonical action route descriptor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AgentActionDescriptor {
    pub kind: AgentActionKind,
    pub method: &'static str,
    pub path: &'static str,
    /// Stable OpenAPI operation identifier for the Agent action.
    pub operation_id: &'static str,
    pub transport: AgentActionTransport,
    /// Whether the action uses the signed `AgentAuthenticatedRequest` envelope.
    pub requires_agent_proof: bool,
}

/// The single source of truth for Agent action paths and transport semantics.
pub const AGENT_ACTION_REGISTRY: &[AgentActionDescriptor] = &[
    AgentActionDescriptor {
        kind: AgentActionKind::EnrollmentBootstrap,
        method: "POST",
        path: AGENT_ENROLLMENT_BOOTSTRAP_PATH,
        operation_id: "bootstrapAgentEnrollment",
        transport: AgentActionTransport::HttpPost,
        requires_agent_proof: false,
    },
    AgentActionDescriptor {
        kind: AgentActionKind::EnrollmentStatusQuery,
        method: "POST",
        path: AGENT_ENROLLMENT_STATUS_QUERY_PATH,
        operation_id: "queryAgentEnrollmentStatus",
        transport: AgentActionTransport::HttpPost,
        requires_agent_proof: false,
    },
    AgentActionDescriptor {
        kind: AgentActionKind::SessionOpen,
        method: "POST",
        path: AGENT_SESSION_OPEN_PATH,
        operation_id: "openAgentSession",
        transport: AgentActionTransport::HttpPost,
        requires_agent_proof: true,
    },
    AgentActionDescriptor {
        kind: AgentActionKind::SessionChannelOpen,
        method: "POST",
        path: AGENT_SESSION_CHANNEL_OPEN_PATH,
        operation_id: "openAgentSessionChannel",
        transport: AgentActionTransport::Http2Ndjson,
        requires_agent_proof: true,
    },
    AgentActionDescriptor {
        kind: AgentActionKind::SessionHeartbeatReport,
        method: "POST",
        path: AGENT_SESSION_HEARTBEAT_REPORT_PATH,
        operation_id: "reportAgentSessionHeartbeat",
        transport: AgentActionTransport::HttpPost,
        requires_agent_proof: true,
    },
    AgentActionDescriptor {
        kind: AgentActionKind::JobReportCreate,
        method: "POST",
        path: AGENT_JOB_REPORT_CREATE_PATH,
        operation_id: "createAgentJobReport",
        transport: AgentActionTransport::HttpPost,
        requires_agent_proof: true,
    },
    AgentActionDescriptor {
        kind: AgentActionKind::JobMetadataBatchStage,
        method: "POST",
        path: AGENT_JOB_METADATA_BATCH_STAGE_PATH,
        operation_id: "stageAgentJobMetadataBatch",
        transport: AgentActionTransport::HttpPost,
        requires_agent_proof: true,
    },
    AgentActionDescriptor {
        kind: AgentActionKind::JobMetadataPageStage,
        method: "POST",
        path: AGENT_JOB_METADATA_PAGE_STAGE_PATH,
        operation_id: "stageAgentJobMetadataPage",
        transport: AgentActionTransport::HttpPost,
        requires_agent_proof: true,
    },
    AgentActionDescriptor {
        kind: AgentActionKind::JobIndexPageQuery,
        method: "POST",
        path: AGENT_JOB_INDEX_PAGE_QUERY_PATH,
        operation_id: "queryAgentJobIndexPage",
        transport: AgentActionTransport::HttpPost,
        requires_agent_proof: true,
    },
    AgentActionDescriptor {
        kind: AgentActionKind::JobManifestPageQuery,
        method: "POST",
        path: AGENT_JOB_MANIFEST_PAGE_QUERY_PATH,
        operation_id: "queryAgentJobManifestPage",
        transport: AgentActionTransport::HttpPost,
        requires_agent_proof: true,
    },
    AgentActionDescriptor {
        kind: AgentActionKind::SessionClose,
        method: "POST",
        path: AGENT_SESSION_CLOSE_PATH,
        operation_id: "closeAgentSession",
        transport: AgentActionTransport::HttpPost,
        requires_agent_proof: true,
    },
];

/// Gateway listener owning one fixed infrastructure action.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GatewayActionListener {
    All,
    Agent,
    Control,
    Peer,
}

/// One fixed Gateway infrastructure route. Agent forwarding routes remain sourced from
/// [`AGENT_ACTION_REGISTRY`], while S3 bucket/key and static Web paths are data routes rather than
/// fixed actions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GatewayActionDescriptor {
    pub method: &'static str,
    pub path: &'static str,
    pub listener: GatewayActionListener,
}

/// Canonical fixed routes installed by Gateway listeners in addition to Agent forwarding.
pub const GATEWAY_ACTION_REGISTRY: &[GatewayActionDescriptor] = &[
    GatewayActionDescriptor {
        method: "GET",
        path: "/health/live",
        listener: GatewayActionListener::All,
    },
    GatewayActionDescriptor {
        method: "GET",
        path: "/health/ready",
        listener: GatewayActionListener::All,
    },
    GatewayActionDescriptor {
        method: "POST",
        path: crate::GATEWAY_REPLICA_BOOTSTRAP_CHALLENGE_PATH,
        listener: GatewayActionListener::Agent,
    },
    GatewayActionDescriptor {
        method: "POST",
        path: crate::GATEWAY_REPLICA_BOOTSTRAP_CERTIFICATE_PATH,
        listener: GatewayActionListener::Agent,
    },
    GatewayActionDescriptor {
        method: "POST",
        path: crate::S3_READ_CHANNEL_PATH,
        listener: GatewayActionListener::Agent,
    },
    GatewayActionDescriptor {
        method: "POST",
        path: crate::GATEWAY_CONTROL_CHANNEL_PATH,
        listener: GatewayActionListener::Control,
    },
    GatewayActionDescriptor {
        method: "POST",
        path: crate::GATEWAY_PEER_FORWARD_PATH,
        listener: GatewayActionListener::Peer,
    },
    GatewayActionDescriptor {
        method: "POST",
        path: crate::S3_READ_PEER_PATH,
        listener: GatewayActionListener::Peer,
    },
];

/// A public Central action route. The registry deliberately stores only transport metadata;
/// request and response bodies remain owned by the Central API adapter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PublicActionDescriptor {
    pub method: &'static str,
    pub path: &'static str,
    pub operation_id: &'static str,
    /// Whether the current Central binary installs a handler for this documented action.
    pub routed_by_central: bool,
}

const fn public_action(
    method: &'static str,
    path: &'static str,
    operation_id: &'static str,
) -> PublicActionDescriptor {
    PublicActionDescriptor {
        method,
        path,
        operation_id,
        routed_by_central: true,
    }
}

const fn documented_action(
    method: &'static str,
    path: &'static str,
    operation_id: &'static str,
) -> PublicActionDescriptor {
    PublicActionDescriptor {
        method,
        path,
        operation_id,
        routed_by_central: false,
    }
}

/// Canonical public OpenAPI action routes.
///
/// A route with `routed_by_central = false` is an explicit contract-only Web surface. Keeping
/// that state in the registry prevents the OpenAPI document from silently claiming that Central
/// implements it. Controller descriptors and OpenAPI are both checked against this table.
pub const PUBLIC_ACTION_REGISTRY: &[PublicActionDescriptor] = &[
    public_action("POST", "/api/system/version/query", "queryApiVersion"),
    public_action("POST", "/api/tenant/list/query", "queryTenantList"),
    public_action("POST", "/api/tenant/query", "queryTenant"),
    public_action("POST", "/api/tenant/create", "createTenant"),
    public_action(
        "POST",
        "/api/storage/volume/list/query",
        "queryStorageVolumeList",
    ),
    public_action("POST", "/api/storage/volume/query", "queryStorageVolume"),
    public_action("POST", "/api/storage/volume/create", "createStorageVolume"),
    public_action(
        "POST",
        "/api/storage/enrollment/token/create",
        "createStorageEnrollmentToken",
    ),
    public_action(
        "POST",
        "/api/storage/enrollment/list/query",
        "queryStorageEnrollmentList",
    ),
    public_action(
        "POST",
        "/api/storage/enrollment/query",
        "queryStorageEnrollment",
    ),
    public_action(
        "POST",
        "/api/storage/enrollment/approve",
        "approveStorageEnrollment",
    ),
    public_action(
        "POST",
        "/api/storage/enrollment/recovery/complete",
        "completeStorageRecovery",
    ),
    public_action(
        "POST",
        "/api/storage/enrollment/reject",
        "rejectStorageEnrollment",
    ),
    public_action("POST", "/api/project/list/query", "queryProjectList"),
    public_action("POST", "/api/project/create", "createProject"),
    public_action("POST", "/api/artifact/list/query", "queryArtifactList"),
    public_action("POST", "/api/artifact/query", "queryArtifact"),
    public_action("POST", "/api/artifact/create", "createArtifact"),
    public_action(
        "POST",
        "/api/artifact/commit/graph/query",
        "queryArtifactCommitGraph",
    ),
    documented_action(
        "POST",
        "/api/artifact/commit/diff/query",
        "queryArtifactCommitDiff",
    ),
    public_action("POST", "/api/playground/list/query", "queryPlaygroundList"),
    public_action("POST", "/api/playground/query", "queryPlayground"),
    public_action("POST", "/api/playground/create", "createPlayground"),
    public_action(
        "POST",
        "/api/playground/precommit/start",
        "startPlaygroundPreCommit",
    ),
    public_action(
        "POST",
        "/api/playground/precommit/query",
        "queryPlaygroundPreCommit",
    ),
    public_action(
        "POST",
        "/api/playground/precommit/restart",
        "restartPlaygroundPreCommit",
    ),
    public_action(
        "POST",
        "/api/playground/precommit/cancel",
        "cancelPlaygroundPreCommit",
    ),
    public_action(
        "POST",
        "/api/playground/file/list/query",
        "queryPlaygroundFileList",
    ),
    public_action(
        "POST",
        "/api/playground/change/list/query",
        "queryPlaygroundChangeList",
    ),
    public_action(
        "POST",
        "/api/playground/file/metadata/query",
        "queryPlaygroundFileMetadata",
    ),
    public_action(
        "POST",
        "/api/playground/dataset/profile/query",
        "queryPlaygroundDatasetProfile",
    ),
    public_action("POST", "/api/playground/commit/create", "commitPlayground"),
    public_action("POST", "/api/snapshot/list/query", "querySnapshotList"),
    public_action("POST", "/api/snapshot/query", "querySnapshot"),
    public_action("POST", "/api/snapshot/create", "createSnapshot"),
    public_action(
        "POST",
        "/api/snapshot/delivery/retry",
        "retrySnapshotDelivery",
    ),
    public_action(
        "POST",
        "/api/snapshot/delivery/create",
        "createSnapshotDelivery",
    ),
    public_action(
        "POST",
        "/api/snapshot/delivery/query",
        "querySnapshotDelivery",
    ),
    public_action(
        "POST",
        "/api/snapshot/delivery/list/query",
        "querySnapshotDeliveryList",
    ),
    public_action(
        "POST",
        "/api/snapshot/delivery/delete",
        "deleteSnapshotDelivery",
    ),
    documented_action(
        "POST",
        "/api/snapshot/file/list/query",
        "querySnapshotFileList",
    ),
    documented_action(
        "POST",
        "/api/snapshot/activity/list/query",
        "querySnapshotActivityList",
    ),
    documented_action(
        "POST",
        "/api/snapshot/dataset/profile/query",
        "querySnapshotDatasetProfile",
    ),
    public_action("POST", "/api/s3/access-point/create", "createS3AccessPoint"),
    public_action(
        "POST",
        "/api/s3/access-point/list/query",
        "queryS3AccessPointList",
    ),
    public_action("POST", "/api/s3/access-point/query", "queryS3AccessPoint"),
    public_action("POST", "/api/s3/access-point/enable", "enableS3AccessPoint"),
    public_action(
        "POST",
        "/api/s3/access-point/disable",
        "disableS3AccessPoint",
    ),
    public_action("POST", "/api/s3/credential/create", "createS3Credential"),
    public_action(
        "POST",
        "/api/s3/credential/list/query",
        "queryS3CredentialList",
    ),
    public_action("POST", "/api/s3/credential/revoke", "revokeS3Credential"),
    public_action("POST", "/api/s3/object/list/query", "queryS3ObjectList"),
    public_action(
        "POST",
        "/api/s3/object/download-url/create",
        "createS3DownloadUrl",
    ),
    public_action(
        "POST",
        "/api/resource/deletion/impact/query",
        "queryResourceDeletionImpact",
    ),
    public_action(
        "POST",
        "/api/resource/deletion/create",
        "createResourceDeletion",
    ),
    public_action(
        "POST",
        "/api/resource/deletion/query",
        "queryResourceDeletion",
    ),
    public_action(
        "POST",
        "/api/resource/deletion/list/query",
        "queryResourceDeletionList",
    ),
    public_action(
        "POST",
        "/api/resource/deletion/restore",
        "restoreResourceDeletion",
    ),
    public_action(
        "POST",
        "/api/resource/deletion/retry",
        "retryResourceDeletion",
    ),
    public_action(
        "POST",
        "/api/resource/retention-hold/create",
        "createResourceRetentionHold",
    ),
    public_action(
        "POST",
        "/api/resource/retention-hold/release",
        "releaseResourceRetentionHold",
    ),
    public_action("POST", "/api/gateway/pool/create", "createGatewayPool"),
    public_action("POST", "/api/gateway/pool/query", "queryGatewayPool"),
    public_action(
        "POST",
        "/api/gateway/pool/list/query",
        "queryGatewayPoolList",
    ),
    public_action("POST", "/api/gateway/pool/update", "updateGatewayPool"),
    public_action("POST", "/api/gateway/pool/drain", "drainGatewayPool"),
    public_action(
        "POST",
        "/api/gateway/replica/create",
        "createGatewayReplica",
    ),
    public_action(
        "POST",
        "/api/gateway/replica/activate",
        "activateGatewayReplica",
    ),
    public_action(
        "POST",
        "/api/gateway/replica/list/query",
        "queryGatewayReplicaList",
    ),
    public_action("POST", "/api/gateway/replica/drain", "drainGatewayReplica"),
    public_action(
        "POST",
        "/api/gateway/replica/revoke",
        "revokeGatewayReplica",
    ),
    public_action("POST", "/api/job/add/create", "createAddJob"),
    public_action("POST", "/api/job/query", "queryJob"),
    public_action("POST", "/api/job/add/finalize", "finalizeAddJob"),
    public_action("GET", "/health/live", "liveProbe"),
    public_action("GET", "/health/ready", "readyProbe"),
];

/// Private Central routes intentionally excluded from the public OpenAPI document.
pub const INTERNAL_ACTION_REGISTRY: &[PublicActionDescriptor] = &[public_action(
    "POST",
    crate::S3_AUTHORIZE_PATH,
    "authorizeS3Request",
)];

/// Returns every HTTP action installed by the current Central binary.
pub fn central_action_registry() -> impl Iterator<Item = &'static PublicActionDescriptor> + Clone {
    PUBLIC_ACTION_REGISTRY
        .iter()
        .filter(|descriptor| descriptor.routed_by_central)
        .chain(INTERNAL_ACTION_REGISTRY)
}

#[must_use]
pub fn public_action_descriptor_for_path(path: &str) -> Option<&'static PublicActionDescriptor> {
    PUBLIC_ACTION_REGISTRY
        .iter()
        .find(|descriptor| descriptor.path == path)
}

/// Returns whether an exact action identity belongs to the current action registry.
///
/// Public `/api/` routes omit that transport prefix from their envelope action. Other fixed
/// routes retain every path segment. For example, `/api/artifact/create` maps to
/// `artifact.create`, while `/agent/session/open` maps to `agent.session.open`.
#[must_use]
pub fn is_registered_action(action: &str) -> bool {
    CONTROL_ACTIONS.contains(&action)
        || AGENT_ACTION_REGISTRY
            .iter()
            .any(|descriptor| route_path_matches_action(descriptor.path, action))
        || GATEWAY_ACTION_REGISTRY
            .iter()
            .any(|descriptor| route_path_matches_action(descriptor.path, action))
        || PUBLIC_ACTION_REGISTRY
            .iter()
            .any(|descriptor| route_path_matches_action(descriptor.path, action))
        || INTERNAL_ACTION_REGISTRY
            .iter()
            .any(|descriptor| route_path_matches_action(descriptor.path, action))
}

pub(crate) fn registered_actions() -> std::collections::BTreeSet<String> {
    CONTROL_ACTIONS
        .iter()
        .map(|action| (*action).to_owned())
        .chain(
            AGENT_ACTION_REGISTRY
                .iter()
                .filter_map(|descriptor| action_from_route_path(descriptor.path)),
        )
        .chain(
            GATEWAY_ACTION_REGISTRY
                .iter()
                .filter_map(|descriptor| action_from_route_path(descriptor.path)),
        )
        .chain(
            PUBLIC_ACTION_REGISTRY
                .iter()
                .filter_map(|descriptor| action_from_route_path(descriptor.path)),
        )
        .chain(
            INTERNAL_ACTION_REGISTRY
                .iter()
                .filter_map(|descriptor| action_from_route_path(descriptor.path)),
        )
        .collect()
}

fn route_path_matches_action(path: &str, action: &str) -> bool {
    action_from_route_path(path).as_deref() == Some(action)
}

fn action_from_route_path(path: &str) -> Option<String> {
    let path = path
        .strip_prefix("/api/")
        .or_else(|| path.strip_prefix('/'))?;
    if path.is_empty() || path.split('/').any(str::is_empty) {
        return None;
    }
    Some(path.replace('/', "."))
}

impl AgentActionKind {
    /// Returns the descriptor for this action.
    #[must_use]
    pub const fn descriptor(self) -> &'static AgentActionDescriptor {
        &AGENT_ACTION_REGISTRY[self as usize]
    }

    /// Returns the canonical action path.
    #[must_use]
    pub const fn path(self) -> &'static str {
        self.descriptor().path
    }
}

/// Resolves one exact canonical path through the shared registry.
#[must_use]
pub fn agent_action_descriptor_for_path(path: &str) -> Option<&'static AgentActionDescriptor> {
    AGENT_ACTION_REGISTRY
        .iter()
        .find(|descriptor| descriptor.path == path)
}

/// Resolves one exact canonical path to its protocol-level action identity.
#[must_use]
pub fn agent_action_from_path(path: &str) -> Option<AgentActionKind> {
    agent_action_descriptor_for_path(path).map(|descriptor| descriptor.kind)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;

    #[test]
    fn registry_paths_are_unique_and_round_trip() {
        let paths = AGENT_ACTION_REGISTRY
            .iter()
            .map(|descriptor| descriptor.path)
            .collect::<BTreeSet<_>>();
        assert_eq!(paths.len(), AGENT_ACTION_REGISTRY.len());
        let operation_ids = AGENT_ACTION_REGISTRY
            .iter()
            .map(|descriptor| descriptor.operation_id)
            .collect::<BTreeSet<_>>();
        assert_eq!(operation_ids.len(), AGENT_ACTION_REGISTRY.len());
        assert!(AGENT_ACTION_REGISTRY
            .iter()
            .all(|descriptor| descriptor.method == "POST"));

        for descriptor in AGENT_ACTION_REGISTRY {
            assert_eq!(descriptor.kind.path(), descriptor.path);
            assert_eq!(
                agent_action_from_path(descriptor.path),
                Some(descriptor.kind)
            );
        }
    }

    #[test]
    fn registry_rejects_unknown_path() {
        assert_eq!(agent_action_from_path("/agent/unknown"), None);
        assert_eq!(
            agent_action_descriptor_for_path("/api/artifact/create"),
            None
        );
    }

    #[test]
    fn action_identities_are_derived_from_the_current_route_registry() {
        assert!(is_registered_action("artifact.create"));
        assert!(is_registered_action("agent.session.open"));
        assert!(is_registered_action("health.ready"));
        assert!(is_registered_action("gateway.control.channel.open"));
        assert!(is_registered_action("internal.s3.authorize"));
        assert!(is_registered_action(AGENT_JOB_ASSIGNMENT_ACTION));
        assert!(!is_registered_action("artifact.future"));
        assert!(!is_registered_action("api.artifact.create"));

        let actions = registered_actions();
        assert!(actions.contains("artifact.create"));
        assert_eq!(actions.len(), actions.iter().collect::<BTreeSet<_>>().len());
    }

    #[test]
    fn only_the_channel_uses_http2() {
        let channels = AGENT_ACTION_REGISTRY
            .iter()
            .filter(|descriptor| descriptor.transport == AgentActionTransport::Http2Ndjson)
            .collect::<Vec<_>>();
        assert_eq!(channels.len(), 1);
        assert_eq!(channels[0].kind, AgentActionKind::SessionChannelOpen);
    }

    #[test]
    fn public_routes_are_unique_and_action_style() {
        let paths = PUBLIC_ACTION_REGISTRY
            .iter()
            .map(|descriptor| descriptor.path)
            .collect::<BTreeSet<_>>();
        assert_eq!(paths.len(), PUBLIC_ACTION_REGISTRY.len());
        let operation_ids = PUBLIC_ACTION_REGISTRY
            .iter()
            .map(|descriptor| descriptor.operation_id)
            .collect::<BTreeSet<_>>();
        assert_eq!(operation_ids.len(), PUBLIC_ACTION_REGISTRY.len());
        assert!(PUBLIC_ACTION_REGISTRY
            .iter()
            .all(|descriptor| descriptor.method == "GET" || descriptor.method == "POST"));
        assert!(PUBLIC_ACTION_REGISTRY
            .iter()
            .all(|descriptor| !descriptor.path.contains('{')));
        assert!(PUBLIC_ACTION_REGISTRY
            .iter()
            .all(|descriptor| !descriptor.path.starts_with("/internal/")));
    }

    #[test]
    fn central_and_gateway_route_exports_are_unique() {
        let central_routes = central_action_registry()
            .map(|descriptor| (descriptor.method, descriptor.path))
            .collect::<BTreeSet<_>>();
        assert_eq!(central_routes.len(), central_action_registry().count());

        let gateway_routes = GATEWAY_ACTION_REGISTRY
            .iter()
            .map(|descriptor| (descriptor.method, descriptor.path))
            .collect::<BTreeSet<_>>();
        assert_eq!(gateway_routes.len(), GATEWAY_ACTION_REGISTRY.len());
    }

    #[test]
    fn contract_only_routes_are_explicit() {
        let paths = PUBLIC_ACTION_REGISTRY
            .iter()
            .filter(|descriptor| !descriptor.routed_by_central)
            .map(|descriptor| descriptor.path)
            .collect::<BTreeSet<_>>();
        assert_eq!(
            paths,
            BTreeSet::from([
                "/api/artifact/commit/diff/query",
                "/api/snapshot/activity/list/query",
                "/api/snapshot/dataset/profile/query",
                "/api/snapshot/file/list/query",
            ])
        );
    }
}
