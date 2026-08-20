use std::collections::BTreeSet;

use neoengram_domain::protocol::{
    jcs_blake3, jcs_bytes, AgentHello, AgentId, ComputeNodeId, ControlMessage, EdgeClusterId,
    Envelope, EnvelopeHeader, Extensions, GatewayConnectionId, GatewayControlFrame,
    GatewayControlMessage, GatewayOpaqueBytes, GatewayPeerDirectory, GatewayPeerDirectoryEntry,
    GatewayPeerForwardRequest, GatewayPoolId, GatewayReplicaId, GatewayRouteLeaseRequest,
    GatewayS3ReadRevocation, Generation, LifecycleGeneration, RequestId, ResourceVersion,
    RouteGeneration, SequenceNumber, SessionGeneration, SnapshotId, TenantId, TraceId, UnixMillis,
    CURRENT_WIRE_VERSION,
};
use serde_json::json;

#[test]
fn hello_wire_shape_is_stable() {
    let envelope = Envelope {
        header: EnvelopeHeader {
            wire_version: CURRENT_WIRE_VERSION,
            action: "agent.hello".to_owned(),
            request_id: RequestId::new("request-1").unwrap(),
            trace_id: TraceId::new("trace-1").unwrap(),
            tenant_scope: Some(TenantId::new("tenant-a").unwrap()),
            actor: None,
            session_generation: Some(SessionGeneration::new(7)),
            route_generation: None,
            deadline: UnixMillis::new(1_721_821_605_000),
        },
        body: ControlMessage::Hello(AgentHello {
            agent_id: AgentId::new("agent-a").unwrap(),
            edge_cluster_id: EdgeClusterId::new("cluster-a").unwrap(),
            compute_node_id: ComputeNodeId::new("node-a").unwrap(),
            agent_version: "0.1.0".to_owned(),
            wire_version: CURRENT_WIRE_VERSION,
            capabilities: BTreeSet::from(["metadata_batch_v1".to_owned()]),
            extensions: Extensions::new(),
        }),
    };

    envelope.header.validate().unwrap();
    assert_eq!(
        serde_json::to_value(&envelope).unwrap(),
        json!({
            "wire_version": 1,
            "action": "agent.hello",
            "request_id": "request-1",
            "trace_id": "trace-1",
            "tenant_scope": "tenant-a",
            "actor": null,
            "session_generation": "7",
            "route_generation": null,
            "deadline": "1721821605000",
            "body": {
                "type": "agent.hello",
                "payload": {
                    "agent_id": "agent-a",
                    "edge_cluster_id": "cluster-a",
                    "compute_node_id": "node-a",
                    "agent_version": "0.1.0",
                    "wire_version": 1,
                    "capabilities": ["metadata_batch_v1"]
                }
            }
        })
    );
}

#[test]
fn jcs_golden_vector_is_stable() {
    let input = json!({
        "b": "two",
        "a": 1,
        "nested": {"z": false, "a": null}
    });
    assert_eq!(
        jcs_bytes(&input).unwrap(),
        br#"{"a":1,"b":"two","nested":{"a":null,"z":false}}"#
    );
    assert_eq!(
        jcs_blake3(&input).unwrap().to_string(),
        "d2d0a5d982f7757f7a38a4caeda88d3be3df1ae395f00a84357b60b04422f924"
    );
}

#[test]
fn gateway_route_acquire_wire_shape_is_stable() {
    let frame = GatewayControlFrame {
        wire_version: CURRENT_WIRE_VERSION,
        gateway_pool_id: GatewayPoolId::new("gateway-pool-a").unwrap(),
        gateway_replica_id: GatewayReplicaId::new("gateway-replica-2").unwrap(),
        connection_id: GatewayConnectionId::new("central-connection-7").unwrap(),
        sequence: SequenceNumber::new(9),
        request_id: RequestId::new("request-route-11").unwrap(),
        trace_id: None,
        sent_at_unix_ms: UnixMillis::new(1_721_821_600_000),
        deadline_unix_ms: UnixMillis::new(1_721_821_605_000),
        hop_count: 0,
        message: GatewayControlMessage::RouteAcquire(GatewayRouteLeaseRequest {
            agent_id: AgentId::new("agent-a").unwrap(),
            owner_replica_id: GatewayReplicaId::new("gateway-replica-2").unwrap(),
            agent_connection_id: GatewayConnectionId::new("agent-connection-4").unwrap(),
            session_generation: SessionGeneration::new(3),
            route_generation: None,
            requested_expires_at_unix_ms: UnixMillis::new(1_721_821_630_000),
        }),
        extensions: Extensions::new(),
    };

    frame.validate().unwrap();
    assert_eq!(
        serde_json::to_value(frame).unwrap(),
        json!({
            "wire_version": 1,
            "gateway_pool_id": "gateway-pool-a",
            "gateway_replica_id": "gateway-replica-2",
            "connection_id": "central-connection-7",
            "sequence": "9",
            "request_id": "request-route-11",
            "sent_at_unix_ms": "1721821600000",
            "deadline_unix_ms": "1721821605000",
            "hop_count": 0,
            "type": "route_acquire",
            "payload": {
                "agent_id": "agent-a",
                "owner_replica_id": "gateway-replica-2",
                "agent_connection_id": "agent-connection-4",
                "session_generation": "3",
                "requested_expires_at_unix_ms": "1721821630000"
            }
        })
    );
}

#[test]
fn gateway_peer_forward_wire_shape_is_stable() {
    let frame = GatewayControlFrame {
        wire_version: CURRENT_WIRE_VERSION,
        gateway_pool_id: GatewayPoolId::new("gateway-pool-a").unwrap(),
        gateway_replica_id: GatewayReplicaId::new("gateway-replica-1").unwrap(),
        connection_id: GatewayConnectionId::new("peer-connection-9").unwrap(),
        sequence: SequenceNumber::new(1),
        request_id: RequestId::new("request-forward-12").unwrap(),
        trace_id: None,
        sent_at_unix_ms: UnixMillis::new(1_721_821_600_000),
        deadline_unix_ms: UnixMillis::new(1_721_821_605_000),
        hop_count: 1,
        message: GatewayControlMessage::PeerForward(GatewayPeerForwardRequest {
            source_replica_id: GatewayReplicaId::new("gateway-replica-1").unwrap(),
            target_replica_id: GatewayReplicaId::new("gateway-replica-2").unwrap(),
            target_peer_endpoint: "https://gateway-replica-2.example".to_owned(),
            agent_id: AgentId::new("agent-a").unwrap(),
            agent_connection_id: GatewayConnectionId::new("agent-connection-4").unwrap(),
            session_generation: SessionGeneration::new(3),
            route_generation: RouteGeneration::new(5),
            frame: GatewayOpaqueBytes::new(b"{}\n".to_vec()).unwrap(),
        }),
        extensions: Extensions::new(),
    };

    frame.validate().unwrap();
    assert_eq!(
        serde_json::to_value(frame).unwrap(),
        json!({
            "wire_version": 1,
            "gateway_pool_id": "gateway-pool-a",
            "gateway_replica_id": "gateway-replica-1",
            "connection_id": "peer-connection-9",
            "sequence": "1",
            "request_id": "request-forward-12",
            "sent_at_unix_ms": "1721821600000",
            "deadline_unix_ms": "1721821605000",
            "hop_count": 1,
            "type": "peer_forward",
            "payload": {
                "source_replica_id": "gateway-replica-1",
                "target_replica_id": "gateway-replica-2",
                "target_peer_endpoint": "https://gateway-replica-2.example",
                "agent_id": "agent-a",
                "agent_connection_id": "agent-connection-4",
                "session_generation": "3",
                "route_generation": "5",
                "frame": "e30K"
            }
        })
    );
}

#[test]
fn gateway_peer_directory_wire_shape_is_stable() {
    let frame = GatewayControlFrame {
        wire_version: CURRENT_WIRE_VERSION,
        gateway_pool_id: GatewayPoolId::new("gateway-pool-a").unwrap(),
        gateway_replica_id: GatewayReplicaId::new("gateway-replica-1").unwrap(),
        connection_id: GatewayConnectionId::new("central-connection-7").unwrap(),
        sequence: SequenceNumber::new(2),
        request_id: RequestId::new("peer-directory-1").unwrap(),
        trace_id: None,
        sent_at_unix_ms: UnixMillis::new(1_721_821_600_000),
        deadline_unix_ms: UnixMillis::new(1_721_821_605_000),
        hop_count: 0,
        message: GatewayControlMessage::PeerDirectory(GatewayPeerDirectory {
            directory_generation: Generation::new(3),
            issued_at_unix_ms: UnixMillis::new(1_721_821_600_000),
            expires_at_unix_ms: UnixMillis::new(1_721_821_630_000),
            replicas: vec![GatewayPeerDirectoryEntry {
                gateway_replica_id: GatewayReplicaId::new("gateway-replica-2").unwrap(),
                certificate_generation: neoengram_domain::protocol::CertificateGeneration::new(4),
                certificate_fingerprint: neoengram_domain::protocol::ContentDigest::hash(
                    b"replica-certificate-2",
                ),
            }],
        }),
        extensions: Extensions::new(),
    };

    frame.validate().unwrap();
    assert_eq!(
        serde_json::to_value(frame).unwrap(),
        json!({
            "wire_version": 1,
            "gateway_pool_id": "gateway-pool-a",
            "gateway_replica_id": "gateway-replica-1",
            "connection_id": "central-connection-7",
            "sequence": "2",
            "request_id": "peer-directory-1",
            "sent_at_unix_ms": "1721821600000",
            "deadline_unix_ms": "1721821605000",
            "hop_count": 0,
            "type": "peer_directory",
            "payload": {
                "directory_generation": "3",
                "issued_at_unix_ms": "1721821600000",
                "expires_at_unix_ms": "1721821630000",
                "replicas": [{
                    "gateway_replica_id": "gateway-replica-2",
                    "certificate_generation": "4",
                    "certificate_fingerprint": "d3ae3a0a039e6c0e1cb65a6888e7d9c8e10abfd3c993b69db903b85cca50686b"
                }]
            }
        })
    );
}

#[test]
fn gateway_s3_read_revocation_wire_shape_is_stable() {
    let frame = GatewayControlFrame {
        wire_version: CURRENT_WIRE_VERSION,
        gateway_pool_id: GatewayPoolId::new("gateway-pool-a").unwrap(),
        gateway_replica_id: GatewayReplicaId::new("gateway-replica-1").unwrap(),
        connection_id: GatewayConnectionId::new("central-connection-7").unwrap(),
        sequence: SequenceNumber::new(3),
        request_id: RequestId::new("s3-revocation-1").unwrap(),
        trace_id: None,
        sent_at_unix_ms: UnixMillis::new(1_721_821_600_000),
        deadline_unix_ms: UnixMillis::new(1_721_821_605_000),
        hop_count: 0,
        message: GatewayControlMessage::S3ReadRevocation(GatewayS3ReadRevocation {
            tenant_id: TenantId::new("tenant-a").unwrap(),
            snapshot_id: SnapshotId::new("snapshot-a").unwrap(),
            minimum_snapshot_lifecycle_generation: LifecycleGeneration::new(4),
            bucket: "dataset-a".to_owned(),
            minimum_access_point_policy_generation: ResourceVersion::new(7),
            reason: "snapshot deletion requested".to_owned(),
        }),
        extensions: Extensions::new(),
    };

    frame.validate().unwrap();
    assert_eq!(
        serde_json::to_value(frame).unwrap(),
        json!({
            "wire_version": 1,
            "gateway_pool_id": "gateway-pool-a",
            "gateway_replica_id": "gateway-replica-1",
            "connection_id": "central-connection-7",
            "sequence": "3",
            "request_id": "s3-revocation-1",
            "sent_at_unix_ms": "1721821600000",
            "deadline_unix_ms": "1721821605000",
            "hop_count": 0,
            "type": "s3_read_revocation",
            "payload": {
                "tenant_id": "tenant-a",
                "snapshot_id": "snapshot-a",
                "minimum_snapshot_lifecycle_generation": "4",
                "bucket": "dataset-a",
                "minimum_access_point_policy_generation": "7",
                "reason": "snapshot deletion requested"
            }
        })
    );
}
