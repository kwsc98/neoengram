use std::collections::BTreeSet;

use neoengram_protocol::{
    jcs_blake3, jcs_bytes, AgentHello, AgentId, ComputeNodeId, ControlEnvelope, ControlMessage,
    EdgeClusterId, Extensions, MessageId, SessionGeneration, UnixMillis, PROTOCOL_VERSION_V1,
};
use serde_json::json;

#[test]
fn hello_wire_shape_is_stable() {
    let envelope = ControlEnvelope {
        protocol_version: PROTOCOL_VERSION_V1,
        message_id: MessageId::new("msg-1").unwrap(),
        session_generation: SessionGeneration::new(7),
        resource_version: None,
        request_id: None,
        trace_id: None,
        sent_at_unix_ms: UnixMillis::new(1_721_821_600_000),
        message: ControlMessage::Hello(AgentHello {
            agent_id: AgentId::new("agent-a").unwrap(),
            edge_cluster_id: EdgeClusterId::new("cluster-a").unwrap(),
            compute_node_id: ComputeNodeId::new("node-a").unwrap(),
            agent_version: "0.1.0".to_owned(),
            supported_protocol_versions: vec![PROTOCOL_VERSION_V1],
            capabilities: BTreeSet::from(["metadata_batch_v1".to_owned()]),
            extensions: Extensions::new(),
        }),
        extensions: Extensions::new(),
    };

    envelope.validate().unwrap();
    assert_eq!(
        serde_json::to_value(&envelope).unwrap(),
        json!({
            "protocol_version": 1,
            "message_id": "msg-1",
            "session_generation": "7",
            "sent_at_unix_ms": "1721821600000",
            "type": "agent.hello",
            "payload": {
                "agent_id": "agent-a",
                "edge_cluster_id": "cluster-a",
                "compute_node_id": "node-a",
                "agent_version": "0.1.0",
                "supported_protocol_versions": [1],
                "capabilities": ["metadata_batch_v1"]
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
