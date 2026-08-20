use neoengram_domain::protocol::{
    Envelope, ProtocolError, RequestId, RouteGeneration, SessionGeneration, TenantId, TraceId,
    UnixMillis, CURRENT_WIRE_VERSION,
};
use serde_json::json;

#[test]
fn action_envelope_wire_shape_is_stable() {
    let envelope = Envelope {
        header: neoengram_domain::protocol::EnvelopeHeader {
            wire_version: CURRENT_WIRE_VERSION,
            action: "agent.session.open".to_owned(),
            request_id: RequestId::new("request-open").unwrap(),
            trace_id: TraceId::new("trace-open").unwrap(),
            tenant_scope: Some(TenantId::new("tenant-a").unwrap()),
            actor: None,
            session_generation: Some(SessionGeneration::new(1)),
            route_generation: Some(RouteGeneration::new(2)),
            deadline: UnixMillis::new(1_800_000_000_000),
        },
        body: json!({"mount_identity_digest": "abc"}),
    };

    assert_eq!(
        serde_json::to_value(&envelope).unwrap(),
        json!({
            "wire_version": 1,
            "action": "agent.session.open",
            "request_id": "request-open",
            "trace_id": "trace-open",
            "tenant_scope": "tenant-a",
            "actor": null,
            "session_generation": "1",
            "route_generation": "2",
            "deadline": "1800000000000",
            "body": {"mount_identity_digest": "abc"}
        })
    );
}

#[test]
fn action_envelope_decode_rejects_duplicate_and_unknown_members() {
    let duplicate = br#"{"wire_version":1,"wire_version":1,"action":"x","request_id":"r","trace_id":"t","tenant_scope":null,"actor":null,"session_generation":null,"route_generation":null,"deadline":"1","body":{}}"#;
    assert!(Envelope::decode_json(duplicate).is_err());

    let unknown = br#"{"wire_version":1,"action":"x","request_id":"r","trace_id":"t","tenant_scope":null,"actor":null,"session_generation":null,"route_generation":null,"deadline":"1","body":{},"legacy":true}"#;
    assert!(matches!(
        Envelope::decode_json(unknown),
        Err(ProtocolError::Serialization(_))
    ));
}

#[test]
fn action_envelope_decode_rejects_non_current_version() {
    let body = br#"{"wire_version":2,"action":"x","request_id":"r","trace_id":"t","tenant_scope":null,"actor":null,"session_generation":null,"route_generation":null,"deadline":"1","body":{}}"#;
    assert_eq!(
        Envelope::decode_json(body).unwrap_err(),
        ProtocolError::UnsupportedProtocolVersion(2)
    );
}

#[test]
fn action_envelope_decode_rejects_unknown_action() {
    let body = br#"{"wire_version":1,"action":"artifact.future","request_id":"r","trace_id":"t","tenant_scope":null,"actor":null,"session_generation":null,"route_generation":null,"deadline":"1","body":{}}"#;
    let error = Envelope::decode_json(body).unwrap_err();
    assert_eq!(
        error,
        ProtocolError::UnsupportedMessageType("artifact.future".to_owned())
    );
    assert_eq!(error.stable_code(), "PROTOCOL_UNSUPPORTED");
}

#[test]
fn action_envelope_round_trip_preserves_json_body() {
    let value = Envelope {
        header: neoengram_domain::protocol::EnvelopeHeader {
            wire_version: CURRENT_WIRE_VERSION,
            action: "artifact.query".to_owned(),
            request_id: RequestId::new("request-query").unwrap(),
            trace_id: TraceId::new("trace-query").unwrap(),
            tenant_scope: None,
            actor: None,
            session_generation: None,
            route_generation: None,
            deadline: UnixMillis::new(1),
        },
        body: json!({"nested": [1, true, null], "key": "value"}),
    };
    let bytes = serde_json::to_vec(&value).unwrap();
    assert_eq!(Envelope::decode_json(&bytes).unwrap(), value);
}
