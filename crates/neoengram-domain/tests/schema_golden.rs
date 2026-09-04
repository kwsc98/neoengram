use std::{fs, path::Path};

use neoengram_domain::protocol::{
    action_schema, control_schema, enrollment_schema, gateway_schema, is_registered_action,
    materialization_schema, metadata_schema, operation_task_schema, snapshot_delivery_schema,
    DecimalU64, Generation, ProtocolVersion, WireObjectSpec,
};
use serde_json::{json, Value};

#[test]
fn committed_current_schemas_match_the_generator() {
    assert_schema(
        include_str!("../schemas/current/control-envelope.schema.json"),
        control_schema(),
    );
    assert_schema(
        include_str!("../schemas/current/action-envelope.schema.json"),
        action_schema(),
    );
    assert_schema(
        include_str!("../schemas/current/agent-enrollment.schema.json"),
        enrollment_schema(),
    );
    assert_schema(
        include_str!("../schemas/current/metadata-batch.schema.json"),
        metadata_schema(),
    );
    assert_schema(
        include_str!("../schemas/current/gateway-control.schema.json"),
        gateway_schema(),
    );
    assert_schema(
        include_str!("../schemas/current/snapshot-delivery.schema.json"),
        snapshot_delivery_schema(),
    );
    assert_schema(
        include_str!("../schemas/current/materialization-v2.schema.json"),
        materialization_schema(),
    );
    assert_schema(
        include_str!("../schemas/current/operation-task.schema.json"),
        operation_task_schema(),
    );
}

#[test]
fn committed_current_schema_catalog_publishes_only_supported_protocols() {
    let schema_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("schemas/current");
    let mut published = fs::read_dir(schema_dir)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().into_string().unwrap())
        .collect::<Vec<_>>();
    published.sort();

    assert_eq!(
        published,
        [
            "action-envelope.schema.json",
            "agent-enrollment.schema.json",
            "control-envelope.schema.json",
            "gateway-control.schema.json",
            "materialization-v2.schema.json",
            "metadata-batch.schema.json",
            "operation-task.schema.json",
            "snapshot-delivery.schema.json",
        ]
    );
}

#[test]
fn wire_object_spec_rejects_unknown_fields_in_serde_and_schema() {
    let value = json!({
        "object_id": "22".repeat(32),
        "size": "12"
    });
    let object: WireObjectSpec = serde_json::from_value(value.clone()).unwrap();
    object.validate().unwrap();
    assert_eq!(serde_json::to_value(object).unwrap(), value);

    let unknown = json!({
        "object_id": "22".repeat(32),
        "size": "12",
        "future_object_attribute": {"placement": "volume-local"}
    });
    let error = serde_json::from_value::<WireObjectSpec>(unknown).unwrap_err();
    assert!(error.to_string().contains("future_object_attribute"));

    let schema = serde_json::to_value(schemars::schema_for!(WireObjectSpec)).unwrap();
    assert_eq!(
        schema.pointer("/properties/object_id/pattern"),
        Some(&json!("^[0-9a-f]{64}$"))
    );
    assert_eq!(
        schema.pointer("/properties/size/$ref"),
        Some(&json!("#/$defs/DecimalU64"))
    );
    assert_eq!(schema.pointer("/additionalProperties"), Some(&json!(false)));
}

#[test]
fn schemas_publish_the_runtime_wire_limits() {
    let control = serde_json::to_value(control_schema()).unwrap();
    assert_eq!(
        control.pointer("/$defs/AgentHeartbeat/properties/running_jobs/maxItems"),
        Some(&json!(4096))
    );
    assert_eq!(
        control.pointer("/$defs/AgentHello/properties/agent_version/maxLength"),
        Some(&json!(128))
    );
    assert_eq!(
        control.pointer("/$defs/AddAssignment/properties/request_digest/pattern"),
        Some(&json!("^[0-9a-f]{64}$"))
    );
    assert_eq!(
        control.pointer("/$defs/WorkspaceMaterializeAssignment/properties/request_digest/pattern"),
        Some(&json!("^[0-9a-f]{64}$"))
    );
    assert_eq!(
        control.pointer("/$defs/JobPrepared/properties/candidate_digest/pattern"),
        Some(&json!("^[0-9a-f]{64}$"))
    );

    let enrollment = serde_json::to_value(enrollment_schema()).unwrap();
    assert_eq!(
        enrollment
            .pointer("/$defs/AgentBootstrapRequest/properties/public_key_fingerprint/pattern"),
        Some(&json!("^[0-9a-f]{64}$"))
    );
    assert_eq!(
        enrollment.pointer("/$defs/AgentBootstrapRequest/properties/capabilities/uniqueItems"),
        Some(&json!(true))
    );
    assert_eq!(
        enrollment.pointer("/$defs/Ed25519PublicKeySpki/minLength"),
        Some(&json!(59))
    );
    assert_eq!(
        enrollment.pointer("/$defs/Ed25519PublicKeySpki/maxLength"),
        Some(&json!(59))
    );
    assert_eq!(
        enrollment.pointer("/$defs/Ed25519Signature/minLength"),
        Some(&json!(86))
    );
    assert_eq!(
        enrollment.pointer("/$defs/AgentSignatureAlgorithm/enum/0"),
        Some(&json!("ed25519"))
    );
    assert_eq!(
        enrollment.pointer("/$defs/AgentBootstrapStatusRequest/properties/wire_version/const"),
        Some(&json!(1))
    );
    assert_eq!(
        enrollment.pointer("/$defs/AgentBootstrapStatusResponse/properties/wire_version/const"),
        Some(&json!(1))
    );
    assert_eq!(
        enrollment.pointer("/$defs/AgentMountIdentityDigest/pattern"),
        Some(&json!("^[0-9a-f]{64}$"))
    );
    assert_eq!(
        enrollment.pointer("/$defs/PvcIdentityDigest/pattern"),
        Some(&json!("^[0-9a-f]{64}$"))
    );
    assert_eq!(
        enrollment.pointer("/$defs/AgentBootstrapProbe/properties/mount_identity_digest/$ref"),
        Some(&json!("#/$defs/AgentMountIdentityDigest"))
    );
    assert_eq!(
        enrollment.pointer("/$defs/AgentMountStatusReport/properties/mount_identity_digest/$ref"),
        Some(&json!("#/$defs/AgentMountIdentityDigest"))
    );
    assert_eq!(
        enrollment.pointer(
            "/$defs/AgentEnrollmentTokenCreateRequest/properties/pvc_identity_digest/$ref"
        ),
        Some(&json!("#/$defs/PvcIdentityDigest"))
    );
    assert_eq!(
        enrollment.pointer(
            "/$defs/AgentEnrollmentTokenCreateRequest/properties/bootstrap_token/minLength"
        ),
        Some(&json!(32))
    );

    let metadata = serde_json::to_value(metadata_schema()).unwrap();
    assert_eq!(
        metadata.pointer("/anyOf/0/$ref"),
        Some(&json!("#/$defs/MetadataBatchDescriptor"))
    );
    assert_eq!(
        metadata.pointer("/anyOf/1/$ref"),
        Some(&json!("#/$defs/MetadataBatchPage"))
    );
    assert_eq!(
        metadata.pointer("/$defs/MetadataBatchPage/oneOf/0/properties/records/maxItems"),
        Some(&json!(4096))
    );
    assert_eq!(
        metadata.pointer("/$defs/ManifestRecord/properties/chunks/maxItems"),
        None
    );
    assert_eq!(
        metadata.pointer("/$defs/ManifestRecord/properties/chunk_start/$ref"),
        Some(&json!("#/$defs/DecimalU64"))
    );
    assert_eq!(
        metadata.pointer("/$defs/DecimalU64/pattern"),
        Some(&json!(decimal_u64_pattern()))
    );
    assert_eq!(
        metadata.pointer("/$defs/MetadataBatchPage/properties/page_digest/pattern"),
        Some(&json!("^[0-9a-f]{64}$"))
    );

    let gateway = serde_json::to_value(gateway_schema()).unwrap();
    assert_eq!(
        gateway.pointer("/properties/wire_version/const"),
        Some(&json!(1))
    );
    assert_eq!(
        gateway.pointer("/properties/hop_count/maximum"),
        Some(&json!(1))
    );
    assert_eq!(
        gateway.pointer("/$defs/GatewayOpaqueBytes/maxLength"),
        Some(&json!(12_582_912))
    );
    assert_eq!(
        gateway.pointer("/$defs/GatewayOpaqueBytes/pattern"),
        Some(&json!("^[A-Za-z0-9_-]*$"))
    );
    assert_eq!(
        gateway.pointer("/$defs/GatewayAgentStreamData/properties/chunk/maxLength"),
        Some(&json!(349_526))
    );
    assert_eq!(
        gateway.pointer("/$defs/GatewayReplicaHello/properties/wire_version/const"),
        Some(&json!(1))
    );
    assert_eq!(
        gateway.pointer("/$defs/GatewayReplicaHello/properties/capabilities/maxItems"),
        Some(&json!(128))
    );
    assert_eq!(
        gateway.pointer("/$defs/GatewayAgentResponse/properties/status/minimum"),
        Some(&json!(100))
    );
    assert_eq!(
        gateway.pointer("/$defs/GatewayAgentResponse/properties/status/maximum"),
        Some(&json!(599))
    );
    assert_eq!(
        gateway.pointer("/$defs/RouteGeneration/pattern"),
        Some(&json!(positive_decimal_u64_pattern()))
    );
    assert_eq!(
        gateway.pointer("/$defs/GatewayPeerForwardRequest/properties/frame/maxLength"),
        Some(&json!(1_398_103))
    );
    assert_eq!(
        gateway
            .pointer("/$defs/GatewayPeerForwardRequest/properties/target_peer_endpoint/maxLength"),
        Some(&json!(2_048))
    );
    assert_eq!(
        gateway.pointer("/$defs/GatewayPeerDirectory/properties/replicas/maxItems"),
        Some(&json!(256))
    );
    assert_eq!(
        gateway
            .pointer("/$defs/GatewayPeerDirectoryEntry/properties/certificate_fingerprint/pattern"),
        Some(&json!("^[0-9a-f]{64}$"))
    );
    for payload in [
        "GatewayReplicaHello",
        "GatewayReplicaHeartbeat",
        "GatewayDrain",
        "GatewayAgentRequest",
        "GatewayAgentResponse",
        "GatewayAgentStreamOpen",
        "GatewayAgentStreamData",
        "GatewayAgentStreamEnd",
        "GatewayRouteLeaseRequest",
        "GatewayRouteLeaseGranted",
        "GatewayRouteFence",
        "GatewayPeerForwardRequest",
        "GatewayPeerForwardAccepted",
        "GatewayPeerDirectoryEntry",
        "GatewayPeerDirectory",
        "GatewayControlError",
        "GatewayBackpressure",
    ] {
        assert_eq!(
            gateway.pointer(&format!("/$defs/{payload}/additionalProperties")),
            Some(&json!(false)),
            "{payload} must reject unknown message payload fields"
        );
    }
}

#[test]
fn decimal_u64_schema_and_runtime_share_the_exact_range() {
    let control = serde_json::to_value(control_schema()).unwrap();
    assert_eq!(
        control.pointer("/$defs/AssignmentGeneration/pattern"),
        Some(&json!(positive_decimal_u64_pattern()))
    );

    let metadata = serde_json::to_value(metadata_schema()).unwrap();
    assert_eq!(
        metadata.pointer("/$defs/DecimalU64/pattern"),
        Some(&json!(decimal_u64_pattern()))
    );
    assert_eq!(
        metadata.pointer("/$defs/UnixMillis/pattern"),
        Some(&json!(decimal_u64_pattern()))
    );

    for value in ["0", "1", "9999999999999999999", "18446744073709551615"] {
        assert!(serde_json::from_value::<DecimalU64>(json!(value)).is_ok());
    }
    for value in ["00", "01", "18446744073709551616", "99999999999999999999"] {
        assert!(serde_json::from_value::<DecimalU64>(json!(value)).is_err());
    }
    assert!(serde_json::from_value::<Generation>(json!("1")).is_ok());
    assert!(serde_json::from_value::<Generation>(json!("0")).is_err());
    assert!(serde_json::from_value::<Generation>(json!("18446744073709551615")).is_ok());
    assert!(serde_json::from_value::<Generation>(json!("18446744073709551616")).is_err());
}

#[test]
fn current_wire_version_and_action_schemas_are_strict() {
    let control = serde_json::to_value(control_schema()).unwrap();
    assert_eq!(
        control.pointer("/properties/wire_version/const"),
        Some(&json!(1))
    );
    assert_eq!(
        control.pointer("/$defs/ProtocolVersion/const"),
        Some(&json!(1))
    );
    assert_eq!(control.pointer("/$defs/ProtocolVersion/minimum"), None);
    assert_eq!(control.pointer("/$defs/ProtocolVersion/maximum"), None);
    assert_eq!(ProtocolVersion::new(2).get(), 2);
    assert_eq!(
        serde_json::from_value::<ProtocolVersion>(json!(2))
            .unwrap()
            .get(),
        2
    );

    let action = serde_json::to_value(action_schema()).unwrap();
    let actions = action
        .pointer("/properties/action/enum")
        .and_then(Value::as_array)
        .expect("action schema must publish the current registry");
    for expected in [
        "artifact.create",
        "agent.session.open",
        "agent.job.assignment",
        "health.ready",
    ] {
        assert!(is_registered_action(expected));
        assert!(actions.contains(&json!(expected)));
    }
    assert!(!is_registered_action("artifact.future"));
    assert!(!actions.contains(&json!("artifact.future")));
    assert!(actions
        .iter()
        .all(|value| is_registered_action(value.as_str().unwrap())));

    let metadata = serde_json::to_value(metadata_schema()).unwrap();
    assert_eq!(
        metadata.pointer("/$defs/MetadataBatchDescriptor/properties/schema_version/const"),
        Some(&json!(1))
    );
    assert_eq!(
        metadata.pointer("/$defs/MetadataBatchPage/properties/schema_version/const"),
        Some(&json!(1))
    );
}

fn decimal_u64_pattern() -> &'static str {
    r"^(0|[1-9][0-9]{0,18}|1[0-7][0-9]{18}|18[0-3][0-9]{17}|184[0-3][0-9]{16}|1844[0-5][0-9]{15}|18446[0-6][0-9]{14}|184467[0-3][0-9]{13}|1844674[0-3][0-9]{12}|184467440[0-6][0-9]{10}|1844674407[0-2][0-9]{9}|18446744073[0-6][0-9]{8}|1844674407370[0-8][0-9]{6}|18446744073709[0-4][0-9]{5}|184467440737095[0-4][0-9]{4}|18446744073709550[0-9]{3}|18446744073709551[0-5][0-9]{2}|1844674407370955160[0-9]|1844674407370955161[0-5])$"
}

fn positive_decimal_u64_pattern() -> &'static str {
    r"^([1-9][0-9]{0,18}|1[0-7][0-9]{18}|18[0-3][0-9]{17}|184[0-3][0-9]{16}|1844[0-5][0-9]{15}|18446[0-6][0-9]{14}|184467[0-3][0-9]{13}|1844674[0-3][0-9]{12}|184467440[0-6][0-9]{10}|1844674407[0-2][0-9]{9}|18446744073[0-6][0-9]{8}|1844674407370[0-8][0-9]{6}|18446744073709[0-4][0-9]{5}|184467440737095[0-4][0-9]{4}|18446744073709550[0-9]{3}|18446744073709551[0-5][0-9]{2}|1844674407370955160[0-9]|1844674407370955161[0-5])$"
}

fn assert_schema(committed: &str, generated: schemars::Schema) {
    let committed: Value = serde_json::from_str(committed).unwrap();
    let generated = serde_json::to_value(generated).unwrap();
    assert_eq!(committed, generated);
}
