use neoengram_protocol::{
    control_schema, enrollment_schema, metadata_schema, s3_schema, DecimalU64, Generation,
    ProtocolVersion,
};
use serde_json::{json, Value};

#[test]
fn committed_v1_schemas_match_the_generator() {
    assert_schema(
        include_str!("../schemas/v1/control-envelope.schema.json"),
        control_schema(),
    );
    assert_schema(
        include_str!("../schemas/v1/agent-enrollment.schema.json"),
        enrollment_schema(),
    );
    assert_schema(
        include_str!("../schemas/v1/metadata-batch.schema.json"),
        metadata_schema(),
    );
    assert_schema(
        include_str!("../schemas/v1/s3-data-plane.schema.json"),
        s3_schema(),
    );
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

    let s3 = serde_json::to_value(s3_schema()).unwrap();
    assert_eq!(
        s3.pointer("/anyOf/0/$ref"),
        Some(&json!("#/$defs/MissingObjectsRequest"))
    );
    assert_eq!(
        s3.pointer("/anyOf/4/$ref"),
        Some(&json!("#/$defs/ObjectDurabilityReceipt"))
    );
    assert_eq!(
        s3.pointer("/$defs/MissingObjectsRequest/properties/objects/maxItems"),
        Some(&json!(4096))
    );
    assert_eq!(
        s3.pointer("/$defs/S3ObjectTicket/properties/required_headers/maxProperties"),
        Some(&json!(64))
    );
    assert_eq!(
        s3.pointer("/$defs/WireObjectSpec/properties/object_id/pattern"),
        Some(&json!("^[0-9a-f]{64}$"))
    );
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
fn v1_field_schemas_are_constant_without_narrowing_capability_versions() {
    let control = serde_json::to_value(control_schema()).unwrap();
    assert_eq!(
        control.pointer("/properties/protocol_version/const"),
        Some(&json!(1))
    );
    assert_eq!(control.pointer("/$defs/ProtocolVersion/const"), None);
    assert_eq!(
        control.pointer("/$defs/ProtocolVersion/maximum"),
        Some(&json!(u16::MAX))
    );
    assert_eq!(ProtocolVersion::new(2).get(), 2);

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
