use schemars::{schema_for, Schema};

use crate::{AgentApiSchema, AgentEnrollmentEnvelope, ControlEnvelope, MetadataProtocolSchema};

/// Restricts a field in a version-specific schema without narrowing the reusable wire scalar.
pub(crate) fn require_protocol_v1(schema: &mut Schema) {
    schema.insert("const".to_owned(), 1_u64.into());
}

/// Generates the complete v1 control-envelope JSON Schema.
#[must_use]
pub fn control_schema() -> Schema {
    schema_for!(ControlEnvelope)
}

/// Generates the complete v1 action-style Agent HTTP API JSON Schema.
#[must_use]
pub fn agent_api_schema() -> Schema {
    schema_for!(AgentApiSchema)
}

/// Generates the complete v1 Agent enrollment/bootstrap JSON Schema.
#[must_use]
pub fn enrollment_schema() -> Schema {
    schema_for!(AgentEnrollmentEnvelope)
}

/// Generates the complete v1 metadata-batch JSON Schema.
#[must_use]
pub fn metadata_schema() -> Schema {
    schema_for!(MetadataProtocolSchema)
}
