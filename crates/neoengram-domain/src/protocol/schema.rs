use schemars::{schema_for, Schema};
use serde_json::Value;

use crate::{
    AgentApiSchema, AgentEnrollmentEnvelope, ControlMessage, Envelope, GatewayControlFrame,
    MetadataProtocolSchema, SnapshotDeliveryAssignment,
};

/// Restricts a field in a version-specific schema without narrowing the reusable wire scalar.
pub(crate) fn require_current_wire_version(schema: &mut Schema) {
    schema.insert("const".to_owned(), 1_u64.into());
}

/// Restricts action envelopes to the exact identities produced by the current route registry.
pub(crate) fn require_registered_action(schema: &mut Schema) {
    let actions = super::action_registry::registered_actions()
        .into_iter()
        .map(serde_json::Value::String)
        .collect();
    schema.insert("enum".to_owned(), serde_json::Value::Array(actions));
}

/// Schemars must allow unknown members while merging a `flatten` field.  That default is useful
/// for open-ended data models, but it would make the current wire schemas advertise the very
/// fallback fields that the protocol rejects.  Close only object schemas that have declared
/// properties; arbitrary JSON values used as action bodies remain unconstrained and are validated
/// by their action implementation.
fn close_flattened_objects(mut schema: Schema) -> Schema {
    fn visit(value: &mut Value) {
        match value {
            Value::Array(items) => items.iter_mut().for_each(visit),
            Value::Object(object) => {
                if object.get("additionalProperties") == Some(&Value::Bool(true))
                    && object.contains_key("properties")
                {
                    object.insert("additionalProperties".to_owned(), Value::Bool(false));
                }
                object.values_mut().for_each(visit);
            }
            _ => {}
        }
    }

    let mut value = serde_json::to_value(&schema).expect("Schema must be JSON serializable");
    visit(&mut value);
    schema = serde_json::from_value(value).expect("Schema JSON must round-trip");
    schema
}

/// Generates the complete current control action-envelope JSON Schema.
#[must_use]
pub fn control_schema() -> Schema {
    close_flattened_objects(schema_for!(Envelope<ControlMessage>))
}

/// Generates the strict current action envelope schema used by Central, Gateway, and Agent.
#[must_use]
pub fn action_schema() -> Schema {
    close_flattened_objects(schema_for!(Envelope))
}

/// Generates the complete current action-style Agent HTTP API JSON Schema.
#[must_use]
pub fn agent_api_schema() -> Schema {
    close_flattened_objects(schema_for!(AgentApiSchema))
}

/// Generates the complete current Agent enrollment/bootstrap JSON Schema.
#[must_use]
pub fn enrollment_schema() -> Schema {
    close_flattened_objects(schema_for!(AgentEnrollmentEnvelope))
}

/// Generates the complete current metadata-batch JSON Schema.
#[must_use]
pub fn metadata_schema() -> Schema {
    close_flattened_objects(schema_for!(MetadataProtocolSchema))
}

/// Generates the complete current Gateway control-frame JSON Schema.
#[must_use]
pub fn gateway_schema() -> Schema {
    close_flattened_objects(schema_for!(GatewayControlFrame))
}

/// Generates the current SnapshotDelivery assignment JSON Schema.
#[must_use]
pub fn snapshot_delivery_schema() -> Schema {
    close_flattened_objects(schema_for!(SnapshotDeliveryAssignment))
}

/// Generates the strict clean-slate v2 object materialization schema.
#[must_use]
pub fn materialization_schema() -> Schema {
    close_flattened_objects(schema_for!(
        crate::protocol::materialization::MaterializationProtocolSchema
    ))
}

/// Generates the strict unified operation-task and audit schema.
#[must_use]
pub fn operation_task_schema() -> Schema {
    close_flattened_objects(schema_for!(crate::protocol::task::TaskProtocolSchema))
}

/// Short alias used by callers that refer to the contract as the task schema.
#[must_use]
pub fn task_schema() -> Schema {
    operation_task_schema()
}
