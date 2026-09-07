//! Versioned, transport-independent NeoEngram wire contracts.
//!
//! This crate contains only serializable protocol types, validation, canonical JSON digest and
//! signature helpers, and JSON Schema generation. It intentionally has no dependency on an HTTP
//! runtime, a database, a filesystem, the CLI, or the execution engine.

mod action_registry;
mod agent_api;
mod control;
mod delivery;
mod digest;
mod enrollment;
mod envelope;
mod error;
mod gateway;
mod ids;
mod lifecycle;
/// Clean-slate v2 object placement and multi-source materialization contracts.
pub mod materialization;
mod metadata;
mod placement;
mod s3;
mod scalars;
pub(crate) mod schema;
/// Unified write-operation task lifecycle and append-only audit contracts.
pub mod task;
mod transfer;
pub(crate) mod validation;
mod validation_profile;

pub use crate::core::ContentDigest;
pub use action_registry::*;
pub use agent_api::*;
pub use control::*;
pub use delivery::*;
pub use digest::{domain_separated_jcs_bytes, jcs_blake3, jcs_bytes};
pub use enrollment::*;
pub use envelope::{Envelope, EnvelopeHeader};
pub use error::{ProtocolError, ProtocolResult};
pub use gateway::*;
pub use ids::*;
pub use lifecycle::*;
pub use metadata::*;
pub use task::*;
// Keep v2 names namespaced under `protocol::materialization` so legacy v1 callers cannot
// accidentally mix the two placement models during the destructive protocol migration.
pub use materialization::CommitObjectSet as CommitObjectSetV2;
pub use materialization::{
    materialization_target_placement_id, object_read_lease_id, staging_lease_id,
    AvailabilityStatus, BatchManifest, BatchManifestPage, CommitAvailability, CoverageGoal,
    CoverageState, DurabilityPolicy, IntegrityScanReport, MaterializationAssignment,
    MaterializationBatch, MaterializationBatchState, MaterializationBatchTicket,
    MaterializationJob, MaterializationJobKey, MaterializationJobState, MaterializationLease,
    MaterializationLeaseState, MaterializationManifestSource, MaterializationObject,
    MaterializationObjectReceipt, MaterializationObjectState, MaterializationProtocolSchema,
    MaterializationReport, MaterializationSource, MaterializationTarget, MaterializationTicket,
    NamespaceObjectSet, ObjectPlacement as MaterializationObjectPlacement, ObjectPlacementState,
    ObjectReadLease, ObjectReceipt, ObjectRef, PlacementHealthObservation, PlacementHealthState,
    SignedMaterializationBatchTicket, SignedMaterializationTicket, StagingLease, ViewReadiness,
    VolumeCommitCoverage, VolumeIntegrityScan, VolumeIntegrityScanState,
    COMMIT_MATERIALIZATION_CAPABILITY_V2, MATERIALIZATION_PROTOCOL_VERSION,
    MATERIALIZATION_TRANSFER_ALPN_V2, MAX_DURABILITY_REGIONS, MAX_MATERIALIZATION_BATCH_OBJECTS,
    MAX_MATERIALIZATION_ERROR_BYTES, MAX_MATERIALIZATION_MANIFEST_OBJECTS,
    MAX_MATERIALIZATION_SOURCES, MAX_STAGING_KEY_BYTES,
};
pub use placement::*;
pub use s3::*;
pub use scalars::*;
pub use schema::{
    action_schema, agent_api_schema, control_schema, enrollment_schema, gateway_schema,
    materialization_schema, metadata_schema, operation_task_schema, snapshot_delivery_schema,
    task_schema,
};
pub use transfer::*;
pub use validation::decode_bounded_unique_json;
pub use validation_profile::TransportValidationProfile;

/// The only wire version emitted by the current protocol.
pub const CURRENT_WIRE_VERSION: ProtocolVersion = ProtocolVersion::new(1);

/// Maximum encoded size of an in-band control message.
pub const MAX_CONTROL_MESSAGE_BYTES: usize = 1024 * 1024;

/// Maximum encoded size of one enrollment or bootstrap message.
pub const MAX_AGENT_ENROLLMENT_MESSAGE_BYTES: usize = 1024 * 1024;

/// Maximum encoded size of one out-of-band metadata page.
pub const MAX_METADATA_PAGE_BYTES: usize = 8 * 1024 * 1024;

/// Maximum number of records carried by one metadata page or negotiation request.
pub const MAX_RECORDS_PER_PAGE: usize = 4096;

/// Closed extension object retained for the in-memory model.
///
/// The clean-slate wire contract has no extension/fallback members.  Protocol structs still keep
/// this zero-sized-in-practice field so canonical signing inputs and constructors do not need a
/// second representation, but deserializing or serializing a non-empty map is rejected.  This is
/// important for structs that use `#[serde(flatten)]`: without a rejecting deserializer, Serde
/// would silently capture unknown wire members here and accept an invalid protocol message.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Extensions(std::collections::BTreeMap<String, serde_json::Value>);

impl<'de> serde::Deserialize<'de> for Extensions {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let values = <std::collections::BTreeMap<String, serde_json::Value> as serde::Deserialize>::deserialize(deserializer)?;
        if let Some(key) = values.keys().next() {
            return Err(serde::de::Error::custom(format!(
                "unknown protocol field {key:?}"
            )));
        }
        Ok(Self::new())
    }
}

impl serde::Serialize for Extensions {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        if let Some(key) = self.0.keys().next() {
            return Err(serde::ser::Error::custom(format!(
                "protocol extension field {key:?} is not supported"
            )));
        }
        use serde::ser::SerializeMap;
        serializer.serialize_map(Some(0))?.end()
    }
}

impl schemars::JsonSchema for Extensions {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "Extensions".into()
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        concat!(module_path!(), "::Extensions::closed").into()
    }

    fn json_schema(_generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
        schemars::json_schema!({
            "description": "No extension members are accepted by the current protocol.",
            "type": "object",
            "additionalProperties": false
        })
    }
}

impl Extensions {
    #[must_use]
    pub const fn new() -> Self {
        Self(std::collections::BTreeMap::new())
    }
}

impl std::ops::Deref for Extensions {
    type Target = std::collections::BTreeMap<String, serde_json::Value>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for Extensions {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<const N: usize> From<[(String, serde_json::Value); N]> for Extensions {
    fn from(entries: [(String, serde_json::Value); N]) -> Self {
        Self(entries.into_iter().collect())
    }
}
