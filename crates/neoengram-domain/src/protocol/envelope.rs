//! Strict action-style control envelopes.
//!
//! This module provides the clean-slate action envelope used by Central, Gateway, and Agent
//! request paths. Agent reports use the same envelope and carry their concrete report variant in
//! the typed body.

use schemars::JsonSchema;
use serde::{de::DeserializeOwned, Deserialize, Deserializer, Serialize};
use serde_json::Value;

use super::validation::{validate_nonempty_limited, validate_positive};
use crate::{
    is_registered_action, PrincipalRef, ProtocolError, ProtocolResult, ProtocolVersion, RequestId,
    RouteGeneration, SessionGeneration, TenantId, TraceId, UnixMillis, CURRENT_WIRE_VERSION,
    MAX_CONTROL_MESSAGE_BYTES,
};

/// Metadata shared by every clean-slate action envelope.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct EnvelopeHeader {
    /// Current wire contract version. Unsupported versions are rejected by [`Envelope::validate`].
    #[schemars(transform = super::schema::require_current_wire_version)]
    pub wire_version: ProtocolVersion,
    /// Stable action identifier, for example `artifact.create` or `agent.session.open`.
    #[schemars(transform = super::schema::require_registered_action)]
    pub action: String,
    pub request_id: RequestId,
    pub trace_id: TraceId,
    #[serde(deserialize_with = "deserialize_required_nullable")]
    #[schemars(required)]
    /// Tenant scope is always present on the wire; `null` is used for actions that are not
    /// tenant-scoped.
    pub tenant_scope: Option<TenantId>,
    #[serde(deserialize_with = "deserialize_required_nullable")]
    #[schemars(required)]
    /// Actor is always present on the wire; `null` is used before an authenticated adapter binds
    /// an external principal.
    pub actor: Option<PrincipalRef>,
    #[serde(deserialize_with = "deserialize_required_nullable")]
    #[schemars(required)]
    /// Session generation is always present on the wire; `null` is used for pre-session actions.
    pub session_generation: Option<SessionGeneration>,
    #[serde(deserialize_with = "deserialize_required_nullable")]
    #[schemars(required)]
    /// Route generation is always present on the wire; `null` is used for actions without a
    /// Gateway route fence.
    pub route_generation: Option<RouteGeneration>,
    /// Absolute Unix timestamp in milliseconds by which the action must complete.
    pub deadline: UnixMillis,
}

fn deserialize_required_nullable<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer)
}

impl EnvelopeHeader {
    /// Validates the common metadata independently of an action body.
    pub fn validate(&self) -> ProtocolResult<()> {
        if self.wire_version != CURRENT_WIRE_VERSION {
            return Err(ProtocolError::UnsupportedProtocolVersion(
                self.wire_version.get(),
            ));
        }
        validate_nonempty_limited("action", &self.action, 128)?;
        if !is_registered_action(&self.action) {
            return Err(ProtocolError::UnsupportedMessageType(self.action.clone()));
        }
        validate_positive("deadline", self.deadline.get())?;
        if let Some(generation) = self.session_generation {
            validate_positive("session_generation", generation.get())?;
        }
        if let Some(generation) = self.route_generation {
            validate_positive("route_generation", generation.get())?;
        }
        if let Some(actor) = &self.actor {
            actor.validate()?;
        }
        Ok(())
    }

    /// Validates the header and rejects work whose absolute deadline has already elapsed.
    pub fn validate_at(&self, now: UnixMillis) -> ProtocolResult<()> {
        self.validate()?;
        if now.get() >= self.deadline.get() {
            return Err(ProtocolError::InvalidField {
                field: "deadline",
                reason: "action deadline has elapsed".to_owned(),
            });
        }
        Ok(())
    }
}

/// Strict clean-slate action envelope.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Envelope<T = Value> {
    #[serde(flatten)]
    pub header: EnvelopeHeader,
    pub body: T,
}

impl<T> Envelope<T> {
    /// Validates the header. Body-specific validation belongs to the action implementation.
    pub fn validate(&self) -> ProtocolResult<()> {
        self.header.validate()
    }

    /// Validates the envelope at an explicit clock value for deterministic deadline fencing.
    pub fn validate_at(&self, now: UnixMillis) -> ProtocolResult<()> {
        self.header.validate_at(now)
    }
}

impl Envelope<Value> {
    /// Decodes one bounded JSON envelope, rejecting duplicate and unknown object members.
    pub fn decode_json(bytes: &[u8]) -> ProtocolResult<Self> {
        let envelope: Self = crate::decode_bounded_unique_json(bytes, MAX_CONTROL_MESSAGE_BYTES)?;
        envelope.validate()?;
        Ok(envelope)
    }

    /// Decodes one bounded envelope into a caller-provided strongly typed body.
    pub fn decode_json_as<T: DeserializeOwned>(bytes: &[u8]) -> ProtocolResult<Envelope<T>> {
        let envelope: Envelope<T> =
            crate::decode_bounded_unique_json(bytes, MAX_CONTROL_MESSAGE_BYTES)?;
        envelope.validate()?;
        Ok(envelope)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn envelope() -> Envelope<Value> {
        Envelope {
            header: EnvelopeHeader {
                wire_version: CURRENT_WIRE_VERSION,
                action: "artifact.create".to_owned(),
                request_id: RequestId::new("request-1").unwrap(),
                trace_id: TraceId::new("trace-1").unwrap(),
                tenant_scope: Some(TenantId::new("tenant-a").unwrap()),
                actor: None,
                session_generation: Some(SessionGeneration::new(3)),
                route_generation: Some(RouteGeneration::new(4)),
                deadline: UnixMillis::new(1_800_000_000_000),
            },
            body: json!({"artifact_id": "artifact-a", "dry_run": false}),
        }
    }

    #[test]
    fn strict_envelope_round_trips() {
        let original = envelope();
        original.validate().unwrap();
        let encoded = serde_json::to_vec(&original).unwrap();
        let decoded = Envelope::decode_json(&encoded).unwrap();
        assert_eq!(decoded, original);
    }

    #[test]
    fn strict_envelope_rejects_unknown_top_level_fields() {
        let mut value = serde_json::to_value(envelope()).unwrap();
        value
            .as_object_mut()
            .unwrap()
            .insert("future_field".to_owned(), json!(true));
        let error = Envelope::decode_json(&serde_json::to_vec(&value).unwrap()).unwrap_err();
        assert!(matches!(error, ProtocolError::Serialization(_)));
    }

    #[test]
    fn strict_envelope_rejects_missing_context_fields() {
        for field in [
            "tenant_scope",
            "actor",
            "session_generation",
            "route_generation",
        ] {
            let mut value = serde_json::to_value(envelope()).unwrap();
            value.as_object_mut().unwrap().remove(field);
            let error = Envelope::decode_json(&serde_json::to_vec(&value).unwrap()).unwrap_err();
            assert!(matches!(error, ProtocolError::Serialization(_)), "{field}");
        }
    }

    #[test]
    fn strict_envelope_rejects_unknown_wire_version() {
        let mut value = serde_json::to_value(envelope()).unwrap();
        value
            .as_object_mut()
            .unwrap()
            .insert("wire_version".to_owned(), json!(99));
        let error = Envelope::decode_json(&serde_json::to_vec(&value).unwrap()).unwrap_err();
        assert_eq!(error, ProtocolError::UnsupportedProtocolVersion(99));
    }

    #[test]
    fn strict_envelope_rejects_invalid_generation_and_deadline() {
        let mut value = envelope();
        value.header.session_generation = Some(SessionGeneration::new(0));
        assert!(matches!(
            value.validate(),
            Err(ProtocolError::InvalidField {
                field: "session_generation",
                ..
            })
        ));

        value.header.session_generation = None;
        value.header.deadline = UnixMillis::new(0);
        assert!(matches!(
            value.validate(),
            Err(ProtocolError::InvalidField {
                field: "deadline",
                ..
            })
        ));
    }

    #[test]
    fn strict_envelope_fences_elapsed_deadlines() {
        let value = envelope();
        value
            .validate_at(UnixMillis::new(1_799_999_999_999))
            .unwrap();
        let error = value
            .validate_at(UnixMillis::new(1_800_000_000_000))
            .unwrap_err();
        assert!(matches!(
            error,
            ProtocolError::InvalidField {
                field: "deadline",
                ..
            }
        ));
    }
}
