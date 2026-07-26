use std::collections::{BTreeMap, BTreeSet};

use neoengram_core::{ContentDigest, ObjectId};
use schemars::{JsonSchema, Schema};
use serde::{Deserialize, Serialize};

use crate::validation::{
    parse_unique_json, validate_collection_limit, validate_extension_keys,
    validate_nonempty_limited, CONTENT_DIGEST_PATTERN,
};
use crate::{
    jcs_blake3, ArtifactId, DecimalU64, Extensions, JobId, ObjectTicketId, ProjectId,
    ProtocolError, ProtocolResult, SessionId, TenantId, UnixMillis, MAX_RECORDS_PER_PAGE,
};

pub const MAX_OBJECT_TICKET_TTL_MS: u64 = 15 * 60 * 1000;
const MAX_SIGNED_URL_LENGTH: usize = 16 * 1024;
const MAX_TICKET_HEADERS: usize = 64;

#[allow(dead_code)]
#[derive(JsonSchema)]
#[schemars(transparent)]
struct StorageVersionSchema(#[schemars(length(min = 1, max = 1024))] String);

fn limit_ticket_headers_schema(schema: &mut Schema) {
    schema.insert("maxProperties".to_owned(), 64_u64.into());
}

/// Object identity and byte length without a physical storage path.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct WireObjectSpec {
    #[schemars(
        with = "String",
        length(equal = 64),
        regex(pattern = CONTENT_DIGEST_PATTERN)
    )]
    pub object_id: ObjectId,
    pub size: DecimalU64,
    #[serde(default, flatten)]
    pub extensions: Extensions,
}

impl WireObjectSpec {
    fn validate(&self) -> ProtocolResult<()> {
        validate_extension_keys(&self.extensions, &["object_id", "size"])
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct MissingObjectsRequest {
    pub tenant_id: TenantId,
    pub project_id: ProjectId,
    pub artifact_id: ArtifactId,
    pub session_id: SessionId,
    pub job_id: JobId,
    #[schemars(length(max = 4096))]
    pub objects: Vec<WireObjectSpec>,
    #[schemars(
        with = "String",
        length(equal = 64),
        regex(pattern = CONTENT_DIGEST_PATTERN)
    )]
    pub request_digest: ContentDigest,
    #[serde(default, flatten)]
    pub extensions: Extensions,
}

impl MissingObjectsRequest {
    /// Returns the canonical digest of every request field except the digest itself.
    pub fn computed_request_digest(&self) -> ProtocolResult<ContentDigest> {
        jcs_blake3(&MissingObjectsRequestDigestInput {
            tenant_id: &self.tenant_id,
            project_id: &self.project_id,
            artifact_id: &self.artifact_id,
            session_id: &self.session_id,
            job_id: &self.job_id,
            objects: &self.objects,
            extensions: &self.extensions,
        })
    }

    pub fn validate(&self) -> ProtocolResult<()> {
        validate_object_count("missing negotiation objects", self.objects.len())?;
        validate_canonical_object_specs("missing negotiation objects", &self.objects)?;
        self.objects.iter().try_for_each(WireObjectSpec::validate)?;
        validate_extension_keys(
            &self.extensions,
            &[
                "tenant_id",
                "project_id",
                "artifact_id",
                "session_id",
                "job_id",
                "objects",
                "request_digest",
            ],
        )?;
        let computed = self.computed_request_digest()?;
        if self.request_digest != computed {
            return Err(ProtocolError::InvalidDigest(format!(
                "missing-object request digest mismatch: expected {}, observed {}",
                self.request_digest, computed
            )));
        }
        Ok(())
    }
}

#[derive(Serialize)]
struct MissingObjectsRequestDigestInput<'a> {
    tenant_id: &'a TenantId,
    project_id: &'a ProjectId,
    artifact_id: &'a ArtifactId,
    session_id: &'a SessionId,
    job_id: &'a JobId,
    objects: &'a [WireObjectSpec],
    extensions: &'a Extensions,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct MissingObjectsResponse {
    pub tenant_id: TenantId,
    pub project_id: ProjectId,
    pub artifact_id: ArtifactId,
    pub session_id: SessionId,
    pub job_id: JobId,
    #[schemars(
        with = "String",
        length(equal = 64),
        regex(pattern = CONTENT_DIGEST_PATTERN)
    )]
    pub request_digest: ContentDigest,
    #[schemars(length(max = 4096))]
    pub missing: Vec<WireObjectSpec>,
    #[schemars(length(max = 4096))]
    pub already_durable: Vec<WireObjectSpec>,
    #[serde(default, flatten)]
    pub extensions: Extensions,
}

impl MissingObjectsResponse {
    pub fn validate(&self) -> ProtocolResult<()> {
        let response_count = self
            .missing
            .len()
            .checked_add(self.already_durable.len())
            .ok_or_else(|| ProtocolError::InvalidField {
                field: "missing-object response",
                reason: "object count overflow".to_owned(),
            })?;
        validate_object_count("missing negotiation response objects", response_count)?;
        validate_canonical_object_specs("missing objects", &self.missing)?;
        validate_canonical_object_specs("already durable objects", &self.already_durable)?;
        self.missing.iter().try_for_each(WireObjectSpec::validate)?;
        self.already_durable
            .iter()
            .try_for_each(WireObjectSpec::validate)?;
        let missing_ids = self
            .missing
            .iter()
            .map(|object| object.object_id)
            .collect::<BTreeSet<_>>();
        if self
            .already_durable
            .iter()
            .any(|object| missing_ids.contains(&object.object_id))
        {
            return Err(ProtocolError::InvalidField {
                field: "missing-object response",
                reason: "missing and already_durable must be disjoint".to_owned(),
            });
        }
        validate_extension_keys(
            &self.extensions,
            &[
                "tenant_id",
                "project_id",
                "artifact_id",
                "session_id",
                "job_id",
                "request_digest",
                "missing",
                "already_durable",
            ],
        )
    }

    /// Validates that this response is an exact partition of a specific request.
    pub fn validate_for(&self, request: &MissingObjectsRequest) -> ProtocolResult<()> {
        request.validate()?;
        self.validate()?;
        if self.tenant_id != request.tenant_id
            || self.project_id != request.project_id
            || self.artifact_id != request.artifact_id
            || self.session_id != request.session_id
            || self.job_id != request.job_id
            || self.request_digest != request.request_digest
        {
            return Err(ProtocolError::InvalidField {
                field: "missing-object response scope",
                reason: "scope or request_digest does not match the request".to_owned(),
            });
        }

        let mut requested = request
            .objects
            .iter()
            .map(|object| (object.object_id, object))
            .collect::<BTreeMap<_, _>>();
        for object in self.missing.iter().chain(&self.already_durable) {
            let Some(expected) = requested.remove(&object.object_id) else {
                return Err(ProtocolError::InvalidField {
                    field: "missing-object response",
                    reason: format!(
                        "object {} was duplicated or was not present in the request",
                        object.object_id
                    ),
                });
            };
            if object != expected {
                return Err(ProtocolError::InvalidField {
                    field: "missing-object response",
                    reason: format!(
                        "object {} does not match the requested specification",
                        object.object_id
                    ),
                });
            }
        }
        if !requested.is_empty() {
            return Err(ProtocolError::InvalidField {
                field: "missing-object response",
                reason: "missing and already_durable do not cover every requested object"
                    .to_owned(),
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "UPPERCASE")]
pub enum ObjectTicketMethod {
    Get,
    Put,
}

/// A short-lived, exact-object S3-compatible data-plane capability.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct S3ObjectTicket {
    pub ticket_id: ObjectTicketId,
    pub tenant_id: TenantId,
    pub project_id: ProjectId,
    pub artifact_id: ArtifactId,
    pub session_id: SessionId,
    pub job_id: JobId,
    pub object: WireObjectSpec,
    pub method: ObjectTicketMethod,
    #[schemars(length(min = 1, max = 16384))]
    pub signed_url: String,
    #[serde(default)]
    #[schemars(transform = limit_ticket_headers_schema)]
    pub required_headers: BTreeMap<String, String>,
    pub issued_at_unix_ms: UnixMillis,
    pub expires_at_unix_ms: UnixMillis,
    #[serde(default, flatten)]
    pub extensions: Extensions,
}

impl S3ObjectTicket {
    pub fn validate(&self) -> ProtocolResult<()> {
        self.object.validate()?;
        validate_nonempty_limited("signed_url", &self.signed_url, MAX_SIGNED_URL_LENGTH)?;
        if self.required_headers.len() > MAX_TICKET_HEADERS {
            return Err(ProtocolError::LimitExceeded {
                limit_name: "ticket required headers",
                limit: MAX_TICKET_HEADERS,
                actual: self.required_headers.len(),
            });
        }
        let issued = self.issued_at_unix_ms.get();
        let expires = self.expires_at_unix_ms.get();
        let ttl = expires
            .checked_sub(issued)
            .ok_or_else(|| ProtocolError::InvalidField {
                field: "expires_at_unix_ms",
                reason: "must be later than issued_at_unix_ms".to_owned(),
            })?;
        if ttl == 0 || ttl > MAX_OBJECT_TICKET_TTL_MS {
            return Err(ProtocolError::InvalidField {
                field: "expires_at_unix_ms",
                reason: format!("ticket TTL must be in 1..={MAX_OBJECT_TICKET_TTL_MS} ms"),
            });
        }
        validate_extension_keys(
            &self.extensions,
            &[
                "ticket_id",
                "tenant_id",
                "project_id",
                "artifact_id",
                "session_id",
                "job_id",
                "object",
                "method",
                "signed_url",
                "required_headers",
                "issued_at_unix_ms",
                "expires_at_unix_ms",
            ],
        )
    }
}

/// Agent/client evidence that one ticket transfer finished and was content-verified locally.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct S3TransferCompletion {
    pub ticket_id: ObjectTicketId,
    pub tenant_id: TenantId,
    pub project_id: ProjectId,
    pub artifact_id: ArtifactId,
    pub session_id: SessionId,
    pub job_id: JobId,
    pub method: ObjectTicketMethod,
    pub object: WireObjectSpec,
    pub transferred_bytes: DecimalU64,
    #[schemars(
        with = "String",
        length(equal = 64),
        regex(pattern = CONTENT_DIGEST_PATTERN)
    )]
    pub observed_digest: ContentDigest,
    pub completed_at_unix_ms: UnixMillis,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(with = "Option<StorageVersionSchema>")]
    pub storage_version: Option<String>,
    #[serde(default, flatten)]
    pub extensions: Extensions,
}

impl S3TransferCompletion {
    pub fn validate(&self) -> ProtocolResult<()> {
        self.object.validate()?;
        if self.transferred_bytes != self.object.size {
            return Err(ProtocolError::InvalidField {
                field: "transferred_bytes",
                reason: "does not match the ticketed object size".to_owned(),
            });
        }
        if self.observed_digest.as_bytes() != self.object.object_id.as_bytes() {
            return Err(ProtocolError::InvalidDigest(
                "observed transfer digest does not match object ID".to_owned(),
            ));
        }
        if let Some(storage_version) = &self.storage_version {
            validate_nonempty_limited("storage_version", storage_version, 1024)?;
        }
        validate_extension_keys(
            &self.extensions,
            &[
                "ticket_id",
                "tenant_id",
                "project_id",
                "artifact_id",
                "session_id",
                "job_id",
                "method",
                "object",
                "transferred_bytes",
                "observed_digest",
                "completed_at_unix_ms",
                "storage_version",
            ],
        )
    }

    /// Verifies that this completion is exact evidence for one unexpired object ticket.
    pub fn validate_for(&self, ticket: &S3ObjectTicket) -> ProtocolResult<()> {
        ticket.validate()?;
        self.validate()?;
        if self.ticket_id != ticket.ticket_id
            || self.tenant_id != ticket.tenant_id
            || self.project_id != ticket.project_id
            || self.artifact_id != ticket.artifact_id
            || self.session_id != ticket.session_id
            || self.job_id != ticket.job_id
        {
            return Err(ProtocolError::InvalidField {
                field: "transfer completion scope",
                reason: "ticket identity or resource scope does not match the completion"
                    .to_owned(),
            });
        }
        if self.method != ticket.method || self.object != ticket.object {
            return Err(ProtocolError::InvalidField {
                field: "transfer completion object",
                reason: "ticket method or exact object specification does not match the completion"
                    .to_owned(),
            });
        }
        let completed = self.completed_at_unix_ms.get();
        if completed < ticket.issued_at_unix_ms.get()
            || completed >= ticket.expires_at_unix_ms.get()
        {
            return Err(ProtocolError::InvalidField {
                field: "completed_at_unix_ms",
                reason: "completion must occur within the ticket validity interval".to_owned(),
            });
        }
        Ok(())
    }
}

/// Authoritative storage-side result returned before metadata may reference an uploaded object.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ObjectDurabilityReceipt {
    pub tenant_id: TenantId,
    pub artifact_id: ArtifactId,
    pub session_id: SessionId,
    pub job_id: JobId,
    pub object: WireObjectSpec,
    pub state: DurabilityState,
    pub checked_at_unix_ms: UnixMillis,
    #[serde(default, flatten)]
    pub extensions: Extensions,
}

impl ObjectDurabilityReceipt {
    pub fn validate(&self) -> ProtocolResult<()> {
        self.object.validate()?;
        self.state.validate()?;
        if let DurabilityState::Durable {
            verified_digest, ..
        } = &self.state
        {
            if verified_digest.as_bytes() != self.object.object_id.as_bytes() {
                return Err(ProtocolError::InvalidDigest(
                    "durability digest does not match object ID".to_owned(),
                ));
            }
        }
        validate_extension_keys(
            &self.extensions,
            &[
                "tenant_id",
                "artifact_id",
                "session_id",
                "job_id",
                "object",
                "state",
                "checked_at_unix_ms",
            ],
        )
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum DurabilityState {
    Pending {
        #[serde(default, flatten)]
        extensions: Extensions,
    },
    Durable {
        #[schemars(
            with = "String",
            length(equal = 64),
            regex(pattern = CONTENT_DIGEST_PATTERN)
        )]
        verified_digest: ContentDigest,
        #[schemars(length(min = 1, max = 1024))]
        storage_version: String,
        #[serde(default, flatten)]
        extensions: Extensions,
    },
    Rejected {
        #[schemars(length(min = 1, max = 96))]
        code: String,
        #[schemars(length(min = 1, max = 4096))]
        message: String,
        #[serde(default, flatten)]
        extensions: Extensions,
    },
}

impl DurabilityState {
    fn validate(&self) -> ProtocolResult<()> {
        match self {
            Self::Pending { extensions } => validate_extension_keys(extensions, &["status"]),
            Self::Durable {
                storage_version,
                extensions,
                ..
            } => {
                validate_nonempty_limited("storage_version", storage_version, 1024)?;
                validate_extension_keys(
                    extensions,
                    &["status", "verified_digest", "storage_version"],
                )?;
                Ok(())
            }
            DurabilityState::Rejected {
                code,
                message,
                extensions,
            } => {
                validate_nonempty_limited("durability rejection code", code, 96)?;
                validate_nonempty_limited("durability rejection message", message, 4096)?;
                validate_extension_keys(extensions, &["status", "code", "message"])
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(untagged)]
pub enum S3ProtocolSchema {
    MissingRequest(MissingObjectsRequest),
    MissingResponse(MissingObjectsResponse),
    Ticket(S3ObjectTicket),
    Completion(S3TransferCompletion),
    Durability(ObjectDurabilityReceipt),
}

impl S3ProtocolSchema {
    /// Decodes one S3 data-plane DTO through the shared duplicate-key and runtime validator.
    pub fn decode_json(bytes: &[u8]) -> ProtocolResult<Self> {
        let value = parse_unique_json(bytes)?;
        let message: Self = serde_json::from_value(value)?;
        message.validate()?;
        Ok(message)
    }

    pub fn validate(&self) -> ProtocolResult<()> {
        match self {
            Self::MissingRequest(message) => message.validate(),
            Self::MissingResponse(message) => message.validate(),
            Self::Ticket(message) => message.validate(),
            Self::Completion(message) => message.validate(),
            Self::Durability(message) => message.validate(),
        }
    }
}

fn validate_object_count(limit_name: &'static str, count: usize) -> ProtocolResult<()> {
    validate_collection_limit(limit_name, count, MAX_RECORDS_PER_PAGE)
}

fn validate_canonical_object_specs(
    field: &'static str,
    objects: &[WireObjectSpec],
) -> ProtocolResult<()> {
    if objects
        .windows(2)
        .any(|pair| pair[0].object_id >= pair[1].object_id)
    {
        return Err(ProtocolError::InvalidField {
            field,
            reason: "objects must be uniquely and strictly ordered by object_id".to_owned(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use serde_json::json;

    use super::*;

    fn object_id() -> ObjectId {
        ObjectId::from_str(&"22".repeat(32)).unwrap()
    }

    fn numbered_object(number: u64) -> WireObjectSpec {
        let mut bytes = [0_u8; 32];
        bytes[24..].copy_from_slice(&number.to_be_bytes());
        WireObjectSpec {
            object_id: ObjectId::from_bytes(bytes),
            size: DecimalU64::new(number + 1),
            extensions: Extensions::new(),
        }
    }

    fn missing_request(objects: Vec<WireObjectSpec>) -> MissingObjectsRequest {
        let mut request = MissingObjectsRequest {
            tenant_id: TenantId::new("tenant-a").unwrap(),
            project_id: ProjectId::new("project-a").unwrap(),
            artifact_id: ArtifactId::new("artifact-a").unwrap(),
            session_id: SessionId::new("session-a").unwrap(),
            job_id: JobId::new("job-a").unwrap(),
            objects,
            request_digest: ContentDigest::from_bytes([0; 32]),
            extensions: Extensions::new(),
        };
        request.request_digest = request.computed_request_digest().unwrap();
        request
    }

    fn missing_response(
        request: &MissingObjectsRequest,
        missing: Vec<WireObjectSpec>,
        already_durable: Vec<WireObjectSpec>,
    ) -> MissingObjectsResponse {
        MissingObjectsResponse {
            tenant_id: request.tenant_id.clone(),
            project_id: request.project_id.clone(),
            artifact_id: request.artifact_id.clone(),
            session_id: request.session_id.clone(),
            job_id: request.job_id.clone(),
            request_digest: request.request_digest,
            missing,
            already_durable,
            extensions: Extensions::new(),
        }
    }

    fn object_ticket() -> S3ObjectTicket {
        S3ObjectTicket {
            ticket_id: ObjectTicketId::new("ticket-1").unwrap(),
            tenant_id: TenantId::new("tenant-a").unwrap(),
            project_id: ProjectId::new("project-a").unwrap(),
            artifact_id: ArtifactId::new("artifact-a").unwrap(),
            session_id: SessionId::new("session-a").unwrap(),
            job_id: JobId::new("job-a").unwrap(),
            object: WireObjectSpec {
                object_id: object_id(),
                size: DecimalU64::new(12),
                extensions: Extensions::new(),
            },
            method: ObjectTicketMethod::Put,
            signed_url: "https://objects.invalid/signed".to_owned(),
            required_headers: BTreeMap::new(),
            issued_at_unix_ms: UnixMillis::new(100),
            expires_at_unix_ms: UnixMillis::new(100 + MAX_OBJECT_TICKET_TTL_MS),
            extensions: Extensions::new(),
        }
    }

    fn completion_for(ticket: &S3ObjectTicket) -> S3TransferCompletion {
        S3TransferCompletion {
            ticket_id: ticket.ticket_id.clone(),
            tenant_id: ticket.tenant_id.clone(),
            project_id: ticket.project_id.clone(),
            artifact_id: ticket.artifact_id.clone(),
            session_id: ticket.session_id.clone(),
            job_id: ticket.job_id.clone(),
            method: ticket.method,
            object: ticket.object.clone(),
            transferred_bytes: ticket.object.size,
            observed_digest: ContentDigest::from(ticket.object.object_id),
            completed_at_unix_ms: UnixMillis::new(ticket.issued_at_unix_ms.get() + 1),
            storage_version: Some("version-1".to_owned()),
            extensions: Extensions::new(),
        }
    }

    #[test]
    fn ticket_ttl_is_bounded() {
        let ticket = object_ticket();
        assert!(ticket.validate().is_ok());
        let mut expired = ticket;
        expired.expires_at_unix_ms = UnixMillis::new(100 + MAX_OBJECT_TICKET_TTL_MS + 1);
        assert!(expired.validate().is_err());
    }

    #[test]
    fn transfer_completion_is_bound_to_the_exact_live_ticket() {
        let ticket = object_ticket();
        let completion = completion_for(&ticket);
        completion.validate_for(&ticket).unwrap();

        let mut wrong_scope = completion.clone();
        wrong_scope.project_id = ProjectId::new("project-b").unwrap();
        assert!(wrong_scope.validate().is_ok());
        assert!(wrong_scope.validate_for(&ticket).is_err());

        let mut wrong_ticket = completion.clone();
        wrong_ticket.ticket_id = ObjectTicketId::new("ticket-2").unwrap();
        assert!(wrong_ticket.validate_for(&ticket).is_err());

        let mut wrong_method = completion.clone();
        wrong_method.method = ObjectTicketMethod::Get;
        assert!(wrong_method.validate_for(&ticket).is_err());

        let mut wrong_object = completion.clone();
        wrong_object.object = numbered_object(9);
        wrong_object.transferred_bytes = wrong_object.object.size;
        wrong_object.observed_digest = ContentDigest::from(wrong_object.object.object_id);
        assert!(wrong_object.validate().is_ok());
        assert!(wrong_object.validate_for(&ticket).is_err());

        let mut before_issue = completion.clone();
        before_issue.completed_at_unix_ms = UnixMillis::new(ticket.issued_at_unix_ms.get() - 1);
        assert!(before_issue.validate_for(&ticket).is_err());

        let mut at_expiry = completion;
        at_expiry.completed_at_unix_ms = ticket.expires_at_unix_ms;
        assert!(at_expiry.validate_for(&ticket).is_err());
    }

    #[test]
    fn missing_negotiation_is_bounded() {
        let object = WireObjectSpec {
            object_id: object_id(),
            size: DecimalU64::new(12),
            extensions: Extensions::new(),
        };
        let request = missing_request(vec![object; MAX_RECORDS_PER_PAGE + 1]);
        assert!(matches!(
            request.validate(),
            Err(ProtocolError::LimitExceeded { .. })
        ));
    }

    #[test]
    fn missing_request_digest_detects_object_tampering() {
        let object = WireObjectSpec {
            object_id: object_id(),
            size: DecimalU64::new(12),
            extensions: Extensions::new(),
        };
        let mut request = missing_request(vec![object]);
        assert!(request.validate().is_ok());
        assert_eq!(
            request.request_digest.to_string(),
            "385099d4db27366bf5a961c9d7bded404cd7ca33ae0173b56aac7e559e58c227"
        );
        request.objects[0].size = DecimalU64::new(13);
        assert!(matches!(
            request.validate(),
            Err(ProtocolError::InvalidDigest(_))
        ));
    }

    #[test]
    fn missing_request_requires_unique_canonical_object_order() {
        let mut unsorted = missing_request(vec![numbered_object(2), numbered_object(1)]);
        unsorted.request_digest = unsorted.computed_request_digest().unwrap();
        assert!(matches!(
            unsorted.validate(),
            Err(ProtocolError::InvalidField {
                field: "missing negotiation objects",
                ..
            })
        ));

        let mut different_size = numbered_object(1);
        different_size.size = DecimalU64::new(99);
        let duplicate = missing_request(vec![numbered_object(1), different_size]);
        assert!(matches!(
            duplicate.validate(),
            Err(ProtocolError::InvalidField {
                field: "missing negotiation objects",
                ..
            })
        ));
    }

    #[test]
    fn missing_response_is_bound_to_and_partitions_its_request() {
        let first = numbered_object(1);
        let second = numbered_object(2);
        let request = missing_request(vec![first.clone(), second.clone()]);
        let response = missing_response(&request, vec![first.clone()], vec![second.clone()]);
        response.validate_for(&request).unwrap();

        let incomplete = missing_response(&request, vec![first.clone()], Vec::new());
        assert!(incomplete.validate_for(&request).is_err());

        let overlapping = missing_response(
            &request,
            vec![first.clone()],
            vec![first.clone(), second.clone()],
        );
        assert!(overlapping.validate_for(&request).is_err());

        let mut wrong_size = first;
        wrong_size.size = DecimalU64::new(500);
        let mismatched = missing_response(&request, vec![wrong_size], vec![second]);
        assert!(mismatched.validate_for(&request).is_err());

        let mut wrong_scope = response;
        wrong_scope.project_id = ProjectId::new("project-b").unwrap();
        assert!(wrong_scope.validate_for(&request).is_err());
    }

    #[test]
    fn missing_response_uses_one_combined_object_limit() {
        let request = missing_request(Vec::new());
        let missing = (0..=MAX_RECORDS_PER_PAGE / 2)
            .map(|number| numbered_object(number as u64))
            .collect();
        let already_durable = ((MAX_RECORDS_PER_PAGE / 2 + 1)..=MAX_RECORDS_PER_PAGE)
            .map(|number| numbered_object(number as u64))
            .collect();
        let response = missing_response(&request, missing, already_durable);
        assert!(matches!(
            response.validate(),
            Err(ProtocolError::LimitExceeded { .. })
        ));
    }

    #[test]
    fn unknown_durability_fields_round_trip() {
        let state: DurabilityState = serde_json::from_value(json!({
            "status": "durable",
            "verified_digest": "22".repeat(32),
            "storage_version": "v1",
            "future_durability": {"replicas": 3}
        }))
        .unwrap();
        let DurabilityState::Durable { extensions, .. } = &state else {
            panic!("expected durable state");
        };
        assert_eq!(extensions["future_durability"], json!({"replicas": 3}));
        assert_eq!(
            serde_json::to_value(state).unwrap()["future_durability"],
            json!({"replicas": 3})
        );
    }

    #[test]
    fn durability_string_limits_count_unicode_scalars_like_json_schema() {
        let valid = DurabilityState::Rejected {
            code: "错".repeat(96),
            message: "界".repeat(4096),
            extensions: Extensions::new(),
        };
        valid.validate().unwrap();

        let invalid = DurabilityState::Rejected {
            code: "错".repeat(97),
            message: "failure".to_owned(),
            extensions: Extensions::new(),
        };
        assert!(matches!(
            invalid.validate(),
            Err(ProtocolError::LimitExceeded {
                limit_name: "durability rejection code",
                limit: 96,
                actual: 97,
            })
        ));
    }

    #[test]
    fn object_validators_reject_reserved_extension_keys_recursively() {
        let mut object_extensions = Extensions::new();
        object_extensions.insert("object_id".to_owned(), json!("shadow"));
        let request = missing_request(vec![WireObjectSpec {
            object_id: object_id(),
            size: DecimalU64::new(12),
            extensions: object_extensions,
        }]);
        assert!(matches!(
            request.validate(),
            Err(ProtocolError::InvalidField {
                field: "extensions",
                ..
            })
        ));
    }

    #[test]
    fn s3_decode_rejects_duplicate_members_before_variant_decode() {
        let error =
            S3ProtocolSchema::decode_json(br#"{"tenant_id":"tenant-a","tenant_id":"tenant-b"}"#)
                .unwrap_err();

        assert!(error.to_string().contains("duplicate JSON object member"));
    }
}
