use fusen_rs::{
    EncodedProblem, Error, ErrorCategory, ErrorDetails, ErrorKind, ErrorOrigin, ProblemEncoder,
    ProblemEncoding,
};
use http::{
    header::{RETRY_AFTER, WWW_AUTHENTICATE},
    HeaderValue, StatusCode,
};
use neoengramd::{CentralError, CentralErrorCode};
use serde::Serialize;
use serde_json::{json, Value};

const MAX_PROBLEM_DETAIL_CHARS: usize = 4096;
const MAX_VIOLATIONS: usize = 256;
const MAX_VIOLATION_FIELD_CHARS: usize = 256;
const MAX_VIOLATION_REASON_CHARS: usize = 1024;

/// Creates an application error with NeoEngram's stable metadata attached.
pub fn application_error(
    category: ErrorCategory,
    code: &'static str,
    neo_code: &'static str,
    message: impl Into<String>,
    retryable: bool,
) -> Error {
    let error = Error::application(category, code, message)
        .expect("server-owned Fusen error codes and categories are valid");
    with_neo_details(error, neo_code, retryable, None, None)
}

fn with_neo_details(
    error: Error,
    neo_code: &str,
    retryable: bool,
    retry_after_ms: Option<u64>,
    violations: Option<Value>,
) -> Error {
    let mut details = ErrorDetails::new();
    details.insert("neo_code", Value::String(neo_code.to_owned()));
    details.insert("retryable", Value::Bool(retryable));
    if let Some(retry_after_ms) = retry_after_ms {
        details.insert("retry_after_ms", Value::String(retry_after_ms.to_string()));
    }
    if let Some(violations) = violations {
        details.insert("violations", violations);
    }
    error.with_details(details)
}

/// Builds the public API-version negotiation error with a header violation.
pub fn api_version_unsupported() -> Error {
    let error = Error::application_status(
        StatusCode::UNPROCESSABLE_ENTITY,
        "api_version_unsupported",
        "NeoEngram-API-Version must be 1",
    )
    .expect("server-owned API-version error is valid");
    with_neo_details(
        error,
        "API_VERSION_UNSUPPORTED",
        false,
        None,
        Some(json!([{
            "field": "NeoEngram-API-Version",
            "reason": "unsupported value"
        }])),
    )
}

/// Converts a domain error without exposing internal persistence diagnostics.
pub fn map_central_error(error: CentralError) -> Error {
    let code = error.code();
    let (category, status, public_message, retryable, retry_after_ms) = match code {
        CentralErrorCode::JobNotFound => {
            (ErrorCategory::NotFound, None, "job not found", false, None)
        }
        CentralErrorCode::EnrollmentNotFound => (
            ErrorCategory::NotFound,
            None,
            "storage enrollment not found",
            false,
            None,
        ),
        CentralErrorCode::ArtifactNotFound | CentralErrorCode::StorageVolumeNotFound => (
            ErrorCategory::NotFound,
            None,
            "catalog parent resource not found",
            false,
            None,
        ),
        CentralErrorCode::ProtocolInvalid
        | CentralErrorCode::BatchTampered
        | CentralErrorCode::MetadataInvalid => (
            ErrorCategory::InvalidArgument,
            Some(StatusCode::UNPROCESSABLE_ENTITY),
            "managed Add metadata does not satisfy the public protocol",
            false,
            None,
        ),
        CentralErrorCode::DeadlineExceeded => (
            ErrorCategory::DeadlineExceeded,
            Some(StatusCode::REQUEST_TIMEOUT),
            "the managed Add deadline has elapsed",
            false,
            None,
        ),
        CentralErrorCode::JobAlreadyExists
        | CentralErrorCode::JobIdReused
        | CentralErrorCode::InvalidState
        | CentralErrorCode::AssignmentMismatch
        | CentralErrorCode::GenerationMismatch
        | CentralErrorCode::ConcurrentUpdate
        | CentralErrorCode::EnrollmentIdReused
        | CentralErrorCode::EnrollmentExpired
        | CentralErrorCode::EnrollmentDecisionConflict
        | CentralErrorCode::EnrollmentProbeFailed
        | CentralErrorCode::VolumeOwnerConflict
        | CentralErrorCode::ArtifactHeadMismatch
        | CentralErrorCode::StorageVolumeNotReady
        | CentralErrorCode::StorageVolumeRegionMismatch
        | CentralErrorCode::AgentIdentityMismatch
        | CentralErrorCode::AgentSessionActive
        | CentralErrorCode::ApprovalRequired
        | CentralErrorCode::BootstrapDenied => (
            ErrorCategory::Conflict,
            None,
            conflict_message(code),
            error.retryable(),
            (code == CentralErrorCode::ConcurrentUpdate).then_some(100),
        ),
        CentralErrorCode::LegacyEnrollmentRequiresReissue => (
            ErrorCategory::Conflict,
            None,
            "legacy enrollment requires a replacement enrollment",
            false,
            None,
        ),
        CentralErrorCode::BatchUndeclared | CentralErrorCode::BatchIncomplete => (
            ErrorCategory::Conflict,
            None,
            "required managed Add metadata is incomplete",
            error.retryable(),
            None,
        ),
        CentralErrorCode::ObjectNotDurable => (
            ErrorCategory::Conflict,
            None,
            "a referenced object is not yet durable",
            true,
            Some(1_000),
        ),
        CentralErrorCode::Unauthorized => (
            ErrorCategory::PermissionDenied,
            None,
            "permission denied",
            false,
            None,
        ),
        CentralErrorCode::StorageFailure => (
            ErrorCategory::Unavailable,
            None,
            "authority storage is temporarily unavailable",
            true,
            Some(1_000),
        ),
        CentralErrorCode::Internal => (
            ErrorCategory::Internal,
            None,
            "the server could not complete the request",
            error.retryable(),
            None,
        ),
    };
    let neo_code = match code {
        CentralErrorCode::EnrollmentNotFound => "STORAGE_ENROLLMENT_NOT_FOUND",
        _ => code.as_str(),
    };
    let wire_code = neo_code.to_ascii_lowercase();
    let mapped = if let Some(status) = status {
        Error::application_status(status, wire_code, public_message)
    } else {
        Error::application(category, wire_code, public_message)
    }
    .expect("domain error codes normalize to valid Fusen codes");
    with_neo_details(mapped, neo_code, retryable, retry_after_ms, None)
}

/// Returns the same 404 for an absent enrollment and an enrollment outside the caller's scope.
pub fn storage_enrollment_not_found() -> Error {
    application_error(
        ErrorCategory::NotFound,
        "storage_enrollment_not_found",
        "STORAGE_ENROLLMENT_NOT_FOUND",
        "storage enrollment not found",
        false,
    )
}

fn conflict_message(code: CentralErrorCode) -> &'static str {
    match code {
        CentralErrorCode::JobAlreadyExists | CentralErrorCode::JobIdReused => {
            "the Job ID already belongs to a different request"
        }
        CentralErrorCode::InvalidState => "the Job is not in a valid state for this operation",
        CentralErrorCode::AssignmentMismatch | CentralErrorCode::GenerationMismatch => {
            "the Job execution identity conflicts with its authoritative state"
        }
        CentralErrorCode::ConcurrentUpdate => "the Job was updated concurrently",
        CentralErrorCode::ArtifactHeadMismatch => {
            "the Playground base or head differs from the Artifact head"
        }
        CentralErrorCode::StorageVolumeNotReady => "the StorageVolume is not ready",
        CentralErrorCode::StorageVolumeRegionMismatch => {
            "the Playground region differs from the StorageVolume region"
        }
        _ => "the requested operation conflicts with current state",
    }
}

/// Builds a 401 response and advertises Bearer authentication.
pub fn unauthenticated(code: &'static str, message: impl Into<String>) -> Error {
    let mut error = application_error(
        ErrorCategory::Unauthenticated,
        code,
        "AUTHENTICATION_REQUIRED",
        message,
        false,
    );
    error
        .headers_mut()
        .insert(WWW_AUTHENTICATE, HeaderValue::from_static("Bearer"));
    error
}

/// Builds a non-disclosing authorization error.
pub fn permission_denied() -> Error {
    application_error(
        ErrorCategory::PermissionDenied,
        "authorization_denied",
        "AUTHORIZATION_DENIED",
        "permission denied",
        false,
    )
}

/// Builds a validation error matching the public HTTP contract.
pub fn invalid_request(message: impl Into<String>) -> Error {
    let message = message.into();
    let error = Error::application_status(
        StatusCode::UNPROCESSABLE_ENTITY,
        "protocol_invalid",
        message.clone(),
    )
    .expect("server-owned validation status and code are valid");
    with_neo_details(
        error,
        "PROTOCOL_INVALID",
        false,
        None,
        Some(json!([{ "field": "$", "reason": message }])),
    )
}

/// RFC 9457 encoder matching the public NeoEngram error contract.
#[derive(Clone, Copy, Debug, Default)]
pub struct NeoEngramProblemEncoder;

#[derive(Serialize)]
struct ProblemDocument<'a> {
    #[serde(rename = "type")]
    type_uri: String,
    title: &'static str,
    status: u16,
    detail: String,
    instance: &'a str,
    code: String,
    request_id: &'a str,
    retryable: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    retry_after_ms: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    violations: Option<Value>,
}

impl ProblemEncoder for NeoEngramProblemEncoder {
    fn encode(&self, context: ProblemEncoding<'_>) -> EncodedProblem {
        let error = context.error();
        let (status, neo_code, type_name) = problem_semantics(error);
        let retryable = detail_bool(error, "retryable").unwrap_or_else(|| {
            error.kind() == ErrorKind::Framework && error.retry_hint().is_retryable()
        });
        let retry_after_ms = detail_decimal(error, "retry_after_ms").or_else(|| {
            error
                .retry_hint()
                .retry_after()
                .map(|delay| delay.as_millis().min(u128::from(u64::MAX)).to_string())
        });
        let violations = normalized_violations(error);
        let detail = if error.kind() == ErrorKind::Framework
            && status == StatusCode::INTERNAL_SERVER_ERROR
        {
            "Internal server error".to_owned()
        } else {
            bounded_text(error.message(), MAX_PROBLEM_DETAIL_CHARS)
        };
        let problem = ProblemDocument {
            type_uri: format!("urn:neoengram:problem:{type_name}"),
            title: problem_title(error.category(), status),
            status: status.as_u16(),
            detail,
            instance: context.instance().unwrap_or("/"),
            code: neo_code,
            request_id: context.request_id(),
            retryable,
            retry_after_ms: retry_after_ms.clone(),
            violations,
        };
        let body = serde_json::to_vec(&problem)
            .expect("NeoEngram Problem Details contains only JSON-compatible fields");
        let mut encoded = EncodedProblem::new(status, body);
        if error.origin() == ErrorOrigin::Local {
            *encoded.headers_mut() = error.headers().clone();
        }
        if let Some(milliseconds) = retry_after_ms.and_then(|value| value.parse::<u64>().ok()) {
            let seconds = milliseconds.saturating_add(999) / 1_000;
            if let Ok(value) = HeaderValue::from_str(&seconds.to_string()) {
                encoded.headers_mut().insert(RETRY_AFTER, value);
            }
        }
        encoded
    }
}

fn normalized_violations(error: &Error) -> Option<Value> {
    let violations = error
        .details()?
        .get("violations")?
        .as_array()?
        .iter()
        .take(MAX_VIOLATIONS)
        .filter_map(|violation| {
            let field = violation.get("field")?.as_str()?;
            let reason = violation.get("reason")?.as_str()?;
            if field.is_empty() || reason.is_empty() {
                return None;
            }
            Some(json!({
                "field": bounded_text(field, MAX_VIOLATION_FIELD_CHARS),
                "reason": bounded_text(reason, MAX_VIOLATION_REASON_CHARS),
            }))
        })
        .collect::<Vec<_>>();
    (!violations.is_empty()).then_some(Value::Array(violations))
}

fn bounded_text(value: &str, max_chars: usize) -> String {
    if value.chars().count() <= max_chars {
        return value.to_owned();
    }
    let mut bounded = value
        .chars()
        .take(max_chars.saturating_sub(3))
        .collect::<String>();
    bounded.push_str("...");
    bounded
}

fn problem_semantics(error: &Error) -> (StatusCode, String, String) {
    if error.kind() == ErrorKind::Application {
        let code = detail_string(error, "neo_code")
            .unwrap_or_else(|| error.code().as_str().to_ascii_uppercase());
        let type_name = code.to_ascii_lowercase().replace('_', "-");
        return (error.status(), code, type_name);
    }
    match error.category() {
        ErrorCategory::PayloadTooLarge => (
            StatusCode::PAYLOAD_TOO_LARGE,
            "PROTOCOL_LIMIT_EXCEEDED".to_owned(),
            "payload-too-large".to_owned(),
        ),
        ErrorCategory::InvalidArgument => (
            StatusCode::UNPROCESSABLE_ENTITY,
            "PROTOCOL_INVALID".to_owned(),
            "protocol-invalid".to_owned(),
        ),
        ErrorCategory::Internal | ErrorCategory::Unknown => (
            StatusCode::INTERNAL_SERVER_ERROR,
            "INTERNAL".to_owned(),
            "internal".to_owned(),
        ),
        category => {
            let status = category
                .canonical_status()
                .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
            let code = error.code().as_str().to_ascii_uppercase();
            let type_name = code.to_ascii_lowercase().replace('_', "-");
            (status, code, type_name)
        }
    }
}

fn detail_string(error: &Error, name: &str) -> Option<String> {
    error
        .details()
        .and_then(|details| details.get(name))
        .and_then(Value::as_str)
        .map(str::to_owned)
}

fn detail_bool(error: &Error, name: &str) -> Option<bool> {
    error
        .details()
        .and_then(|details| details.get(name))
        .and_then(Value::as_bool)
}

fn detail_decimal(error: &Error, name: &str) -> Option<String> {
    let value = error.details()?.get(name)?;
    match value {
        Value::String(value) if value.parse::<u64>().is_ok() => Some(value.clone()),
        Value::Number(value) => value.as_u64().map(|value| value.to_string()),
        _ => None,
    }
}

fn problem_title(category: ErrorCategory, status: StatusCode) -> &'static str {
    match category {
        ErrorCategory::InvalidArgument => "Invalid request",
        ErrorCategory::NotFound => "Resource not found",
        ErrorCategory::Conflict => "Request conflict",
        ErrorCategory::Unauthenticated => "Authentication required",
        ErrorCategory::PermissionDenied => "Authorization denied",
        ErrorCategory::PayloadTooLarge => "Payload too large",
        ErrorCategory::ResourceExhausted => "Resource exhausted",
        ErrorCategory::Unavailable => "Service unavailable",
        ErrorCategory::DeadlineExceeded => "Request deadline exceeded",
        ErrorCategory::Cancelled => "Request cancelled",
        ErrorCategory::Unimplemented => "Not implemented",
        ErrorCategory::DataLoss => "Invalid upstream response",
        ErrorCategory::Internal | ErrorCategory::Unknown => {
            status.canonical_reason().unwrap_or("Internal server error")
        }
        _ => status.canonical_reason().unwrap_or("Service error"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn domain_metadata_is_available_for_problem_encoder() {
        let error = map_central_error(CentralError::new(
            CentralErrorCode::ConcurrentUpdate,
            "private compare-and-swap detail",
        ));
        let details = error.details().unwrap();
        assert_eq!(
            details.get("neo_code"),
            Some(&Value::String("CONCURRENT_UPDATE".into()))
        );
        assert_eq!(details.get("retryable"), Some(&Value::Bool(true)));
    }

    #[test]
    fn public_problem_text_and_violations_respect_openapi_limits() {
        let error = invalid_request("x".repeat(MAX_PROBLEM_DETAIL_CHARS + 1));
        let detail = bounded_text(error.message(), MAX_PROBLEM_DETAIL_CHARS);
        assert_eq!(detail.chars().count(), MAX_PROBLEM_DETAIL_CHARS);
        assert!(detail.ends_with("..."));

        let violations = normalized_violations(&error).unwrap();
        let reason = violations[0]["reason"].as_str().unwrap();
        assert_eq!(reason.chars().count(), MAX_VIOLATION_REASON_CHARS);
        assert!(reason.ends_with("..."));
    }
}
