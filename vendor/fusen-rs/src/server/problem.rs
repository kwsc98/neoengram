use crate::{Error, ErrorCategory};
use bytes::Bytes;
use http::{HeaderMap, StatusCode};
use serde_json::Value;
use std::{
    panic::{AssertUnwindSafe, catch_unwind},
    sync::Arc,
};

const EMERGENCY_PROBLEM_LIMIT: usize = 4 * 1024;
const EMERGENCY_REQUEST_ID: &str = "00000000000000000000000000000000";
const EMERGENCY_INSTANCE: &str = "/__fusen_problem_encoder_emergency__";

/// Immutable error context passed to a custom server Problem Details encoder.
pub struct ProblemEncoding<'a> {
    error: &'a Error,
    request_id: &'a str,
    instance: Option<&'a str>,
    include_request_id: bool,
}

impl<'a> ProblemEncoding<'a> {
    pub(crate) const fn new(
        error: &'a Error,
        request_id: &'a str,
        instance: Option<&'a str>,
        include_request_id: bool,
    ) -> Self {
        Self {
            error,
            request_id,
            instance,
            include_request_id,
        }
    }

    /// Returns the normalized invocation error.
    pub const fn error(&self) -> &Error {
        self.error
    }

    /// Returns the validated or generated request identifier.
    pub const fn request_id(&self) -> &str {
        self.request_id
    }

    /// Returns the request URI path used as the RFC 9457 instance.
    pub const fn instance(&self) -> Option<&str> {
        self.instance
    }

    /// Returns whether the response must expose the request identifier.
    pub const fn include_request_id(&self) -> bool {
        self.include_request_id
    }
}

/// Encoded non-success response returned by a custom [`ProblemEncoder`].
#[derive(Clone, Debug)]
pub struct EncodedProblem {
    status: StatusCode,
    headers: HeaderMap,
    body: Bytes,
}

impl EncodedProblem {
    /// Creates an encoded Problem Details response.
    pub fn new(status: StatusCode, body: impl Into<Bytes>) -> Self {
        Self {
            status,
            headers: HeaderMap::new(),
            body: body.into(),
        }
    }

    /// Returns the HTTP failure status.
    pub const fn status(&self) -> StatusCode {
        self.status
    }

    /// Returns application response headers.
    pub const fn headers(&self) -> &HeaderMap {
        &self.headers
    }

    /// Returns mutable application response headers.
    pub fn headers_mut(&mut self) -> &mut HeaderMap {
        &mut self.headers
    }

    /// Returns the encoded JSON document.
    pub const fn body(&self) -> &Bytes {
        &self.body
    }

    pub(crate) fn into_parts(self) -> (StatusCode, HeaderMap, Bytes) {
        (self.status, self.headers, self.body)
    }
}

/// Server-side extension point for application-specific RFC 9457 documents.
///
/// The runtime validates that each body is a bounded JSON object, removes transport/control
/// headers, forces `application/problem+json`, and applies its request-ID response policy.
pub trait ProblemEncoder: Send + Sync + 'static {
    /// Encodes one normalized invocation failure.
    fn encode(&self, context: ProblemEncoding<'_>) -> EncodedProblem;

    /// Encodes the stable emergency response validated while the server is built.
    ///
    /// The default delegates to [`ProblemEncoder::encode`]. Implementations with stateful
    /// encoding should override this method with a deterministic, side-effect-free response.
    /// The runtime may substitute the supplied request ID in matching JSON string values.
    fn encode_emergency(&self, context: ProblemEncoding<'_>) -> EncodedProblem {
        self.encode(context)
    }
}

#[derive(Clone)]
pub(crate) struct PreparedProblemEncoder {
    encoder: Arc<dyn ProblemEncoder>,
    exposed: PreparedEmergencyProblem,
    hidden: Option<PreparedEmergencyProblem>,
    emergency_reservation_bytes: usize,
}

impl PreparedProblemEncoder {
    pub(crate) fn new(
        encoder: Arc<dyn ProblemEncoder>,
        max_response_body: usize,
        max_request_id_bytes: usize,
        prepare_hidden: bool,
    ) -> Result<Self, &'static str> {
        let error = Error::framework(
            ErrorCategory::Internal,
            "problem_encoder_failure",
            "request processing failed",
        );
        let exposed = PreparedEmergencyProblem::new(
            &encoder,
            &error,
            true,
            max_response_body,
            max_request_id_bytes,
        )?;
        let hidden = prepare_hidden
            .then(|| {
                PreparedEmergencyProblem::new(
                    &encoder,
                    &error,
                    false,
                    max_response_body,
                    max_request_id_bytes,
                )
            })
            .transpose()?;
        let emergency_reservation_bytes = hidden
            .as_ref()
            .map_or(exposed.max_rendered_bytes, |hidden| {
                exposed.max_rendered_bytes.max(hidden.max_rendered_bytes)
            });
        Ok(Self {
            encoder,
            exposed,
            hidden,
            emergency_reservation_bytes,
        })
    }

    pub(crate) fn encoder(&self) -> &dyn ProblemEncoder {
        self.encoder.as_ref()
    }

    pub(crate) const fn emergency_reservation_bytes(&self) -> usize {
        self.emergency_reservation_bytes
    }

    pub(crate) fn emergency(&self, request_id: &str, include_request_id: bool) -> EncodedProblem {
        let prepared = if include_request_id {
            &self.exposed
        } else {
            self.hidden.as_ref().unwrap_or(&self.exposed)
        };
        prepared.render(request_id, include_request_id)
    }
}

#[derive(Clone)]
struct PreparedEmergencyProblem {
    status: StatusCode,
    headers: HeaderMap,
    document: Value,
    max_rendered_bytes: usize,
}

impl PreparedEmergencyProblem {
    fn new(
        encoder: &Arc<dyn ProblemEncoder>,
        error: &Error,
        include_request_id: bool,
        max_response_body: usize,
        max_request_id_bytes: usize,
    ) -> Result<Self, &'static str> {
        let encoded = catch_unwind(AssertUnwindSafe(|| {
            encoder.encode_emergency(ProblemEncoding::new(
                error,
                EMERGENCY_REQUEST_ID,
                Some(EMERGENCY_INSTANCE),
                include_request_id,
            ))
        }))
        .map_err(|_| "custom problem emergency encoder panicked")?;
        let (status, headers, body) =
            validate_encoded_problem(encoded, max_response_body.min(EMERGENCY_PROBLEM_LIMIT))?;
        let document = serde_json::from_slice::<Value>(&body)
            .map_err(|_| "custom problem emergency body is not valid JSON")?;
        if !include_request_id && contains_string(&document, EMERGENCY_REQUEST_ID) {
            return Err("custom problem emergency body exposed a disabled request ID");
        }
        let mut worst_case = document.clone();
        replace_string(
            &mut worst_case,
            EMERGENCY_REQUEST_ID,
            &"a".repeat(max_request_id_bytes),
        );
        replace_string(&mut worst_case, EMERGENCY_INSTANCE, "/");
        let max_rendered_bytes = serde_json::to_vec(&worst_case)
            .map_err(|_| "custom problem emergency body could not be serialized")?
            .len();
        if max_rendered_bytes > max_response_body.min(EMERGENCY_PROBLEM_LIMIT) {
            return Err("custom problem emergency body exceeds the response limit");
        }
        Ok(Self {
            status,
            headers,
            document,
            max_rendered_bytes,
        })
    }

    fn render(&self, request_id: &str, include_request_id: bool) -> EncodedProblem {
        let mut document = self.document.clone();
        if include_request_id {
            replace_string(&mut document, EMERGENCY_REQUEST_ID, request_id);
        }
        replace_string(&mut document, EMERGENCY_INSTANCE, "/");
        let body = serde_json::to_vec(&document)
            .expect("validated emergency Problem Details remains serializable");
        debug_assert!(body.len() <= self.max_rendered_bytes);
        let mut encoded = EncodedProblem::new(self.status, body);
        *encoded.headers_mut() = self.headers.clone();
        encoded
    }
}

pub(crate) fn validate_encoded_problem(
    encoded: EncodedProblem,
    max_response_body: usize,
) -> Result<(StatusCode, HeaderMap, Bytes), &'static str> {
    let (status, headers, body) = encoded.into_parts();
    if !status.is_client_error() && !status.is_server_error() {
        return Err("custom problem status is not 4xx or 5xx");
    }
    if body.len() > max_response_body {
        return Err("custom problem body exceeds the response limit");
    }
    let value = serde_json::from_slice::<Value>(&body)
        .map_err(|_| "custom problem body is not valid JSON")?;
    if !value.is_object() {
        return Err("custom problem body is not a JSON object");
    }
    Ok((status, headers, body))
}

fn contains_string(value: &Value, needle: &str) -> bool {
    match value {
        Value::String(value) => value == needle,
        Value::Array(values) => values.iter().any(|value| contains_string(value, needle)),
        Value::Object(values) => values.values().any(|value| contains_string(value, needle)),
        _ => false,
    }
}

fn replace_string(value: &mut Value, needle: &str, replacement: &str) {
    match value {
        Value::String(value) if value == needle => replacement.clone_into(value),
        Value::Array(values) => {
            for value in values {
                replace_string(value, needle, replacement);
            }
        }
        Value::Object(values) => {
            for value in values.values_mut() {
                replace_string(value, needle, replacement);
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::HeaderValue;
    use serde_json::json;

    struct ContractEncoder;

    impl ProblemEncoder for ContractEncoder {
        fn encode(&self, context: ProblemEncoding<'_>) -> EncodedProblem {
            let mut document = json!({
                "type": "urn:test:problem",
                "status": 500,
                "instance": context.instance().unwrap_or("/"),
            });
            if context.include_request_id() {
                document["request_id"] = Value::String(context.request_id().to_owned());
            }
            let mut encoded = EncodedProblem::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                serde_json::to_vec(&document).unwrap(),
            );
            encoded
                .headers_mut()
                .insert("x-contract", HeaderValue::from_static("test"));
            encoded
        }
    }

    #[test]
    fn prepared_emergency_is_custom_valid_and_request_specific() {
        let prepared =
            PreparedProblemEncoder::new(Arc::new(ContractEncoder), 1024, 128, true).unwrap();
        let exposed = prepared.emergency("caller:request", true);
        let value: Value = serde_json::from_slice(exposed.body()).unwrap();
        assert_eq!(value["type"], "urn:test:problem");
        assert_eq!(value["request_id"], "caller:request");
        assert_eq!(value["instance"], "/");
        assert!(prepared.emergency_reservation_bytes() >= exposed.body().len());

        let hidden = prepared.emergency("private-request", false);
        let value: Value = serde_json::from_slice(hidden.body()).unwrap();
        assert!(value.get("request_id").is_none());
        assert!(!String::from_utf8_lossy(hidden.body()).contains("private-request"));
    }

    #[test]
    fn encoded_problem_requires_an_error_status_bounded_json_object() {
        for encoded in [
            EncodedProblem::new(StatusCode::OK, br#"{}"#.as_slice()),
            EncodedProblem::new(StatusCode::BAD_REQUEST, br#"[]"#.as_slice()),
            EncodedProblem::new(StatusCode::BAD_REQUEST, br#"not-json"#.as_slice()),
            EncodedProblem::new(StatusCode::BAD_REQUEST, br#"{"too":"large"}"#.as_slice()),
        ] {
            assert!(validate_encoded_problem(encoded, 4).is_err());
        }
        assert!(
            validate_encoded_problem(
                EncodedProblem::new(StatusCode::BAD_REQUEST, br#"{}"#.as_slice()),
                2,
            )
            .is_ok()
        );
    }

    struct PanickingEmergencyEncoder;

    impl ProblemEncoder for PanickingEmergencyEncoder {
        fn encode(&self, _context: ProblemEncoding<'_>) -> EncodedProblem {
            EncodedProblem::new(StatusCode::BAD_REQUEST, br#"{}"#.as_slice())
        }

        fn encode_emergency(&self, _context: ProblemEncoding<'_>) -> EncodedProblem {
            panic!("private emergency panic")
        }
    }

    #[test]
    fn panicking_emergency_encoder_is_rejected_during_preparation() {
        let result =
            PreparedProblemEncoder::new(Arc::new(PanickingEmergencyEncoder), 1024, 64, false);
        assert!(matches!(
            result,
            Err("custom problem emergency encoder panicked")
        ));
    }
}
