//! Read-only S3 data-plane contracts.
//!
//! Control traffic in NeoEngram uses bounded JSON/NDJSON frames. Object bytes deliberately use
//! this small length-delimited binary protocol instead so a Gateway can apply HTTP/2 flow
//! control without base64 expansion or buffering a complete object.

use std::{collections::BTreeSet, convert::TryFrom};

use percent_encoding::percent_decode_str;
use ring::{digest, hmac};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{
    domain_separated_jcs_bytes, AgentId, CentralSignedPayload, ContentDigest, GatewayConnectionId,
    GatewayOpaqueBytes, GatewayReplicaId, LifecycleGeneration, MountGeneration, OwnerGeneration,
    ResourceVersion, RouteGeneration, SessionGeneration, UnixMillis,
};

pub const S3_READ_FRAME_MAX_BYTES: usize = 256 * 1024;
pub const S3_READ_TICKET_MAX_BYTES: usize = 16 * 1024;
/// Dedicated full-duplex HTTP/2 endpoint used by an Agent and its Gateway Replica.
pub const S3_READ_CHANNEL_PATH: &str = "/internal/s3/read-channel";
/// The channel carries length-delimited binary frames rather than NDJSON.
pub const S3_READ_CHANNEL_CONTENT_TYPE: &str = "application/vnd.neoengram.s3-read";
/// One-hop Gateway-to-owner-Replica binary object stream. Request metadata uses the bounded S3
/// read frame codec; the response body contains raw object bytes under HTTP/2 flow control.
pub const S3_READ_PEER_PATH: &str = "/internal/s3/read-peer";
/// Private workload-authenticated Gateway-to-Central authorization endpoint.
pub const S3_AUTHORIZE_PATH: &str = "/internal/s3/authorize";
const FRAME_HEADER_BYTES: usize = 5;

/// Transport-neutral view of the exact public HTTP request covered by AWS SigV4. Gateway sends
/// this bounded representation to Central; Central owns credential lookup and HMAC verification.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct S3SigV4Request {
    pub method: String,
    pub path: String,
    /// Raw query string without the leading `?`.
    pub query: String,
    /// Header names and values as received. Repeated headers remain repeated entries.
    pub headers: Vec<(String, String)>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct S3SigV4Claims {
    pub access_key_id: String,
    pub region: String,
    pub service: String,
    pub expires_at_unix_seconds: u64,
}

/// Inputs for generating one bounded read-only S3 presigned URL.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct S3PresignRequest<'a> {
    pub endpoint: &'a str,
    pub bucket: &'a str,
    pub key: &'a str,
    pub region: &'a str,
    pub access_key_id: &'a str,
    pub secret: &'a [u8],
    pub now_unix_seconds: u64,
    pub expires_seconds: u32,
}

/// Private Gateway-to-Central authorization request. It contains no object bytes and is accepted
/// only on the workload-authenticated Central upstream.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct S3AuthorizeRequest {
    pub gateway_pool_id: String,
    pub bucket: String,
    pub operation: S3AuthorizeOperation,
    pub sigv4: S3SigV4Request,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum S3AuthorizeOperation {
    HeadBucket,
    GetBucketLocation,
    ListObjectsV2 {
        prefix: String,
        delimiter: Option<String>,
        continuation_token: Option<String>,
        start_after: Option<String>,
        max_keys: u16,
    },
    HeadObject {
        key: String,
        range: Option<String>,
        if_match: Option<String>,
        if_none_match: Option<String>,
    },
    GetObject {
        key: String,
        range: Option<String>,
        if_match: Option<String>,
        if_none_match: Option<String>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum S3AuthorizeResponse {
    Bucket {
        region: String,
    },
    ListObjectsV2 {
        objects: Vec<S3AuthorizedObject>,
        common_prefixes: Vec<String>,
        next_continuation_token: Option<String>,
    },
    Object {
        object: S3AuthorizedObject,
        status: u16,
        content_length: u64,
        content_range: Option<String>,
        ticket: Option<Box<S3ReadTicket>>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct S3AuthorizedObject {
    pub key: String,
    pub size_bytes: u64,
    pub etag: ContentDigest,
    pub last_modified_unix_ms: UnixMillis,
    pub content_type: String,
}

impl S3AuthorizeRequest {
    /// Proves that the authorization operation is the same resource and semantics covered by the
    /// forwarded SigV4 request. Central must call this before using any body field for lookup or
    /// ticket issuance so a Gateway cannot replay a valid signature while substituting another
    /// bucket, key, range, condition, or LIST scope.
    pub fn validate_signed_operation_binding(&self) -> Result<(), &'static str> {
        ParsedSigV4::parse(&self.sigv4)?;
        let host = self
            .sigv4
            .header("host")?
            .ok_or("SigV4 host header is missing")?;
        let key = signed_s3_resource(&self.sigv4.path, host, &self.bucket)?;
        let query = parse_query_pairs(&self.sigv4.query)?;

        match &self.operation {
            S3AuthorizeOperation::HeadBucket => {
                require_s3_method_and_key(&self.sigv4.method, "HEAD", key.as_deref(), None)
            }
            S3AuthorizeOperation::GetBucketLocation => {
                require_s3_method_and_key(&self.sigv4.method, "GET", key.as_deref(), None)?;
                if optional_query_value(&query, "location")? != Some("") {
                    return Err("S3 location operation is not bound to the signed query");
                }
                Ok(())
            }
            S3AuthorizeOperation::ListObjectsV2 {
                prefix,
                delimiter,
                continuation_token,
                start_after,
                max_keys,
            } => {
                require_s3_method_and_key(&self.sigv4.method, "GET", key.as_deref(), None)?;
                if optional_query_value(&query, "list-type")? != Some("2")
                    || optional_query_value(&query, "prefix")?.unwrap_or_default() != prefix
                    || normalize_optional_query(optional_query_value(&query, "delimiter")?)
                        != delimiter.as_deref()
                    || optional_query_value(&query, "continuation-token")?
                        != continuation_token.as_deref()
                    || optional_query_value(&query, "start-after")? != start_after.as_deref()
                {
                    return Err("S3 LIST operation is not bound to the signed query");
                }
                let signed_max_keys = optional_query_value(&query, "max-keys")?
                    .map(str::parse::<u16>)
                    .transpose()
                    .map_err(|_| "S3 signed max-keys is invalid")?
                    .unwrap_or(1000);
                if signed_max_keys != *max_keys {
                    return Err("S3 LIST max-keys is not bound to the signed query");
                }
                Ok(())
            }
            S3AuthorizeOperation::HeadObject {
                key: expected,
                range,
                if_match,
                if_none_match,
            } => {
                require_s3_method_and_key(
                    &self.sigv4.method,
                    "HEAD",
                    key.as_deref(),
                    Some(expected),
                )?;
                require_forwarded_header_binding(&self.sigv4, "range", range.as_deref())?;
                require_forwarded_header_binding(&self.sigv4, "if-match", if_match.as_deref())?;
                require_forwarded_header_binding(
                    &self.sigv4,
                    "if-none-match",
                    if_none_match.as_deref(),
                )?;
                Ok(())
            }
            S3AuthorizeOperation::GetObject {
                key: expected,
                range,
                if_match,
                if_none_match,
            } => {
                require_s3_method_and_key(
                    &self.sigv4.method,
                    "GET",
                    key.as_deref(),
                    Some(expected),
                )?;
                require_forwarded_header_binding(&self.sigv4, "range", range.as_deref())?;
                require_forwarded_header_binding(&self.sigv4, "if-match", if_match.as_deref())?;
                require_forwarded_header_binding(
                    &self.sigv4,
                    "if-none-match",
                    if_none_match.as_deref(),
                )?;
                Ok(())
            }
        }
    }
}

impl S3SigV4Request {
    /// Extracts the access-key identity without treating the request as authorized. Central uses
    /// this only to select candidate secret material before calling [`Self::verify_at`].
    pub fn credential_scope(&self) -> Result<(String, String, String), &'static str> {
        let parsed = ParsedSigV4::parse(self)?;
        Ok((parsed.access_key_id, parsed.region, parsed.service))
    }

    /// Verifies header-authenticated and presigned GET/HEAD requests using AWS Signature V4.
    pub fn verify_at(
        &self,
        secret: &[u8],
        now_unix_seconds: u64,
    ) -> Result<S3SigV4Claims, &'static str> {
        if !matches!(self.method.as_str(), "GET" | "HEAD") {
            return Err("SigV4 request method is not read-only");
        }
        if self.path.is_empty()
            || !self.path.starts_with('/')
            || self.path.len() > 4096
            || self.query.len() > 16 * 1024
            || self.headers.len() > 128
        {
            return Err("SigV4 request metadata exceeds its bounds");
        }
        let parsed = ParsedSigV4::parse(self)?;
        if parsed.service != "s3" {
            return Err("SigV4 credential scope service must be s3");
        }
        if parsed.expires_seconds.is_none() && !parsed.has_signed_header("x-amz-date") {
            return Err("SigV4 x-amz-date header must be signed");
        }
        let signed_headers = canonical_signed_headers(self, &parsed.signed_headers)?;
        let query_pairs = parse_query_pairs(&self.query)?;
        let canonical_query = canonical_query(&query_pairs, parsed.expires_seconds.is_some());
        let canonical_uri = aws_encode(
            &percent_decode_str(&self.path)
                .decode_utf8()
                .map_err(|_| "SigV4 URI is not UTF-8")?,
            false,
        );
        let payload_hash = self
            .header("x-amz-content-sha256")?
            .unwrap_or("UNSIGNED-PAYLOAD");
        if payload_hash != "UNSIGNED-PAYLOAD"
            && payload_hash != "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        {
            return Err("SigV4 read request payload hash is invalid");
        }
        let canonical_request = format!(
            "{}\n{}\n{}\n{}\n{}\n{}",
            self.method,
            canonical_uri,
            canonical_query,
            signed_headers.0,
            signed_headers.1,
            payload_hash
        );
        let scope = format!(
            "{}/{}/{}/aws4_request",
            parsed.scope_date, parsed.region, parsed.service
        );
        let string_to_sign = format!(
            "AWS4-HMAC-SHA256\n{}\n{}\n{}",
            parsed.amz_date,
            scope,
            hex_digest(canonical_request.as_bytes())
        );
        let signing_key =
            derive_signing_key(secret, &parsed.scope_date, &parsed.region, &parsed.service);
        let expected = hex_hmac(&signing_key, string_to_sign.as_bytes());
        if !constant_time_eq(expected.as_bytes(), parsed.signature.as_bytes()) {
            return Err("SigV4 signature mismatch");
        }
        let request_seconds = parse_amz_date(&parsed.amz_date)?;
        let expires_at = if let Some(expires) = parsed.expires_seconds {
            if expires == 0
                || expires > 7 * 24 * 60 * 60
                || request_seconds > now_unix_seconds.saturating_add(30)
            {
                return Err("SigV4 presigned expiry is invalid");
            }
            request_seconds.saturating_add(expires)
        } else {
            if request_seconds > now_unix_seconds.saturating_add(900)
                || now_unix_seconds > request_seconds.saturating_add(900)
            {
                return Err("SigV4 request timestamp is outside clock skew");
            }
            request_seconds.saturating_add(900)
        };
        if now_unix_seconds > expires_at {
            return Err("SigV4 request has expired");
        }
        Ok(S3SigV4Claims {
            access_key_id: parsed.access_key_id,
            region: parsed.region,
            service: parsed.service,
            expires_at_unix_seconds: expires_at,
        })
    }

    fn header(&self, name: &str) -> Result<Option<&str>, &'static str> {
        let values = self
            .headers
            .iter()
            .filter(|(candidate, _)| candidate.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
            .collect::<Vec<_>>();
        match values.as_slice() {
            [] => Ok(None),
            [value] => Ok(Some(*value)),
            _ => Err("SigV4 security header was repeated"),
        }
    }
}

/// Creates a bounded, read-only AWS SigV4 presigned GET URL.
///
/// Presigning belongs to the S3 domain contract rather than an HTTP adapter so Central and
/// Gateway cannot drift in their canonical URI, query, or signing-key implementations.
pub fn presign_s3_get(request: &S3PresignRequest<'_>) -> Result<String, &'static str> {
    if request.bucket.is_empty()
        || request.bucket.len() > 255
        || request.key.is_empty()
        || request.key.len() > 4096
        || request.region.is_empty()
        || request.region.len() > 128
        || request.access_key_id.is_empty()
        || request.access_key_id.len() > 128
        || request.secret.is_empty()
        || !(1..=7 * 24 * 60 * 60).contains(&request.expires_seconds)
    {
        return Err("S3 presign input is invalid");
    }
    let endpoint_url = url::Url::parse(request.endpoint).map_err(|_| "S3 endpoint is invalid")?;
    let loopback_http = endpoint_url.scheme() == "http"
        && endpoint_url.host().is_some_and(|host| match host {
            url::Host::Domain(domain) => domain.eq_ignore_ascii_case("localhost"),
            url::Host::Ipv4(address) => address.is_loopback(),
            url::Host::Ipv6(address) => address.is_loopback(),
        });
    if (endpoint_url.scheme() != "https" && !loopback_http)
        || endpoint_url.cannot_be_a_base()
        || endpoint_url.host().is_none()
        || !endpoint_url.username().is_empty()
        || endpoint_url.password().is_some()
        || endpoint_url.path() != "/"
        || endpoint_url.query().is_some()
        || endpoint_url.fragment().is_some()
    {
        return Err("S3 endpoint is invalid");
    }
    let host = match endpoint_url.host().ok_or("S3 endpoint is invalid")? {
        url::Host::Domain(domain) => domain.to_owned(),
        url::Host::Ipv4(address) => address.to_string(),
        url::Host::Ipv6(address) => format!("[{address}]"),
    };
    let host = match endpoint_url.port() {
        Some(port) => format!("{host}:{port}"),
        None => host,
    };
    let timestamp = time::OffsetDateTime::from_unix_timestamp(
        i64::try_from(request.now_unix_seconds).map_err(|_| "S3 presign timestamp is invalid")?,
    )
    .map_err(|_| "S3 presign timestamp is invalid")?;
    let amz_date = format!(
        "{:04}{:02}{:02}T{:02}{:02}{:02}Z",
        timestamp.year(),
        timestamp.month() as u8,
        timestamp.day(),
        timestamp.hour(),
        timestamp.minute(),
        timestamp.second()
    );
    let date = &amz_date[..8];
    let scope = format!("{date}/{}/s3/aws4_request", request.region);
    let canonical_uri = format!(
        "/{}/{}",
        aws_encode(request.bucket, true),
        aws_encode(request.key, false)
    );
    let mut query = [
        ("X-Amz-Algorithm", "AWS4-HMAC-SHA256".to_owned()),
        (
            "X-Amz-Credential",
            format!("{}/{scope}", request.access_key_id),
        ),
        ("X-Amz-Date", amz_date.clone()),
        ("X-Amz-Expires", request.expires_seconds.to_string()),
        ("X-Amz-SignedHeaders", "host".to_owned()),
        ("response-content-disposition", "attachment".to_owned()),
    ];
    query.sort_by(|left, right| left.0.cmp(right.0));
    let canonical_query = query
        .iter()
        .map(|(name, value)| format!("{}={}", aws_encode(name, true), aws_encode(value, true)))
        .collect::<Vec<_>>()
        .join("&");
    let canonical_request =
        format!("GET\n{canonical_uri}\n{canonical_query}\nhost:{host}\n\nhost\nUNSIGNED-PAYLOAD");
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
        hex_digest(canonical_request.as_bytes())
    );
    let signing_key = derive_signing_key(request.secret, date, request.region, "s3");
    let signature = hex_hmac(&signing_key, string_to_sign.as_bytes());
    Ok(format!(
        "{}{}?{}&X-Amz-Signature={signature}",
        request.endpoint.trim_end_matches('/'),
        canonical_uri,
        canonical_query
    ))
}

fn signed_s3_resource(
    path: &str,
    host: &str,
    expected_bucket: &str,
) -> Result<Option<String>, &'static str> {
    if expected_bucket.is_empty() || path.as_bytes().contains(&0) {
        return Err("S3 signed resource is invalid");
    }
    let path = percent_decode_str(path)
        .decode_utf8()
        .map_err(|_| "S3 signed resource path is not UTF-8")?;
    let path = path
        .strip_prefix('/')
        .ok_or("S3 signed resource path is not absolute")?;
    if path.starts_with('/') {
        return Err("S3 signed resource path has an empty leading component");
    }
    let host = s3_dns_host(host)?;
    let virtual_hosted = host
        .strip_prefix(expected_bucket)
        .is_some_and(|suffix| suffix.starts_with('.'));
    let key = if virtual_hosted {
        path.trim_start_matches('/')
    } else {
        let resource = path.trim_start_matches('/');
        let (bucket, key) = resource.split_once('/').unwrap_or((resource, ""));
        if bucket != expected_bucket {
            return Err("S3 bucket is not bound to the signed path or host");
        }
        key
    };
    Ok((!key.is_empty()).then(|| key.to_owned()))
}

fn s3_dns_host(value: &str) -> Result<String, &'static str> {
    let value = value.trim();
    if value.is_empty() || value.starts_with('[') || value.contains('/') {
        return Err("S3 signed host is invalid");
    }
    let host = match value.rsplit_once(':') {
        Some((host, port))
            if !port.is_empty() && port.bytes().all(|byte| byte.is_ascii_digit()) =>
        {
            host
        }
        Some(_) => return Err("S3 signed host port is invalid"),
        None => value,
    };
    if host.is_empty() {
        return Err("S3 signed host is invalid");
    }
    Ok(host.to_ascii_lowercase())
}

fn require_s3_method_and_key(
    actual_method: &str,
    expected_method: &str,
    actual_key: Option<&str>,
    expected_key: Option<&str>,
) -> Result<(), &'static str> {
    if actual_method != expected_method || actual_key != expected_key {
        return Err("S3 operation is not bound to the signed method and resource");
    }
    Ok(())
}

fn require_forwarded_header_binding(
    request: &S3SigV4Request,
    name: &str,
    expected: Option<&str>,
) -> Result<(), &'static str> {
    if request.header(name)? != expected {
        return Err("S3 object conditions are not bound to the forwarded headers");
    }
    Ok(())
}

fn normalize_optional_query(value: Option<&str>) -> Option<&str> {
    value.filter(|value| !value.is_empty())
}

fn optional_query_value<'a>(
    pairs: &'a [(String, String)],
    name: &str,
) -> Result<Option<&'a str>, &'static str> {
    let values = pairs
        .iter()
        .filter(|(candidate, _)| candidate.eq_ignore_ascii_case(name))
        .map(|(_, value)| value.as_str())
        .collect::<Vec<_>>();
    match values.as_slice() {
        [] => Ok(None),
        [value] => Ok(Some(*value)),
        _ => Err("S3 signed query field was repeated"),
    }
}

struct ParsedSigV4 {
    access_key_id: String,
    scope_date: String,
    region: String,
    service: String,
    signed_headers: String,
    signature: String,
    amz_date: String,
    expires_seconds: Option<u64>,
}

impl ParsedSigV4 {
    fn parse(request: &S3SigV4Request) -> Result<Self, &'static str> {
        let query = parse_query_pairs(&request.query)?;
        let authorization = request.header("authorization")?;
        let (credential, signed_headers, signature, amz_date, expires_seconds) =
            if let Some(authorization) = authorization {
                let (algorithm, fields) = authorization
                    .split_once(' ')
                    .ok_or("SigV4 Authorization header is invalid")?;
                if algorithm != "AWS4-HMAC-SHA256" {
                    return Err("SigV4 algorithm is unsupported");
                }
                let (credential, signed_headers, signature) = parse_authorization_fields(fields)?;
                (
                    credential,
                    signed_headers,
                    signature,
                    request
                        .header("x-amz-date")?
                        .ok_or("SigV4 x-amz-date header is missing")?
                        .to_owned(),
                    None,
                )
            } else {
                let get = |name: &str| query_value(&query, name);
                if get("X-Amz-Algorithm")? != "AWS4-HMAC-SHA256" {
                    return Err("SigV4 algorithm is unsupported");
                }
                (
                    get("X-Amz-Credential")?.to_owned(),
                    get("X-Amz-SignedHeaders")?.to_owned(),
                    get("X-Amz-Signature")?.to_owned(),
                    get("X-Amz-Date")?.to_owned(),
                    Some(
                        get("X-Amz-Expires")?
                            .parse::<u64>()
                            .map_err(|_| "SigV4 expiry is invalid")?,
                    ),
                )
            };
        if signature.len() != 64 || !signature.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err("SigV4 signature is invalid");
        }
        let parts = credential.split('/').collect::<Vec<_>>();
        if parts.len() != 5 || parts[4] != "aws4_request" || parts[0].is_empty() {
            return Err("SigV4 credential scope is invalid");
        }
        if amz_date.len() != 16
            || amz_date.as_bytes().get(8) != Some(&b'T')
            || amz_date.as_bytes().get(15) != Some(&b'Z')
            || parts[1] != &amz_date[..8]
        {
            return Err("SigV4 request date is invalid");
        }
        Ok(Self {
            access_key_id: parts[0].to_owned(),
            scope_date: parts[1].to_owned(),
            region: parts[2].to_owned(),
            service: parts[3].to_owned(),
            signed_headers: signed_headers.to_ascii_lowercase(),
            signature: signature.to_ascii_lowercase(),
            amz_date,
            expires_seconds,
        })
    }

    fn has_signed_header(&self, name: &str) -> bool {
        self.signed_headers
            .split(';')
            .any(|candidate| candidate == name)
    }
}

fn parse_authorization_fields(value: &str) -> Result<(String, String, String), &'static str> {
    let mut credential = None;
    let mut signed_headers = None;
    let mut signature = None;
    for part in value.split(',') {
        let (name, value) = part
            .trim()
            .split_once('=')
            .ok_or("SigV4 Authorization fields are invalid")?;
        let slot = match name {
            "Credential" => &mut credential,
            "SignedHeaders" => &mut signed_headers,
            "Signature" => &mut signature,
            _ => return Err("SigV4 Authorization contains an unknown field"),
        };
        if slot.replace(value.to_owned()).is_some() {
            return Err("SigV4 Authorization field was repeated");
        }
    }
    Ok((
        credential.ok_or("SigV4 Credential is missing")?,
        signed_headers.ok_or("SigV4 SignedHeaders is missing")?,
        signature.ok_or("SigV4 Signature is missing")?,
    ))
}

fn parse_query_pairs(value: &str) -> Result<Vec<(String, String)>, &'static str> {
    value
        .split('&')
        .filter(|part| !part.is_empty())
        .map(|part| {
            let (name, value) = part.split_once('=').unwrap_or((part, ""));
            Ok((
                percent_decode_str(name)
                    .decode_utf8()
                    .map_err(|_| "SigV4 query name is not UTF-8")?
                    .into_owned(),
                percent_decode_str(value)
                    .decode_utf8()
                    .map_err(|_| "SigV4 query value is not UTF-8")?
                    .into_owned(),
            ))
        })
        .collect()
}

fn query_value<'a>(pairs: &'a [(String, String)], name: &str) -> Result<&'a str, &'static str> {
    let values = pairs
        .iter()
        .filter(|(candidate, _)| candidate.eq_ignore_ascii_case(name))
        .map(|(_, value)| value.as_str())
        .collect::<Vec<_>>();
    match values.as_slice() {
        [value] => Ok(*value),
        [] => Err("SigV4 presign field is missing"),
        _ => Err("SigV4 presign field was repeated"),
    }
}

fn canonical_signed_headers(
    request: &S3SigV4Request,
    names: &str,
) -> Result<(String, String), &'static str> {
    let names = names.split(';').collect::<Vec<_>>();
    if names.is_empty() || !names.contains(&"host") {
        return Err("SigV4 host header must be signed");
    }
    let mut seen = BTreeSet::new();
    let mut canonical = String::new();
    for name in &names {
        if name.is_empty()
            || !name.bytes().all(is_lowercase_header_name_byte)
            || !seen.insert(*name)
        {
            return Err("SigV4 signed header list is invalid");
        }
        let values = request
            .headers
            .iter()
            .filter(|(candidate, _)| candidate.eq_ignore_ascii_case(name))
            .map(|(_, value)| collapse_ascii_whitespace(value))
            .collect::<Vec<_>>();
        if values.is_empty() {
            return Err("SigV4 signed header is missing");
        }
        canonical.push_str(name);
        canonical.push(':');
        canonical.push_str(&values.join(","));
        canonical.push('\n');
    }
    Ok((canonical, names.join(";")))
}

fn is_lowercase_header_name_byte(byte: u8) -> bool {
    byte.is_ascii_lowercase()
        || byte.is_ascii_digit()
        || matches!(
            byte,
            b'!' | b'#'
                | b'$'
                | b'%'
                | b'&'
                | b'\''
                | b'*'
                | b'+'
                | b'-'
                | b'.'
                | b'^'
                | b'_'
                | b'`'
                | b'|'
                | b'~'
        )
}

fn collapse_ascii_whitespace(value: &str) -> String {
    value.split_ascii_whitespace().collect::<Vec<_>>().join(" ")
}

fn canonical_query(pairs: &[(String, String)], presigned: bool) -> String {
    let mut encoded = pairs
        .iter()
        .filter(|(name, _)| !presigned || !name.eq_ignore_ascii_case("X-Amz-Signature"))
        .map(|(name, value)| (aws_encode(name, true), aws_encode(value, true)))
        .collect::<Vec<_>>();
    encoded.sort();
    encoded
        .into_iter()
        .map(|(name, value)| format!("{name}={value}"))
        .collect::<Vec<_>>()
        .join("&")
}

fn aws_encode(value: &str, encode_slash: bool) -> String {
    let mut output = String::new();
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
            output.push(char::from(byte));
        } else if byte == b'/' && !encode_slash {
            output.push('/');
        } else {
            output.push_str(&format!("%{byte:02X}"));
        }
    }
    output
}

fn derive_signing_key(secret: &[u8], date: &str, region: &str, service: &str) -> [u8; 32] {
    let mut seed = b"AWS4".to_vec();
    seed.extend_from_slice(secret);
    let date_key = hmac_sha256(&seed, date.as_bytes());
    let region_key = hmac_sha256(&date_key, region.as_bytes());
    let service_key = hmac_sha256(&region_key, service.as_bytes());
    hmac_sha256(&service_key, b"aws4_request")
}

fn hmac_sha256(key: &[u8], value: &[u8]) -> [u8; 32] {
    let key = hmac::Key::new(hmac::HMAC_SHA256, key);
    hmac::sign(&key, value)
        .as_ref()
        .try_into()
        .expect("SHA-256 output has a fixed length")
}

fn hex_hmac(key: &[u8], value: &[u8]) -> String {
    hex_bytes(&hmac_sha256(key, value))
}

fn hex_digest(value: &[u8]) -> String {
    hex_bytes(digest::digest(&digest::SHA256, value).as_ref())
}

fn hex_bytes(value: &[u8]) -> String {
    value.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    left.iter()
        .zip(right)
        .fold(0_u8, |difference, (left, right)| {
            difference | (left ^ right)
        })
        == 0
}

fn parse_amz_date(value: &str) -> Result<u64, &'static str> {
    if value.len() != 16 || value.as_bytes()[8] != b'T' || value.as_bytes()[15] != b'Z' {
        return Err("SigV4 date is invalid");
    }
    let year = value[0..4]
        .parse::<i32>()
        .map_err(|_| "SigV4 date is invalid")?;
    let month = value[4..6]
        .parse::<u8>()
        .map_err(|_| "SigV4 date is invalid")?;
    let day = value[6..8]
        .parse::<u8>()
        .map_err(|_| "SigV4 date is invalid")?;
    let hour = value[9..11]
        .parse::<u8>()
        .map_err(|_| "SigV4 date is invalid")?;
    let minute = value[11..13]
        .parse::<u8>()
        .map_err(|_| "SigV4 date is invalid")?;
    let second = value[13..15]
        .parse::<u8>()
        .map_err(|_| "SigV4 date is invalid")?;
    if hour > 23 || minute > 59 || second > 59 || day > days_in_month(year, month) {
        return Err("SigV4 date is invalid");
    }
    let days = days_from_civil(year, month, day).ok_or("SigV4 date is invalid")?;
    u64::try_from(
        days.saturating_mul(86_400)
            + i64::from(hour) * 3_600
            + i64::from(minute) * 60
            + i64::from(second),
    )
    .map_err(|_| "SigV4 date is invalid")
}

fn days_in_month(year: i32, month: u8) -> u8 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if year % 4 == 0 && (year % 100 != 0 || year % 400 == 0) => 29,
        2 => 28,
        _ => 0,
    }
}

fn days_from_civil(year: i32, month: u8, day: u8) -> Option<i64> {
    if !(1..=12).contains(&month) || day == 0 || day > days_in_month(year, month) {
        return None;
    }
    let year = i64::from(year) - i64::from(month <= 2);
    let era = (if year >= 0 { year } else { year - 399 }) / 400;
    let year_of_era = year - era * 400;
    let month = i64::from(month);
    let day_of_year = (153 * (month + if month > 2 { -3 } else { 9 }) + 2) / 5 + i64::from(day) - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    Some(era * 146_097 + day_of_era - 719_468)
}

/// Central's immutable authorization proof for one S3 read.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct S3ReadTicket {
    pub ticket_id: String,
    pub tenant_id: String,
    pub project_id: String,
    pub artifact_id: String,
    pub snapshot_id: String,
    pub snapshot_lifecycle_generation: LifecycleGeneration,
    #[schemars(with = "String")]
    pub commit_id: ContentDigest,
    #[schemars(with = "String")]
    pub index_digest: ContentDigest,
    pub bucket: String,
    pub access_point_policy_generation: ResourceVersion,
    pub logical_path: String,
    #[schemars(with = "String")]
    pub manifest_id: ContentDigest,
    pub size_bytes: u64,
    pub allowed_start: u64,
    pub allowed_end_exclusive: u64,
    pub gateway_pool_id: String,
    /// Central-authoritative owner route captured when the ticket is issued. The ingress Gateway
    /// may use exactly this endpoint for one peer hop; the owner Replica must match the remaining
    /// fields against its current local RouteLease before forwarding the ticket to the Agent.
    pub owner_replica_id: GatewayReplicaId,
    pub owner_peer_endpoint: String,
    pub agent_connection_id: GatewayConnectionId,
    pub route_generation: RouteGeneration,
    pub agent_id: AgentId,
    pub owner_generation: OwnerGeneration,
    pub mount_generation: MountGeneration,
    pub session_generation: SessionGeneration,
    pub issued_at_unix_ms: UnixMillis,
    pub expires_at_unix_ms: UnixMillis,
    /// Ed25519 signature bytes encoded by Central's signing adapter.
    pub signature: GatewayOpaqueBytes,
}

impl S3ReadTicket {
    /// Returns the exact bytes Central signs.  The signature is excluded from the signed view,
    /// which prevents a ticket from becoming self-referential while keeping every routing and
    /// generation fence in the authenticated payload.
    pub fn signing_bytes(&self) -> Result<Vec<u8>, &'static str> {
        let mut unsigned = self.clone();
        unsigned.signature = GatewayOpaqueBytes::new(Vec::new()).map_err(|_| "ticket signature")?;
        domain_separated_jcs_bytes("neoengram-s3-read-ticket-v1", &unsigned)
            .map_err(|_| "ticket signing bytes")
    }

    /// Encodes a Central signed envelope into the opaque ticket signature field.  Agents decode
    /// and verify this envelope against their generation-bound Central trust bundle.
    pub fn with_central_signature(
        mut self,
        signed: &CentralSignedPayload,
    ) -> Result<Self, &'static str> {
        let payload = self.signing_bytes()?;
        if signed.payload.as_bytes() != payload {
            return Err("Central S3 ticket signature payload does not match the ticket");
        }
        let encoded = serde_json::to_vec(signed).map_err(|_| "ticket signature encoding")?;
        self.signature = GatewayOpaqueBytes::new(encoded).map_err(|_| "ticket signature")?;
        Ok(self)
    }

    /// Decodes the Central signed envelope carried by the ticket.  Cryptographic verification is
    /// intentionally performed by the Agent runtime, which owns the trust bundle.
    pub fn central_signature(&self) -> Result<CentralSignedPayload, &'static str> {
        if self.signature.as_bytes().is_empty() {
            return Err("S3 read ticket has no Central signature");
        }
        serde_json::from_slice(self.signature.as_bytes())
            .map_err(|_| "S3 read ticket Central signature is invalid")
    }

    pub fn validate_at(&self, now_unix_ms: UnixMillis) -> Result<(), &'static str> {
        if self.ticket_id.is_empty()
            || self.tenant_id.is_empty()
            || self.project_id.is_empty()
            || self.artifact_id.is_empty()
            || self.snapshot_id.is_empty()
            || self.snapshot_lifecycle_generation.get() == 0
            || self.bucket.is_empty()
            || self.access_point_policy_generation.get() == 0
            || self.logical_path.is_empty()
            || self.gateway_pool_id.is_empty()
            || self.owner_peer_endpoint.is_empty()
            || self.route_generation.get() == 0
            || self.owner_generation.get() == 0
            || self.mount_generation.get() == 0
            || self.session_generation.get() == 0
            || self.issued_at_unix_ms.get() == 0
            || self.expires_at_unix_ms.get() <= self.issued_at_unix_ms.get()
            || now_unix_ms.get() < self.issued_at_unix_ms.get()
            || now_unix_ms.get() >= self.expires_at_unix_ms.get()
            || self.allowed_start > self.allowed_end_exclusive
            || self.allowed_end_exclusive > self.size_bytes
            || self.signature.as_bytes().is_empty()
        {
            return Err("S3 read ticket is invalid or expired");
        }
        if serde_json::to_vec(self)
            .map(|bytes| bytes.len() > S3_READ_TICKET_MAX_BYTES)
            .unwrap_or(true)
        {
            return Err("S3 read ticket exceeds its size limit");
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct S3ReadOpen {
    pub stream_id: String,
    pub ticket: S3ReadTicket,
    pub start: u64,
    pub end_exclusive: u64,
}

/// First frame sent by an Agent on the dedicated S3 read channel.  The mTLS certificate remains
/// the source of the Agent identity; these fields are a replay/fencing binding, not credentials.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct S3ReadChannelHello {
    pub agent_id: AgentId,
    pub session_generation: SessionGeneration,
}

/// Gateway acknowledgement for the exact session generation accepted into its read-channel
/// registry.  No object data can be sent before this frame is observed by the Agent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct S3ReadChannelReady {
    pub agent_id: AgentId,
    pub session_generation: SessionGeneration,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct S3ReadHead {
    pub stream_id: String,
    pub size_bytes: u64,
    pub last_modified_unix_ms: UnixMillis,
    pub etag: ContentDigest,
    pub content_type: u16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct S3ReadData {
    pub stream_id: String,
    pub offset: u64,
    pub bytes: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct S3ReadEnd {
    pub stream_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct S3ReadCancel {
    pub stream_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct S3ReadError {
    pub stream_id: String,
    pub code: String,
    pub detail: String,
}

/// One frame on the Gateway/Agent binary read stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum S3ReadFrame {
    Open(Box<S3ReadOpen>),
    Head(S3ReadHead),
    Data(S3ReadData),
    End(S3ReadEnd),
    Cancel(S3ReadCancel),
    Error(S3ReadError),
}

/// Transport envelope for the dedicated Agent/Gateway channel.  The nested Read frame retains the
/// existing 256 KiB data-frame limit and ticket validation rules.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum S3ReadChannelFrame {
    Hello(S3ReadChannelHello),
    Ready(S3ReadChannelReady),
    Read(S3ReadFrame),
}

impl S3ReadChannelFrame {
    const HELLO: u8 = 0x11;
    const READY: u8 = 0x12;
    const READ: u8 = 0x20;

    /// Encodes one complete channel frame.  Each invocation produces one bounded length-delimited
    /// frame; callers may concatenate frames in one HTTP/2 DATA chunk.
    pub fn encode(&self) -> Result<Vec<u8>, &'static str> {
        let (kind, payload) = match self {
            Self::Hello(value) => (
                Self::HELLO,
                serde_json::to_vec(value).map_err(|_| "encode S3 channel hello")?,
            ),
            Self::Ready(value) => (
                Self::READY,
                serde_json::to_vec(value).map_err(|_| "encode S3 channel ready")?,
            ),
            Self::Read(value) => (Self::READ, value.encode()?),
        };
        if payload.len() > S3_READ_FRAME_MAX_BYTES + S3_READ_TICKET_MAX_BYTES {
            return Err("S3 channel frame exceeds its size limit");
        }
        let length = u32::try_from(payload.len()).map_err(|_| "S3 channel frame is too large")?;
        let mut output = Vec::with_capacity(FRAME_HEADER_BYTES + payload.len());
        output.push(kind);
        output.extend_from_slice(&length.to_be_bytes());
        output.extend_from_slice(&payload);
        Ok(output)
    }

    /// Decodes exactly one complete frame and rejects trailing bytes.
    pub fn decode(input: &[u8]) -> Result<Self, &'static str> {
        if input.len() < FRAME_HEADER_BYTES {
            return Err("S3 channel frame header is truncated");
        }
        let length = u32::from_be_bytes(
            input[1..5]
                .try_into()
                .map_err(|_| "S3 channel frame length")?,
        ) as usize;
        if length > S3_READ_FRAME_MAX_BYTES + S3_READ_TICKET_MAX_BYTES
            || input.len() != FRAME_HEADER_BYTES + length
        {
            return Err("S3 channel frame length is invalid");
        }
        let payload = &input[FRAME_HEADER_BYTES..];
        match input[0] {
            Self::HELLO => serde_json::from_slice(payload)
                .map(Self::Hello)
                .map_err(|_| "invalid S3 channel hello"),
            Self::READY => serde_json::from_slice(payload)
                .map(Self::Ready)
                .map_err(|_| "invalid S3 channel ready"),
            Self::READ => S3ReadFrame::decode(payload).map(Self::Read),
            _ => Err("unknown S3 channel frame type"),
        }
    }
}

/// Incremental decoder for HTTP/2 DATA chunks.  H2 boundaries are deliberately ignored: callers
/// feed arbitrary chunks and receive only complete protocol frames.
#[derive(Debug, Default)]
pub struct S3ReadChannelDecoder {
    buffer: Vec<u8>,
}

impl S3ReadChannelDecoder {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push(&mut self, chunk: &[u8]) -> Result<Vec<S3ReadChannelFrame>, &'static str> {
        if self.buffer.len().saturating_add(chunk.len())
            > (S3_READ_FRAME_MAX_BYTES + S3_READ_TICKET_MAX_BYTES + FRAME_HEADER_BYTES) * 2
        {
            return Err("S3 channel decoder buffer exceeds its bound");
        }
        self.buffer.extend_from_slice(chunk);
        let mut frames = Vec::new();
        loop {
            if self.buffer.len() < FRAME_HEADER_BYTES {
                break;
            }
            let length = u32::from_be_bytes(
                self.buffer[1..5]
                    .try_into()
                    .map_err(|_| "S3 channel frame length")?,
            ) as usize;
            if length > S3_READ_FRAME_MAX_BYTES + S3_READ_TICKET_MAX_BYTES {
                return Err("S3 channel frame length is invalid");
            }
            let total = FRAME_HEADER_BYTES + length;
            if self.buffer.len() < total {
                break;
            }
            let frame = S3ReadChannelFrame::decode(&self.buffer[..total])?;
            self.buffer.drain(..total);
            frames.push(frame);
        }
        Ok(frames)
    }

    pub fn finish(&self) -> Result<(), &'static str> {
        if self.buffer.is_empty() {
            Ok(())
        } else {
            Err("S3 channel ended with a truncated frame")
        }
    }
}

impl S3ReadFrame {
    const OPEN: u8 = 1;
    const HEAD: u8 = 2;
    const DATA: u8 = 3;
    const END: u8 = 4;
    const CANCEL: u8 = 5;
    const ERROR: u8 = 6;

    /// Encodes one length-delimited frame. Data frames are capped at 256 KiB.
    pub fn encode(&self) -> Result<Vec<u8>, &'static str> {
        let (kind, payload) = match self {
            Self::Open(value) => (Self::OPEN, serde_json::to_vec(value).map_err(|_| "encode")?),
            Self::Head(value) => (Self::HEAD, serde_json::to_vec(value).map_err(|_| "encode")?),
            Self::Data(value) => {
                if value.bytes.len() > S3_READ_FRAME_MAX_BYTES {
                    return Err("S3 read data frame exceeds 256 KiB");
                }
                let stream = value.stream_id.as_bytes();
                if stream.len() > u16::MAX as usize {
                    return Err("S3 read stream ID is too long");
                }
                let mut payload = Vec::with_capacity(2 + stream.len() + 8 + value.bytes.len());
                payload.extend_from_slice(&(stream.len() as u16).to_be_bytes());
                payload.extend_from_slice(stream);
                payload.extend_from_slice(&value.offset.to_be_bytes());
                payload.extend_from_slice(&value.bytes);
                (Self::DATA, payload)
            }
            Self::End(value) => (Self::END, serde_json::to_vec(value).map_err(|_| "encode")?),
            Self::Cancel(value) => (
                Self::CANCEL,
                serde_json::to_vec(value).map_err(|_| "encode")?,
            ),
            Self::Error(value) => (
                Self::ERROR,
                serde_json::to_vec(value).map_err(|_| "encode")?,
            ),
        };
        if payload.len() > S3_READ_FRAME_MAX_BYTES + S3_READ_TICKET_MAX_BYTES {
            return Err("S3 read frame exceeds its size limit");
        }
        let length = u32::try_from(payload.len()).map_err(|_| "S3 read frame is too large")?;
        let mut output = Vec::with_capacity(FRAME_HEADER_BYTES + payload.len());
        output.push(kind);
        output.extend_from_slice(&length.to_be_bytes());
        output.extend_from_slice(&payload);
        Ok(output)
    }

    /// Decodes exactly one frame and rejects trailing bytes or oversized payloads.
    pub fn decode(input: &[u8]) -> Result<Self, &'static str> {
        if input.len() < FRAME_HEADER_BYTES {
            return Err("S3 read frame header is truncated");
        }
        let kind = input[0];
        let length =
            u32::from_be_bytes(input[1..5].try_into().map_err(|_| "frame length")?) as usize;
        if length > S3_READ_FRAME_MAX_BYTES + S3_READ_TICKET_MAX_BYTES
            || input.len() != FRAME_HEADER_BYTES + length
        {
            return Err("S3 read frame length is invalid");
        }
        let payload = &input[FRAME_HEADER_BYTES..];
        match kind {
            Self::OPEN => serde_json::from_slice(payload)
                .map(Box::new)
                .map(Self::Open)
                .map_err(|_| "invalid ReadOpen"),
            Self::HEAD => serde_json::from_slice(payload)
                .map(Self::Head)
                .map_err(|_| "invalid ReadHead"),
            Self::DATA => decode_data(payload).map(Self::Data),
            Self::END => serde_json::from_slice(payload)
                .map(Self::End)
                .map_err(|_| "invalid ReadEnd"),
            Self::CANCEL => serde_json::from_slice(payload)
                .map(Self::Cancel)
                .map_err(|_| "invalid ReadCancel"),
            Self::ERROR => serde_json::from_slice(payload)
                .map(Self::Error)
                .map_err(|_| "invalid ReadError"),
            _ => Err("unknown S3 read frame type"),
        }
    }
}

fn decode_data(payload: &[u8]) -> Result<S3ReadData, &'static str> {
    if payload.len() < 10 {
        return Err("ReadData frame is truncated");
    }
    let stream_len =
        u16::from_be_bytes(payload[..2].try_into().map_err(|_| "stream length")?) as usize;
    let stream_end = 2 + stream_len;
    if payload.len() < stream_end + 8 || stream_len == 0 {
        return Err("ReadData stream ID is invalid");
    }
    let offset = u64::from_be_bytes(
        payload[stream_end..stream_end + 8]
            .try_into()
            .map_err(|_| "offset")?,
    );
    let bytes = &payload[stream_end + 8..];
    if bytes.len() > S3_READ_FRAME_MAX_BYTES {
        return Err("ReadData exceeds 256 KiB");
    }
    Ok(S3ReadData {
        stream_id: String::from_utf8(payload[2..stream_end].to_vec())
            .map_err(|_| "ReadData stream ID is not UTF-8")?,
        offset,
        bytes: bytes.to_vec(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn signed_request(
        method: &str,
        path: &str,
        query: &str,
        host: &str,
        extra_headers: &[(&str, &str)],
    ) -> S3SigV4Request {
        let mut headers = vec![("host".to_owned(), host.to_owned())];
        headers.extend(
            extra_headers
                .iter()
                .map(|(name, value)| ((*name).to_owned(), (*value).to_owned())),
        );
        headers.push(("x-amz-date".to_owned(), "20260817T000000Z".to_owned()));
        let mut signed_headers = headers
            .iter()
            .map(|(name, _)| name.to_ascii_lowercase())
            .collect::<Vec<_>>();
        signed_headers.sort();
        headers.push((
            "authorization".to_owned(),
            format!(
                "AWS4-HMAC-SHA256 Credential=NGS3EXAMPLE/20260817/us-east-1/s3/aws4_request,SignedHeaders={},Signature={}",
                signed_headers.join(";"),
                "0".repeat(64)
            ),
        ));
        S3SigV4Request {
            method: method.to_owned(),
            path: path.to_owned(),
            query: query.to_owned(),
            headers,
        }
    }

    #[test]
    fn authorization_body_cannot_replace_a_path_style_object_or_conditions() {
        let request = S3AuthorizeRequest {
            gateway_pool_id: "gateway-pool-a".to_owned(),
            bucket: "dataset-a".to_owned(),
            operation: S3AuthorizeOperation::GetObject {
                key: "folder/file.csv".to_owned(),
                range: Some("bytes=10-19".to_owned()),
                if_match: Some("\"manifest-a\"".to_owned()),
                if_none_match: None,
            },
            sigv4: signed_request(
                "GET",
                "/dataset-a/folder%2Ffile.csv",
                "response-content-disposition=attachment",
                "s3.example.test:443",
                &[("range", "bytes=10-19"), ("if-match", "\"manifest-a\"")],
            ),
        };
        assert!(request.validate_signed_operation_binding().is_ok());

        let mut replaced_key = request.clone();
        let S3AuthorizeOperation::GetObject { key, .. } = &mut replaced_key.operation else {
            unreachable!();
        };
        *key = "folder/other.csv".to_owned();
        assert!(replaced_key.validate_signed_operation_binding().is_err());

        let mut expanded_range = request;
        let S3AuthorizeOperation::GetObject { range, .. } = &mut expanded_range.operation else {
            unreachable!();
        };
        *range = Some("bytes=0-99".to_owned());
        assert!(expanded_range.validate_signed_operation_binding().is_err());

        let mut unsigned_range = S3AuthorizeRequest {
            gateway_pool_id: "gateway-pool-a".to_owned(),
            bucket: "dataset-a".to_owned(),
            operation: S3AuthorizeOperation::GetObject {
                key: "folder/file.csv".to_owned(),
                range: Some("bytes=10-19".to_owned()),
                if_match: None,
                if_none_match: None,
            },
            sigv4: signed_request(
                "GET",
                "/dataset-a/folder/file.csv",
                "",
                "s3.example.test",
                &[],
            ),
        };
        unsigned_range
            .sigv4
            .headers
            .push(("range".to_owned(), "bytes=10-19".to_owned()));
        assert!(unsigned_range.validate_signed_operation_binding().is_ok());

        let mut aliased_path = unsigned_range;
        aliased_path.sigv4.path = "//dataset-a/folder/file.csv".to_owned();
        assert!(aliased_path.validate_signed_operation_binding().is_err());
    }

    #[test]
    fn authorization_body_cannot_replace_a_virtual_hosted_list_scope() {
        let request = S3AuthorizeRequest {
            gateway_pool_id: "gateway-pool-a".to_owned(),
            bucket: "dataset-a".to_owned(),
            operation: S3AuthorizeOperation::ListObjectsV2 {
                prefix: "year/2026/".to_owned(),
                delimiter: Some("/".to_owned()),
                continuation_token: Some("cursor-a".to_owned()),
                start_after: None,
                max_keys: 25,
            },
            sigv4: signed_request(
                "GET",
                "/",
                "list-type=2&prefix=year%2F2026%2F&delimiter=%2F&continuation-token=cursor-a&max-keys=25&encoding-type=url",
                "dataset-a.s3.example.test",
                &[],
            ),
        };
        assert!(request.validate_signed_operation_binding().is_ok());

        let mut replaced_prefix = request.clone();
        let S3AuthorizeOperation::ListObjectsV2 { prefix, .. } = &mut replaced_prefix.operation
        else {
            unreachable!();
        };
        *prefix = "private/".to_owned();
        assert!(replaced_prefix.validate_signed_operation_binding().is_err());

        let mut replaced_bucket = request;
        replaced_bucket.bucket = "dataset-b".to_owned();
        assert!(replaced_bucket.validate_signed_operation_binding().is_err());
    }

    #[test]
    fn head_object_range_and_conditions_are_bound_to_forwarded_headers() {
        let request = S3AuthorizeRequest {
            gateway_pool_id: "gateway-pool-a".to_owned(),
            bucket: "dataset-a".to_owned(),
            operation: S3AuthorizeOperation::HeadObject {
                key: "folder/file.csv".to_owned(),
                range: Some("bytes=0-9".to_owned()),
                if_match: None,
                if_none_match: Some("\"manifest-a\"".to_owned()),
            },
            sigv4: signed_request(
                "HEAD",
                "/dataset-a/folder/file.csv",
                "",
                "s3.example.test",
                &[("range", "bytes=0-9"), ("if-none-match", "\"manifest-a\"")],
            ),
        };
        assert!(request.validate_signed_operation_binding().is_ok());

        let mut expanded = request;
        let S3AuthorizeOperation::HeadObject { range, .. } = &mut expanded.operation else {
            unreachable!();
        };
        *range = Some("bytes=0-99".to_owned());
        assert!(expanded.validate_signed_operation_binding().is_err());
    }

    #[test]
    fn bucket_location_requires_the_signed_location_query() {
        let mut request = S3AuthorizeRequest {
            gateway_pool_id: "gateway-pool-a".to_owned(),
            bucket: "dataset-a".to_owned(),
            operation: S3AuthorizeOperation::GetBucketLocation,
            sigv4: signed_request("GET", "/dataset-a", "location=", "s3.example.test", &[]),
        };
        assert!(request.validate_signed_operation_binding().is_ok());
        request.sigv4.query.clear();
        assert!(request.validate_signed_operation_binding().is_err());
    }

    #[test]
    fn canonical_query_only_removes_the_presigned_signature_field() {
        let pairs = vec![
            ("X-Amz-Signature".to_owned(), "abc123".to_owned()),
            ("prefix".to_owned(), "folder/one".to_owned()),
        ];

        assert_eq!(
            canonical_query(&pairs, false),
            "X-Amz-Signature=abc123&prefix=folder%2Fone"
        );
        assert_eq!(canonical_query(&pairs, true), "prefix=folder%2Fone");
    }

    #[test]
    fn header_sigv4_requires_the_request_timestamp_to_be_signed() {
        let request = S3SigV4Request {
            method: "GET".to_owned(),
            path: "/dataset-a/file.txt".to_owned(),
            query: String::new(),
            headers: vec![
                ("host".to_owned(), "s3.example.test".to_owned()),
                ("x-amz-date".to_owned(), "20260817T000000Z".to_owned()),
                (
                    "authorization".to_owned(),
                    format!(
                        "AWS4-HMAC-SHA256 Credential=NGS3EXAMPLE/20260817/us-east-1/s3/aws4_request,SignedHeaders=host,Signature={}",
                        "0".repeat(64)
                    ),
                ),
            ],
        };

        assert_eq!(
            request.verify_at(b"secret", 1_786_924_800).unwrap_err(),
            "SigV4 x-amz-date header must be signed"
        );
    }

    #[test]
    fn verifies_aws_s3_get_object_authorization_example() {
        // AWS S3 Signature Version 4 documentation: GET /test.txt with a byte range.
        // Keeping the published signature literal makes this an external golden vector rather
        // than checking the verifier against another copy of our own signing implementation.
        let request = S3SigV4Request {
            method: "GET".to_owned(),
            path: "/test.txt".to_owned(),
            query: String::new(),
            headers: vec![
                (
                    "host".to_owned(),
                    "examplebucket.s3.amazonaws.com".to_owned(),
                ),
                ("range".to_owned(), "bytes=0-9".to_owned()),
                (
                    "x-amz-content-sha256".to_owned(),
                    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855".to_owned(),
                ),
                ("x-amz-date".to_owned(), "20130524T000000Z".to_owned()),
                (
                    "authorization".to_owned(),
                    concat!(
                        "AWS4-HMAC-SHA256 ",
                        "Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request,",
                        "SignedHeaders=host;range;x-amz-content-sha256;x-amz-date,",
                        "Signature=f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41"
                    )
                    .to_owned(),
                ),
            ],
        };

        let claims = request
            .verify_at(b"wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", 1_369_353_600)
            .unwrap();
        assert_eq!(claims.access_key_id, "AKIAIOSFODNN7EXAMPLE");
        assert_eq!(claims.region, "us-east-1");
        assert_eq!(claims.service, "s3");
    }

    #[test]
    fn s3_read_channel_decoder_handles_fragmented_and_concatenated_frames() {
        let hello = S3ReadChannelFrame::Hello(S3ReadChannelHello {
            agent_id: AgentId::new("agent-a").unwrap(),
            session_generation: SessionGeneration::new(7),
        })
        .encode()
        .unwrap();
        let data = S3ReadChannelFrame::Read(S3ReadFrame::Data(S3ReadData {
            stream_id: "stream-a".to_owned(),
            offset: 0,
            bytes: b"hello".to_vec(),
        }))
        .encode()
        .unwrap();
        let mut decoder = S3ReadChannelDecoder::new();
        let mut joined = hello.clone();
        joined.extend_from_slice(&data);
        assert!(decoder.push(&joined[..3]).unwrap().is_empty());
        let frames = decoder.push(&joined[3..]).unwrap();
        assert_eq!(frames.len(), 2);
        assert!(matches!(frames[0], S3ReadChannelFrame::Hello(_)));
        assert!(matches!(
            frames[1],
            S3ReadChannelFrame::Read(S3ReadFrame::Data(_))
        ));
        decoder.finish().unwrap();
    }

    #[test]
    fn s3_read_channel_decoder_rejects_oversized_frame() {
        let mut decoder = S3ReadChannelDecoder::new();
        let oversized = [0x20_u8, 0xff, 0xff, 0xff, 0xff];
        assert!(decoder.push(&oversized).is_err());
    }
}
