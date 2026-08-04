use std::{
    collections::{BTreeMap, BTreeSet, HashSet},
    fmt,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use fusen_rs::{Context, Interceptor, InterceptorFuture, Next};
use http::{header::AUTHORIZATION, HeaderMap};
use jsonwebtoken::{
    decode, decode_header,
    jwk::{AlgorithmParameters, Jwk, JwkSet, KeyOperations, PublicKeyUse},
    Algorithm, DecodingKey, Validation,
};
use neoengram_protocol::{PrincipalId, PrincipalKind, PrincipalRef, TenantId};
use neoengramd::{
    Action, Actor, AuthorizationRequest, Authorizer, CentralError, CentralErrorCode, CentralResult,
};
use serde::Deserialize;
use tokio::sync::{Mutex, RwLock};
use url::Url;

use crate::error::{api_version_unsupported, application_error, unauthenticated};

const API_VERSION_HEADER: &str = "neoengram-api-version";
const MAX_BEARER_BYTES: usize = 16 * 1024;
const MAX_OIDC_DOCUMENT_BYTES: u64 = 1024 * 1024;
const MAX_TOKEN_LIFETIME_SECS: u64 = 60 * 60;
const MAX_MISSING_KIDS: usize = 256;
const UNKNOWN_KID_REFRESH_COOLDOWN: Duration = Duration::from_secs(30);

/// Authenticated identity inserted into Fusen call extensions.
#[derive(Clone)]
pub struct AuthenticatedIdentity {
    principal: PrincipalRef,
    issuer: Arc<str>,
    subject: Arc<str>,
}

/// Request-scoped canonical principal injected by the authentication interceptor.
pub type PrincipalContext = AuthenticatedIdentity;

impl AuthenticatedIdentity {
    /// Creates a validated identity.
    pub fn new(
        principal_id: impl Into<String>,
        kind: PrincipalKind,
        issuer: impl Into<Arc<str>>,
        subject: impl Into<Arc<str>>,
    ) -> Result<Self, AuthenticationFailure> {
        let principal_id = PrincipalId::new(principal_id.into())
            .map_err(|_| AuthenticationFailure::Invalid("authenticated principal ID is invalid"))?;
        Ok(Self {
            principal: PrincipalRef {
                kind,
                id: principal_id,
                extensions: Default::default(),
            },
            issuer: issuer.into(),
            subject: subject.into(),
        })
    }

    /// Returns the canonical domain principal.
    pub const fn principal(&self) -> &PrincipalRef {
        &self.principal
    }

    /// Returns the verified issuer identifier.
    pub fn issuer(&self) -> &str {
        &self.issuer
    }

    /// Returns the verified OIDC subject.
    pub fn subject(&self) -> &str {
        &self.subject
    }
}

impl fmt::Debug for AuthenticatedIdentity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AuthenticatedIdentity")
            .field("principal_id", &self.principal.id.as_str())
            .field("principal_kind", &self.principal.kind)
            .field("issuer", &self.issuer)
            .field("subject", &"<redacted>")
            .finish()
    }
}

/// Authentication failure classification used by the head interceptor.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum AuthenticationFailure {
    #[error("Bearer authentication is required")]
    Missing,
    #[error("Bearer token is invalid: {0}")]
    Invalid(&'static str),
    #[error("identity provider is temporarily unavailable: {0}")]
    Unavailable(&'static str),
}

/// Pluggable Bearer-token authentication boundary.
#[async_trait]
pub trait Authenticator: Send + Sync + 'static {
    /// Authenticates application headers and returns a canonical identity.
    async fn authenticate(
        &self,
        headers: &HeaderMap,
    ) -> Result<AuthenticatedIdentity, AuthenticationFailure>;
}

/// Development-only fixed token authenticator.
pub struct StaticTokenAuthenticator {
    token: Vec<u8>,
    identity: AuthenticatedIdentity,
}

impl StaticTokenAuthenticator {
    /// Creates a fixed-token authenticator. Runtime configuration restricts it to loopback dev mode.
    pub fn new(token: impl Into<String>, identity: AuthenticatedIdentity) -> Result<Self, String> {
        let token = token.into().into_bytes();
        if token.is_empty() || token.len() > MAX_BEARER_BYTES || contains_ascii_whitespace(&token) {
            return Err(
                "development Bearer token must be non-empty and contain no whitespace".into(),
            );
        }
        Ok(Self { token, identity })
    }
}

#[async_trait]
impl Authenticator for StaticTokenAuthenticator {
    async fn authenticate(
        &self,
        headers: &HeaderMap,
    ) -> Result<AuthenticatedIdentity, AuthenticationFailure> {
        let presented = bearer_token(headers)?;
        if constant_time_eq(presented.as_bytes(), &self.token) {
            Ok(self.identity.clone())
        } else {
            Err(AuthenticationFailure::Invalid("signature mismatch"))
        }
    }
}

/// OIDC discovery and JWKS validation settings.
#[derive(Clone, Debug)]
pub struct OidcConfig {
    pub issuer: Url,
    pub audience: String,
    pub jwks_uri_override: Option<Url>,
    pub cache_ttl: Duration,
    pub stale_ttl: Duration,
    pub request_timeout: Duration,
    pub clock_skew: Duration,
    pub allow_insecure_http: bool,
}

impl OidcConfig {
    /// Creates secure defaults around a configured issuer and audience.
    pub fn new(issuer: Url, audience: impl Into<String>) -> Self {
        Self {
            issuer,
            audience: audience.into(),
            jwks_uri_override: None,
            cache_ttl: Duration::from_secs(15 * 60),
            stale_ttl: Duration::from_secs(24 * 60 * 60),
            request_timeout: Duration::from_secs(5),
            clock_skew: Duration::from_secs(60),
            allow_insecure_http: false,
        }
    }

    /// Validates local OIDC settings without performing discovery or network I/O.
    pub fn validate(&self) -> Result<(), OidcInitializationError> {
        validate_oidc_url(&self.issuer, self.allow_insecure_http)?;
        if let Some(jwks_uri) = &self.jwks_uri_override {
            validate_oidc_url(jwks_uri, self.allow_insecure_http)?;
        }
        if self.audience.trim().is_empty() {
            return Err(OidcInitializationError::Configuration(
                "OIDC audience must not be empty".into(),
            ));
        }
        if self.cache_ttl.is_zero()
            || self.stale_ttl < self.cache_ttl
            || self.request_timeout.is_zero()
            || self.clock_skew > Duration::from_secs(60)
        {
            return Err(OidcInitializationError::Configuration(
                "OIDC cache/request durations are inconsistent".into(),
            ));
        }
        Ok(())
    }
}

/// OIDC initialization failure.
#[derive(Debug, thiserror::Error)]
pub enum OidcInitializationError {
    #[error("invalid OIDC configuration: {0}")]
    Configuration(String),
    #[error("OIDC discovery failed: {0}")]
    Discovery(String),
    #[error("initial JWKS fetch failed: {0}")]
    Jwks(String),
}

/// OIDC authenticator with bounded discovery/JWKS fetches and rotation support.
pub struct OidcAuthenticator {
    issuer: Arc<str>,
    audience: Arc<str>,
    jwks_uri: Url,
    client: reqwest::Client,
    cache_ttl: Duration,
    stale_ttl: Duration,
    clock_skew: Duration,
    allowed_algorithms: Arc<[Algorithm]>,
    cache: RwLock<JwkCache>,
    refresh: Mutex<()>,
}

struct JwkCache {
    set: JwkSet,
    fetched_at: Instant,
    last_refresh_attempt: Option<Instant>,
    missing_kids: BTreeMap<String, Instant>,
}

#[derive(Debug, Deserialize)]
struct DiscoveryDocument {
    issuer: String,
    jwks_uri: Url,
    #[serde(default)]
    id_token_signing_alg_values_supported: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct VerifiedClaims {
    sub: String,
    iat: u64,
    exp: u64,
    #[serde(default)]
    nbf: Option<u64>,
}

impl OidcAuthenticator {
    /// Discovers the provider and loads its initial signing-key set.
    pub async fn discover(config: OidcConfig) -> Result<Self, OidcInitializationError> {
        config.validate()?;

        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(config.request_timeout)
            .build()
            .map_err(|error| OidcInitializationError::Configuration(error.to_string()))?;
        let discovery_url = discovery_url(&config.issuer)?;
        let document: DiscoveryDocument = fetch_json(&client, &discovery_url)
            .await
            .map_err(OidcInitializationError::Discovery)?;
        if !issuer_matches_config(&document.issuer, &config.issuer) {
            return Err(OidcInitializationError::Configuration(
                "discovery issuer does not exactly match configured issuer".into(),
            ));
        }
        let jwks_uri = config
            .jwks_uri_override
            .clone()
            .unwrap_or(document.jwks_uri);
        validate_oidc_url(&jwks_uri, config.allow_insecure_http)?;

        let mut allowed_algorithms = asymmetric_algorithms();
        if !document.id_token_signing_alg_values_supported.is_empty() {
            let advertised: HashSet<&str> = document
                .id_token_signing_alg_values_supported
                .iter()
                .map(String::as_str)
                .collect();
            allowed_algorithms.retain(|algorithm| advertised.contains(algorithm_name(*algorithm)));
        }
        if allowed_algorithms.is_empty() {
            return Err(OidcInitializationError::Configuration(
                "provider advertises no supported asymmetric signing algorithm".into(),
            ));
        }

        let set: JwkSet = fetch_json(&client, &jwks_uri)
            .await
            .map_err(OidcInitializationError::Jwks)?;
        validate_jwk_set(&set).map_err(OidcInitializationError::Jwks)?;
        Ok(Self {
            issuer: Arc::from(document.issuer),
            audience: Arc::from(config.audience),
            jwks_uri,
            client,
            cache_ttl: config.cache_ttl,
            stale_ttl: config.stale_ttl,
            clock_skew: config.clock_skew,
            allowed_algorithms: Arc::from(allowed_algorithms),
            cache: RwLock::new(JwkCache {
                set,
                fetched_at: Instant::now(),
                last_refresh_attempt: None,
                missing_kids: BTreeMap::new(),
            }),
            refresh: Mutex::new(()),
        })
    }

    async fn decoding_key(
        &self,
        kid: &str,
        algorithm: Algorithm,
    ) -> Result<DecodingKey, AuthenticationFailure> {
        {
            let cache = self.cache.read().await;
            let key = select_key(&cache.set, kid, algorithm)?;
            if cache.fetched_at.elapsed() <= self.cache_ttl {
                if let Some(key) = key {
                    return Ok(key);
                }
                if cache
                    .missing_kids
                    .get(kid)
                    .is_some_and(|observed| observed.elapsed() <= self.cache_ttl)
                {
                    return Err(AuthenticationFailure::Invalid("unknown signing key"));
                }
            }
            if cache
                .last_refresh_attempt
                .is_some_and(|attempt| attempt.elapsed() <= UNKNOWN_KID_REFRESH_COOLDOWN)
            {
                if let Some(key) = key.filter(|_| cache.fetched_at.elapsed() <= self.stale_ttl) {
                    return Ok(key);
                }
                return Err(AuthenticationFailure::Invalid("unknown signing key"));
            }
        }

        let _refresh = self.refresh.lock().await;
        {
            let cache = self.cache.read().await;
            let key = select_key(&cache.set, kid, algorithm)?;
            if cache.fetched_at.elapsed() <= self.cache_ttl {
                if let Some(key) = key {
                    return Ok(key);
                }
                if cache
                    .missing_kids
                    .get(kid)
                    .is_some_and(|observed| observed.elapsed() <= self.cache_ttl)
                {
                    return Err(AuthenticationFailure::Invalid("unknown signing key"));
                }
            }
            if cache
                .last_refresh_attempt
                .is_some_and(|attempt| attempt.elapsed() <= UNKNOWN_KID_REFRESH_COOLDOWN)
            {
                if let Some(key) = key.filter(|_| cache.fetched_at.elapsed() <= self.stale_ttl) {
                    return Ok(key);
                }
                return Err(AuthenticationFailure::Invalid("unknown signing key"));
            }
        }

        self.cache.write().await.last_refresh_attempt = Some(Instant::now());

        match fetch_json::<JwkSet>(&self.client, &self.jwks_uri).await {
            Ok(set) => {
                validate_jwk_set(&set)
                    .map_err(|_| AuthenticationFailure::Unavailable("invalid JWKS response"))?;
                let key = select_key(&set, kid, algorithm)?;
                let now = Instant::now();
                let mut cache = self.cache.write().await;
                cache.missing_kids.retain(|_, observed| {
                    now.saturating_duration_since(*observed) <= self.cache_ttl
                });
                if key.is_none() {
                    remember_missing_kid(&mut cache.missing_kids, kid, now);
                }
                cache.set = set;
                cache.fetched_at = now;
                key.ok_or(AuthenticationFailure::Invalid("unknown signing key"))
            }
            Err(_) => {
                let cache = self.cache.read().await;
                if cache.fetched_at.elapsed() <= self.stale_ttl {
                    if let Some(key) = select_key(&cache.set, kid, algorithm)? {
                        tracing::warn!(
                            "OIDC JWKS refresh failed; using a bounded stale signing key"
                        );
                        return Ok(key);
                    }
                }
                Err(AuthenticationFailure::Unavailable("JWKS refresh failed"))
            }
        }
    }
}

fn remember_missing_kid(missing_kids: &mut BTreeMap<String, Instant>, kid: &str, now: Instant) {
    if missing_kids.len() >= MAX_MISSING_KIDS && !missing_kids.contains_key(kid) {
        if let Some(oldest) = missing_kids
            .iter()
            .min_by_key(|(_, observed)| **observed)
            .map(|(kid, _)| kid.clone())
        {
            missing_kids.remove(&oldest);
        }
    }
    missing_kids.insert(kid.to_owned(), now);
}

#[async_trait]
impl Authenticator for OidcAuthenticator {
    async fn authenticate(
        &self,
        headers: &HeaderMap,
    ) -> Result<AuthenticatedIdentity, AuthenticationFailure> {
        let token = bearer_token(headers)?;
        let header = decode_header(token)
            .map_err(|_| AuthenticationFailure::Invalid("malformed JWT header"))?;
        if header
            .crit
            .as_ref()
            .is_some_and(|values| !values.is_empty())
            || header.jku.is_some()
            || header.jwk.is_some()
        {
            return Err(AuthenticationFailure::Invalid(
                "unsupported JWT header parameters",
            ));
        }
        if let Some(kind) = header.typ.as_deref() {
            if !kind.eq_ignore_ascii_case("JWT") && !kind.eq_ignore_ascii_case("at+jwt") {
                return Err(AuthenticationFailure::Invalid("unsupported JWT type"));
            }
        }
        if !self.allowed_algorithms.contains(&header.alg) {
            return Err(AuthenticationFailure::Invalid(
                "signing algorithm is not allowed",
            ));
        }
        let kid = header
            .kid
            .as_deref()
            .filter(|value| !value.is_empty() && value.len() <= 256)
            .ok_or(AuthenticationFailure::Invalid("JWT kid is required"))?;
        let key = self.decoding_key(kid, header.alg).await?;

        let mut validation = Validation::new(header.alg);
        validation.leeway = self.clock_skew.as_secs();
        validation.validate_nbf = true;
        validation.set_audience(&[self.audience.as_ref()]);
        validation.set_issuer(&[self.issuer.as_ref()]);
        validation.set_required_spec_claims(&["exp", "iat", "iss", "aud", "sub"]);
        let token = decode::<VerifiedClaims>(token, &key, &validation)
            .map_err(|_| AuthenticationFailure::Invalid("JWT validation failed"))?;
        if token.claims.sub.is_empty() || token.claims.sub.len() > 1024 {
            return Err(AuthenticationFailure::Invalid("OIDC subject is invalid"));
        }
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| AuthenticationFailure::Invalid("system clock is invalid"))?
            .as_secs();
        validate_claim_times(&token.claims, now, self.clock_skew.as_secs())?;
        let principal_id = canonical_oidc_principal(&self.issuer, &token.claims.sub);
        AuthenticatedIdentity::new(
            principal_id,
            PrincipalKind::User,
            self.issuer.clone(),
            Arc::<str>::from(token.claims.sub),
        )
    }
}

fn validate_claim_times(
    claims: &VerifiedClaims,
    now: u64,
    clock_skew_secs: u64,
) -> Result<(), AuthenticationFailure> {
    if claims.exp < claims.iat
        || claims.exp - claims.iat > MAX_TOKEN_LIFETIME_SECS
        || claims.nbf.is_some_and(|nbf| nbf > claims.exp)
    {
        return Err(AuthenticationFailure::Invalid("JWT lifetime is invalid"));
    }
    if claims.iat > now.saturating_add(clock_skew_secs) {
        return Err(AuthenticationFailure::Invalid(
            "JWT issued-at time is in the future",
        ));
    }
    Ok(())
}

/// Fusen head interceptor that validates API version and authenticates before polling a body.
#[derive(Clone)]
pub struct AuthenticationInterceptor {
    authenticator: Arc<dyn Authenticator>,
}

impl AuthenticationInterceptor {
    /// Creates an authentication interceptor.
    pub fn new(authenticator: Arc<dyn Authenticator>) -> Self {
        Self { authenticator }
    }
}

impl Interceptor for AuthenticationInterceptor {
    fn intercept<'a>(&'a self, mut context: Context, next: Next<'a>) -> InterceptorFuture<'a> {
        Box::pin(async move {
            if is_public_method(&context) {
                return next.run(context).await;
            }
            validate_api_version(context.headers())?;
            let identity = self
                .authenticator
                .authenticate(context.headers())
                .await
                .map_err(map_authentication_failure)?;
            context.extensions_mut().insert(identity);
            next.run(context).await
        })
    }
}

fn is_public_method(context: &Context) -> bool {
    let service = context.interface().selector().service_id();
    let method = context.method().invocation_name();
    service == "neoengram.system"
        && matches!(method, "query_api_version" | "live_probe" | "ready_probe")
}

fn validate_api_version(headers: &HeaderMap) -> Result<(), fusen_rs::Error> {
    let mut values = headers.get_all(API_VERSION_HEADER).iter();
    let version = values.next();
    if values.next().is_some() || version.and_then(|value| value.to_str().ok()) != Some("1") {
        return Err(api_version_unsupported());
    }
    Ok(())
}

fn map_authentication_failure(error: AuthenticationFailure) -> fusen_rs::Error {
    match error {
        AuthenticationFailure::Missing => unauthenticated(
            "authentication_required",
            "a valid Bearer token is required",
        ),
        AuthenticationFailure::Invalid(_) => unauthenticated(
            "invalid_bearer_token",
            "the Bearer token is invalid or expired",
        ),
        AuthenticationFailure::Unavailable(_) => application_error(
            fusen_rs::ErrorCategory::Unavailable,
            "authentication_unavailable",
            "AUTHENTICATION_UNAVAILABLE",
            "identity verification is temporarily unavailable",
            true,
        ),
    }
}

fn bearer_token(headers: &HeaderMap) -> Result<&str, AuthenticationFailure> {
    let mut values = headers.get_all(AUTHORIZATION).iter();
    let value = values.next().ok_or(AuthenticationFailure::Missing)?;
    if values.next().is_some() {
        return Err(AuthenticationFailure::Invalid(
            "multiple Authorization headers",
        ));
    }
    let value = value
        .to_str()
        .map_err(|_| AuthenticationFailure::Invalid("non-text Authorization header"))?;
    let (scheme, token) = value.split_once(' ').ok_or(AuthenticationFailure::Invalid(
        "invalid Authorization scheme",
    ))?;
    if !scheme.eq_ignore_ascii_case("Bearer")
        || token.is_empty()
        || token.len() > MAX_BEARER_BYTES
        || token.bytes().any(|byte| byte.is_ascii_whitespace())
    {
        return Err(AuthenticationFailure::Invalid("invalid Bearer value"));
    }
    Ok(token)
}

fn contains_ascii_whitespace(value: &[u8]) -> bool {
    value.iter().any(u8::is_ascii_whitespace)
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    let mut difference = left.len() ^ right.len();
    let length = left.len().max(right.len());
    for index in 0..length {
        let left = left.get(index).copied().unwrap_or(0);
        let right = right.get(index).copied().unwrap_or(0);
        difference |= usize::from(left ^ right);
    }
    difference == 0
}

fn discovery_url(issuer: &Url) -> Result<Url, OidcInitializationError> {
    Url::parse(&format!(
        "{}/.well-known/openid-configuration",
        issuer.as_str().trim_end_matches('/')
    ))
    .map_err(|error| OidcInitializationError::Configuration(error.to_string()))
}

fn issuer_matches_config(discovered: &str, configured: &Url) -> bool {
    if discovered == configured.as_str() {
        return true;
    }
    configured.path() == "/"
        && configured.query().is_none()
        && configured
            .as_str()
            .strip_suffix('/')
            .is_some_and(|root| discovered == root)
}

fn validate_oidc_url(url: &Url, allow_insecure_http: bool) -> Result<(), OidcInitializationError> {
    if !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(OidcInitializationError::Configuration(
            "OIDC URLs must not contain credentials, query parameters, or fragments".into(),
        ));
    }
    if url.scheme() == "https" {
        return Ok(());
    }
    if allow_insecure_http && url.scheme() == "http" && is_loopback_url(url) {
        return Ok(());
    }
    Err(OidcInitializationError::Configuration(
        "OIDC URLs must use HTTPS (HTTP is allowed only for loopback development)".into(),
    ))
}

fn is_loopback_url(url: &Url) -> bool {
    match url.host() {
        Some(url::Host::Ipv4(address)) => address.is_loopback(),
        Some(url::Host::Ipv6(address)) => address.is_loopback(),
        Some(url::Host::Domain(domain)) => domain.eq_ignore_ascii_case("localhost"),
        None => false,
    }
}

async fn fetch_json<T: for<'de> Deserialize<'de>>(
    client: &reqwest::Client,
    url: &Url,
) -> Result<T, String> {
    let mut response = client
        .get(url.clone())
        .header(http::header::ACCEPT, "application/json")
        .send()
        .await
        .map_err(|error| error.to_string())?
        .error_for_status()
        .map_err(|error| error.to_string())?;
    if response
        .content_length()
        .is_some_and(|length| length > MAX_OIDC_DOCUMENT_BYTES)
    {
        return Err("OIDC response exceeds size limit".into());
    }
    let capacity = response
        .content_length()
        .unwrap_or(0)
        .min(MAX_OIDC_DOCUMENT_BYTES) as usize;
    let mut bytes = Vec::with_capacity(capacity);
    while let Some(chunk) = response.chunk().await.map_err(|error| error.to_string())? {
        if bytes.len().saturating_add(chunk.len()) > MAX_OIDC_DOCUMENT_BYTES as usize {
            return Err("OIDC response exceeds size limit".into());
        }
        bytes.extend_from_slice(&chunk);
    }
    serde_json::from_slice(&bytes).map_err(|error| error.to_string())
}

fn asymmetric_algorithms() -> Vec<Algorithm> {
    vec![
        Algorithm::RS256,
        Algorithm::RS384,
        Algorithm::RS512,
        Algorithm::PS256,
        Algorithm::PS384,
        Algorithm::PS512,
        Algorithm::ES256,
        Algorithm::ES384,
        Algorithm::EdDSA,
    ]
}

fn algorithm_name(algorithm: Algorithm) -> &'static str {
    match algorithm {
        Algorithm::RS256 => "RS256",
        Algorithm::RS384 => "RS384",
        Algorithm::RS512 => "RS512",
        Algorithm::PS256 => "PS256",
        Algorithm::PS384 => "PS384",
        Algorithm::PS512 => "PS512",
        Algorithm::ES256 => "ES256",
        Algorithm::ES384 => "ES384",
        Algorithm::EdDSA => "EdDSA",
        Algorithm::HS256 | Algorithm::HS384 | Algorithm::HS512 => "HMAC",
    }
}

fn validate_jwk_set(set: &JwkSet) -> Result<(), String> {
    if set.keys.is_empty() {
        return Err("JWKS contains no keys".into());
    }
    let mut kids = HashSet::new();
    for key in &set.keys {
        if let Some(kid) = key.common.key_id.as_deref() {
            if kid.is_empty() || kid.len() > 256 || !kids.insert(kid) {
                return Err("JWKS contains an invalid or duplicate kid".into());
            }
        }
    }
    Ok(())
}

fn select_key(
    set: &JwkSet,
    kid: &str,
    algorithm: Algorithm,
) -> Result<Option<DecodingKey>, AuthenticationFailure> {
    let Some(jwk) = set.find(kid) else {
        return Ok(None);
    };
    validate_jwk_for_verification(jwk, algorithm)?;
    DecodingKey::from_jwk(jwk)
        .map(Some)
        .map_err(|_| AuthenticationFailure::Invalid("signing key is malformed"))
}

fn validate_jwk_for_verification(
    jwk: &Jwk,
    algorithm: Algorithm,
) -> Result<(), AuthenticationFailure> {
    if matches!(jwk.algorithm, AlgorithmParameters::OctetKey(_)) {
        return Err(AuthenticationFailure::Invalid(
            "symmetric JWKS keys are not accepted",
        ));
    }
    if let Some(key_use) = &jwk.common.public_key_use {
        if key_use != &PublicKeyUse::Signature {
            return Err(AuthenticationFailure::Invalid("JWK is not a signing key"));
        }
    }
    if let Some(operations) = &jwk.common.key_operations {
        if !operations.contains(&KeyOperations::Verify) {
            return Err(AuthenticationFailure::Invalid(
                "JWK does not allow signature verification",
            ));
        }
    }
    if let Some(key_algorithm) = jwk.common.key_algorithm {
        if key_algorithm.to_string() != algorithm_name(algorithm) {
            return Err(AuthenticationFailure::Invalid(
                "JWT and JWK algorithms differ",
            ));
        }
    }
    Ok(())
}

fn canonical_oidc_principal(issuer: &str, subject: &str) -> String {
    if PrincipalId::new(subject).is_ok() {
        return subject.to_owned();
    }
    let mut input = Vec::with_capacity(issuer.len() + subject.len() + 1);
    input.extend_from_slice(issuer.as_bytes());
    input.push(0);
    input.extend_from_slice(subject.as_bytes());
    format!("oidc:{}", blake3::hash(&input).to_hex())
}

/// Permissions exposed by the public control-plane API.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Deserialize)]
pub enum Permission {
    #[serde(rename = "job.create", alias = "create_add_job")]
    CreateAddJob,
    #[serde(rename = "job.read", alias = "query_job")]
    QueryJob,
    #[serde(rename = "job.finalize", alias = "finalize_add")]
    FinalizeAdd,
    #[serde(rename = "tenant.read")]
    TenantRead,
    #[serde(rename = "tenant.create")]
    TenantCreate,
    #[serde(rename = "storage.read")]
    StorageRead,
    #[serde(rename = "storage.create")]
    StorageCreate,
    #[serde(rename = "storage.enrollment.create")]
    StorageEnrollmentCreate,
    #[serde(rename = "storage.enrollment.read")]
    StorageEnrollmentRead,
    #[serde(rename = "storage.enrollment.review")]
    StorageEnrollmentReview,
    #[serde(rename = "playground.read")]
    PlaygroundRead,
    #[serde(rename = "playground.create")]
    PlaygroundCreate,
}

impl Permission {
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::CreateAddJob => "job.create",
            Self::QueryJob => "job.read",
            Self::FinalizeAdd => "job.finalize",
            Self::TenantRead => "tenant.read",
            Self::TenantCreate => "tenant.create",
            Self::StorageRead => "storage.read",
            Self::StorageCreate => "storage.create",
            Self::StorageEnrollmentCreate => "storage.enrollment.create",
            Self::StorageEnrollmentRead => "storage.enrollment.read",
            Self::StorageEnrollmentReview => "storage.enrollment.review",
            Self::PlaygroundRead => "playground.read",
            Self::PlaygroundCreate => "playground.create",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TenantVisibility {
    None,
    All,
    Explicit(Vec<TenantId>),
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PolicyDocument {
    roles: BTreeMap<String, RoleDocument>,
    bindings: Vec<BindingDocument>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RoleDocument {
    permissions: BTreeSet<Permission>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct BindingDocument {
    principal_id: String,
    roles: BTreeSet<String>,
    tenants: BTreeSet<String>,
    #[serde(default)]
    disabled: bool,
}

#[derive(Clone)]
struct PrincipalGrant {
    permissions: BTreeSet<Permission>,
    tenants: BTreeSet<String>,
}

/// Immutable, deny-by-default RBAC policy shared by HTTP and domain authorization.
#[derive(Clone, Default)]
pub struct StaticRbacPolicy {
    grants: Arc<BTreeMap<String, PrincipalGrant>>,
}

impl StaticRbacPolicy {
    /// Parses and validates a static role/binding policy document.
    pub fn from_json(bytes: &[u8]) -> Result<Self, String> {
        let document: PolicyDocument =
            serde_json::from_slice(bytes).map_err(|error| error.to_string())?;
        let mut grants = BTreeMap::new();
        let mut bound_principals = BTreeSet::new();
        for binding in document.bindings {
            PrincipalId::new(&binding.principal_id)
                .map_err(|error| format!("invalid RBAC principal ID: {error}"))?;
            if !bound_principals.insert(binding.principal_id.clone()) {
                return Err(format!(
                    "RBAC principal {} has more than one binding",
                    binding.principal_id
                ));
            }
            if binding.tenants.is_empty() {
                return Err(format!(
                    "RBAC principal {} has no tenant scope",
                    binding.principal_id
                ));
            }
            for tenant in &binding.tenants {
                if tenant != "*" {
                    TenantId::new(tenant)
                        .map_err(|error| format!("invalid RBAC tenant ID: {error}"))?;
                }
            }
            let mut permissions = BTreeSet::new();
            for role_name in &binding.roles {
                let role = document
                    .roles
                    .get(role_name)
                    .ok_or_else(|| format!("RBAC role {role_name:?} does not exist"))?;
                permissions.extend(role.permissions.iter().copied());
            }
            if binding.disabled {
                continue;
            }
            grants.insert(
                binding.principal_id,
                PrincipalGrant {
                    permissions,
                    tenants: binding.tenants,
                },
            );
        }
        Ok(Self {
            grants: Arc::new(grants),
        })
    }

    /// Creates one explicit grant, primarily for local development and tests.
    pub fn one_principal(
        principal_id: impl Into<String>,
        tenants: impl IntoIterator<Item = String>,
        permissions: impl IntoIterator<Item = Permission>,
    ) -> Result<Self, String> {
        let principal_id = principal_id.into();
        PrincipalId::new(&principal_id).map_err(|error| error.to_string())?;
        let tenants: BTreeSet<String> = tenants.into_iter().collect();
        for tenant in &tenants {
            if tenant != "*" {
                TenantId::new(tenant)
                    .map_err(|error| format!("invalid RBAC tenant ID: {error}"))?;
            }
        }
        let grant = PrincipalGrant {
            permissions: permissions.into_iter().collect(),
            tenants,
        };
        if grant.tenants.is_empty() {
            return Err("RBAC grant must contain at least one tenant or wildcard".into());
        }
        Ok(Self {
            grants: Arc::new(BTreeMap::from([(principal_id, grant)])),
        })
    }

    /// Authorizes an authenticated identity for an exposed permission and tenant.
    pub fn authorize_identity(
        &self,
        identity: &AuthenticatedIdentity,
        permission: Permission,
        tenant_id: &TenantId,
    ) -> Result<(), fusen_rs::Error> {
        if self.is_allowed(identity.principal(), permission, tenant_id) {
            Ok(())
        } else {
            Err(crate::error::permission_denied())
        }
    }

    /// Returns whether a principal has an explicit permission in the tenant scope.
    pub fn is_allowed(
        &self,
        principal: &PrincipalRef,
        permission: Permission,
        tenant_id: &TenantId,
    ) -> bool {
        self.grants.get(principal.id.as_str()).is_some_and(|grant| {
            grant.permissions.contains(&permission)
                && (grant.tenants.contains("*") || grant.tenants.contains(tenant_id.as_str()))
        })
    }

    /// Returns the tenant scope for one permission without exposing policy internals.
    #[must_use]
    pub fn tenant_visibility(
        &self,
        principal: &PrincipalRef,
        permission: Permission,
    ) -> TenantVisibility {
        let Some(grant) = self.grants.get(principal.id.as_str()) else {
            return TenantVisibility::None;
        };
        if !grant.permissions.contains(&permission) {
            return TenantVisibility::None;
        }
        if grant.tenants.contains("*") {
            return TenantVisibility::All;
        }
        TenantVisibility::Explicit(
            grant
                .tenants
                .iter()
                .map(|tenant| TenantId::new(tenant.clone()).expect("RBAC tenant was validated"))
                .collect(),
        )
    }

    /// Stable public permission names granted in one Tenant scope.
    #[must_use]
    pub fn permission_names(&self, principal: &PrincipalRef, tenant_id: &TenantId) -> Vec<String> {
        let Some(grant) = self.grants.get(principal.id.as_str()) else {
            return Vec::new();
        };
        if !(grant.tenants.contains("*") || grant.tenants.contains(tenant_id.as_str())) {
            return Vec::new();
        }
        grant
            .permissions
            .iter()
            .map(|permission| permission.name().to_owned())
            .collect()
    }
}

#[async_trait]
impl Authorizer for StaticRbacPolicy {
    async fn authorize(&self, request: &AuthorizationRequest) -> CentralResult<()> {
        if matches!(request.actor, Actor::Agent(_))
            && matches!(
                request.action,
                Action::ReceiveReport | Action::StageMetadataBatch
            )
        {
            // Agent transport authenticates the signed session and exact assignment scope before
            // entering the control plane. RBAC has no principal binding for Agent identities.
            return Ok(());
        }
        let Actor::Principal(principal) = &request.actor else {
            return Err(authorization_error());
        };
        let permission = match request.action {
            Action::CreateAddJob | Action::AssignJob | Action::ExpireAddJob => {
                Permission::CreateAddJob
            }
            Action::QueryJob => Permission::QueryJob,
            Action::FinalizeAdd => Permission::FinalizeAdd,
            Action::ReceiveReport | Action::StageMetadataBatch => return Err(authorization_error()),
        };
        if self.is_allowed(principal, permission, &request.tenant_id) {
            Ok(())
        } else {
            Err(authorization_error())
        }
    }
}

fn authorization_error() -> CentralError {
    CentralError::new(
        CentralErrorCode::Unauthorized,
        "principal is not authorized",
    )
    .with_retryable(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::HeaderValue;

    fn identity() -> AuthenticatedIdentity {
        AuthenticatedIdentity::new("user-a", PrincipalKind::User, "test", "subject-a").unwrap()
    }

    #[tokio::test]
    async fn static_authenticator_requires_the_exact_bearer_token() {
        let authenticator = StaticTokenAuthenticator::new("secret-token", identity()).unwrap();
        let mut headers = HeaderMap::new();
        assert_eq!(
            authenticator.authenticate(&headers).await.unwrap_err(),
            AuthenticationFailure::Missing
        );
        headers.insert(AUTHORIZATION, HeaderValue::from_static("Bearer wrong"));
        assert!(matches!(
            authenticator.authenticate(&headers).await,
            Err(AuthenticationFailure::Invalid(_))
        ));
        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_static("Bearer secret-token"),
        );
        assert_eq!(
            authenticator
                .authenticate(&headers)
                .await
                .unwrap()
                .principal()
                .id
                .as_str(),
            "user-a"
        );
    }

    #[tokio::test]
    async fn oidc_rejects_an_excessive_clock_skew_before_discovery() {
        let mut config =
            OidcConfig::new(Url::parse("https://issuer.example").unwrap(), "neoengram");
        config.clock_skew = Duration::from_secs(61);
        assert!(matches!(
            OidcAuthenticator::discover(config).await,
            Err(OidcInitializationError::Configuration(_))
        ));
    }

    #[test]
    fn rbac_is_deny_by_default_and_tenant_scoped() {
        let policy = StaticRbacPolicy::one_principal(
            "user-a",
            ["tenant-a".to_owned()],
            [Permission::QueryJob],
        )
        .unwrap();
        assert!(policy.is_allowed(
            identity().principal(),
            Permission::QueryJob,
            &TenantId::new("tenant-a").unwrap()
        ));
        assert!(!policy.is_allowed(
            identity().principal(),
            Permission::CreateAddJob,
            &TenantId::new("tenant-a").unwrap()
        ));
        assert!(!policy.is_allowed(
            identity().principal(),
            Permission::QueryJob,
            &TenantId::new("tenant-b").unwrap()
        ));
    }

    #[test]
    fn oidc_subjects_that_are_not_protocol_ids_are_stably_hashed() {
        let first = canonical_oidc_principal("https://issuer.example", "auth0|user@example.com");
        let second = canonical_oidc_principal("https://issuer.example", "auth0|user@example.com");
        assert_eq!(first, second);
        assert!(PrincipalId::new(first).is_ok());
    }

    #[test]
    fn root_issuer_accepts_url_parser_slash_but_preserves_discovery_value() {
        let configured = Url::parse("https://issuer.example").unwrap();
        assert!(issuer_matches_config("https://issuer.example", &configured));
        assert!(issuer_matches_config(
            "https://issuer.example/",
            &configured
        ));
        let nested = Url::parse("https://issuer.example/tenant/").unwrap();
        assert!(!issuer_matches_config(
            "https://issuer.example/tenant",
            &nested
        ));
    }

    #[test]
    fn disabled_rbac_binding_never_grants_permissions() {
        let policy = StaticRbacPolicy::from_json(
            br#"{
                "roles": {"reader": {"permissions": ["query_job"]}},
                "bindings": [{
                    "principal_id": "user-a",
                    "roles": ["reader"],
                    "tenants": ["tenant-a"],
                    "disabled": true
                }]
            }"#,
        )
        .unwrap();
        assert!(!policy.is_allowed(
            identity().principal(),
            Permission::QueryJob,
            &TenantId::new("tenant-a").unwrap()
        ));
    }

    #[test]
    fn development_grants_validate_tenant_ids() {
        assert!(StaticRbacPolicy::one_principal(
            "user-a",
            ["invalid tenant".to_owned()],
            [Permission::QueryJob]
        )
        .is_err());
    }

    #[test]
    fn oidc_claim_times_require_a_bounded_lifetime() {
        let valid = VerifiedClaims {
            sub: "user-a".to_owned(),
            iat: 1_000,
            exp: 4_600,
            nbf: Some(1_000),
        };
        assert!(validate_claim_times(&valid, 1_000, 60).is_ok());
        assert!(validate_claim_times(
            &VerifiedClaims {
                exp: 4_601,
                ..valid
            },
            1_000,
            60
        )
        .is_err());
        assert!(serde_json::from_value::<VerifiedClaims>(serde_json::json!({
            "sub": "user-a",
            "exp": 4_600
        }))
        .is_err());
    }
}
