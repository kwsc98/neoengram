use crate::{ConfigValidationError, ConfigValidationErrorKind};
use fusen_contract::{EndpointCapabilities, HttpBindingId, HttpVersionSet};
use std::time::Duration;

const MIB: usize = 1024 * 1024;

/// Server request admission, deadline, and body limits.
#[derive(Clone, Debug)]
pub struct ServerRequestConfig {
    timeout: Duration,
    max_concurrent_requests: usize,
    max_request_body_bytes: usize,
    max_response_body_bytes: usize,
    max_inflight_request_body_bytes: usize,
    max_inflight_response_body_bytes: usize,
    queue_capacity: usize,
    queue_max_wait: Duration,
}

impl Default for ServerRequestConfig {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(30),
            max_concurrent_requests: 1024,
            max_request_body_bytes: 2 * MIB,
            max_response_body_bytes: 2 * MIB,
            max_inflight_request_body_bytes: 64 * MIB,
            max_inflight_response_body_bytes: 64 * MIB,
            queue_capacity: 0,
            queue_max_wait: Duration::from_millis(50),
        }
    }
}

impl ServerRequestConfig {
    /// Starts a builder with bounded production defaults.
    pub fn builder() -> ServerRequestConfigBuilder {
        ServerRequestConfigBuilder(Self::default())
    }

    /// Returns the local upper bound for one request deadline.
    pub const fn timeout(&self) -> Duration {
        self.timeout
    }

    /// Returns the server-wide in-flight request limit.
    pub const fn max_concurrent_requests(&self) -> usize {
        self.max_concurrent_requests
    }

    /// Returns the maximum request body size.
    pub const fn max_request_body_bytes(&self) -> usize {
        self.max_request_body_bytes
    }

    /// Returns the maximum response body size.
    pub const fn max_response_body_bytes(&self) -> usize {
        self.max_response_body_bytes
    }

    /// Returns the global buffered-request byte budget.
    pub const fn max_inflight_request_body_bytes(&self) -> usize {
        self.max_inflight_request_body_bytes
    }

    /// Returns the global buffered-response byte budget.
    pub const fn max_inflight_response_body_bytes(&self) -> usize {
        self.max_inflight_response_body_bytes
    }

    /// Returns the optional queue capacity.
    pub const fn queue_capacity(&self) -> usize {
        self.queue_capacity
    }

    /// Returns the queue wait cap.
    pub const fn queue_max_wait(&self) -> Duration {
        self.queue_max_wait
    }
}

/// Builder for [`ServerRequestConfig`].
#[derive(Clone, Debug)]
pub struct ServerRequestConfigBuilder(ServerRequestConfig);

impl ServerRequestConfigBuilder {
    /// Sets the local upper bound for one request deadline.
    pub const fn timeout(mut self, value: Duration) -> Self {
        self.0.timeout = value;
        self
    }

    /// Sets the maximum number of admitted requests.
    pub const fn max_concurrent_requests(mut self, value: usize) -> Self {
        self.0.max_concurrent_requests = value;
        self
    }

    /// Sets the maximum request body size.
    pub const fn max_request_body_bytes(mut self, value: usize) -> Self {
        self.0.max_request_body_bytes = value;
        self
    }

    /// Sets the maximum response body size.
    pub const fn max_response_body_bytes(mut self, value: usize) -> Self {
        self.0.max_response_body_bytes = value;
        self
    }

    /// Sets the global buffered-request byte budget.
    pub const fn max_inflight_request_body_bytes(mut self, value: usize) -> Self {
        self.0.max_inflight_request_body_bytes = value;
        self
    }

    /// Sets the global buffered-response byte budget.
    pub const fn max_inflight_response_body_bytes(mut self, value: usize) -> Self {
        self.0.max_inflight_response_body_bytes = value;
        self
    }

    /// Sets the optional queue capacity. Zero preserves fail-fast behavior.
    pub const fn queue_capacity(mut self, value: usize) -> Self {
        self.0.queue_capacity = value;
        self
    }

    /// Sets the queue wait cap.
    pub const fn queue_max_wait(mut self, value: Duration) -> Self {
        self.0.queue_max_wait = value;
        self
    }

    /// Validates and builds request limits.
    pub fn build(self) -> Result<ServerRequestConfig, ConfigValidationError> {
        validate_request(&self.0)?;
        Ok(self.0)
    }
}

/// Plain HTTP/1.1 and h2c server settings.
#[derive(Clone, Debug)]
pub struct HttpServerConfig {
    max_connections: usize,
    max_uri_bytes: usize,
    max_query_pairs: usize,
    max_headers: usize,
    max_header_bytes: usize,
    http1_header_read_timeout: Duration,
    http2_max_concurrent_streams: u32,
    http2_keep_alive_interval: Option<Duration>,
    http2_keep_alive_timeout: Duration,
}

impl Default for HttpServerConfig {
    fn default() -> Self {
        Self {
            max_connections: 2048,
            max_uri_bytes: 8 * 1024,
            max_query_pairs: 128,
            max_headers: 64,
            max_header_bytes: 32 * 1024,
            http1_header_read_timeout: Duration::from_secs(10),
            http2_max_concurrent_streams: 128,
            http2_keep_alive_interval: Some(Duration::from_secs(30)),
            http2_keep_alive_timeout: Duration::from_secs(10),
        }
    }
}

impl HttpServerConfig {
    /// Starts a builder with bounded production defaults.
    pub fn builder() -> HttpServerConfigBuilder {
        HttpServerConfigBuilder(Self::default())
    }

    /// Returns the TCP connection limit.
    pub const fn max_connections(&self) -> usize {
        self.max_connections
    }

    /// Returns the request URI byte limit.
    pub const fn max_uri_bytes(&self) -> usize {
        self.max_uri_bytes
    }

    /// Returns the query pair limit.
    pub const fn max_query_pairs(&self) -> usize {
        self.max_query_pairs
    }

    /// Returns the header count limit.
    pub const fn max_headers(&self) -> usize {
        self.max_headers
    }

    /// Returns the aggregate header byte limit.
    pub const fn max_header_bytes(&self) -> usize {
        self.max_header_bytes
    }

    /// Returns the HTTP/1.1 header deadline.
    pub const fn http1_header_read_timeout(&self) -> Duration {
        self.http1_header_read_timeout
    }

    /// Returns the HTTP/2 stream limit.
    pub const fn http2_max_concurrent_streams(&self) -> u32 {
        self.http2_max_concurrent_streams
    }

    /// Returns the HTTP/2 keep-alive interval.
    pub const fn http2_keep_alive_interval(&self) -> Option<Duration> {
        self.http2_keep_alive_interval
    }

    /// Returns the HTTP/2 keep-alive acknowledgement timeout.
    pub const fn http2_keep_alive_timeout(&self) -> Duration {
        self.http2_keep_alive_timeout
    }
}

/// Builder for [`HttpServerConfig`].
#[derive(Clone, Debug)]
pub struct HttpServerConfigBuilder(HttpServerConfig);

impl HttpServerConfigBuilder {
    /// Sets the accepted TCP connection limit.
    pub const fn max_connections(mut self, value: usize) -> Self {
        self.0.max_connections = value;
        self
    }

    /// Sets the request URI byte limit.
    pub const fn max_uri_bytes(mut self, value: usize) -> Self {
        self.0.max_uri_bytes = value;
        self
    }

    /// Sets the query pair limit.
    pub const fn max_query_pairs(mut self, value: usize) -> Self {
        self.0.max_query_pairs = value;
        self
    }

    /// Sets the header count limit.
    pub const fn max_headers(mut self, value: usize) -> Self {
        self.0.max_headers = value;
        self
    }

    /// Sets the aggregate header byte limit.
    pub const fn max_header_bytes(mut self, value: usize) -> Self {
        self.0.max_header_bytes = value;
        self
    }

    /// Sets the HTTP/1.1 header deadline.
    pub const fn http1_header_read_timeout(mut self, value: Duration) -> Self {
        self.0.http1_header_read_timeout = value;
        self
    }

    /// Sets the HTTP/2 stream limit.
    pub const fn http2_max_concurrent_streams(mut self, value: u32) -> Self {
        self.0.http2_max_concurrent_streams = value;
        self
    }

    /// Sets the HTTP/2 keep-alive interval.
    pub const fn http2_keep_alive_interval(mut self, value: Option<Duration>) -> Self {
        self.0.http2_keep_alive_interval = value;
        self
    }

    /// Sets the HTTP/2 keep-alive acknowledgement timeout.
    pub const fn http2_keep_alive_timeout(mut self, value: Duration) -> Self {
        self.0.http2_keep_alive_timeout = value;
        self
    }

    /// Validates and builds HTTP settings.
    pub fn build(self) -> Result<HttpServerConfig, ConfigValidationError> {
        validate_http(&self.0)?;
        Ok(self.0)
    }
}

/// Registry startup operation limits.
#[derive(Clone, Debug)]
pub struct ServerRegistryConfig {
    startup_timeout: Duration,
    operation_timeout: Duration,
    max_concurrent_operations: usize,
}

impl Default for ServerRegistryConfig {
    fn default() -> Self {
        Self {
            startup_timeout: Duration::from_secs(30),
            operation_timeout: Duration::from_secs(5),
            max_concurrent_operations: 8,
        }
    }
}

impl ServerRegistryConfig {
    /// Starts a builder with bounded production defaults.
    pub fn builder() -> ServerRegistryConfigBuilder {
        ServerRegistryConfigBuilder(Self::default())
    }

    /// Returns the total server-ready deadline.
    pub const fn startup_timeout(&self) -> Duration {
        self.startup_timeout
    }

    /// Returns one registry operation deadline.
    pub const fn operation_timeout(&self) -> Duration {
        self.operation_timeout
    }

    /// Returns the activation and close concurrency window.
    pub const fn max_concurrent_operations(&self) -> usize {
        self.max_concurrent_operations
    }
}

/// Builder for [`ServerRegistryConfig`].
#[derive(Clone, Debug)]
pub struct ServerRegistryConfigBuilder(ServerRegistryConfig);

impl ServerRegistryConfigBuilder {
    /// Sets the total server-ready deadline.
    pub const fn startup_timeout(mut self, value: Duration) -> Self {
        self.0.startup_timeout = value;
        self
    }

    /// Sets one registry operation deadline.
    pub const fn operation_timeout(mut self, value: Duration) -> Self {
        self.0.operation_timeout = value;
        self
    }

    /// Sets the activation and close concurrency window.
    pub const fn max_concurrent_operations(mut self, value: usize) -> Self {
        self.0.max_concurrent_operations = value;
        self
    }

    /// Validates and builds registry settings.
    pub fn build(self) -> Result<ServerRegistryConfig, ConfigValidationError> {
        validate_registry(&self.0)?;
        Ok(self.0)
    }
}

/// Immutable production server configuration.
#[derive(Clone, Debug)]
pub struct ServerConfig {
    capabilities: EndpointCapabilities,
    request: ServerRequestConfig,
    request_id: ServerRequestIdConfig,
    http: HttpServerConfig,
    registry: ServerRegistryConfig,
    graceful_shutdown_timeout: Duration,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            capabilities: EndpointCapabilities::new(
                HttpVersionSet::ALL,
                [HttpBindingId::default()],
                true,
            )
            .expect("built-in server capabilities are valid"),
            request: ServerRequestConfig::default(),
            request_id: ServerRequestIdConfig::default(),
            http: HttpServerConfig::default(),
            registry: ServerRegistryConfig::default(),
            graceful_shutdown_timeout: Duration::from_secs(30),
        }
    }
}

impl ServerConfig {
    /// Starts a builder with bounded production defaults.
    pub fn builder() -> ServerConfigBuilder {
        ServerConfigBuilder(Self::default())
    }

    /// Returns HTTP endpoint capabilities advertised by the built-in server.
    pub const fn capabilities(&self) -> &EndpointCapabilities {
        &self.capabilities
    }

    /// Returns request limits.
    pub const fn request(&self) -> &ServerRequestConfig {
        &self.request
    }

    /// Returns server-side request-ID validation and response behavior.
    pub const fn request_id(&self) -> &ServerRequestIdConfig {
        &self.request_id
    }

    /// Returns HTTP limits.
    pub const fn http(&self) -> &HttpServerConfig {
        &self.http
    }

    /// Returns registry lifecycle limits.
    pub const fn registry(&self) -> &ServerRegistryConfig {
        &self.registry
    }

    /// Returns the deadline shared by deregistration and connection drain.
    pub const fn graceful_shutdown_timeout(&self) -> Duration {
        self.graceful_shutdown_timeout
    }

    pub(crate) fn validate(&self) -> Result<(), ConfigValidationError> {
        if self.capabilities.bindings() != [HttpBindingId::default()] {
            return Err(inconsistent(
                "server.capabilities.bindings",
                "built-in server supports only http-json-v1",
            ));
        }
        validate_request(&self.request)?;
        validate_request_id(&self.request_id)?;
        validate_http(&self.http)?;
        validate_registry(&self.registry)?;
        positive_duration(
            self.graceful_shutdown_timeout,
            "server.graceful_shutdown_timeout",
        )
    }
}

/// Builder for [`ServerConfig`].
#[derive(Clone, Debug)]
pub struct ServerConfigBuilder(ServerConfig);

impl ServerConfigBuilder {
    /// Replaces the endpoint capabilities published by the built-in server.
    pub fn capabilities(mut self, value: EndpointCapabilities) -> Self {
        self.0.capabilities = value;
        self
    }

    /// Replaces request limits.
    pub fn request(mut self, value: ServerRequestConfig) -> Self {
        self.0.request = value;
        self
    }

    /// Replaces server-side request-ID behavior.
    pub fn request_id(mut self, value: ServerRequestIdConfig) -> Self {
        self.0.request_id = value;
        self
    }

    /// Replaces HTTP limits.
    pub fn http(mut self, value: HttpServerConfig) -> Self {
        self.0.http = value;
        self
    }

    /// Replaces registry limits.
    pub fn registry(mut self, value: ServerRegistryConfig) -> Self {
        self.0.registry = value;
        self
    }

    /// Sets the total graceful shutdown budget.
    pub const fn graceful_shutdown_timeout(mut self, value: Duration) -> Self {
        self.0.graceful_shutdown_timeout = value;
        self
    }

    /// Validates and builds the configuration.
    pub fn build(self) -> Result<ServerConfig, ConfigValidationError> {
        self.0.validate()?;
        Ok(self.0)
    }
}

/// Server-side validation and response policy for `X-Request-ID`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ServerRequestIdConfig {
    max_bytes: usize,
    allow_colon: bool,
    require_alphanumeric_prefix: bool,
    always_emit: bool,
}

impl Default for ServerRequestIdConfig {
    fn default() -> Self {
        Self {
            max_bytes: 64,
            allow_colon: false,
            require_alphanumeric_prefix: false,
            always_emit: false,
        }
    }
}

impl ServerRequestIdConfig {
    /// Starts a builder with the Fusen 0.9 wire defaults.
    pub fn builder() -> ServerRequestIdConfigBuilder {
        ServerRequestIdConfigBuilder(Self::default())
    }

    /// Returns the maximum accepted ASCII byte length.
    pub const fn max_bytes(&self) -> usize {
        self.max_bytes
    }

    /// Returns whether `:` is accepted in addition to the default identifier characters.
    pub const fn allow_colon(&self) -> bool {
        self.allow_colon
    }

    /// Returns whether request IDs must begin with an ASCII letter or digit.
    pub const fn require_alphanumeric_prefix(&self) -> bool {
        self.require_alphanumeric_prefix
    }

    /// Returns whether every response exposes its validated or generated request ID.
    pub const fn always_emit(&self) -> bool {
        self.always_emit
    }
}

/// Builder for [`ServerRequestIdConfig`].
#[derive(Clone, Debug)]
pub struct ServerRequestIdConfigBuilder(ServerRequestIdConfig);

impl ServerRequestIdConfigBuilder {
    /// Sets the maximum accepted request-ID byte length.
    pub const fn max_bytes(mut self, value: usize) -> Self {
        self.0.max_bytes = value;
        self
    }

    /// Enables or disables `:` in request IDs.
    pub const fn allow_colon(mut self, value: bool) -> Self {
        self.0.allow_colon = value;
        self
    }

    /// Requires or permits a non-alphanumeric first character.
    pub const fn require_alphanumeric_prefix(mut self, value: bool) -> Self {
        self.0.require_alphanumeric_prefix = value;
        self
    }

    /// Controls whether the runtime returns `X-Request-ID` without negotiated invocation controls.
    pub const fn always_emit(mut self, value: bool) -> Self {
        self.0.always_emit = value;
        self
    }

    /// Validates and builds the request-ID policy.
    pub fn build(self) -> Result<ServerRequestIdConfig, ConfigValidationError> {
        validate_request_id(&self.0)?;
        Ok(self.0)
    }
}

fn validate_request(config: &ServerRequestConfig) -> Result<(), ConfigValidationError> {
    positive_duration(config.timeout, "server.request.timeout")?;
    positive_usize(
        config.max_concurrent_requests,
        "server.request.max_concurrent_requests",
    )?;
    positive_usize(
        config.max_request_body_bytes,
        "server.request.max_request_body_bytes",
    )?;
    positive_usize(
        config.max_response_body_bytes,
        "server.request.max_response_body_bytes",
    )?;
    if config.max_inflight_request_body_bytes < config.max_request_body_bytes {
        return Err(inconsistent(
            "server.request.max_inflight_request_body_bytes",
            "must be at least max_request_body_bytes",
        ));
    }
    if config.max_inflight_response_body_bytes < config.max_response_body_bytes {
        return Err(inconsistent(
            "server.request.max_inflight_response_body_bytes",
            "must be at least max_response_body_bytes",
        ));
    }
    if config.queue_capacity > 0 && config.queue_max_wait.is_zero() {
        return Err(inconsistent(
            "server.request.queue_max_wait",
            "must be positive when queue_capacity is non-zero",
        ));
    }
    Ok(())
}

fn validate_request_id(config: &ServerRequestIdConfig) -> Result<(), ConfigValidationError> {
    if !(32..=1024).contains(&config.max_bytes) {
        return Err(out_of_range(
            "server.request_id.max_bytes",
            "must be from 32 through 1024 so generated request IDs satisfy the policy",
        ));
    }
    Ok(())
}

fn validate_http(config: &HttpServerConfig) -> Result<(), ConfigValidationError> {
    positive_usize(config.max_connections, "server.http.max_connections")?;
    positive_usize(config.max_uri_bytes, "server.http.max_uri_bytes")?;
    positive_usize(config.max_query_pairs, "server.http.max_query_pairs")?;
    positive_usize(config.max_headers, "server.http.max_headers")?;
    positive_usize(config.max_header_bytes, "server.http.max_header_bytes")?;
    positive_duration(
        config.http1_header_read_timeout,
        "server.http.http1_header_read_timeout",
    )?;
    if config.http2_max_concurrent_streams == 0 {
        return Err(out_of_range(
            "server.http.http2_max_concurrent_streams",
            "must be greater than zero",
        ));
    }
    if config
        .http2_keep_alive_interval
        .is_some_and(|value| value.is_zero())
    {
        return Err(out_of_range(
            "server.http.http2_keep_alive_interval",
            "must be positive when configured",
        ));
    }
    positive_duration(
        config.http2_keep_alive_timeout,
        "server.http.http2_keep_alive_timeout",
    )
}

fn validate_registry(config: &ServerRegistryConfig) -> Result<(), ConfigValidationError> {
    positive_duration(config.startup_timeout, "server.registry.startup_timeout")?;
    positive_duration(
        config.operation_timeout,
        "server.registry.operation_timeout",
    )?;
    positive_usize(
        config.max_concurrent_operations,
        "server.registry.max_concurrent_operations",
    )
}

fn positive_duration(
    value: Duration,
    field_path: &'static str,
) -> Result<(), ConfigValidationError> {
    if value.is_zero() {
        Err(out_of_range(field_path, "must be greater than zero"))
    } else {
        Ok(())
    }
}

fn positive_usize(value: usize, field_path: &'static str) -> Result<(), ConfigValidationError> {
    if value == 0 {
        Err(out_of_range(field_path, "must be greater than zero"))
    } else {
        Ok(())
    }
}

const fn out_of_range(field_path: &'static str, reason: &'static str) -> ConfigValidationError {
    ConfigValidationError::new(ConfigValidationErrorKind::OutOfRange, field_path, reason)
}

const fn inconsistent(field_path: &'static str, reason: &'static str) -> ConfigValidationError {
    ConfigValidationError::new(ConfigValidationErrorKind::Inconsistent, field_path, reason)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_getters_match_the_runtime_contract() {
        let config = ServerConfig::default();
        assert_eq!(
            config.capabilities().bindings(),
            &[HttpBindingId::default()]
        );
        assert_eq!(config.capabilities().http_versions(), HttpVersionSet::ALL);
        assert!(config.capabilities().invocation_controls());
        assert_eq!(config.request().timeout(), Duration::from_secs(30));
        assert_eq!(config.request().max_concurrent_requests(), 1024);
        assert_eq!(config.http().max_connections(), 2048);
        assert_eq!(config.http().http2_max_concurrent_streams(), 128);
        assert_eq!(
            config.registry().operation_timeout(),
            Duration::from_secs(5)
        );
        assert_eq!(config.registry().max_concurrent_operations(), 8);
    }

    #[test]
    fn validation_reports_stable_kind_path_and_reason() {
        let error = ServerRequestConfig::builder()
            .max_concurrent_requests(0)
            .build()
            .unwrap_err();
        assert_eq!(error.kind(), ConfigValidationErrorKind::OutOfRange);
        assert_eq!(error.field_path(), "server.request.max_concurrent_requests");
        assert_eq!(error.reason(), "must be greater than zero");

        let error = ServerRequestConfig::builder()
            .max_request_body_bytes(1025)
            .max_inflight_request_body_bytes(1024)
            .build()
            .unwrap_err();
        assert_eq!(error.kind(), ConfigValidationErrorKind::Inconsistent);
        assert_eq!(
            error.field_path(),
            "server.request.max_inflight_request_body_bytes"
        );
    }

    #[test]
    fn independent_builders_accept_disabled_queue_boundary() {
        let request = ServerRequestConfig::builder()
            .queue_capacity(0)
            .queue_max_wait(Duration::ZERO)
            .build()
            .unwrap();
        let http = HttpServerConfig::builder().build().unwrap();
        let registry = ServerRegistryConfig::builder().build().unwrap();
        ServerConfig::builder()
            .request(request)
            .http(http)
            .registry(registry)
            .build()
            .unwrap();
    }

    #[test]
    fn request_id_policy_is_explicit_and_bounded() {
        let request_id = ServerRequestIdConfig::builder()
            .max_bytes(128)
            .allow_colon(true)
            .require_alphanumeric_prefix(true)
            .always_emit(true)
            .build()
            .unwrap();
        assert_eq!(request_id.max_bytes(), 128);
        assert!(request_id.allow_colon());
        assert!(request_id.require_alphanumeric_prefix());
        assert!(request_id.always_emit());

        let error = ServerRequestIdConfig::builder()
            .max_bytes(31)
            .build()
            .unwrap_err();
        assert_eq!(error.field_path(), "server.request_id.max_bytes");

        assert!(
            ServerRequestIdConfig::builder()
                .max_bytes(32)
                .build()
                .is_ok()
        );
        assert!(
            ServerRequestIdConfig::builder()
                .max_bytes(1024)
                .build()
                .is_ok()
        );
        assert!(
            ServerRequestIdConfig::builder()
                .max_bytes(1025)
                .build()
                .is_err()
        );
    }
}
