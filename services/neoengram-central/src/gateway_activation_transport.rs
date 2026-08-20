//! Central-initiated network adapter for Gateway Replica activation.
//!
//! The activation domain service remains the authority for token lookup, challenge expiry,
//! proof verification, certificate issuance, and the prepare/commit Registry CAS boundaries. This
//! module transports the challenge/proof and certificate over the Replica's bootstrap origin.

use std::{sync::Arc, time::Duration};

use crate::GatewayReplicaRecord;
use async_trait::async_trait;
use neoengram_domain::protocol::{
    GatewayBootstrapCertificateDelivery, GatewayBootstrapChallenge,
    GatewayBootstrapChallengeRequest, GatewayBootstrapProofResponse, GatewayOpaqueBytes,
    CURRENT_WIRE_VERSION, GATEWAY_REPLICA_BOOTSTRAP_CERTIFICATE_PATH,
    GATEWAY_REPLICA_BOOTSTRAP_CHALLENGE_PATH, MAX_GATEWAY_CONTROL_FRAME_BYTES,
};
use reqwest::{header::CONTENT_TYPE, Client};
use serde::Serialize;
use thiserror::Error;
use url::Url;

use crate::service::{
    GatewayReplicaActivationChallenge, GatewayReplicaActivationError,
    GatewayReplicaActivationProof, GatewayReplicaActivationResult, GatewayReplicaActivationService,
    IssuedWorkloadCertificate,
};

const DEFAULT_BOOTSTRAP_TIMEOUT: Duration = Duration::from_secs(10);
const BOOTSTRAP_JSON_CONTENT_TYPE: &str = "application/json";

#[derive(Debug, Error)]
pub enum GatewayBootstrapTransportError {
    #[error("Gateway bootstrap HTTP client configuration failed: {0}")]
    ClientConfiguration(String),
    #[error("Gateway bootstrap endpoint is invalid: {0}")]
    Endpoint(String),
    #[error("Gateway bootstrap endpoint requires HTTPS except on loopback")]
    InsecureEndpoint,
    #[error("Gateway bootstrap request failed: {0}")]
    Request(String),
    #[error("Gateway bootstrap returned HTTP status {0}")]
    Status(u16),
    #[error("Gateway bootstrap response exceeded the configured limit")]
    ResponseTooLarge,
    #[error("Gateway bootstrap response has an invalid content type")]
    ContentType,
    #[error("Gateway bootstrap response is invalid: {0}")]
    Decode(String),
    #[error("Gateway bootstrap protocol version is unsupported")]
    UnsupportedVersion,
    #[error("Gateway certificate delivery failed: {0}")]
    Delivery(String),
}

/// Transport boundary used by the activation orchestration. A production implementation must
/// authenticate the Gateway server certificate; tests can inject a deterministic mock.
#[async_trait]
pub trait GatewayBootstrapTransport: Send + Sync {
    async fn prove(
        &self,
        bootstrap_endpoint: &str,
        challenge: &GatewayReplicaActivationChallenge,
    ) -> Result<GatewayReplicaActivationProof, GatewayBootstrapTransportError>;

    async fn deliver_certificate(
        &self,
        bootstrap_endpoint: &str,
        certificate: &IssuedWorkloadCertificate,
    ) -> Result<(), GatewayBootstrapTransportError>;
}

/// HTTPS/HTTP2 adapter. The constructor applies Central's trust/transport settings from a
/// `reqwest::ClientBuilder` and unconditionally disables redirects; no activation token is
/// included in any request body or log message.
#[derive(Clone)]
pub struct HttpGatewayBootstrapTransport {
    client: Client,
    timeout: Duration,
}

impl HttpGatewayBootstrapTransport {
    /// Builds a bootstrap HTTP client with redirects disabled at the reqwest client boundary.
    ///
    /// The bootstrap origin is Registry-authoritative. Following a 3xx response here could send
    /// the proof or certificate payload to an unregistered host before the transport had a chance
    /// to inspect the final URL, so callers provide a builder (including their CA/proxy settings)
    /// and this constructor unconditionally installs `Policy::none()` before building it.
    pub fn new(builder: reqwest::ClientBuilder) -> Result<Self, GatewayBootstrapTransportError> {
        let client = builder
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|error| {
                GatewayBootstrapTransportError::ClientConfiguration(error.to_string())
            })?;
        Ok(Self {
            client,
            timeout: DEFAULT_BOOTSTRAP_TIMEOUT,
        })
    }

    #[must_use]
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    async fn post_json<T: Serialize>(
        &self,
        endpoint: &str,
        path: &str,
        body: &T,
    ) -> Result<reqwest::Response, GatewayBootstrapTransportError> {
        let url = bootstrap_url(endpoint, path)?;
        let response = tokio::time::timeout(
            self.timeout,
            self.client
                .post(url)
                .header(CONTENT_TYPE, BOOTSTRAP_JSON_CONTENT_TYPE)
                .timeout(self.timeout)
                .json(body)
                .send(),
        )
        .await
        .map_err(|_| GatewayBootstrapTransportError::Request("request timed out".into()))?
        .map_err(|error| GatewayBootstrapTransportError::Request(error.to_string()))?;
        if !response.status().is_success() {
            return Err(GatewayBootstrapTransportError::Status(
                response.status().as_u16(),
            ));
        }
        Ok(response)
    }

    async fn bounded_json(
        mut response: reqwest::Response,
    ) -> Result<Vec<u8>, GatewayBootstrapTransportError> {
        if response
            .content_length()
            .is_some_and(|length| length > MAX_GATEWAY_CONTROL_FRAME_BYTES as u64)
        {
            return Err(GatewayBootstrapTransportError::ResponseTooLarge);
        }

        let capacity = response
            .content_length()
            .unwrap_or(0)
            .min(MAX_GATEWAY_CONTROL_FRAME_BYTES as u64) as usize;
        let mut body = Vec::with_capacity(capacity);
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|error| GatewayBootstrapTransportError::Request(error.to_string()))?
        {
            if body.len().saturating_add(chunk.len()) > MAX_GATEWAY_CONTROL_FRAME_BYTES {
                return Err(GatewayBootstrapTransportError::ResponseTooLarge);
            }
            body.extend_from_slice(&chunk);
        }
        Ok(body)
    }
}

#[async_trait]
impl GatewayBootstrapTransport for HttpGatewayBootstrapTransport {
    async fn prove(
        &self,
        bootstrap_endpoint: &str,
        challenge: &GatewayReplicaActivationChallenge,
    ) -> Result<GatewayReplicaActivationProof, GatewayBootstrapTransportError> {
        let request = GatewayBootstrapChallengeRequest {
            wire_version: CURRENT_WIRE_VERSION,
            challenge: GatewayBootstrapChallenge {
                request_id: challenge.request_id().clone(),
                edge_cluster_id: challenge.edge_cluster_id().clone(),
                gateway_pool_id: challenge.gateway_pool_id().clone(),
                gateway_replica_id: challenge.gateway_replica_id().clone(),
                activation_token_digest: challenge.activation_token_digest(),
                nonce: challenge.nonce().to_owned(),
                issued_at_unix_ms: challenge.issued_at_unix_ms(),
                expires_at_unix_ms: challenge.expires_at_unix_ms(),
            },
        };
        request
            .validate()
            .map_err(|error| GatewayBootstrapTransportError::Decode(error.to_string()))?;
        let response = self
            .post_json(
                bootstrap_endpoint,
                GATEWAY_REPLICA_BOOTSTRAP_CHALLENGE_PATH,
                &request,
            )
            .await?;
        if !response
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .is_some_and(is_json_content_type)
        {
            return Err(GatewayBootstrapTransportError::ContentType);
        }
        let body = Self::bounded_json(response).await?;
        let response: GatewayBootstrapProofResponse = serde_json::from_slice(&body)
            .map_err(|error| GatewayBootstrapTransportError::Decode(error.to_string()))?;
        response
            .validate()
            .map_err(|error| GatewayBootstrapTransportError::Decode(error.to_string()))?;
        Ok(response.proof)
    }

    async fn deliver_certificate(
        &self,
        bootstrap_endpoint: &str,
        certificate: &IssuedWorkloadCertificate,
    ) -> Result<(), GatewayBootstrapTransportError> {
        let leaf_certificate_der = GatewayOpaqueBytes::new(certificate.leaf_certificate_der())
            .map_err(|error| GatewayBootstrapTransportError::Delivery(error.to_string()))?;
        let issuer_chain_der = certificate
            .issuer_chain_der()
            .iter()
            .map(|chain| {
                GatewayOpaqueBytes::new(chain.clone())
                    .map_err(|error| GatewayBootstrapTransportError::Delivery(error.to_string()))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let request = GatewayBootstrapCertificateDelivery {
            wire_version: CURRENT_WIRE_VERSION,
            request_id: certificate.request().request_id().clone(),
            certificate_generation: certificate.request().certificate_generation(),
            leaf_certificate_der,
            issuer_chain_der,
        };
        request
            .validate()
            .map_err(|error| GatewayBootstrapTransportError::Delivery(error.to_string()))?;
        let response = self
            .post_json(
                bootstrap_endpoint,
                GATEWAY_REPLICA_BOOTSTRAP_CERTIFICATE_PATH,
                &request,
            )
            .await
            .map_err(|error| GatewayBootstrapTransportError::Delivery(error.to_string()))?;
        if !response
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .is_some_and(is_json_content_type)
        {
            return Err(GatewayBootstrapTransportError::Delivery(
                GatewayBootstrapTransportError::ContentType.to_string(),
            ));
        }
        Self::bounded_json(response)
            .await
            .map_err(|error| GatewayBootstrapTransportError::Delivery(error.to_string()))?;
        Ok(())
    }
}

fn is_json_content_type(value: &str) -> bool {
    value.split(';').next().is_some_and(|media_type| {
        media_type
            .trim()
            .eq_ignore_ascii_case(BOOTSTRAP_JSON_CONTENT_TYPE)
    })
}

/// Coordinates the Central domain service with a Central-initiated bootstrap transport.
pub struct GatewayReplicaActivationClient {
    service: Arc<GatewayReplicaActivationService>,
    transport: Arc<dyn GatewayBootstrapTransport>,
}

impl GatewayReplicaActivationClient {
    #[must_use]
    pub fn new(
        service: Arc<GatewayReplicaActivationService>,
        transport: Arc<dyn GatewayBootstrapTransport>,
    ) -> Self {
        Self { service, transport }
    }

    /// Performs a recoverable prepare -> deliver -> commit activation.  Certificate material is
    /// persisted by Central before delivery, so a transport failure leaves a retryable pending
    /// activation instead of consuming the one-time token.
    pub async fn activate(
        &self,
        replica: &GatewayReplicaRecord,
        activation_token: &str,
    ) -> Result<GatewayReplicaActivationResult, GatewayReplicaActivationClientError> {
        // Resolve the destination from Central's Registry for every attempt.  The record passed
        // by a management caller is only an identity/optimistic-concurrency hint; its endpoint
        // must never be trusted as a credential-delivery destination.
        let canonical = self
            .service
            .replica_for_activation(activation_token)
            .await
            .map_err(GatewayReplicaActivationClientError::Activation)?;
        if canonical.gateway_replica_id != replica.gateway_replica_id
            || canonical.gateway_pool_id != replica.gateway_pool_id
            || canonical.edge_cluster_id != replica.edge_cluster_id
            || canonical.bootstrap_endpoint != replica.bootstrap_endpoint
        {
            return Err(GatewayReplicaActivationClientError::Endpoint(
                "Gateway Replica record does not match the Registry-owned activation endpoint"
                    .into(),
            ));
        }
        let canonical_bootstrap_endpoint = canonical.bootstrap_endpoint.clone();
        let prepared = if let Some(prepared) = self
            .service
            .resume_prepared(activation_token)
            .await
            .map_err(GatewayReplicaActivationClientError::Activation)?
        {
            if prepared.replica.gateway_replica_id != replica.gateway_replica_id {
                return Err(GatewayReplicaActivationClientError::Activation(
                    GatewayReplicaActivationError::CredentialRejected,
                ));
            }
            if prepared.replica.bootstrap_endpoint != canonical_bootstrap_endpoint {
                return Err(GatewayReplicaActivationClientError::Endpoint(
                    "prepared Gateway Replica endpoint no longer matches the Registry".into(),
                ));
            }
            prepared
        } else {
            if canonical_bootstrap_endpoint.is_empty() {
                return Err(GatewayReplicaActivationClientError::Endpoint(
                    "Gateway Replica bootstrap endpoint is empty".into(),
                ));
            }
            let challenge = self
                .service
                .issue_challenge(activation_token)
                .await
                .map_err(GatewayReplicaActivationClientError::Activation)?;
            if challenge.gateway_replica_id() != &replica.gateway_replica_id {
                return Err(GatewayReplicaActivationClientError::Activation(
                    GatewayReplicaActivationError::CredentialRejected,
                ));
            }
            let proof = self
                .transport
                .prove(&canonical_bootstrap_endpoint, &challenge)
                .await
                .map_err(GatewayReplicaActivationClientError::Transport)?;
            self.service
                .prepare(&challenge, activation_token, &proof)
                .await
                .map_err(GatewayReplicaActivationClientError::Activation)?
        };
        self.transport
            .deliver_certificate(&canonical_bootstrap_endpoint, &prepared.certificate)
            .await
            .map_err(GatewayReplicaActivationClientError::Transport)?;
        self.service
            .commit(&prepared)
            .await
            .map_err(GatewayReplicaActivationClientError::Activation)
    }
}

#[derive(Debug, Error)]
pub enum GatewayReplicaActivationClientError {
    #[error("Gateway Replica bootstrap endpoint is invalid: {0}")]
    Endpoint(String),
    #[error(transparent)]
    Activation(#[from] GatewayReplicaActivationError),
    #[error(transparent)]
    Transport(#[from] GatewayBootstrapTransportError),
}

fn bootstrap_url(endpoint: &str, path: &str) -> Result<Url, GatewayBootstrapTransportError> {
    let mut url = Url::parse(endpoint)
        .map_err(|error| GatewayBootstrapTransportError::Endpoint(error.to_string()))?;
    let parsed_origin = url.as_str().strip_suffix('/').unwrap_or(url.as_str());
    let supplied_origin = endpoint.strip_suffix('/').unwrap_or(endpoint);
    if parsed_origin != supplied_origin
        || url.path() != "/"
        || url.query().is_some()
        || url.fragment().is_some()
        || !url.username().is_empty()
        || url.password().is_some()
    {
        return Err(GatewayBootstrapTransportError::Endpoint(
            "Gateway bootstrap endpoint must be a canonical origin".into(),
        ));
    }
    if url.scheme() == "http" && !is_loopback_host(&url) {
        return Err(GatewayBootstrapTransportError::InsecureEndpoint);
    }
    if url.scheme() != "http" && url.scheme() != "https" {
        return Err(GatewayBootstrapTransportError::Endpoint(
            "Gateway bootstrap endpoint must use HTTP or HTTPS".into(),
        ));
    }
    url.set_path(path);
    Ok(url)
}

fn is_loopback_host(endpoint: &Url) -> bool {
    endpoint.host().is_some_and(|host| match host {
        url::Host::Domain(domain) => domain.eq_ignore_ascii_case("localhost"),
        url::Host::Ipv4(address) => address.is_loopback(),
        url::Host::Ipv6(address) => address.is_loopback(),
    })
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeSet, VecDeque};
    use std::convert::Infallible;
    use std::pin::Pin;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::task::{Context, Poll};

    use crate::{
        GatewayCredentialState, GatewayPoolRecord, GatewayPoolState, GatewayRegistryRepository,
        GatewayReplicaCredential, GatewayReplicaState, InMemoryClock, InMemoryGatewayRegistry,
    };
    use async_trait::async_trait;
    use bytes::Bytes;
    use hyper::body::{Body, Frame};
    use neoengram_domain::core::ContentDigest;
    use neoengram_domain::protocol::{
        Ed25519PublicKeySpki, Ed25519Signature, EdgeClusterId, Extensions, GatewayPoolId,
        GatewayReplicaId, Generation, PrincipalId, PrincipalKind, PrincipalRef, ResourceVersion,
        UnixMillis,
    };
    use ring::signature::{Ed25519KeyPair, KeyPair};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use tokio::sync::{Barrier, Mutex};

    use super::*;

    const TOKEN: &str = "nggw_v1_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";

    struct UnknownLengthBody {
        chunks: VecDeque<Bytes>,
    }

    impl Body for UnknownLengthBody {
        type Data = Bytes;
        type Error = Infallible;

        fn poll_frame(
            mut self: Pin<&mut Self>,
            _context: &mut Context<'_>,
        ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
            Poll::Ready(self.chunks.pop_front().map(Frame::data).map(Ok))
        }
    }

    fn unknown_length_response(chunks: impl IntoIterator<Item = Bytes>) -> reqwest::Response {
        let body = reqwest::Body::wrap(UnknownLengthBody {
            chunks: chunks.into_iter().collect(),
        });
        http::Response::builder().body(body).unwrap().into()
    }

    #[derive(Clone)]
    struct CountingIssuer {
        calls: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl crate::service::WorkloadCertificateIssuer for CountingIssuer {
        async fn issue(
            &self,
            request: crate::service::WorkloadCertificateRequest,
        ) -> Result<
            crate::service::IssuedWorkloadCertificate,
            crate::service::WorkloadCertificateIssuerError,
        > {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(crate::service::test_issue_workload_certificate(request))
        }
    }

    struct MockBootstrapTransport {
        fail_first_delivery: AtomicBool,
        prove_calls: AtomicUsize,
        deliver_calls: AtomicUsize,
        delivered_fingerprints: Mutex<Vec<ContentDigest>>,
        prove_barrier: Option<Arc<Barrier>>,
    }

    impl MockBootstrapTransport {
        fn new(fail_first_delivery: bool, prove_barrier: Option<Arc<Barrier>>) -> Self {
            Self {
                fail_first_delivery: AtomicBool::new(fail_first_delivery),
                prove_calls: AtomicUsize::new(0),
                deliver_calls: AtomicUsize::new(0),
                delivered_fingerprints: Mutex::new(Vec::new()),
                prove_barrier,
            }
        }
    }

    #[async_trait]
    impl GatewayBootstrapTransport for MockBootstrapTransport {
        async fn prove(
            &self,
            _bootstrap_endpoint: &str,
            challenge: &GatewayReplicaActivationChallenge,
        ) -> Result<GatewayReplicaActivationProof, GatewayBootstrapTransportError> {
            self.prove_calls.fetch_add(1, Ordering::SeqCst);
            if let Some(barrier) = &self.prove_barrier {
                barrier.wait().await;
            }
            let key = Ed25519KeyPair::from_seed_unchecked(&[7; 32])
                .map_err(|error| GatewayBootstrapTransportError::Request(error.to_string()))?;
            let public_key = Ed25519PublicKeySpki::from_public_key_bytes(
                key.public_key()
                    .as_ref()
                    .try_into()
                    .map_err(|_| GatewayBootstrapTransportError::Request("invalid key".into()))?,
            );
            let signature =
                Ed25519Signature::new(
                    key.sign(&challenge.signing_bytes().map_err(|error| {
                        GatewayBootstrapTransportError::Request(error.to_string())
                    })?)
                    .as_ref()
                    .to_vec(),
                )
                .map_err(|error| GatewayBootstrapTransportError::Request(error.to_string()))?;
            Ok(GatewayReplicaActivationProof::new(public_key, signature))
        }

        async fn deliver_certificate(
            &self,
            _bootstrap_endpoint: &str,
            certificate: &IssuedWorkloadCertificate,
        ) -> Result<(), GatewayBootstrapTransportError> {
            self.deliver_calls.fetch_add(1, Ordering::SeqCst);
            self.delivered_fingerprints
                .lock()
                .await
                .push(certificate.leaf_certificate_fingerprint());
            if self.fail_first_delivery.swap(false, Ordering::SeqCst) {
                return Err(GatewayBootstrapTransportError::Delivery(
                    "injected delivery failure".into(),
                ));
            }
            Ok(())
        }
    }

    fn actor() -> PrincipalRef {
        PrincipalRef {
            kind: PrincipalKind::System,
            id: PrincipalId::new("system").unwrap(),
            extensions: Extensions::new(),
        }
    }

    fn pool() -> GatewayPoolRecord {
        GatewayPoolRecord {
            gateway_pool_id: GatewayPoolId::new("pool-a").unwrap(),
            edge_cluster_id: EdgeClusterId::new("edge-a").unwrap(),
            display_name: "pool".to_owned(),
            agent_endpoint: "https://gateway.example.test".to_owned(),
            s3_endpoint: None,
            desired_replicas: 1,
            minimum_ready_replicas: 1,
            state: GatewayPoolState::Provisioning,
            config_generation: Generation::new(1),
            resource_version: ResourceVersion::new(1),
            created_at_unix_ms: UnixMillis::new(1_000),
            updated_at_unix_ms: UnixMillis::new(1_000),
            created_by: actor(),
            updated_by: actor(),
        }
    }

    fn replica() -> GatewayReplicaRecord {
        GatewayReplicaRecord {
            gateway_replica_id: GatewayReplicaId::new("replica-a").unwrap(),
            gateway_pool_id: GatewayPoolId::new("pool-a").unwrap(),
            edge_cluster_id: EdgeClusterId::new("edge-a").unwrap(),
            control_endpoint: "https://control.gateway.example.test".to_owned(),
            peer_endpoint: "https://peer.gateway.example.test".to_owned(),
            bootstrap_endpoint: "https://bootstrap.gateway.example.test".to_owned(),
            software_version: "test".to_owned(),
            wire_version: CURRENT_WIRE_VERSION,
            capabilities: BTreeSet::from(["agent_edge".to_owned()]),
            last_heartbeat_at_unix_ms: None,
            state: GatewayReplicaState::Pending,
            credential: GatewayReplicaCredential {
                activation_token_digest: ContentDigest::hash(TOKEN.as_bytes()),
                activation_created_at_unix_ms: UnixMillis::new(1_000),
                activation_expires_at_unix_ms: UnixMillis::new(901_000),
                activation_consumed_at_unix_ms: None,
                public_key_fingerprint: None,
                certificate_generation: None,
                certificate_fingerprint: None,
                certificate_not_after_unix_ms: None,
                certificate: None,
                state: GatewayCredentialState::PendingActivation,
            },
            resource_version: ResourceVersion::new(1),
            created_at_unix_ms: UnixMillis::new(1_000),
            updated_at_unix_ms: UnixMillis::new(1_000),
        }
    }

    async fn client_fixture(
        fail_first_delivery: bool,
        prove_barrier: Option<Arc<Barrier>>,
    ) -> (
        Arc<GatewayReplicaActivationService>,
        Arc<InMemoryGatewayRegistry>,
        Arc<MockBootstrapTransport>,
        Arc<AtomicUsize>,
        GatewayReplicaRecord,
    ) {
        let repository = Arc::new(InMemoryGatewayRegistry::new());
        repository.insert_pool(pool()).await.unwrap();
        let record = replica();
        repository.insert_replica(record.clone()).await.unwrap();
        let issuer_calls = Arc::new(AtomicUsize::new(0));
        let service = Arc::new(GatewayReplicaActivationService::new(
            repository.clone(),
            Arc::new(CountingIssuer {
                calls: issuer_calls.clone(),
            }),
            Arc::new(InMemoryClock::new(2_000)),
            "mesh.example.test",
        ));
        let transport = Arc::new(MockBootstrapTransport::new(
            fail_first_delivery,
            prove_barrier,
        ));
        (service, repository, transport, issuer_calls, record)
    }

    #[test]
    fn bootstrap_url_requires_a_canonical_secure_origin() {
        assert_eq!(
            bootstrap_url(
                "https://gateway.example.test/",
                GATEWAY_REPLICA_BOOTSTRAP_CHALLENGE_PATH
            )
            .unwrap()
            .as_str(),
            "https://gateway.example.test/gateway/bootstrap/challenge"
        );
        assert_eq!(
            bootstrap_url(
                "https://gateway.example.test",
                GATEWAY_REPLICA_BOOTSTRAP_CHALLENGE_PATH
            )
            .unwrap()
            .as_str(),
            "https://gateway.example.test/gateway/bootstrap/challenge"
        );
        assert!(matches!(
            bootstrap_url(
                "https://gateway.example.test/not-an-origin",
                GATEWAY_REPLICA_BOOTSTRAP_CHALLENGE_PATH
            ),
            Err(GatewayBootstrapTransportError::Endpoint(_))
        ));
        assert!(matches!(
            bootstrap_url(
                "http://public.gateway.example.test/",
                GATEWAY_REPLICA_BOOTSTRAP_CHALLENGE_PATH
            ),
            Err(GatewayBootstrapTransportError::InsecureEndpoint)
        ));
        assert!(bootstrap_url(
            "http://127.0.0.1:18081/",
            GATEWAY_REPLICA_BOOTSTRAP_CERTIFICATE_PATH
        )
        .is_ok());
        assert!(bootstrap_url(
            "http://[::1]:18081/",
            GATEWAY_REPLICA_BOOTSTRAP_CERTIFICATE_PATH
        )
        .is_ok());
    }

    #[test]
    fn json_content_type_requires_the_exact_media_type() {
        assert!(is_json_content_type("application/json"));
        assert!(is_json_content_type(" Application/JSON ; charset=utf-8"));
        assert!(!is_json_content_type("application/jsonx"));
        assert!(!is_json_content_type("text/application/json"));
    }

    #[tokio::test]
    async fn bounded_json_rejects_an_oversized_stream_without_a_known_length() {
        let chunk_size = (MAX_GATEWAY_CONTROL_FRAME_BYTES / 2) + 1;
        let response = unknown_length_response([
            Bytes::from(vec![b'a'; chunk_size]),
            Bytes::from(vec![b'b'; chunk_size]),
        ]);
        assert_eq!(response.content_length(), None);

        let error = HttpGatewayBootstrapTransport::bounded_json(response)
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            GatewayBootstrapTransportError::ResponseTooLarge
        ));
    }

    #[tokio::test]
    async fn constructor_disables_redirects_before_any_bootstrap_payload_is_sent() {
        let sink_hits = Arc::new(AtomicUsize::new(0));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let sink_hits_for_server = sink_hits.clone();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            let mut buffer = [0_u8; 1024];
            loop {
                let read = stream.read(&mut buffer).await.unwrap();
                if read == 0 {
                    break;
                }
                request.extend_from_slice(&buffer[..read]);
                if request.windows(4).any(|window| window == b"\r\n\r\n") {
                    break;
                }
                assert!(request.len() <= 16 * 1024);
            }
            let request_line = request
                .split(|byte| *byte == b'\n')
                .next()
                .unwrap_or_default();
            if request_line.starts_with(b"POST /sink ") {
                sink_hits_for_server.fetch_add(1, Ordering::SeqCst);
                stream
                    .write_all(b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\n\r\n")
                    .await
                    .unwrap();
            } else {
                stream
                    .write_all(
                        b"HTTP/1.1 307 Temporary Redirect\r\nLocation: /sink\r\nContent-Length: 0\r\n\r\n",
                    )
                    .await
                    .unwrap();
            }
        });

        let transport = HttpGatewayBootstrapTransport::new(reqwest::Client::builder()).unwrap();
        let result = transport
            .post_json(
                &format!("http://{address}"),
                GATEWAY_REPLICA_BOOTSTRAP_CHALLENGE_PATH,
                &serde_json::json!({"probe": true}),
            )
            .await;
        assert!(matches!(
            result,
            Err(GatewayBootstrapTransportError::Status(307))
        ));
        assert_eq!(sink_hits.load(Ordering::SeqCst), 0);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn client_rejects_a_caller_supplied_bootstrap_endpoint_override() {
        let (service, _repository, transport, issuer_calls, mut record) =
            client_fixture(false, None).await;
        record.bootstrap_endpoint = "https://attacker.example.test".to_owned();
        let client = GatewayReplicaActivationClient::new(service, transport.clone());

        let result = client.activate(&record, TOKEN).await;
        assert!(matches!(
            result,
            Err(GatewayReplicaActivationClientError::Endpoint(_))
        ));
        assert_eq!(transport.prove_calls.load(Ordering::SeqCst), 0);
        assert_eq!(transport.deliver_calls.load(Ordering::SeqCst), 0);
        assert_eq!(issuer_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn client_retries_delivery_from_persisted_prepare_without_reissuing() {
        let (service, repository, transport, issuer_calls, record) =
            client_fixture(true, None).await;
        let client = GatewayReplicaActivationClient::new(service, transport.clone());
        let first = client.activate(&record, TOKEN).await;
        assert!(matches!(
            first,
            Err(GatewayReplicaActivationClientError::Transport(
                GatewayBootstrapTransportError::Delivery(_)
            ))
        ));
        let pending = repository
            .get_replica(&record.gateway_replica_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(pending.state, crate::GatewayReplicaState::Pending);
        assert_eq!(
            pending.credential.state,
            crate::GatewayCredentialState::PendingCertificateDelivery
        );
        assert!(pending.credential.activation_consumed_at_unix_ms.is_none());
        assert!(pending.credential.certificate.is_some());

        let second = client.activate(&record, TOKEN).await.unwrap();
        assert_eq!(second.replica.state, crate::GatewayReplicaState::Active);
        assert_eq!(issuer_calls.load(Ordering::SeqCst), 1);
        assert_eq!(transport.prove_calls.load(Ordering::SeqCst), 1);
        assert_eq!(transport.deliver_calls.load(Ordering::SeqCst), 2);
        let delivered = transport.delivered_fingerprints.lock().await.clone();
        assert_eq!(delivered.len(), 2);
        assert_eq!(delivered[0], delivered[1]);
    }

    #[tokio::test]
    async fn concurrent_clients_have_one_commit_winner() {
        let (service, repository, transport, _issuer_calls, record) =
            client_fixture(false, Some(Arc::new(Barrier::new(2)))).await;
        let first_client = Arc::new(GatewayReplicaActivationClient::new(
            service.clone(),
            transport.clone(),
        ));
        let second_client = Arc::new(GatewayReplicaActivationClient::new(
            service,
            transport.clone(),
        ));
        let first_record = record.clone();
        let second_record = record;
        let first = tokio::spawn(async move { first_client.activate(&first_record, TOKEN).await });
        let second =
            tokio::spawn(async move { second_client.activate(&second_record, TOKEN).await });
        let first = first.await.unwrap();
        let second = second.await.unwrap();
        assert_eq!(usize::from(first.is_ok()) + usize::from(second.is_ok()), 1);
        assert!(matches!(
            (&first, &second),
            (
                Err(GatewayReplicaActivationClientError::Activation(
                    GatewayReplicaActivationError::ConcurrentActivation
                )),
                Ok(_)
            ) | (
                Ok(_),
                Err(GatewayReplicaActivationClientError::Activation(
                    GatewayReplicaActivationError::ConcurrentActivation
                ))
            ) | (
                Err(GatewayReplicaActivationClientError::Activation(
                    GatewayReplicaActivationError::CredentialRejected
                )),
                Ok(_)
            ) | (
                Ok(_),
                Err(GatewayReplicaActivationClientError::Activation(
                    GatewayReplicaActivationError::CredentialRejected
                ))
            )
        ));
        let stored = repository
            .get_replica(&GatewayReplicaId::new("replica-a").unwrap())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.state, crate::GatewayReplicaState::Active);
    }
}
