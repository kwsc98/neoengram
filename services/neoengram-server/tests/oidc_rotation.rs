use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use http::{header::AUTHORIZATION, HeaderMap, HeaderValue};
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use neoengram_server::identity::{
    AuthenticationFailure, Authenticator, OidcAuthenticator, OidcConfig,
};
use serde::Serialize;
use serde_json::json;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{oneshot, RwLock},
    task::{JoinHandle, JoinSet},
};
use url::Url;

const AUDIENCE: &str = "neoengram-api";
const SUBJECT: &str = "oidc-test-user";
const ED25519_PUBLIC_X: &str = "2-Jj2UvNCvQiUPNYRgSi0cJSPiJI6Rs6D0UTeEpQVj8";

// PKCS#8 DER for a test-only Ed25519 key. Its public component is ED25519_PUBLIC_X.
const ED25519_PRIVATE_KEY_DER: &[u8] = &[
    0x30, 0x2e, 0x02, 0x01, 0x00, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x70, 0x04, 0x22, 0x04, 0x20,
    0x6a, 0xc3, 0xfd, 0xee, 0xee, 0x29, 0x8a, 0x92, 0x63, 0x8b, 0x70, 0x0c, 0x4b, 0x11, 0x7c, 0xc3,
    0x2e, 0x2d, 0x2a, 0xce, 0x0d, 0xfd, 0x78, 0x76, 0x94, 0xe2, 0x4c, 0xae, 0x8a, 0xd5, 0x82, 0x34,
];

#[derive(Serialize)]
struct TestClaims<'a> {
    iss: &'a str,
    aud: &'a str,
    sub: &'a str,
    exp: u64,
    iat: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    nbf: Option<u64>,
}

struct TestProvider {
    issuer: Url,
    active_kid: Arc<RwLock<String>>,
    jwks_requests: Arc<AtomicUsize>,
    shutdown: Option<oneshot::Sender<()>>,
    task: JoinHandle<()>,
}

impl TestProvider {
    async fn start(initial_kid: &str) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let issuer = Url::parse(&format!("http://{address}")).unwrap();
        let active_kid = Arc::new(RwLock::new(initial_kid.to_owned()));
        let jwks_requests = Arc::new(AtomicUsize::new(0));
        let (shutdown, mut shutdown_rx) = oneshot::channel();

        let task_issuer = issuer.to_string();
        let task_kid = Arc::clone(&active_kid);
        let task_requests = Arc::clone(&jwks_requests);
        let task = tokio::spawn(async move {
            let mut connections = JoinSet::new();
            loop {
                tokio::select! {
                    result = listener.accept() => {
                        let Ok((stream, _peer)) = result else {
                            break;
                        };
                        let issuer = task_issuer.clone();
                        let kid = Arc::clone(&task_kid);
                        let requests = Arc::clone(&task_requests);
                        connections.spawn(async move {
                            let _ = tokio::time::timeout(
                                Duration::from_secs(2),
                                serve_connection(stream, &issuer, &kid, &requests),
                            )
                            .await;
                        });
                    }
                    _ = &mut shutdown_rx => break,
                }
            }
            connections.abort_all();
            while connections.join_next().await.is_some() {}
        });

        Self {
            issuer,
            active_kid,
            jwks_requests,
            shutdown: Some(shutdown),
            task,
        }
    }

    async fn rotate_to(&self, kid: &str) {
        *self.active_kid.write().await = kid.to_owned();
    }

    fn jwks_request_count(&self) -> usize {
        self.jwks_requests.load(Ordering::SeqCst)
    }

    async fn shutdown(mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        tokio::time::timeout(Duration::from_secs(2), self.task)
            .await
            .expect("OIDC test server did not stop in time")
            .expect("OIDC test server task failed");
    }
}

async fn serve_connection(
    mut stream: TcpStream,
    issuer: &str,
    active_kid: &RwLock<String>,
    jwks_requests: &AtomicUsize,
) {
    let mut request = Vec::with_capacity(1024);
    let mut buffer = [0_u8; 512];
    while !request.windows(4).any(|window| window == b"\r\n\r\n") {
        let read = stream.read(&mut buffer).await.unwrap();
        if read == 0 || request.len() + read > 8 * 1024 {
            return;
        }
        request.extend_from_slice(&buffer[..read]);
    }

    let request_line = request.split(|byte| *byte == b'\n').next().unwrap();
    let path = std::str::from_utf8(request_line)
        .unwrap()
        .split_ascii_whitespace()
        .nth(1)
        .unwrap();
    let (status, body) = match path {
        "/.well-known/openid-configuration" => (
            "200 OK",
            json!({
                "issuer": issuer,
                "jwks_uri": format!("{}/jwks", issuer.trim_end_matches('/')),
                "id_token_signing_alg_values_supported": ["EdDSA"]
            })
            .to_string(),
        ),
        "/jwks" => {
            jwks_requests.fetch_add(1, Ordering::SeqCst);
            let kid = active_kid.read().await.clone();
            (
                "200 OK",
                json!({
                    "keys": [{
                        "kty": "OKP",
                        "use": "sig",
                        "key_ops": ["verify"],
                        "crv": "Ed25519",
                        "x": ED25519_PUBLIC_X,
                        "kid": kid,
                        "alg": "EdDSA"
                    }]
                })
                .to_string(),
            )
        }
        _ => ("404 Not Found", json!({"error": "not_found"}).to_string()),
    };

    let response = format!(
        "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(response.as_bytes()).await.unwrap();
    stream.shutdown().await.unwrap();
}

async fn authenticator(provider: &TestProvider) -> OidcAuthenticator {
    let mut config = OidcConfig::new(provider.issuer.clone(), AUDIENCE);
    config.allow_insecure_http = true;
    config.cache_ttl = Duration::from_secs(2);
    config.stale_ttl = Duration::from_secs(5);
    config.request_timeout = Duration::from_secs(1);
    config.clock_skew = Duration::from_secs(1);
    OidcAuthenticator::discover(config).await.unwrap()
}

fn signed_token(
    issuer: &str,
    kid: &str,
    audience: &str,
    iat: u64,
    exp: u64,
    nbf: Option<u64>,
) -> String {
    let mut header = Header::new(Algorithm::EdDSA);
    header.kid = Some(kid.to_owned());
    encode(
        &header,
        &TestClaims {
            iss: issuer,
            aud: audience,
            sub: SUBJECT,
            exp,
            iat,
            nbf,
        },
        &EncodingKey::from_ed_der(ED25519_PRIVATE_KEY_DER),
    )
    .unwrap()
}

fn authorization_headers(token: &str) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(
        AUTHORIZATION,
        HeaderValue::from_str(&format!("Bearer {token}")).unwrap(),
    );
    headers
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

async fn assert_invalid(authenticator: &OidcAuthenticator, token: &str) {
    assert!(matches!(
        authenticator
            .authenticate(&authorization_headers(token))
            .await,
        Err(AuthenticationFailure::Invalid(_))
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn authenticates_valid_jwt_and_refreshes_for_rotated_kid() {
    let provider = TestProvider::start("key-v1").await;
    let authenticator = authenticator(&provider).await;
    assert_eq!(provider.jwks_request_count(), 1);

    let now = unix_now();
    let token = signed_token(
        provider.issuer.as_str(),
        "key-v1",
        AUDIENCE,
        now,
        now + 300,
        None,
    );
    let identity = authenticator
        .authenticate(&authorization_headers(&token))
        .await
        .unwrap();
    assert_eq!(identity.issuer(), provider.issuer.as_str());
    assert_eq!(identity.subject(), SUBJECT);
    assert_eq!(identity.principal().id.as_str(), SUBJECT);
    assert_eq!(provider.jwks_request_count(), 1);

    provider.rotate_to("key-v2").await;
    let rotated = signed_token(
        provider.issuer.as_str(),
        "key-v2",
        AUDIENCE,
        now,
        now + 300,
        None,
    );
    authenticator
        .authenticate(&authorization_headers(&rotated))
        .await
        .unwrap();
    assert_eq!(provider.jwks_request_count(), 2);

    provider.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unknown_kid_refreshes_once_then_fails_closed() {
    let provider = TestProvider::start("known-key").await;
    let authenticator = authenticator(&provider).await;
    let now = unix_now();
    let unknown = signed_token(
        provider.issuer.as_str(),
        "unknown-key",
        AUDIENCE,
        now,
        now + 300,
        None,
    );

    assert_invalid(&authenticator, &unknown).await;
    assert_eq!(provider.jwks_request_count(), 2);
    assert_invalid(&authenticator, &unknown).await;
    assert_eq!(
        provider.jwks_request_count(),
        2,
        "a negatively cached kid must not cause another JWKS fetch"
    );
    let another_unknown = signed_token(
        provider.issuer.as_str(),
        "another-unknown-key",
        AUDIENCE,
        now,
        now + 300,
        None,
    );
    assert_invalid(&authenticator, &another_unknown).await;
    assert_eq!(
        provider.jwks_request_count(),
        2,
        "alternating unknown kids must be covered by the global refresh cooldown"
    );

    provider.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rejects_invalid_audience_and_claim_time_boundaries() {
    let provider = TestProvider::start("claims-key").await;
    let authenticator = authenticator(&provider).await;
    let now = unix_now();

    let wrong_audience = signed_token(
        provider.issuer.as_str(),
        "claims-key",
        "another-service",
        now,
        now + 300,
        None,
    );
    assert_invalid(&authenticator, &wrong_audience).await;

    let expired = signed_token(
        provider.issuer.as_str(),
        "claims-key",
        AUDIENCE,
        now - 300,
        now - 120,
        None,
    );
    assert_invalid(&authenticator, &expired).await;

    let future_nbf = signed_token(
        provider.issuer.as_str(),
        "claims-key",
        AUDIENCE,
        now,
        now + 300,
        Some(now + 120),
    );
    assert_invalid(&authenticator, &future_nbf).await;

    let excessive_lifetime = signed_token(
        provider.issuer.as_str(),
        "claims-key",
        AUDIENCE,
        now,
        now + 3_601,
        None,
    );
    assert_invalid(&authenticator, &excessive_lifetime).await;

    assert_eq!(
        provider.jwks_request_count(),
        1,
        "claim failures with a known kid must not refresh JWKS"
    );
    provider.shutdown().await;
}
