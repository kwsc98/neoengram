//! Shared Gateway-to-Central HTTP client.
//!
//! Public requests must never talk to Central through the process' plain HTTP connector in a
//! deployed Gateway.  This wrapper builds one rustls-backed connector (with the workload client
//! certificate loaded by `GatewayTransportConfig`) and is shared by the S3 authorizer and the
//! browser `/api` reverse proxy.  HTTP remains available only for explicitly loopback development
//! configurations; the production startup path rejects that combination.

use std::{error::Error, sync::Arc};

use bytes::Bytes;
use http::{Request, Response};
use http_body_util::{combinators::UnsyncBoxBody, BodyExt as _};
use hyper::body::{Body, Incoming};
use hyper_rustls::HttpsConnectorBuilder;
use hyper_util::{
    client::legacy::{connect::HttpConnector, Client},
    rt::TokioExecutor,
};
use rustls::ClientConfig;

use crate::public_listener::BoxError;

pub(crate) type CentralBody = UnsyncBoxBody<Bytes, BoxError>;
type Connector = hyper_rustls::HttpsConnector<HttpConnector>;
type ClientImpl = Client<Connector, CentralBody>;

#[derive(Clone)]
pub(crate) struct CentralHttpClient {
    client: ClientImpl,
}

impl std::fmt::Debug for CentralHttpClient {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CentralHttpClient")
            .finish_non_exhaustive()
    }
}

impl CentralHttpClient {
    /// Builds a client using the Gateway workload identity.  `tls` must be present whenever the
    /// process can reach a non-loopback Central endpoint; the caller performs that policy check
    /// before startup.  A missing identity is useful only for loopback HTTP development/tests.
    pub(crate) fn new(tls: Option<Arc<ClientConfig>>) -> Result<Self, String> {
        let mut config = match tls {
            Some(config) => (*config).clone(),
            None => ClientConfig::builder_with_provider(Arc::new(
                rustls::crypto::aws_lc_rs::default_provider(),
            ))
            .with_safe_default_protocol_versions()
            .map_err(|error| format!("failed to select TLS protocol versions: {error}"))?
            .with_root_certificates(rustls::RootCertStore::empty())
            .with_no_client_auth(),
        };
        // `HttpsConnectorBuilder` sets ALPN according to the enabled protocol features.  The
        // shared workload config is also used by the peer H2 connector and already has ALPN set.
        // Clear it before handing ownership to the builder.
        config.alpn_protocols.clear();
        let connector = HttpsConnectorBuilder::new()
            .with_tls_config(config)
            .https_or_http()
            .enable_http1()
            .enable_http2()
            .build();
        Ok(Self {
            client: Client::builder(TokioExecutor::new()).build(connector),
        })
    }

    pub(crate) async fn request<B>(
        &self,
        request: Request<B>,
    ) -> Result<Response<Incoming>, hyper_util::client::legacy::Error>
    where
        B: Body<Data = Bytes> + Send + 'static,
        B::Error: Error + Send + Sync + 'static,
    {
        let request = request.map(box_body);
        self.client.request(request).await
    }
}

fn box_body<B>(body: B) -> CentralBody
where
    B: Body<Data = Bytes> + Send + 'static,
    B::Error: Error + Send + Sync + 'static,
{
    body.map_err(|error| Box::new(error) as BoxError)
        .boxed_unsync()
}

#[cfg(test)]
mod tests {
    use std::{convert::Infallible, net::Ipv4Addr};

    use super::*;
    use http::StatusCode;
    use http_body_util::Full;
    use hyper::{server::conn::http1, service::service_fn};
    use hyper_util::rt::TokioIo;
    use rcgen::{
        BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, KeyPair,
        KeyUsagePurpose, SanType,
    };
    use rustls::{server::WebPkiClientVerifier, RootCertStore, ServerConfig};
    use rustls_pki_types::PrivatePkcs8KeyDer;
    use tokio::{net::TcpListener, sync::oneshot};
    use tokio_rustls::TlsAcceptor;

    #[test]
    fn loopback_http_client_selects_an_explicit_crypto_provider() {
        CentralHttpClient::new(None).expect("loopback client must not depend on global provider");
    }

    #[tokio::test]
    async fn https_client_verifies_central_and_presents_gateway_identity() {
        let ca_key = KeyPair::generate().unwrap();
        let mut ca_parameters = CertificateParams::default();
        ca_parameters.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        ca_parameters.key_usages = vec![
            KeyUsagePurpose::KeyCertSign,
            KeyUsagePurpose::CrlSign,
            KeyUsagePurpose::DigitalSignature,
        ];
        let ca = ca_parameters.self_signed(&ca_key).unwrap();

        let server_key = KeyPair::generate().unwrap();
        let mut server_parameters = CertificateParams::new(Vec::<String>::new()).unwrap();
        server_parameters
            .subject_alt_names
            .push(SanType::IpAddress(Ipv4Addr::LOCALHOST.into()));
        server_parameters.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
        let server_certificate = server_parameters
            .signed_by(&server_key, &ca, &ca_key)
            .unwrap();

        let gateway_key = KeyPair::generate().unwrap();
        let mut gateway_parameters = CertificateParams::new(Vec::<String>::new()).unwrap();
        gateway_parameters.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
        let gateway_certificate = gateway_parameters
            .signed_by(&gateway_key, &ca, &ca_key)
            .unwrap();

        let provider: Arc<rustls::crypto::CryptoProvider> =
            rustls::crypto::aws_lc_rs::default_provider().into();
        let mut client_roots = RootCertStore::empty();
        client_roots.add(ca.der().clone()).unwrap();
        let client_config = ClientConfig::builder_with_provider(provider.clone())
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_root_certificates(client_roots)
            .with_client_auth_cert(
                vec![gateway_certificate.der().clone(), ca.der().clone()],
                PrivatePkcs8KeyDer::from(gateway_key.serialize_der()).into(),
            )
            .unwrap();
        let client = CentralHttpClient::new(Some(Arc::new(client_config))).unwrap();

        let mut server_roots = RootCertStore::empty();
        server_roots.add(ca.der().clone()).unwrap();
        let verifier =
            WebPkiClientVerifier::builder_with_provider(Arc::new(server_roots), provider.clone())
                .build()
                .unwrap();
        let mut server_config = ServerConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_client_cert_verifier(verifier)
            .with_single_cert(
                vec![server_certificate.der().clone(), ca.der().clone()],
                PrivatePkcs8KeyDer::from(server_key.serialize_der()).into(),
            )
            .unwrap();
        server_config.alpn_protocols = vec![b"http/1.1".to_vec()];

        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let address = listener.local_addr().unwrap();
        let expected_gateway_certificate = gateway_certificate.der().clone();
        let (presented_sender, presented_receiver) = oneshot::channel();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let stream = TlsAcceptor::from(Arc::new(server_config))
                .accept(stream)
                .await
                .unwrap();
            let presented = stream
                .get_ref()
                .1
                .peer_certificates()
                .and_then(|certificates| certificates.first())
                .cloned();
            let _ = presented_sender.send(presented);
            let mut builder = http1::Builder::new();
            builder.keep_alive(false);
            builder
                .serve_connection(
                    TokioIo::new(stream),
                    service_fn(|_request| async {
                        Ok::<_, Infallible>(http::Response::new(Full::new(Bytes::from_static(
                            b"ok",
                        ))))
                    }),
                )
                .await
                .unwrap();
        });

        let response = client
            .request(
                Request::builder()
                    .uri(format!("https://{address}/api"))
                    .body(Full::new(Bytes::new()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            presented_receiver.await.unwrap().unwrap(),
            expected_gateway_certificate
        );
        drop(response);
        server.await.unwrap();
    }
}
