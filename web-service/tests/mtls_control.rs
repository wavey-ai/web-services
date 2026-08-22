use async_trait::async_trait;
use base64::{engine::general_purpose::STANDARD, Engine as _};
use bytes::Bytes;
use http::{Request, StatusCode};
use rcgen::{
    BasicConstraints, Certificate, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa,
    KeyPair, KeyUsagePurpose,
};
use std::net::{IpAddr, Ipv4Addr};
use std::time::Duration;
use web_service::{
    H2H3Server, HandlerResponse, HandlerResult, Router, Server, ServerBuilder, ServerError,
    StreamWriter, VerifiedClientCertificate, WebSocketHandler, WebTransportHandler,
};

struct IdentityRouter;

#[async_trait]
impl Router for IdentityRouter {
    async fn route(&self, request: Request<()>) -> HandlerResult<HandlerResponse> {
        let Some(identity) = request.extensions().get::<VerifiedClientCertificate>() else {
            return Ok(HandlerResponse {
                status: StatusCode::UNAUTHORIZED,
                ..HandlerResponse::default()
            });
        };
        Ok(HandlerResponse {
            status: StatusCode::OK,
            body: Some(Bytes::from(format!(
                "{:02x}",
                identity.sha256_fingerprint()[0]
            ))),
            ..HandlerResponse::default()
        })
    }

    fn is_streaming(&self, _path: &str) -> bool {
        false
    }

    async fn route_stream(
        &self,
        _request: Request<()>,
        _stream_writer: Box<dyn StreamWriter>,
    ) -> HandlerResult<()> {
        Err(ServerError::Config("not a streaming route".into()))
    }

    fn webtransport_handler(&self) -> Option<&dyn WebTransportHandler> {
        None
    }

    fn websocket_handler(&self, _path: &str) -> Option<&dyn WebSocketHandler> {
        None
    }
}

struct TestPki {
    server_certificate_base64: String,
    server_key_base64: String,
    server_ca_pem: String,
    worker_ca_base64: String,
    worker_identity_pem: String,
    unknown_identity_pem: String,
}

fn new_ca(common_name: &str) -> (Certificate, KeyPair) {
    let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params
        .distinguished_name
        .push(DnType::CommonName, common_name);
    params.key_usages = vec![
        KeyUsagePurpose::DigitalSignature,
        KeyUsagePurpose::KeyCertSign,
        KeyUsagePurpose::CrlSign,
    ];
    let key = KeyPair::generate().unwrap();
    (params.self_signed(&key).unwrap(), key)
}

fn issue_leaf(
    name: &str,
    usage: ExtendedKeyUsagePurpose,
    ca: &Certificate,
    ca_key: &KeyPair,
) -> (Certificate, KeyPair) {
    let mut params = CertificateParams::new(vec![name.to_string()]).unwrap();
    params.distinguished_name.push(DnType::CommonName, name);
    params.use_authority_key_identifier_extension = true;
    params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
    params.extended_key_usages = vec![usage];
    let key = KeyPair::generate().unwrap();
    (params.signed_by(&key, ca, ca_key).unwrap(), key)
}

fn test_pki() -> TestPki {
    let (server_ca, server_ca_key) = new_ca("test server CA");
    let (worker_ca, worker_ca_key) = new_ca("test worker CA");
    let (unknown_ca, unknown_ca_key) = new_ca("unknown worker CA");
    let (server_certificate, server_key) = issue_leaf(
        "localhost",
        ExtendedKeyUsagePurpose::ServerAuth,
        &server_ca,
        &server_ca_key,
    );
    let (worker_certificate, worker_key) = issue_leaf(
        "worker.test",
        ExtendedKeyUsagePurpose::ClientAuth,
        &worker_ca,
        &worker_ca_key,
    );
    let (unknown_certificate, unknown_key) = issue_leaf(
        "unknown.test",
        ExtendedKeyUsagePurpose::ClientAuth,
        &unknown_ca,
        &unknown_ca_key,
    );

    TestPki {
        server_certificate_base64: STANDARD.encode(server_certificate.pem()),
        server_key_base64: STANDARD.encode(server_key.serialize_pem()),
        server_ca_pem: server_ca.pem(),
        worker_ca_base64: STANDARD.encode(worker_ca.pem()),
        worker_identity_pem: format!("{}{}", worker_certificate.pem(), worker_key.serialize_pem()),
        unknown_identity_pem: format!(
            "{}{}",
            unknown_certificate.pem(),
            unknown_key.serialize_pem()
        ),
    }
}

fn test_client(server_ca_pem: &str, identity_pem: Option<&str>) -> reqwest::Client {
    let roots = reqwest::Certificate::from_pem_bundle(server_ca_pem.as_bytes()).unwrap();
    let mut builder = reqwest::Client::builder()
        .tls_certs_only(roots)
        .connect_timeout(Duration::from_secs(2));
    if let Some(identity_pem) = identity_pem {
        builder = builder.identity(reqwest::Identity::from_pem(identity_pem.as_bytes()).unwrap());
    }
    builder.build().unwrap()
}

#[tokio::test]
async fn mtls_rejects_absent_and_unknown_clients_and_injects_valid_identity() {
    let pki = test_pki();
    let port = portpicker::pick_unused_port().expect("available test port");
    let server = H2H3Server::builder()
        .with_tls(
            pki.server_certificate_base64.clone(),
            pki.server_key_base64.clone(),
        )
        .with_bind_address(IpAddr::V4(Ipv4Addr::LOCALHOST))
        .with_client_ca(pki.worker_ca_base64.clone())
        .with_port(port)
        .enable_h2(true)
        .enable_h3(false)
        .enable_websocket(false)
        .enable_webtransport(false)
        .with_router(Box::new(IdentityRouter))
        .build()
        .unwrap();
    let handle = server.start().await.unwrap();
    let origin = format!("https://localhost:{port}");

    let absent = test_client(&pki.server_ca_pem, None)
        .get(&origin)
        .send()
        .await;
    assert!(
        absent.is_err(),
        "server accepted a missing client certificate"
    );

    let unknown = test_client(&pki.server_ca_pem, Some(&pki.unknown_identity_pem))
        .get(&origin)
        .send()
        .await;
    assert!(unknown.is_err(), "server accepted an unknown client CA");

    let valid = test_client(&pki.server_ca_pem, Some(&pki.worker_identity_pem))
        .get(&origin)
        .send()
        .await
        .unwrap();
    assert_eq!(valid.status(), StatusCode::OK);
    assert_eq!(valid.version(), http::Version::HTTP_2);
    assert_eq!(valid.text().await.unwrap().len(), 2);

    let _ = handle.shutdown_tx.send(());
    tokio::time::timeout(Duration::from_secs(2), handle.finished_rx)
        .await
        .expect("server shutdown timed out")
        .expect("server supervisor dropped completion signal");
}

#[test]
fn client_ca_rejects_mixed_h2_h3_listener() {
    let pki = test_pki();
    let result = H2H3Server::builder()
        .with_tls(pki.server_certificate_base64, pki.server_key_base64)
        .with_client_ca(pki.worker_ca_base64)
        .enable_h2(true)
        .enable_h3(true)
        .with_router(Box::new(IdentityRouter))
        .build();
    let error = match result {
        Ok(_) => panic!("mixed listener must reject client certificate authentication"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("HTTP/2-only"));
}
