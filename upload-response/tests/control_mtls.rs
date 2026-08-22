use base64::{engine::general_purpose::STANDARD, Engine as _};
use bytes::Bytes;
use http::StatusCode;
use rcgen::{
    BasicConstraints, Certificate, CertificateParams, ExtendedKeyUsagePurpose, IsCa, KeyPair,
    KeyUsagePurpose,
};
use std::net::{IpAddr, Ipv4Addr};
use std::sync::Arc;
use std::time::Duration;
use upload_response::{
    RemoteIngressClient, ResponseWatcher, UploadResponseConfig, UploadResponseControlRouter,
    UploadResponseService,
};
use web_service::{H2H3Server, HandlerResponse, Server, ServerBuilder};

fn new_ca() -> (Certificate, KeyPair) {
    let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
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
    params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
    params.extended_key_usages = vec![usage];
    let key = KeyPair::generate().unwrap();
    (params.signed_by(&key, ca, ca_key).unwrap(), key)
}

#[tokio::test]
async fn remote_client_uses_mtls_capability_and_sequence_end_to_end() {
    let (ca, ca_key) = new_ca();
    let (server_certificate, server_key) = issue_leaf(
        "localhost",
        ExtendedKeyUsagePurpose::ServerAuth,
        &ca,
        &ca_key,
    );
    let (worker_certificate, worker_key) = issue_leaf(
        "worker.test",
        ExtendedKeyUsagePurpose::ClientAuth,
        &ca,
        &ca_key,
    );
    let ca_pem = ca.pem();
    let worker_identity = format!("{}{}", worker_certificate.pem(), worker_key.serialize_pem());
    let service = Arc::new(UploadResponseService::new(UploadResponseConfig {
        num_streams: 1,
        response_timeout_ms: 5_000,
        ..UploadResponseConfig::default()
    }));
    let watcher = ResponseWatcher::new(service.clone()).with_poll_interval_ms(1);
    let _watcher = watcher.spawn();
    let port = portpicker::pick_unused_port().expect("available test port");
    let server = H2H3Server::builder()
        .with_tls(
            STANDARD.encode(server_certificate.pem()),
            STANDARD.encode(server_key.serialize_pem()),
        )
        .with_bind_address(IpAddr::V4(Ipv4Addr::LOCALHOST))
        .with_client_ca(STANDARD.encode(&ca_pem))
        .with_port(port)
        .enable_h2(true)
        .enable_h3(false)
        .enable_websocket(false)
        .enable_webtransport(false)
        .with_router(Box::new(UploadResponseControlRouter::new(service.clone())))
        .build()
        .unwrap();
    let handle = server.start().await.unwrap();
    let origin = format!("https://localhost:{port}");
    let client = RemoteIngressClient::new_with_mtls_pem(
        service.config().slot_bytes(),
        ca_pem.as_bytes(),
        worker_identity.as_bytes(),
    )
    .unwrap();

    let stream = service.open_stream().await.unwrap();
    let stream_id = stream.stream_id();
    let response = service.register_response(stream_id).await;
    assert!(client
        .list_streams(&origin)
        .await
        .unwrap()
        .iter()
        .any(|stream| stream.stream_id == stream_id));
    assert!(client
        .try_claim_response(&origin, stream_id, "worker-1")
        .await
        .unwrap());
    client
        .write_handler_response(
            &origin,
            stream_id,
            HandlerResponse {
                status: StatusCode::CREATED,
                body: Some(Bytes::from_static(b"mtls-ok")),
                ..HandlerResponse::default()
            },
        )
        .await
        .unwrap();

    let cached = tokio::time::timeout(Duration::from_secs(2), response)
        .await
        .expect("response timed out")
        .expect("response sender stopped")
        .expect("response failed");
    assert_eq!(cached.status, StatusCode::CREATED);
    assert_eq!(cached.body, Bytes::from_static(b"mtls-ok"));
    client
        .release_response(&origin, stream_id, "worker-1")
        .await
        .unwrap();
    stream.close().await;

    let _ = handle.shutdown_tx.send(());
    tokio::time::timeout(Duration::from_secs(2), handle.finished_rx)
        .await
        .expect("server shutdown timed out")
        .expect("server supervisor dropped completion signal");
}
