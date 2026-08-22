use std::env;

use base64::{engine::general_purpose::STANDARD, Engine as _};

const DEFAULT_HOST: &str = "127.0.0.1";

pub fn load_test_env() -> Option<(String, String, String)> {
    dotenvy::dotenv().ok();

    let host = env::var("HOSTNAME").unwrap_or_else(|_| DEFAULT_HOST.to_string());
    let cert_env = env::var("TLS_CERT_BASE64").ok();
    let key_env = env::var("TLS_KEY_BASE64").ok();
    if let (Some(cert), Some(key)) = (cert_env, key_env) {
        return Some((cert, key, host));
    }

    let rcgen::CertifiedKey { cert, key_pair } =
        rcgen::generate_simple_self_signed(vec![host.clone()]).ok()?;
    Some((
        STANDARD.encode(cert.pem()),
        STANDARD.encode(key_pair.serialize_pem()),
        host,
    ))
}
