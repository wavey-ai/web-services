use std::env;

use web_service::load_default_tls_base64;

const DEFAULT_HOST: &str = "local.wavey.ai";

pub fn load_test_env() -> Option<(String, String, String)> {
    dotenvy::dotenv().ok();

    let host_env = env::var("HOSTNAME").ok();
    let cert_env = env::var("TLS_CERT_BASE64").ok();
    let key_env = env::var("TLS_KEY_BASE64").ok();
    let env_pair = match (cert_env, key_env) {
        (Some(cert), Some(key)) => Some((cert, key)),
        _ => None,
    };

    if let Some(host) = host_env {
        if let Some((cert, key)) = env_pair {
            return Some((cert, key, host));
        }
        let (cert, key) = load_default_tls_base64().ok()?;
        return Some((cert, key, host));
    }

    if let Ok((cert, key)) = load_default_tls_base64() {
        return Some((cert, key, DEFAULT_HOST.to_string()));
    }

    env_pair.map(|(cert, key)| (cert, key, DEFAULT_HOST.to_string()))
}
