use anyhow::Context;
use base64::engine::general_purpose::STANDARD as base64_engine;
use base64::Engine;
use std::fs;
use std::path::{Path, PathBuf};

const DEFAULT_TLS_CERT_REL_PATH: &str = "../tls/local.wavey.ai/fullchain.pem";
const DEFAULT_TLS_KEY_REL_PATH: &str = "../tls/local.wavey.ai/privkey.pem";

pub fn default_tls_paths() -> (PathBuf, PathBuf) {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    (
        root.join(DEFAULT_TLS_CERT_REL_PATH),
        root.join(DEFAULT_TLS_KEY_REL_PATH),
    )
}

pub fn load_default_tls_base64() -> anyhow::Result<(String, String)> {
    let (cert_path, key_path) = default_tls_paths();
    load_tls_base64_from_paths(cert_path, key_path)
}

pub fn load_tls_base64_from_paths(
    cert_path: impl AsRef<Path>,
    key_path: impl AsRef<Path>,
) -> anyhow::Result<(String, String)> {
    let cert_pem = fs::read(cert_path.as_ref()).with_context(|| {
        format!(
            "failed to read cert PEM: {}",
            cert_path.as_ref().display()
        )
    })?;
    let key_pem = fs::read(key_path.as_ref()).with_context(|| {
        format!(
            "failed to read private key PEM: {}",
            key_path.as_ref().display()
        )
    })?;
    Ok((
        base64_engine.encode(cert_pem),
        base64_engine.encode(key_pem),
    ))
}
