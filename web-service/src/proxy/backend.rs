use serde::{Deserialize, Serialize};
use url::Url;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum BackendScheme {
    Http,
    Https,
    Ws,
    Wss,
}

impl BackendScheme {
    pub fn from_url(url: &Url) -> Option<Self> {
        match url.scheme() {
            "http" => Some(Self::Http),
            "https" => Some(Self::Https),
            "ws" => Some(Self::Ws),
            "wss" => Some(Self::Wss),
            _ => None,
        }
    }

    pub fn is_http(self) -> bool {
        matches!(self, Self::Http | Self::Https)
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct Backend {
    pub id: String,
    pub url: Url,
    pub scheme: BackendScheme,
    pub max_connections: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackendView {
    pub id: String,
    pub url: Url,
    pub scheme: BackendScheme,
    pub max_connections: Option<usize>,
    pub active_connections: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backend_scheme_from_url_and_is_http() {
        let http = Url::parse("http://example.com").unwrap();
        let https = Url::parse("https://example.com").unwrap();
        let ws = Url::parse("ws://example.com").unwrap();
        let wss = Url::parse("wss://example.com").unwrap();
        let ftp = Url::parse("ftp://example.com").unwrap();

        assert_eq!(BackendScheme::from_url(&http), Some(BackendScheme::Http));
        assert_eq!(BackendScheme::from_url(&https), Some(BackendScheme::Https));
        assert_eq!(BackendScheme::from_url(&ws), Some(BackendScheme::Ws));
        assert_eq!(BackendScheme::from_url(&wss), Some(BackendScheme::Wss));
        assert_eq!(BackendScheme::from_url(&ftp), None);

        assert!(BackendScheme::Http.is_http());
        assert!(BackendScheme::Https.is_http());
        assert!(!BackendScheme::Ws.is_http());
        assert!(!BackendScheme::Wss.is_http());
    }
}
