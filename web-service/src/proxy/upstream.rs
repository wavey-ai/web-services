use std::str::FromStr;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UpstreamProtocol {
    Http1,
    Http2,
    Http3,
}

impl UpstreamProtocol {
    pub fn as_str(&self) -> &'static str {
        match self {
            UpstreamProtocol::Http1 => "http1",
            UpstreamProtocol::Http2 => "http2",
            UpstreamProtocol::Http3 => "http3",
        }
    }
}

impl FromStr for UpstreamProtocol {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "http1" | "http1.1" | "h1" | "h1.1" => Ok(UpstreamProtocol::Http1),
            "http2" | "h2" => Ok(UpstreamProtocol::Http2),
            "http3" | "h3" => Ok(UpstreamProtocol::Http3),
            other => Err(format!("unsupported upstream protocol: {other}")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn upstream_protocol_parses_variants() {
        assert_eq!("http1".parse::<UpstreamProtocol>().unwrap(), UpstreamProtocol::Http1);
        assert_eq!("http1.1".parse::<UpstreamProtocol>().unwrap(), UpstreamProtocol::Http1);
        assert_eq!("h1".parse::<UpstreamProtocol>().unwrap(), UpstreamProtocol::Http1);
        assert_eq!("http2".parse::<UpstreamProtocol>().unwrap(), UpstreamProtocol::Http2);
        assert_eq!("h2".parse::<UpstreamProtocol>().unwrap(), UpstreamProtocol::Http2);
        assert_eq!("http3".parse::<UpstreamProtocol>().unwrap(), UpstreamProtocol::Http3);
        assert_eq!("h3".parse::<UpstreamProtocol>().unwrap(), UpstreamProtocol::Http3);
        assert!("unknown".parse::<UpstreamProtocol>().is_err());
    }

    #[test]
    fn upstream_protocol_as_str() {
        assert_eq!(UpstreamProtocol::Http1.as_str(), "http1");
        assert_eq!(UpstreamProtocol::Http2.as_str(), "http2");
        assert_eq!(UpstreamProtocol::Http3.as_str(), "http3");
    }
}
