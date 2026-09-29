//! Static file serving for a built single-page app.
//!
//! Folded in from the old `hyper-static` crate, minus its own TLS/h2/h3
//! server: a [`StaticFiles`] is called from a [`Router`](crate::Router) and
//! returns a [`HandlerResponse`], so it runs on whichever transports the
//! server already has.
//!
//! This is meant for local and test stacks; production serves the bundle from
//! a CDN. It favours being simple and correct over being fast: by default
//! every request reads the file from disk so a rebuilt bundle shows up
//! immediately.

use crate::http_range::apply_byte_range;
use crate::traits::{HandlerResponse, HandlerResult};
use bytes::Bytes;
use flate2::write::GzEncoder;
use flate2::Compression;
use http::header::{
    ACCEPT_ENCODING, CACHE_CONTROL, CONTENT_ENCODING, ETAG, IF_NONE_MATCH, RANGE, VARY,
};
use http::{Method, Request, StatusCode};
use percent_encoding::percent_decode_str;
use std::borrow::Cow;
use std::collections::HashMap;
use std::io::{ErrorKind, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, warn};
use xxhash_rust::xxh3::xxh3_64;

const IMMUTABLE: &str = "public, max-age=31536000, immutable";
const NO_CACHE: &str = "no-cache";

/// MIME prefixes worth gzipping (from hyper-static).
const COMPRESSIBLE_TYPES: &[&str] = &[
    "text/",
    "application/javascript",
    "application/json",
    "application/xml",
    "application/x-yaml",
    "application/graphql",
    "application/x-www-form-urlencoded",
    "application/ld+json",
    "application/manifest+json",
    "image/svg+xml",
];

/// Serves files under a root directory, optionally falling back to an SPA
/// index for client-side routes.
pub struct StaticFiles {
    root: PathBuf,
    spa_fallback: Option<String>,
    immutable_prefix: Option<String>,
    cache: Option<RwLock<HashMap<PathBuf, Arc<Entry>>>>,
}

/// One file's bytes and derived metadata.
struct Entry {
    bytes: Bytes,
    /// Filled eagerly when cached in memory, lazily otherwise.
    gzip: Option<Bytes>,
    hash: u64,
    content_type: String,
    compressible: bool,
}

impl StaticFiles {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            spa_fallback: None,
            immutable_prefix: None,
            cache: None,
        }
    }

    /// Serve this file (relative to root, e.g. "index.html") for GET/HEAD
    /// misses whose last path segment has no extension.
    pub fn spa_fallback(mut self, index: impl Into<String>) -> Self {
        self.spa_fallback = Some(index.into());
        self
    }

    /// Paths under this prefix (e.g. "/assets/") are content-hashed:
    /// `Cache-Control: public, max-age=31536000, immutable`. Everything else,
    /// including the fallback index: `Cache-Control: no-cache`.
    pub fn immutable_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.immutable_prefix = Some(prefix.into());
        self
    }

    /// true: cache file bytes (+gzip copy) in memory after first read
    /// (hyper-static behaviour). false (default): read from disk every
    /// request so a rebuilt bundle shows up immediately.
    pub fn cache_in_memory(mut self, on: bool) -> Self {
        self.cache = on.then(|| RwLock::new(HashMap::new()));
        self
    }

    /// Ok(None) when the request is not GET/HEAD or no file (and no fallback)
    /// matches, so the caller can route elsewhere / 404.
    pub async fn serve(&self, req: &Request<()>) -> HandlerResult<Option<HandlerResponse>> {
        let method = req.method();
        if method != Method::GET && method != Method::HEAD {
            return Ok(None);
        }

        let Some(segments) = normalise_path(req.uri().path()) else {
            debug!("static: rejected path {:?}", req.uri().path());
            return Ok(None);
        };

        let root = match tokio::fs::canonicalize(&self.root).await {
            Ok(root) => root,
            Err(err) => {
                warn!("static: root {:?} unavailable: {}", self.root, err);
                return Ok(None);
            }
        };

        let (file, cache_control) = match self.resolve(&root, &segments).await? {
            Some(file) => {
                let request_path = format!("/{}", segments.join("/"));
                let immutable = self
                    .immutable_prefix
                    .as_deref()
                    .is_some_and(|prefix| request_path.starts_with(prefix));
                (file, if immutable { IMMUTABLE } else { NO_CACHE })
            }
            None => {
                let Some(index) = self.spa_fallback.as_deref() else {
                    return Ok(None);
                };
                let has_extension = segments
                    .last()
                    .is_some_and(|last| Path::new(last).extension().is_some());
                if has_extension {
                    return Ok(None);
                }
                let Some(index_segments) = normalise_path(index) else {
                    return Ok(None);
                };
                match self.resolve(&root, &index_segments).await? {
                    Some(file) => (file, NO_CACHE),
                    None => return Ok(None),
                }
            }
        };

        let entry = self.load(&file).await?;
        let response = respond(req, &entry, cache_control)?;
        Ok(Some(response))
    }

    /// Map path segments to a regular file inside `root`, following a
    /// directory to its `index.html`. Symlinks are resolved and must still
    /// land inside `root`.
    async fn resolve(&self, root: &Path, segments: &[String]) -> HandlerResult<Option<PathBuf>> {
        let mut candidate = root.to_path_buf();
        candidate.extend(segments);

        let Some(mut path) = canonical_within(root, &candidate).await? else {
            return Ok(None);
        };
        let mut meta = tokio::fs::metadata(&path).await?;
        if meta.is_dir() {
            let Some(index) = canonical_within(root, &path.join("index.html")).await? else {
                return Ok(None);
            };
            path = index;
            meta = tokio::fs::metadata(&path).await?;
        }
        Ok(meta.is_file().then_some(path))
    }

    async fn load(&self, path: &Path) -> HandlerResult<Arc<Entry>> {
        let Some(cache) = &self.cache else {
            return Ok(Arc::new(read_entry(path, false).await?));
        };
        if let Some(entry) = cache.read().await.get(path) {
            return Ok(Arc::clone(entry));
        }
        let entry = Arc::new(read_entry(path, true).await?);
        cache
            .write()
            .await
            .insert(path.to_path_buf(), Arc::clone(&entry));
        Ok(entry)
    }
}

/// Canonicalize `candidate` and check it stays under `root` (already
/// canonical). `None` for a missing path or an escape.
async fn canonical_within(root: &Path, candidate: &Path) -> HandlerResult<Option<PathBuf>> {
    let path = match tokio::fs::canonicalize(candidate).await {
        Ok(path) => path,
        Err(err) if is_missing(&err) => return Ok(None),
        Err(err) => return Err(err.into()),
    };
    if !path.starts_with(root) {
        warn!(
            "static: {:?} resolves to {:?}, outside root {:?}",
            candidate, path, root
        );
        return Ok(None);
    }
    Ok(Some(path))
}

fn is_missing(err: &std::io::Error) -> bool {
    // `/file.txt/x` fails with ENOTDIR (20 on Linux and macOS), which has no
    // stable ErrorKind.
    err.kind() == ErrorKind::NotFound || err.raw_os_error() == Some(20)
}

/// Percent-decode and normalise a request path into segments. `.` and empty
/// segments are dropped, `..` pops; `None` if `..` would climb above the root
/// or a segment could never name a file inside it.
fn normalise_path(path: &str) -> Option<Vec<String>> {
    let decoded = percent_decode_str(path).decode_utf8().ok()?;
    let mut segments: Vec<String> = Vec::new();
    for segment in decoded.split('/') {
        match segment {
            "" | "." => {}
            ".." => {
                segments.pop()?;
            }
            s if s.contains('\\') || s.contains('\0') => return None,
            s => segments.push(s.to_string()),
        }
    }
    Some(segments)
}

async fn read_entry(path: &Path, precompress: bool) -> HandlerResult<Entry> {
    let bytes = Bytes::from(tokio::fs::read(path).await?);
    let content_type = content_type_for(path);
    let compressible = should_compress(&content_type);
    let gzip = if precompress && compressible {
        Some(gzip_bytes(&bytes)?)
    } else {
        None
    };
    Ok(Entry {
        hash: xxh3_64(&bytes),
        bytes,
        gzip,
        content_type,
        compressible,
    })
}

fn content_type_for(path: &Path) -> String {
    let mime = mime_guess::from_path(path).first_or_octet_stream();
    let essence = mime.essence_str();
    if mime.type_() == mime_guess::mime::TEXT || essence == "application/javascript" {
        format!("{essence}; charset=utf-8")
    } else {
        essence.to_string()
    }
}

fn should_compress(content_type: &str) -> bool {
    COMPRESSIBLE_TYPES
        .iter()
        .any(|prefix| content_type.starts_with(prefix))
}

fn gzip_bytes(data: &[u8]) -> HandlerResult<Bytes> {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(data)?;
    Ok(Bytes::from(encoder.finish()?))
}

fn respond(
    req: &Request<()>,
    entry: &Entry,
    cache_control: &'static str,
) -> HandlerResult<HandlerResponse> {
    let use_gzip = entry.compressible && accepts_gzip(req);
    let hash = format!("{:016x}", entry.hash);
    // Strong ETags must differ per representation.
    let etag = if use_gzip {
        format!("\"{hash}-gzip\"")
    } else {
        format!("\"{hash}\"")
    };

    let mut headers: Vec<(Cow<'static, str>, Cow<'static, str>)> = vec![
        (Cow::Borrowed(ETAG.as_str()), Cow::Owned(etag)),
        (
            Cow::Borrowed(CACHE_CONTROL.as_str()),
            Cow::Borrowed(cache_control),
        ),
    ];
    if entry.compressible {
        headers.push((
            Cow::Borrowed(VARY.as_str()),
            Cow::Borrowed("Accept-Encoding"),
        ));
    }

    if if_none_match(req, &hash) {
        return Ok(HandlerResponse {
            status: StatusCode::NOT_MODIFIED,
            body: None,
            content_type: None,
            headers,
            etag: None,
        });
    }

    let body = if use_gzip {
        headers.push((
            Cow::Borrowed(CONTENT_ENCODING.as_str()),
            Cow::Borrowed("gzip"),
        ));
        match &entry.gzip {
            Some(compressed) => compressed.clone(),
            None => gzip_bytes(&entry.bytes)?,
        }
    } else {
        entry.bytes.clone()
    };

    let response = HandlerResponse {
        status: StatusCode::OK,
        body: Some(body),
        content_type: Some(Cow::Owned(entry.content_type.clone())),
        headers,
        // The quoted ETag header above is authoritative; the numeric field
        // would be rendered unquoted.
        etag: None,
    };
    // A gzip response carries Content-Encoding, which apply_byte_range
    // leaves whole.
    let mut response = apply_byte_range(req.headers().get(RANGE), response);

    if req.method() == Method::HEAD {
        response.body = None;
    }
    Ok(response)
}

/// `Accept-Encoding` lists gzip without `q=0`.
fn accepts_gzip(req: &Request<()>) -> bool {
    req.headers()
        .get_all(ACCEPT_ENCODING)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .any(|item| {
            let mut parts = item.split(';').map(str::trim);
            let coding = parts.next().unwrap_or_default();
            let refused = parts.any(|param| {
                param
                    .strip_prefix("q=")
                    .and_then(|q| q.parse::<f32>().ok())
                    .is_some_and(|q| q == 0.0)
            });
            (coding.eq_ignore_ascii_case("gzip") || coding.eq_ignore_ascii_case("x-gzip"))
                && !refused
        })
}

/// `If-None-Match` names `*` or either representation of this content.
/// Uses weak comparison, as RFC 9110 requires for this header.
fn if_none_match(req: &Request<()>, hash: &str) -> bool {
    req.headers()
        .get_all(IF_NONE_MATCH)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .map(str::trim)
        .any(|tag| {
            if tag == "*" {
                return true;
            }
            let tag = tag.strip_prefix("W/").unwrap_or(tag).trim_matches('"');
            tag == hash || tag.strip_suffix("-gzip") == Some(hash)
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use flate2::read::GzDecoder;
    use std::io::Read;

    fn header<'a>(response: &'a HandlerResponse, name: &str) -> Option<&'a str> {
        response
            .headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_ref())
    }

    fn get(path: &str) -> Request<()> {
        Request::builder().uri(path).body(()).unwrap()
    }

    fn site() -> tempfile::TempDir {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        std::fs::write(root.join("index.html"), "<!doctype html><p>app</p>").unwrap();
        std::fs::create_dir_all(root.join("assets")).unwrap();
        std::fs::write(
            root.join("assets/app-abc123.js"),
            "console.log('hi');".repeat(50),
        )
        .unwrap();
        std::fs::write(root.join("logo.png"), [0x89, b'P', b'N', b'G']).unwrap();
        std::fs::create_dir_all(root.join("docs")).unwrap();
        std::fs::write(root.join("docs/index.html"), "<p>docs</p>").unwrap();
        dir
    }

    #[tokio::test]
    async fn serves_file_with_type_and_etag() {
        let dir = site();
        let files = StaticFiles::new(dir.path());

        let response = files.serve(&get("/index.html")).await.unwrap().unwrap();

        assert_eq!(response.status, StatusCode::OK);
        assert_eq!(
            response.content_type.as_deref(),
            Some("text/html; charset=utf-8")
        );
        assert_eq!(
            response.body.as_deref(),
            Some(&b"<!doctype html><p>app</p>"[..])
        );
        let expected = format!("\"{:016x}\"", xxh3_64(b"<!doctype html><p>app</p>"));
        assert_eq!(header(&response, "etag"), Some(expected.as_str()));
        assert!(header(&response, "content-encoding").is_none());
    }

    #[tokio::test]
    async fn root_serves_index() {
        let dir = site();
        let files = StaticFiles::new(dir.path());

        let response = files.serve(&get("/")).await.unwrap().unwrap();

        assert_eq!(
            response.body.as_deref(),
            Some(&b"<!doctype html><p>app</p>"[..])
        );
    }

    #[tokio::test]
    async fn matching_if_none_match_returns_304() {
        let dir = site();
        let files = StaticFiles::new(dir.path());
        let first = files.serve(&get("/index.html")).await.unwrap().unwrap();
        let etag = header(&first, "etag").unwrap().to_string();

        let req = Request::builder()
            .uri("/index.html")
            .header("if-none-match", etag.as_str())
            .body(())
            .unwrap();
        let response = files.serve(&req).await.unwrap().unwrap();

        assert_eq!(response.status, StatusCode::NOT_MODIFIED);
        assert!(response.body.is_none());
        assert_eq!(header(&response, "etag"), Some(etag.as_str()));
    }

    #[tokio::test]
    async fn gzips_compressible_types_when_accepted() {
        let dir = site();
        for cached in [false, true] {
            let files = StaticFiles::new(dir.path()).cache_in_memory(cached);
            let req = Request::builder()
                .uri("/assets/app-abc123.js")
                .header("accept-encoding", "br, gzip;q=0.8")
                .body(())
                .unwrap();

            let response = files.serve(&req).await.unwrap().unwrap();

            assert_eq!(response.status, StatusCode::OK);
            assert_eq!(header(&response, "content-encoding"), Some("gzip"));
            assert_eq!(header(&response, "vary"), Some("Accept-Encoding"));
            assert!(header(&response, "etag").unwrap().ends_with("-gzip\""));
            let mut decoded = String::new();
            GzDecoder::new(response.body.as_deref().unwrap())
                .read_to_string(&mut decoded)
                .unwrap();
            assert_eq!(decoded, "console.log('hi');".repeat(50));
        }
    }

    #[tokio::test]
    async fn does_not_gzip_without_accept_encoding_or_for_binary() {
        let dir = site();
        let files = StaticFiles::new(dir.path());

        let plain = files
            .serve(&get("/assets/app-abc123.js"))
            .await
            .unwrap()
            .unwrap();
        assert!(header(&plain, "content-encoding").is_none());
        assert_eq!(header(&plain, "vary"), Some("Accept-Encoding"));

        let req = Request::builder()
            .uri("/logo.png")
            .header("accept-encoding", "gzip")
            .body(())
            .unwrap();
        let png = files.serve(&req).await.unwrap().unwrap();
        assert_eq!(png.content_type.as_deref(), Some("image/png"));
        assert!(header(&png, "content-encoding").is_none());
        assert!(header(&png, "vary").is_none());
    }

    #[tokio::test]
    async fn rejects_traversal() {
        let parent = tempfile::tempdir().unwrap();
        std::fs::write(parent.path().join("secret.txt"), "secret").unwrap();
        let root = parent.path().join("public");
        std::fs::create_dir(&root).unwrap();
        std::fs::write(root.join("index.html"), "ok").unwrap();
        let files = StaticFiles::new(&root).spa_fallback("index.html");

        for path in [
            "/../secret.txt",
            "/%2e%2e/secret.txt",
            "/a/../../secret.txt",
        ] {
            assert!(
                files.serve(&get(path)).await.unwrap().is_none(),
                "{path} escaped the root"
            );
        }

        #[cfg(unix)]
        {
            std::os::unix::fs::symlink(parent.path().join("secret.txt"), root.join("link.txt"))
                .unwrap();
            assert!(files.serve(&get("/link.txt")).await.unwrap().is_none());
        }
    }

    #[tokio::test]
    async fn spa_fallback_only_for_extensionless_paths() {
        let dir = site();
        let files = StaticFiles::new(dir.path()).spa_fallback("index.html");

        let route = files.serve(&get("/projects/123")).await.unwrap().unwrap();
        assert_eq!(route.status, StatusCode::OK);
        assert_eq!(
            route.body.as_deref(),
            Some(&b"<!doctype html><p>app</p>"[..])
        );
        assert_eq!(header(&route, "cache-control"), Some("no-cache"));

        assert!(files.serve(&get("/missing.js")).await.unwrap().is_none());
        assert!(StaticFiles::new(dir.path())
            .serve(&get("/projects/123"))
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn directory_serves_its_index() {
        let dir = site();
        let files = StaticFiles::new(dir.path());

        for path in ["/docs", "/docs/"] {
            let response = files.serve(&get(path)).await.unwrap().unwrap();
            assert_eq!(response.body.as_deref(), Some(&b"<p>docs</p>"[..]));
        }
    }

    #[tokio::test]
    async fn immutable_prefix_sets_cache_control() {
        let dir = site();
        let files = StaticFiles::new(dir.path())
            .immutable_prefix("/assets/")
            .spa_fallback("index.html");

        let asset = files
            .serve(&get("/assets/app-abc123.js"))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(header(&asset, "cache-control"), Some(IMMUTABLE));

        let index = files.serve(&get("/index.html")).await.unwrap().unwrap();
        assert_eq!(header(&index, "cache-control"), Some("no-cache"));

        let fallback = files.serve(&get("/assets")).await.unwrap().unwrap();
        assert_eq!(header(&fallback, "cache-control"), Some("no-cache"));
    }

    #[tokio::test]
    async fn head_has_headers_but_no_body() {
        let dir = site();
        let files = StaticFiles::new(dir.path());
        let req = Request::builder()
            .method(Method::HEAD)
            .uri("/index.html")
            .body(())
            .unwrap();

        let response = files.serve(&req).await.unwrap().unwrap();

        assert_eq!(response.status, StatusCode::OK);
        assert!(response.body.is_none());
        assert!(header(&response, "etag").is_some());
    }

    #[tokio::test]
    async fn range_slices_uncompressed_body() {
        let dir = site();
        let files = StaticFiles::new(dir.path());
        let req = Request::builder()
            .uri("/index.html")
            .header("range", "bytes=0-8")
            .body(())
            .unwrap();

        let response = files.serve(&req).await.unwrap().unwrap();

        assert_eq!(response.status, StatusCode::PARTIAL_CONTENT);
        assert_eq!(response.body.as_deref(), Some(&b"<!doctype"[..]));
    }

    #[tokio::test]
    async fn ignores_other_methods() {
        let dir = site();
        let files = StaticFiles::new(dir.path());
        let req = Request::builder()
            .method(Method::POST)
            .uri("/index.html")
            .body(())
            .unwrap();

        assert!(files.serve(&req).await.unwrap().is_none());
    }
}
