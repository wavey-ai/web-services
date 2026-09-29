# hyper-hls

## Static files

`web_service::StaticFiles` serves a directory (typically a built single-page
app) from inside a `Router`. It is meant for local and test stacks;
production serves the bundle from a CDN.

```rust
use web_service::StaticFiles;

let files = StaticFiles::new("app/dist")
    .spa_fallback("index.html")   // client routes like /projects/123
    .immutable_prefix("/assets/"); // content-hashed files

// in Router::route:
if let Some(response) = files.serve(&req).await? {
    return Ok(response);
}
```

- GET and HEAD only; anything else, or a miss, returns `Ok(None)` so the
  caller can route elsewhere or 404.
- Paths are percent-decoded and normalised; `..` and symlinks cannot escape
  the root. `/` and directories serve their `index.html`.
- The fallback is used only when the last path segment has no extension, so
  a missing `/app.js` is still a 404.
- Strong ETag (xxh3) with `If-None-Match` → 304; gzip for compressible types
  when the client accepts it (`Vary: Accept-Encoding`); byte ranges on
  uncompressed responses.
- `Cache-Control` is `public, max-age=31536000, immutable` under the
  immutable prefix and `no-cache` everywhere else, including the fallback.
- Files are read from disk on every request so a rebuild shows up at once;
  `.cache_in_memory(true)` keeps bytes and a gzip copy after the first read.
