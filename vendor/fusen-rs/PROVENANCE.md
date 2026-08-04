# fusen-rs 0.9.0 provenance

This directory is based on the crates.io `fusen-rs` 0.9.0 package.

- Upstream repository: `https://github.com/kwsc98/fusen-rs`
- Upstream release commit: `eac83b2f9f96409e26c4f7343e9a75738ec0b454`
- crates.io checksum: `ba50c9fbcd1a35e762c731da9ddd456bab829c32a41a86a703351205597fd02c`

The vendored copy keeps version 0.9.0 and adds two backward-compatible server extension APIs:

- a custom RFC 9457 Problem Details encoder;
- configurable server request-ID validation (including an optional alphanumeric prefix rule) and
  response emission.

Default Fusen behavior is unchanged unless a server explicitly enables these APIs.

## Workspace patch strategy

This workspace selects the tracked vendored source with:

```toml
[patch.crates-io]
fusen-rs = { path = "vendor/fusen-rs" }
```

The extension has not been published at a separate upstream Git commit, so this workspace cannot
yet pin the modified source by an exact Git SHA. The release commit above identifies the upstream
0.9.0 baseline, not the modified vendored tree. Replacing the path patch with a Git patch requires
publishing this tree first and then pinning that new commit's full SHA.

## HTTP error-encoding boundary

The custom Problem encoder covers failures after Hyper has parsed a request and invoked Fusen's
`HttpApp`, including Fusen route/head validation, request controls, admission, body limits and
decoding, interceptors, timeouts, controller errors, and guarded application panics.

HTTP parser failures happen before `HttpApp` receives a `Request`. For example, a malformed request
line or header, a request-head/header-count limit violation, or a request-head read timeout is
handled by Hyper at the connection layer. Those failures cannot be encoded by the custom Problem
encoder and therefore are not guaranteed to return NeoEngram Problem Details or `X-Request-ID`.
