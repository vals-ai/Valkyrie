# Changelog

All notable changes to this project are documented in this file.

---

## [2026-07-15]

### Added

- Add `valk run errors <run-id>` command to inspect stored run and current task error messages without downloading a results file, with human-readable and `--format json` output (#536)
- Add `VALKYRIE_ENV`-based CLI environment selection (prod/dev), resolving the tracker URL and config path dynamically while preserving `TRACKER_SERVICE_URL`/`VALKYRIE_CONFIG_PATH` as explicit overrides (#471)
- Add a scheduled hourly Lambda to clean up Daytona sandboxes older than 48 hours (opt-out via a `clean-up=false` label); disabled and dry-run by default (#526)

### Changed

- Automatically rotate matching `benchmark_auth` credentials when the hosted API key is updated via `valk config set`/`config init`, so retries stop failing with 401s while independent per-benchmark credentials are left untouched (#554)

### Fixed

- Use botocore's standard S3 retry mode so transient `HTTPClientError` failures (e.g. uvloop fd/transport errors) are retried instead of surfacing immediately (#556)
- Preserve the underlying WebSocket close code and reason (e.g. `1008 Unauthorized`, `1011 keepalive ping timeout`) in benchmark-service disconnect errors instead of masking them behind a generic inactivity message (#555)
- Self-seed the tracker integration test agent artifact into the test S3 bucket at session startup instead of relying on it already existing (#511)

### Security

- Reject malformed or ambiguous benchmark names, caller-provided benchmark-service URLs, and custom headers before they reach outbound HTTP requests, closing routing/header-injection vectors (#547)
- Replace internal Tracker, Descope, catalog, and benchmark-service exception details in API responses with stable public error messages while retaining full diagnostics in server logs (#547)
- Disable implicit slash redirects so non-canonical paths return `404` without constructing Host- or scheme-derived `Location` headers (#547)
- Filter unsafe S3 keys (path traversal, backslashes, NUL bytes, drive prefixes) before using them as streamed run-output archive member names (#547)
