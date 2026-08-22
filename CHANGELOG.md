# Changelog

All notable changes to this project will be documented in this file.

---

## [2026-08-22]

### Added
- Correlate runs end-to-end across Tracker, the Taskiq queue, ExecutorHost, and the immutable executor: propagate run/task IDs and W3C trace context through queue messages and executor dispatches, tag structured CloudWatch/Sentry logs and Sentry errors/spans with those IDs, emit a correlated Sentry log carrying the exact CloudWatch task-attempt stream URL, publish handled terminal run/task failures to Sentry, and replace multi-hour run transactions with bounded per-phase trace segments (#716, #732)
- Require an account-local Sentry DSN secret for dev and production Tracker/ExecutorHost deployments, tag Tracker, ExecutorHost, and executor telemetry with per-artifact release identifiers, and inject the resolved trace/request context into the immutable executor image (#716, #732)
- Grant ExecutorHost task roles `lambda:InvokeFunction` on the `vals-format-lambda` function in dev and production so managed code-migration runs can call the managed-format Lambda during preflight (#729, #732)

### Changed
- Defer agent contract resolution to Tracker for managed (hosted) runs: the CLI now sends a name-only agent contract (plus model/kwargs) instead of expanding it client-side, letting Tracker resolve the frozen Agent Registry bundle and attest model/variant; client-side contract resolution is preserved for static-key deployments (#728, #732)
- Allow managed runtime contracts to declare `MODEL_GATEWAY_URL`/`MODEL_GATEWAY_API_KEY` secret references without being rejected as arbitrary AWS secret references; Tracker still ignores these refs and injects its own signed managed gateway values (#727)
- Resume interrupted evaluation streams instead of failing the tracker outright (#702, #732)
- Refactor Tracker's AWS integrations behind provider-neutral runtime boundaries, in four staged PRs, without changing existing behavior: object storage (`ObjectStore`/artifact-location contracts backing S3 key construction, presigned URLs, and agent-bundle copy/list) (#656), secret resolution (`SecretStore` contract backing explicit-key, managed-IAM, and maintenance-cleanup secret lookups) (#657), benchmark log writes and CloudWatch console URL generation (`BenchmarkLogSink`/`BenchmarkLogLocations`) (#658), and immutable executor release-artifact verification (`ExecutorArtifactReader`) (#659)
- Build the ExecutorHost container image via an explicit `DockerImageAsset` (instead of `ContainerImage.from_asset`) so its release/version tag can be derived and threaded into Sentry release tagging (#716, #732)

### Fixed
- Correct the executor-build CI workflow's path filters and test invocation to also cover the new `tests/integration/observability` suite (#716, #732)
