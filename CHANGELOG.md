# Changelog

All notable changes to this project are documented in this file.

---

## [2026-08-18]

### Added

- Add managed AWS runtime mode: the tracker and worker can use their ECS task IAM roles instead of long-lived caller-supplied access keys, gated to allowlisted organizations; each run persists an `aws_managed` flag so its credential mode is fixed for its lifetime (#543, #532)
- Add `GET /aws-runtime` endpoint that reports deployment-owned AWS resource locations (region, S3 bucket) without returning credentials (#543)

### Changed

- Introduce a unified `AWSClientProvider`/`AWSRuntime` boundary that centralizes AWS credential selection (explicit access keys vs. SDK credential chain) across tracker and CLI S3, Lambda, and sandbox operations (#532)
- Presigned result URLs now report the provider's actual lifetime instead of assuming a fixed one-day expiration; the CLI displays this as readable days/hours/minutes/seconds (#543)

### Fixed

- Finalize benchmark force-stop database state (tasks, run, and dispatches) immediately and isolate it from Daytona provider cleanup, so provider teardown failures are only logged instead of surfacing as confusing errors in place of a clear stop/timeout result (#694)
- Report a clear configuration error when force-stopping a managed run with no stored sandbox-provider secret, instead of asking for access-key headers (#543)

### Security

- Keep credential-bearing AWS runtime objects out of object representations, queued-request validation errors, and instrumented function arguments (#532)
