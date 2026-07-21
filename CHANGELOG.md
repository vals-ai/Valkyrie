# Changelog

All notable changes to this project are documented in this file.

---

## [2026-07-21]

### Changed

- Drop the AWS account-id suffix from the dev `agentic-harness` S3 bucket name, and pass `bucket_name` (a plain string) into the tracker and worker stacks instead of the bucket construct itself (#597)
- Rework the test-routing skill (`.agents/writing-tests/SKILL.md`) to route changes through owning unit, credential-free local-integration, and approval-gated live-integration suites instead of a 70/30 unit/integration split, and drop the mandatory per-test docstring and committed `.local.env` template rules (#600)

### Fixed

- Make the live Daytona egress test issue real HTTPS HEAD requests instead of raw TCP socket probes, so an off-list domain that accepts TCP connections but blocks HTTP/TLS is no longer misreported as reachable (#576)
