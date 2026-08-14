# Changelog

All notable changes to this project are documented in this file.

---

## [2026-08-14]

### Security
- Restrict Redis network access to explicit source security groups (Tracker, ExecutorHost, and the release-test Driver) instead of VPC-wide ingress; executor-release control tasks now run in a dedicated no-ingress security group with only Postgres, DNS, and HTTPS egress (#681)
- Reject caller-provided custom benchmark service destinations for external tenants that point to `vals.ai`/subdomains, local or internal hostnames, or non-public literal IP addresses (including forms like `127.1`), closing an SSRF-style gap across start/fetch/retry/rescore/executor flows; also normalizes Unicode host separators and fails rejected queued runs (#679)
- Pass the PR title via an environment variable instead of shell interpolation in the PR title-check workflow to prevent shell injection (#675)

### Fixed
- Make ExecutorHost Redis stream cleanup atomic by combining `XACK` and conditional `XDEL` into a single Lua `EVAL` (#681)

### Changed
- Source deployment secrets (Descope project ID/management key, Sentry DSN, AWS deploy role, dev/prod account IDs, sandbox cleanup secret name) from GitHub Environment secrets instead of repository variables, and require the Descope management key whenever `AUTH_REQUIRED` is enabled (#673, #670, #672)
- Restrict the production deploy workflow's core-stack job to the protected `prod` GitHub Environment (#673)
- Read dev environment sizing expectations from stage config instead of hardcoded values in infra tests (#679)
