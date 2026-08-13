# Changelog

All notable changes to this project will be documented in this file.

---

## [2026-08-13]

### Added
- Add provider-generic sandbox orphan cleanup schedule that reaps orphaned sandboxes on an hourly cadence (#524)
- Emit structured `sandbox.delete` audit events from the tracker (#662)

### Changed
- Derive infra maintenance classification from actual executor stack effects; ECS task CPU/memory-only changes are now treated as rolling updates that skip redeploy and maintenance windows (#648, #669)
- Increase tracker and worker CPU/memory and minimum task counts in prod and dev, and run the tracker with 2 uvicorn workers (#669)
- Cache Descope auth token exchanges to cut redundant authentication calls (#653)
- Source Sentry DSN and Descope management key secret names from deployment configuration instead of hardcoded constants, validating that required secrets are set before deploying (#670)
- Move dev deployment secrets (AWS deploy role ARN, dev account ID, Descope/Sentry secret names) from Environment variables to Environment secrets so they're masked in this public repo's CI logs, and scope the production core deploy job under the protected `prod` Environment (#673)
- Bump `create-benchmark-service` dependency to v0.27.4 (#665)
- Pass placeholder Sentry/Descope secret names to offline CDK synth so it no longer requires real secrets (#672)

### Fixed
- Fix CI required-check deadlock by always running path-gated required checks even when their paths aren't touched (#649)
- Close the benchmark service client to prevent a resource leak (#655)
- Preserve task outcome when sandbox teardown fails, and stop forced sandbox teardown from racing live executor calls (#668)

### Security
- Prevent shell injection via PR title in the title-check workflow by passing it through `env` instead of interpolating it into the shell script, and drop the workflow's token permissions to none (#675)
- Restrict benchmark service key forwarding to prevent it leaking beyond its intended scope (#663)
