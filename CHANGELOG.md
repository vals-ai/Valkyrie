# Changelog

All notable changes to this project will be documented in this file.

---

## [2026-08-04]

### Added
- Forward benchmark sandbox secret references (env var name → provider org-secret name) from task retrieval through sandbox creation into provider requests, without resolving the secret value in the tracker (#642)
- Reject sandbox configurations that declare the same environment variable as both a plaintext `env_vars` entry and a provider-managed secret, failing fast with `InvalidSandboxConfigurationError` instead of retrying (#642)
- Forward benchmark volume mounts to sandboxes (#633)
- Support task-targeted Daytona sandbox snapshots (#627)
- Support optional output artifacts (#607)
- Add benchmark URL overrides to retry and resume (#626)
- Measure agent command duration in the tracker process (#630)
- Require CBS and lockfile validation checks before prod deploys (#628)
- Add stable deploys with deployment gates (#601)

### Changed
- Dev Tracker now provisions and owns its ACM certificate in CDK, matching prod, instead of importing a separately-provisioned certificate; the certificate-ARN SSM parameter is no longer required (#645)
- Automatically deploy dev on push (#604)
- Drop the account-id suffix from the dev harness bucket name (#597)
- Improve all-task-error run messages (#593)
- Use CBS v0.17.3 provider-level Daytona retries (#611)
- Bump `create-benchmark-service` dependency to v0.25.0, then v0.26.0 (#641, #642)
- Use HTTP requests instead of raw sockets in the egress live test (#576)
- Improve the Valkyrie test-routing skill and add an explicit CLI section to the docs (#600, #616)

### Fixed
- Fix executor preflight credential ownership; use the release role for preflight (#631)
- Restore the CLI's AWS credential chain, including `AWS_SESSION_TOKEN` passthrough to S3 credentials (#549, #613)
- Fix connected runs during task discovery (#599)
- Fix deployment gates (#632)

### Removed
- Remove the separate `production-release` manual-approval gate for production executor deploys; the production executor job now runs directly under the protected `prod` GitHub Environment (#643, #644)
