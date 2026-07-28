# Changelog

All notable changes to this project will be documented in this file.

---

## [2026-07-28]

### Added

- Support optional output artifacts (`required: false`) that are skipped and logged on collection failure without affecting task status or scoring, and reject duplicate output artifact destination paths (#607)

### Fixed

- Pass `AWS_SESSION_TOKEN` through to CLI S3 credentials so `valkyrie agent push` and other CLI S3 operations work with temporary credentials (SSO / assumed roles) instead of failing with `InvalidAccessKeyId` (#613)
- Restore the AWS SDK credential chain for CLI S3 operations, allowing OIDC, SSO, profiles, and other SDK-provided credentials to be used when `valkyrie.yaml` has no explicit access key pair, while rejecting incomplete explicit credential configuration (#549)
