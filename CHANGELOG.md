# Changelog

All notable changes to this project are documented in this file.

---

## [2026-07-18]

### Added
- Add a dedicated "Infrastructure checks" CI workflow that lints, tests, and typechecks `infra/` on every push and pull request (#570)

### Changed
- Deploy the dev environment to its own dedicated AWS account, replacing automatic push-based dev deploys with a manual, access-gated workflow that validates account/region and requires a dev-scoped Descope project before planning or deploying (#570)
- Rename the dev CloudFormation stack prefix from `Valk-Dev-` to `ValkDev` for PascalCase consistency; dev-only, no effect on production stacks (#577)
- Reorganize and substantially expand the CLI test suite (unit and local tracker-integration tests), raising enforced CLI coverage from ~70% to 80% (#574)
- Reorganize and substantially expand the tracker test suite (unit, local, and live integration tests), raising enforced combined tracker coverage to 85% (#564)

### Fixed
- Validate `--concurrency` CLI options require a positive integer instead of silently accepting zero or negative values (#574)
- Raise a clear tracker error instead of crashing when the tracker service returns a malformed or non-JSON response (#574)
- Prevent orphaned sandboxes by shielding sandbox creation from cancellation and cleaning up sandboxes that finish after the request was cancelled (#564)
- Fix a race condition where retrying or resuming a run could leave a stale final score, by locking the benchmark row and clearing the prior final evaluation before retry (#564)

### Security
- Prevent path traversal when downloading run outputs and S3 artifacts by validating extracted destinations stay within the target directory and extracting tar archives with `filter="data"` (#574)
- Redact provider secrets and kwargs (not just `env`) from downloaded final-view run results to avoid leaking credentials to disk (#574)
