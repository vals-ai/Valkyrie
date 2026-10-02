# Changelog

## [2026-10-02]

### Added

- Add a standalone, isolated ValSmith Production network stack (separate two-zone VPC, isolated application subnets, private Tracker access on TCP 8001, scoped AWS endpoints) with one non-root, read-only outbound proxy task per zone, separate service and Lambda-view proxy listeners, and a proxy health check covering both listeners and the ACL helpers (#871)
- Add default-deny DNS controls for ValSmith: exact destination allowlist, full alias inspection, catch-all block rule, seven-day local DNS logs, and fail-closed post-deploy DNS verification (#871)
- Add `valsmith-*` Makefile targets and a CDK app for synth, diff, and deploy of the image and network stacks, with preflight checks of account, resources, and CIDRs, plus a CI job that synthesizes both stacks without AWS access (#871)

## Valkyrie SDK 0.3.0 (unreleased)

### Changed

- Breaking: CLI and SDK YAML configuration now requires AWS resource settings under `aws` and static access keys under `aws.credentials`. Flat YAML files fail with key-specific migration instructions. Migrate configuration files and any secrets or templates that generate them before upgrading. See the [configuration migration guide](https://docs.valkyrie.vals.ai/get-started/configuration#migrate-an-existing-configuration). (#808)
- Breaking: Python configuration attribute reads now use `config.aws` and `config.aws.credentials`; for example, replace `config.s3_bucket` with `config.aws.s3_bucket`. Both nested models can be absent. Passing flat AWS aliases or field names to `ValkyrieConfig.model_validate()` remains supported with a `DeprecationWarning`. (#808)
