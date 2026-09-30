# Changelog

## Valkyrie SDK 0.3.0 (unreleased)

### Changed

- Breaking: CLI and SDK YAML configuration now requires AWS resource settings under `aws` and static access keys under `aws.credentials`. Flat YAML files fail with key-specific migration instructions. Migrate configuration files and any secrets or templates that generate them before upgrading. See the [configuration migration guide](https://docs.valkyrie.vals.ai/get-started/configuration#migrate-an-existing-configuration). (#808)
- Breaking: Python configuration attribute reads now use `config.aws` and `config.aws.credentials`; for example, replace `config.s3_bucket` with `config.aws.s3_bucket`. Both nested models can be absent. Passing flat AWS aliases or field names to `ValkyrieConfig.model_validate()` remains supported with a `DeprecationWarning`. (#808)
