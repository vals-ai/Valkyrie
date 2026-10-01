# Changelog

## [2026-10-01]

### Added

- Run benchmarks locally through Tracker and ExecutorHost: `tracker.serve --config` selects Docker execution and filesystem storage (`data_root`, optional `secrets_file`), binds to 127.0.0.1, and clients need only a `tracker_url` config key. Local runs serve agent and artifact downloads over HTTP, support image-based Docker tasks, and return 400 for analysis, S3 export, Lambda callbacks, webhooks and service-auth secret references (#808)
- Add `source:///` executor releases so ExecutorHost runs the checkout's executor directly from `EXECUTOR_SOURCE_ROOT`; Tracker registers one reusable source release per checkout (#807)
- Add filesystem runtime adapters for local execution: atomic artifact publishing with conditional `overwrite=False`, path-traversal protection, per-run JSONL logs, and in-memory credential resolution (#806)
- Add `overwrite=False` to agent upload (SDK `agents.push(overwrite=...)`) so an existing alias returns 409, and `update_agent=true` to retry/resume so Tracker refreshes the run's saved agent bundle (rejected while the run is in progress) (#822)
- Add a restricted outbound proxy image (Squid) for ValSmith services and the dataset-view Lambda, with fixed per-listener host allowlists, CONNECT/SNI matching, private and metadata address denial, and AMD64/ARM64 qualification tests in infrastructure CI. No infrastructure is deployed yet (#868)

### Changed

- Breaking: SDK and CLI configuration use a nested `aws` section with optional `aws.credentials`; legacy flat YAML keys are rejected with migration instructions, `valkyrie config init` migrates them, and flat keys passed to `ValkyrieConfig` still work with a `DeprecationWarning`. SDK version is now 0.3.0 (#808)
- Route directory run starts and `resume/retry --update-agent` through Tracker instead of writing to S3 from the CLI, so the server selects storage and applies org scoping. Deploy Tracker before releasing this CLI (#822)
- Breaking: remove the CLI's legacy `DAYTONA_SECRET_NAME` fallback; run `valkyrie config provider set <provider> <secret-name>` instead (#822)
- Make shared runtime operations async: a single async `SecretStore`, directly awaited Lambda invocation (the analyzer Lambda is no longer retried), and ECS task protection updates via `httpx` (ExecutorHost now pins `httpx==0.28.1`) (#819)
- Write executor dispatch payloads to the system temporary directory instead of the cache directory (#807)
- Stop sending local variables to Sentry in all deployments (#808)
- Bump SDK version to 0.2.29 for the new `overwrite` and `update_agent` options (#822)

### Fixed

- Apply the CloudWatch log retention policy when the log group already exists (#819)
- Drain executor claims and terminalization through repeated cancellation so a cancelled dispatch is still cleaned up (#819)
- Read the sandbox cleanup secret on the sweep's event loop, so the cleanup timeout covers it (#819)
- Parse SDK run arguments without an `environment` field as AWS (#808)

### Security

- Reject non-loopback Host headers on a local Tracker (#808)
- Restrict ValSmith outbound traffic to approved hosts, requiring matching TLS SNI and denying private and metadata addresses (#868)

## Valkyrie SDK 0.3.0 (unreleased)

### Changed

- Breaking: CLI and SDK YAML configuration now requires AWS resource settings under `aws` and static access keys under `aws.credentials`. Flat YAML files fail with key-specific migration instructions. Migrate configuration files and any secrets or templates that generate them before upgrading. See the [configuration migration guide](https://docs.valkyrie.vals.ai/get-started/configuration#migrate-an-existing-configuration). (#808)
- Breaking: Python configuration attribute reads now use `config.aws` and `config.aws.credentials`; for example, replace `config.s3_bucket` with `config.aws.s3_bucket`. Both nested models can be absent. Passing flat AWS aliases or field names to `ValkyrieConfig.model_validate()` remains supported with a `DeprecationWarning`. (#808)
