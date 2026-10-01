# Changelog

## Unreleased

- Run each admitted executor dispatch in its own pinned ECS task; local development uses a runner subprocess.
- Seal dispatch inputs with a per-dispatch AES-256-GCM key wrapped by KMS and consume the payload transactionally at claim time.
- Report ECS task startup failures and KMS payload-decrypt errors on the dispatch and run instead of waiting for the claim deadline.
- Stopping a run revokes unclaimed executor dispatches and their sealed payloads immediately, requests ECS task shutdown, and leaves claimed work on graceful-stop handling.
- Start, retry and resume requests larger than 1 MiB now return 413 before any processing.
- The Tracker and runner image is built in two stages; the runtime image no longer contains gcc, git or uv, and commands run from `/app/.venv/bin` on `PATH`.
- Changes to `tracker/runtime/`, `tracker/local/`, `tracker/egress.py` and `tracker/executor/dependencies.py` now publish a new executor release, because the executor imports them.
- Dev and release-test databases now use `db.r7g.large`, the same size as bench and prod, with the connection alarm at 1,400.
- Remove the Redis queue and long-lived executor service. The unused Redis
  cluster, security group, and their CloudFormation exports, the Tracker service
  security group export, and the release-test executor-host ECR repository and
  exports remain for this one deployment so later stacks can drop their imports;
  remove them in the follow-up cleanup PR together with the host-removal
  classifier rule. The one-time cutover uses gated maintenance; later runner
  deployments do not stop active tasks.
- `tracker.serve` accepts `--port` (default 8000).

## Valkyrie SDK 0.3.0 (unreleased)

### Changed

- Breaking: CLI and SDK YAML configuration now requires AWS resource settings under `aws` and static access keys under `aws.credentials`. Flat YAML files fail with key-specific migration instructions. Migrate configuration files and any secrets or templates that generate them before upgrading. See the [configuration migration guide](https://docs.valkyrie.vals.ai/get-started/configuration#migrate-an-existing-configuration). (#808)
- Breaking: Python configuration attribute reads now use `config.aws` and `config.aws.credentials`; for example, replace `config.s3_bucket` with `config.aws.s3_bucket`. Both nested models can be absent. Passing flat AWS aliases or field names to `ValkyrieConfig.model_validate()` remains supported with a `DeprecationWarning`. (#808)
