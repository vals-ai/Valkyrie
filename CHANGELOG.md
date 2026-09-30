# Changelog

All notable changes to this project are documented in this file.

---

## [2026-09-30]

### Added

- Add an optional dataset version selector (`--dataset-version` in the CLI, `dataset_version` in the SDK). The tracker resolves it to an exact dataset reference, saves it with the run, and reuses it through queueing, retries, and recovery, so the dataset stays fixed for the run's lifetime. Run responses now include `dataset_version` and `dataset_version_warning`, and the SDK exports a new `DatasetVersion` model. Selectors that are empty or longer than 1024 characters are rejected (#867)
- Add a `companion_models` agent contract kwarg (comma-separated model keys). A task's Model Gateway token now allows those models as well as the run model, for agents that run a second agent, such as an auditor, in the sandbox (#869)
- Add task-scoped Model Gateway credentials. When the model is attested, the tracker mints a token limited to that model for each sandbox and revokes it on teardown. The static executor gateway key no longer goes into the sandbox. The token lifetime is the task's agent timeout plus 2 hours, capped at 7 days (#843)

### Changed

- Dataset version pinning is enabled in dev only, through the `DATASET_VERSION_PINNING_ENABLED` tracker setting. Bench, prod, and release-test stay disabled for staged rollout (#867)
