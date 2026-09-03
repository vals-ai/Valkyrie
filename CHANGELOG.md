# Changelog

All notable changes to this project are documented in this file.

---

## [2026-09-03]

### Added
- Add an external production deployment lane with its own AWS account, `ValkProd*` roles/stacks, DNS, and `prod-external` GitHub environment, running independently alongside the existing benchmark deployment lane; hosted CLI configuration can now select it via `VALKYRIE_ENV=external` (#733)
- Forward `-H` header options on `run resume`/`run retry`, matching `run start` (#744)

### Changed
- Bump the pinned `create-benchmark-service` dependency to `v0.34.0` across Tracker and the executor artifact (#750)

### Fixed
- Collect an agent's final output and declared artifacts before re-raising a nonzero exit, so terminal artifacts are no longer lost when an agent fails after writing results; preserve the original agent error if artifact collection itself fails; keep persisted run errors free of raw command/output text, recording only the exit code; refuse a local `run start` that would silently overwrite an existing published agent alias (#734)
- Enable encrypted storage for new external-production databases while leaving existing dev/bench database templates unchanged, avoiding a disruptive database replacement during deploy (#750)
- Run maintenance-classification synthesis with a trusted helper script resolved from the target/base revision (falling back to the protected default branch), instead of a revision that could be influenced by untrusted candidate code (#750)
- Keep the AWS CDK import in `infra/stage.py` behind `TYPE_CHECKING` so the standalone executor release publisher, which doesn't have CDK available at runtime, no longer fails on import (#751)
- Pin the Tracker live integration test suite to `python:3.11-alpine3.20` after the mutable `alpine` tag moved to 3.24 and started leaving sandboxes stuck in `ERROR` (#752)
