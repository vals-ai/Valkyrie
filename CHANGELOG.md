# Changelog

All notable changes to this project are documented in this file.

---

## [2026-07-12]

### Added
- Include the persisted benchmark name in the end-of-run lambda payload so `vals-format-lambda` can deterministically select benchmark-specific result files when a benchmark writes multiple result JSONs (#538)

### Changed
- Stream declared output artifacts (e.g. large SkillsBench turn sidecars) through the sandbox file-transfer API instead of base64-encoding them through command stdout, fixing multi-hour stalls on large artifacts (#538)
- Move `Track progress:` inside the Run Details box as a `│`-prefixed row after the `├─` divider, matching `run resume`, and skip it when `--connect` is used (#534)
- Consolidate CLI internals to reduce duplication: reuse the tracker S3 client helper instead of building a separate aioboto3 session, share agent zip download/S3 delete/object-exists handling, share benchmark-service header setup across run start/resume and benchmark tasks, and simplify GitHub install agent-name resolution (#516)

### Removed
- Remove the internal Valkyrie CLI shim modules (`valkyrie.cli.bundler` helpers, `valkyrie.schemas`) and the one-call agent install spinner wrapper, importing the needed tracker agent symbols directly instead (#516)
