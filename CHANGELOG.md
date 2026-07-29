# Changelog

All notable changes to this project will be documented in this file.

---

## [2026-07-29]

### Added
- Support task-targeted Daytona snapshots, allowing sandboxes to be created from a `TargetedSnapshotSource` (snapshot pinned to a specific region/target) while still reporting as `snapshot` in tracker metrics and cleanup; bumps `create-benchmark-service` to v0.20.0 (#627)
- Document how to add a CLI wrapper to an agent that doesn't have one, with a runnable `argparse`-based example, and link to it from the contract overview (#616)
