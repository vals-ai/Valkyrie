# Changelog

All notable changes to this project will be documented in this file.

---

## [2026-09-01]

### Changed
- Grant `ValkyrieTrackerTaskRole` invoke access to Docent analyzer lambdas (`analysis-*`) and expand `ValkyrieExecutorTaskRole` invoke permissions to cover per-benchmark final-view lambdas (harvey-legal-agent, programbench, snap, swebench, terminalbench) alongside the existing `vals-format-lambda` (#740)
- Bump `create-benchmark-service` dependency to v0.32.0 in the tracker and executor_artifact services (#743)
