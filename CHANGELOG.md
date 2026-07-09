# Changelog

All notable changes to this project are documented in this file.

---

## [2026-07-09]

### Added
- Validate agent names against a safe charset (letters, digits, dots, dashes, underscores) on the contract `name` field and the `--name` override, preventing unsafe S3 keys (#510)

### Changed
- Derive agent S3 upload names from the `name` field in `contract.yaml` instead of the directory name for `push`, `install`, and `run <local-dir>`, with `--name` remaining an optional override (#510)
- Tracker health is now checked automatically when entering `TrackerService`, raising `TrackerNotFoundError` on failure instead of each CLI command performing its own inline health check (#514)
- Reorganized CLI commands into per-group subpackages (`agent`, `benchmark`, `config`, `run`) so `main.py` only wires up Click groups (#513)
- Consolidated duplicated CLI pagination logic into a shared pager used by the agent list, run list, and benchmark service list commands (#515)
