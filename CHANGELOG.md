# Changelog

All notable changes to this project will be documented in this file.

---

## [2026-09-26]

### Added
- Add staged, phase-scoped egress policies across the task lifecycle: agents declare install-time network access via `install_egress` (`"*"`, `[]`, or an explicit allowlist of URLs/domains/IPs/CIDRs), while benchmarks separately control setup, run, and evaluation egress via `BenchmarkEgressPlan`, with legacy `egress_allowlist` entries unioned into the run policy and configurable agent-install ordering (`agent_install_order`) relative to benchmark setup (#842)

### Fixed
- Fix declared output artifacts that are legitimately empty (0 bytes) failing to upload: since Daytona's streaming download raises "No file data received" for zero-byte files, the tracker now writes an empty object directly instead of streaming, preventing required-artifact collection failures like the BioMysteryBench `turns.jsonl` case (#849)
