# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

---

## [2026-06-25]

### Added

- Add `--label`/`-l` flag to `valkyrie run start` and `valkyrie run list` to tag runs at start time, filter by label in list (case-insensitive), and expose label in fetch/list responses (#483)
- Display `final_score` in `valkyrie run fetch` output when a benchmark has a final evaluation (#475)
- Add GitHub Actions workflow for automated daily changelog generation (#486)

### Changed

- Abstract sandbox provider configuration from Daytona-specific `daytona_secret_name` to a generic `sandbox_provider_secret_name`, integrating `create-benchmark-service` v0.8.1 provider-selection flow (#454)
- Consolidate run attribution on `started_by_email`; remove redundant `run_by_email` field from `Benchmark` model, response types, and list-row builder, with a migration that backfills historical data (#482)
- Bump `daytona` dependency lower bound from `>=0.180.0` to `>=0.189.0` (#473)
- Rename "Descope API key" to "Vals AI API key" throughout hosted-mode documentation (#481)
- Clarify `-s` secret mapping behavior in README and add note distinguishing `agent outputs` from `run results` (#480)
- Add link to public agent registry in the Agent Management section of the README (#478)

### Fixed

- Retry with `--task-ids` on an errored run now resets only the requested tasks instead of pulling in all ERROR rows (#476)
