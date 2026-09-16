# Changelog

All notable changes to this project are documented in this file.

---

## [2026-09-16]

### Added
- Add Tracker routes, SDK (`client.artifacts`), and CLI (`valkyrie run artifacts`) support for listing and downloading run artifacts via short-lived S3 URLs (#785)
- Add SDK `client.runs.download_outputs()` to stream and extract run/task output archives into a new local directory with size and entry limits (#784)
- Add SDK `client.runs.filter_options()` and CLI `valkyrie run filter-options` to discover benchmark, agent, model, dataset, and starter values from run history (#783)
- Add SDK async iterators `client.runs.iter()` and `client.benchmarks.iter_tasks()` for paginating through runs and tasks (#782)
- Add SDK `client.runs.update_concurrency()` method for adjusting run concurrency (#781)
- Add CLI commands `valkyrie run tasks`, `valkyrie run task`, and `valkyrie run task-artifacts` for filtered task listing, task detail, and temporary artifact download links (#780)
- Add CLI/SDK support for pushing, installing from GitHub, downloading, and removing agents through new Tracker upload endpoints, plus `valkyrie agent list --all/--format json` (#775)

### Changed
- `valkyrie run output` now requires a new, non-existing output directory; existing destinations are refused (#785)

### Fixed
- Isolate tracker admission database transactions from async I/O to prevent an event-loop deadlock where a concurrent lock query could stall a benchmark run's admission and dispatch (#794)
- Raise the RDS connections alarm threshold from 135 to 1400 for the r7g.large tracker database to match its actual connection capacity and stop false alarms (#795)
