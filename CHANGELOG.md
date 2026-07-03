# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

---

## [2026-07-03]

### Added

- Add named sandbox provider selection with `valkyrie config provider set/default/list/remove` and `valkyrie run start --provider`, replacing the single `SANDBOX_PROVIDER_SECRET_NAME` config with multiple named providers that persist through resume and force-stop (#499, #500)
- Show tenant-accessible benchmark services from a hosted catalog (`BENCHMARK_CATALOG_URL`) in `valkyrie config service list`, merged with local custom service overrides and displaying URL domain and latency (#502)
- Preserve prior task attempts across retries and expose them as `history` (newest-first) on exported task results, backed by a new `ErrorResult` table and timestamped evaluation results (#477)

### Changed

- Defer benchmark service health checks in `valkyrie config service list` until a page is rendered, caching per-service results so paging backward doesn't re-ping (#503)
