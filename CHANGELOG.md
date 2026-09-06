# Changelog

All notable changes to this project are documented in this file.

---

## [2026-09-06]

### Added
- Return org-wide `models`, `datasets`, and `started_by_emails` from `GET /benchmarks/filter-options` so dashboard run-list filter dropdowns are no longer limited to values on the current paginated page (#764)
- Add tracker ALB access logging (#755)

### Changed
- Raise tracker output artifact size limits to 250MB and stream uploads instead of buffering (#758)
- Publish the Mintlify documentation site at docs.valkyrie.vals.ai, replacing scattered top-level READMEs/docs with a generated CLI and Python SDK reference, plus CI checks for reference freshness, doc tests, and broken links (#686)

### Fixed
- SDK now accepts the CLI's environment key and targets that environment's matching tracker instead of always using the default (#754)
