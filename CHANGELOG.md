# Changelog

All notable changes to this project are documented in this file.

---

## [2026-09-05]

### Added
- Enable ALB access logging for the Tracker load balancers in dev, bench, and external production, delivering restricted request logs to dedicated encrypted S3 buckets (7-day retention in dev, 365-day in bench/prod) with a projected Glue table and enforced Athena workgroup for querying caller, request, response, latency, and trace fields during incident investigations (#755)

### Changed
- Raise output artifact size limits from 100MB to 250MB per file and per task, and stream sandbox-to-S3 artifact uploads instead of buffering the whole file in memory, lowering peak Tracker memory usage during concurrent uploads (#758)

### Fixed
- Fix `valkyrie config init` so hosted managed setup respects the CLI's selected `VALKYRIE_ENV` instead of always prompting/defaulting to the bench Tracker, preventing bench resource names from being saved into another environment's config (#761)
- Update the removed-static-AWS-credentials message to point users to the legacy recovery console instead of recommending they restore static credentials into a keyless managed config (#761)
