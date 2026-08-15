# Changelog

All notable changes to this project are documented in this file.

---

## [2026-08-15]

### Fixed
- Fix `infra-checks` by having the dev stage config test read its expected task-sizing values from `DEV_CONFIG` instead of asserting hardcoded capacities, so the test no longer breaks whenever dev sizing (e.g. tracker `max_tasks`) changes (#678)
