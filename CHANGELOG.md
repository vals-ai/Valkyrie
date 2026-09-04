# Changelog

All notable changes to this project are documented in this file.

---

## [2026-09-04]

### Added

- Publish Valkyrie's end-user documentation on Mintlify at `docs.valkyrie.vals.ai`, with handwritten task guides plus CLI and Python SDK references generated from source by `scripts/generate_reference.py`; CI now enforces that committed reference output stays byte-identical to a fresh generation, and redirects keep retired command/SDK URLs working (#686)

### Changed

- Move maintainer- and Vals-specific operational docs (development setup, tracker service, database/migrations, infrastructure) out of scattered READMEs and into `docs/contributing/` and `infra/README.md`, with root-level pointers left in place (#686)
- Add a `docs` CI job that validates the Mintlify build and checks for broken links/anchors, and extend `cli-unit-tests` to check generated reference freshness and run the docs reference test suite (#686)

### Fixed

- Add an `environment` field (`bench`/`prod`/`dev`) to `ValkyrieConfig` so SDK clients no longer fail with `extra_forbidden` when loading a `valkyrie.yaml` written by `valk config init`; `ValkyrieClient` now resolves its tracker base URL from `config.tracker_url` for the configured environment instead of always defaulting to the bench tracker (#754)
