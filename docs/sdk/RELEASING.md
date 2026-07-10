# Releasing the Valkyrie SDK

`valkyrie-sdk` is versioned independently from the service and CLI. Its version lives in
`packages/valkyrie-sdk/pyproject.toml`; repository `v*` tags do not publish it.

## Release checklist

1. Bump the package and lockfile with `uv version --package valkyrie-sdk --bump patch|minor|major`.
2. Run the local checks:

   ```bash
   uv sync --locked --group dev
   uv run pytest tests/unit/sdk tests/contract -q
   uv run ruff check scripts packages/valkyrie-sdk/src tests/unit/sdk tests/contract
   uv run basedpyright
   uv build --package valkyrie-sdk --no-sources --out-dir dist/sdk
   uv run python scripts/validate_sdk_artifacts.py dist/sdk/*
   uv run twine check --strict dist/sdk/*
   uv run check-wheel-contents dist/sdk/*.whl
   uv run python scripts/verify_sdk_install.py --dist dist/sdk
   ```

3. Run **Publish Valkyrie SDK** from `dev` with the `testpypi` target and approve the `testpypi`
   environment after checking the source SHA, version, filenames, and hashes.
4. Promote the commit to `prod`. A package change builds the same verified payload and waits for
   approval in the `pypi` environment.
5. Install the released version in a clean environment and verify both PyPI files and attestations.

TestPyPI does not mirror dependencies. Install runtime dependencies from PyPI, then install the
exact TestPyPI SDK version with `--no-deps`.

## One-time configuration

Create `testpypi` and `pypi` GitHub environments with a required `@vals-ai/valkyrie` reviewer,
self-review and administrator bypass disabled, and these branch rules:

| Environment | Allowed branches |
| --- | --- |
| `testpypi` | `dev`, `prod` |
| `pypi` | `prod` |

Create `valkyrie-sdk` under the Vals AI PyPI organization, assign the release team, and configure a
Trusted Publisher on both indexes:

| Field | Value |
| --- | --- |
| Owner | `vals-ai` |
| Repository | `Valkyrie` |
| Workflow | `publish-sdk.yml` |
| Environment | `testpypi` or `pypi` |

A pending publisher does not reserve the project name. Prefer creating the project directly under
the organization. Protect `prod` from direct/force pushes and require CODEOWNERS review.

## Failure policy

Published versions and files are immutable. Normal releases fail if the version already exists;
never enable global `skip-existing`.

For a partial upload, compare the existing filename and SHA-256 against the approved Actions
artifact. A release owner may upload only the missing matching file. If integrity is uncertain,
yank the incomplete release, bump the patch version, rebuild, and publish again.

See [PyPI Trusted Publishers](https://docs.pypi.org/trusted-publishers/) and the
[PyPA publishing guide](https://packaging.python.org/en/latest/guides/publishing-package-distribution-releases-using-github-actions-ci-cd-workflows/).
