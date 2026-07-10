# Releasing the Valkyrie SDK

The SDK is versioned independently from the Valkyrie service and CLI. Its version is defined in
`packages/valkyrie-sdk/pyproject.toml`; repository `v*` tags do not publish the SDK.

## Choose and set the version

Use semantic versioning:

- Patch for compatible fixes and package-documentation changes.
- Minor for backward-compatible public features.
- Major for breaking public API changes.

Update the workspace member and lockfile together:

```bash
uv version --package valkyrie-sdk --bump patch
uv version --package valkyrie-sdk --bump minor
uv version --package valkyrie-sdk --bump major
```

Every change under `packages/valkyrie-sdk` must use a version greater than the version on the pull
request base. The initial release is `0.1.0`.

## Verify locally

From the repository root:

```bash
uv sync --locked --group dev
uv run pytest tests/unit/sdk tests/contract -q
uv run ruff check scripts packages/valkyrie-sdk/src tests/unit/sdk tests/contract
uv run basedpyright
uv build --package valkyrie-sdk --no-sources --out-dir dist/sdk
uv run python scripts/validate_sdk_artifacts.py dist/sdk/*
uv run --package valkyrie-sdk --group test twine check --strict dist/sdk/*
uv run --package valkyrie-sdk --group test check-wheel-contents dist/sdk/*.whl
uv run python scripts/verify_sdk_install.py --dist dist/sdk
```

CI repeats these checks in isolated Python 3.12 environments for the wheel, the sdist, and the root
Valkyrie wheel installed alongside the SDK wheel.

## One-time TestPyPI setup

Create the `testpypi` GitHub Environment with:

- Deployment branches limited to `dev` and `prod`.
- A required reviewer from `@vals-ai/valkyrie`.
- Self-review disabled.
- Administrator bypass disabled.

On TestPyPI, configure a Trusted Publisher for:

- Project: `valkyrie-sdk`
- Owner: `vals-ai`
- Repository: `Valkyrie`
- Workflow: `publish-sdk.yml`
- Environment: `testpypi`

The project can be created directly first or through a pending publisher. A pending publisher does
not reserve the project name.

## Rehearse on TestPyPI

After the release commit reaches `dev`, manually run **Publish Valkyrie SDK** from the `dev` ref with
the `testpypi` target. Review the source commit, version, filenames, and SHA-256 values before
approving the environment.

TestPyPI does not mirror runtime dependencies. Install dependencies from PyPI, then install only the
SDK artifact from TestPyPI:

```bash
python -m venv /tmp/valkyrie-sdk-testpypi
/tmp/valkyrie-sdk-testpypi/bin/python -m pip install \
  "httpx>=0.28.1,<1" "pydantic>=2,<3" "PyYAML>=6.0.3,<7"
/tmp/valkyrie-sdk-testpypi/bin/python -m pip install \
  --index-url https://test.pypi.org/simple \
  --no-deps \
  valkyrie-sdk==0.1.0
/tmp/valkyrie-sdk-testpypi/bin/python -c \
  "from valkyrie.sdk import ValkyrieClient; print(ValkyrieClient)"
```

Replace `0.1.0` with the version being rehearsed.

## One-time PyPI setup

A Vals AI PyPI organization owner or manager should create `valkyrie-sdk` directly under the
organization, assign the SDK release team, and add this Trusted Publisher:

- Project: `valkyrie-sdk`
- Owner: `vals-ai`
- Repository: `Valkyrie`
- Workflow: `publish-sdk.yml`
- Environment: `pypi`

Direct organization creation reserves the name. If it is unavailable, create a pending publisher,
perform the first upload immediately, then transfer the project to the Vals AI organization and
verify organization ownership.

Create the `pypi` GitHub Environment with:

- Deployment branches limited to `prod`.
- A required reviewer from `@vals-ai/valkyrie`.
- Self-review disabled.
- Administrator bypass disabled.

Protect `prod` from direct and force pushes and require CODEOWNERS review for publishing workflow,
package metadata, and release-script changes.

## Publish to PyPI

Promote the verified commit through the normal `dev` to `prod` process. A `prod` push that changes
the SDK package automatically builds and verifies the artifacts, then waits for `pypi` environment
approval. Production publication also checks `github.ref == 'refs/heads/prod'`; a manual run from
another ref cannot publish to PyPI.

Before approval, verify:

- Project name and SDK version.
- Source commit.
- Wheel and sdist filenames.
- SHA-256 hashes.
- Target index is `pypi`.

After upload:

```bash
python -m venv /tmp/valkyrie-sdk-pypi
/tmp/valkyrie-sdk-pypi/bin/python -m pip install valkyrie-sdk==0.1.0
/tmp/valkyrie-sdk-pypi/bin/python -c \
  "from valkyrie.sdk import ValkyrieClient; print(ValkyrieClient)"
```

Verify the PyPI project belongs to the Vals AI organization and that both files and their
attestations match the approved release summary.

## Duplicate or partial uploads

PyPI releases are immutable. Normal publication fails if any file already exists for the version;
do not enable a global skip-existing option.

If only one artifact uploaded:

1. Compare every existing PyPI filename and SHA-256 hash with the approved GitHub Actions manifest.
2. Upload a missing file only when every existing hash matches the approved artifact and a release
   owner explicitly authorizes recovery.
3. If integrity cannot be proven, yank the incomplete release, increment the patch version, rebuild,
   and publish the new version.

Never replace an uploaded file or reuse a version for different bytes.

Trusted Publishing references:

- [PyPI Trusted Publishers](https://docs.pypi.org/trusted-publishers/)
- [Creating a project through OIDC](https://docs.pypi.org/trusted-publishers/creating-a-project-through-oidc/)
- [PyPA GitHub Actions publishing guide](https://packaging.python.org/en/latest/guides/publishing-package-distribution-releases-using-github-actions-ci-cd-workflows/)
