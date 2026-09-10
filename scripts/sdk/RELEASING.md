# Releasing the Valkyrie SDK

`valkyrie-sdk` is versioned independently in `packages/valkyrie-sdk/pyproject.toml`.

## One-time setup

1. Create protected GitHub environments with a required `@vals-ai/valkyrie` reviewer, no
   self-review, and no administrator bypass:

   - `pypi-test`: allow protected branches; the workflow limits releases to `dev` and `prod`.
   - `pypi`: allow only `prod`.

2. Create `valkyrie-sdk` under the Vals AI PyPI organization.
3. Add a Trusted Publisher on PyPI and TestPyPI using owner `vals-ai`, repository `Valkyrie`,
   workflow `publish-sdk.yml`, and environment `pypi` or `pypi-test`, respectively.

## Release

1. Bump the version with `uv version --package valkyrie-sdk --bump patch|minor|major`.
2. Merge the change to `dev` after **SDK package** CI passes.
3. Run **Publish Valkyrie SDK** from `dev` with target `testpypi`; verify the SHA, version, files,
   and hashes before approving the environment.
4. Install and smoke-test the exact TestPyPI version. Install dependencies from PyPI and the SDK
   from TestPyPI with `--no-deps`.
5. Merge the tested commit into `prod`, then verify and approve the `pypi` environment.
6. Install the package from PyPI and check its wheel, source distribution, and attestations.

## Rules

PyPI releases cannot be overwritten. Do not use global `skip-existing`. If a release is incomplete,
yank it and publish a new patch version.
