# Releasing the Valkyrie SDK

`valkyrie-sdk` is versioned independently in `packages/valkyrie-sdk/pyproject.toml`.

## One-time configuration

1. Create protected GitHub environments with a required `@vals-ai/valkyrie` reviewer, no
   self-review, and no administrator bypass:

   - `testpypi`: allow `dev` and `prod`.
   - `pypi`: allow only `prod`.

2. Create `valkyrie-sdk` under the Vals AI PyPI organization.
3. Configure a Trusted Publisher on PyPI and TestPyPI with owner `vals-ai`, repository `Valkyrie`,
   workflow `publish-sdk.yml`, and the matching environment name.

## Release

1. Bump the version with `uv version --package valkyrie-sdk --bump patch|minor|major`.
2. Merge the change to `dev` after **SDK package** CI passes.
3. Run **Publish Valkyrie SDK** from `dev` with target `testpypi`; verify the SHA, version, files,
   and hashes before approving the environment.
4. Install and smoke-test the exact TestPyPI version. Install dependencies from PyPI and the SDK
   from TestPyPI with `--no-deps`.
5. Promote the tested commit to `prod`, then verify and approve the `pypi` environment.
6. Confirm the PyPI wheel, source distribution, installation, and attestations.

## Rules

Published files are immutable. Never use global `skip-existing`; if a release is incomplete or its
integrity is uncertain, yank it and publish a new patch version.
