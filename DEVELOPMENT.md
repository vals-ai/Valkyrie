# Development

You need Python 3.12, [uv](https://docs.astral.sh/uv/), and Docker.

## Install from source

From the repository root:

```bash
make install

# Put an editable `valkyrie` on your PATH
make tool-install
```

Run `uv tool update-shell` if your shell can't find `valkyrie` afterwards.

## Check your changes

```bash
# ruff format and ruff check --fix
make style

# basedpyright in strict mode
make typecheck

# CLI unit and local integration tests
make test
```

## Work on other parts

- The Tracker service, its tests and migrations: [services/tracker/README.md](services/tracker/README.md)
- The documentation site: [docs/README.md](docs/README.md)
- Running benchmarks on your machine without AWS:
  [Run Tracker locally](https://docs.valkyrie.vals.ai/self-hosting/local-tracker)
