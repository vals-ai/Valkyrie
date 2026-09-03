# Valkyrie

[![Paper](https://img.shields.io/badge/Paper-alphaXiv-B31B1B)](https://www.alphaxiv.org/abs/valkyrie)
[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/vals-ai/Valkyrie)
[![Test Coverage](https://codecov.io/gh/vals-ai/Valkyrie/branch/dev/graph/badge.svg)](https://codecov.io/gh/vals-ai/Valkyrie)
[![Doc Coverage](https://vals-ai.github.io/Valkyrie/docstr-coverage.svg)](https://github.com/vals-ai/Valkyrie)

Valkyrie orchestrates scalable, reproducible evaluations for AI agents. It supports Vals-hosted infrastructure and self-hosted AWS deployments.

Guides, the CLI reference, the Python SDK reference, and contributor setup are published at [docs.valkyrie.vals.ai](https://docs.valkyrie.vals.ai/introduction/what-is-valkyrie). Their source lives in [`docs/`](docs).

## Quickstart

> **Note:** Valkyrie can be invoked using either `valkyrie` or the alias `valk`. For example: `valkyrie run start` or `valk run start`.

Install the CLI:

```bash
uv tool install git+https://github.com/vals-ai/Valkyrie@prod
```

Configure credentials and choose a hosting mode:

```bash
valkyrie config init
```

Upload an agent with a valid `contract.yaml`:

```bash
valkyrie agent push ./agents/sweagent --name sweagent
```

Start a benchmark run:

```bash
valkyrie run start \
  --agent sweagent \
  --benchmark swebench \
  --model anthropic/claude-sonnet-4-6 \
  --slice "0:10" \
  --connect
```

## Contributing and operations

| Topic | Location |
| --- | --- |
| Local development | [docs.valkyrie.vals.ai/contributing/local-development](https://docs.valkyrie.vals.ai/contributing/local-development) |
| Tracker service | [docs.valkyrie.vals.ai/contributing/tracker-service](https://docs.valkyrie.vals.ai/contributing/tracker-service) |
| Database and migrations | [docs.valkyrie.vals.ai/contributing/database](https://docs.valkyrie.vals.ai/contributing/database) |
| Versioning a release | [`DEVELOPMENT.md`](DEVELOPMENT.md#versioning) |
| Infrastructure operations | [`infra/README.md`](infra/README.md) |
| Executor releases | [`infra/executor-releases/README.md`](infra/executor-releases/README.md) |
| SDK release procedure | [`scripts/sdk/RELEASING.md`](scripts/sdk/RELEASING.md) |

## Citation

If you use Valkyrie in your research, cite [the paper](https://doi.org/10.1145/3786335.3813231). The BibTeX entry is at [docs.valkyrie.vals.ai/introduction/what-is-valkyrie#paper](https://docs.valkyrie.vals.ai/introduction/what-is-valkyrie#paper).
