# Valkyrie

[![Paper](https://img.shields.io/badge/Paper-alphaXiv-B31B1B)](https://www.alphaxiv.org/abs/valkyrie)
[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/vals-ai/Valkyrie)
[![Test Coverage](https://codecov.io/gh/vals-ai/Valkyrie/branch/dev/graph/badge.svg)](https://codecov.io/gh/vals-ai/Valkyrie)
[![Doc Coverage](https://vals-ai.github.io/Valkyrie/docstr-coverage.svg)](https://github.com/vals-ai/Valkyrie)

Valkyrie orchestrates scalable, reproducible evaluations for AI agents. It supports Vals-hosted infrastructure and self-hosted AWS deployments.

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

## Documentation

Guides, the CLI reference, and the Python SDK reference are published at [docs.valkyrie.vals.ai](https://docs.valkyrie.vals.ai/introduction/what-is-valkyrie). Their source lives in [`docs/`](docs).

## Contributing and operations

- [Local development](DEVELOPMENT.md)
- [Tracker service](services/tracker/README.md)
- [Database and migrations](services/tracker/src/tracker/database/README.md)
- [Infrastructure operations](infra/README.md)

## Citation

If you use Valkyrie in your research, cite the paper:

```bibtex
@inproceedings{forzano2026valkyrie,
  author    = {Forzano, Jarett and Almatov, Omar and Nashold, Langston and Ravi, Nikil and Kassian, Orestes},
  title     = {Valkyrie: A Microservice-Based Framework for Scalable Evaluation of AI Agents},
  booktitle = {Proceedings of the 1st ACM Conference on Agentic and AI Systems},
  series    = {CAIS '26},
  year      = {2026},
  month     = {may},
  address   = {San Jose, CA, USA},
  publisher = {ACM},
  location  = {New York, NY, USA},
  numpages  = {5},
  doi       = {10.1145/3786335.3813231},
  url       = {https://doi.org/10.1145/3786335.3813231}
}
```
