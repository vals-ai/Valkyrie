# Valkyrie

[![Paper](https://img.shields.io/badge/Paper-alphaXiv-B31B1B)](https://www.alphaxiv.org/abs/valkyrie)
[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/vals-ai/Valkyrie)
[![Test Coverage](https://codecov.io/gh/vals-ai/Valkyrie/branch/dev/graph/badge.svg)](https://codecov.io/gh/vals-ai/Valkyrie)
[![Doc Coverage](https://vals-ai.github.io/Valkyrie/docstr-coverage.svg)](https://github.com/vals-ai/Valkyrie)

Valkyrie orchestrates scalable, reproducible evaluations for AI agents. It supports Vals-hosted infrastructure and self-hosted AWS deployments.

Guides, the CLI reference, the Python SDK reference, and contributor setup are published at [docs.valkyrie.vals.ai](https://docs.valkyrie.vals.ai/introduction/what-is-valkyrie).

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

## Where to go next

| Topic | Location |
| --- | --- |
| Install and run your first benchmark | [get-started/quickstart](https://docs.valkyrie.vals.ai/get-started/quickstart) |
| Uploading agents and writing a contract | [agents/agent-contract](https://docs.valkyrie.vals.ai/agents/agent-contract) |
| Starting, monitoring, and managing runs | [runs/start](https://docs.valkyrie.vals.ai/runs/start) |
| Results and output artifacts | [runs/results-and-outputs](https://docs.valkyrie.vals.ai/runs/results-and-outputs) |
| Hosting your own benchmark service | [benchmarks/custom-services](https://docs.valkyrie.vals.ai/benchmarks/custom-services) |
| Every CLI command | [reference/cli/index](https://docs.valkyrie.vals.ai/reference/cli/index) |
| Python SDK | [sdk/quickstart](https://docs.valkyrie.vals.ai/sdk/quickstart) |
| Local development | [contributing/local-development](https://docs.valkyrie.vals.ai/contributing/local-development) |

## Citation

If you use Valkyrie in your research, cite [the paper](https://doi.org/10.1145/3786335.3813231):

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
