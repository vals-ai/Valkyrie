# DatasetTokenAnalysis

Tokenizing the `harvey-legal-agent` dataset to figure out which tasks will instantly DQ a small-context-window model.

## Why this matters

Models with 128 K or 200 K context windows can't even *load* some of the harvey-legal-agent tasks — the task brief + reference docs + working files exceed the input budget. Running them is a waste of money and Sentry noise.

We saw this happen on `kimi-k2.6-thinking` (context = 200 K) — a handful of tasks errored during the planning phase with "context window exceeded" before the model produced any output.

## The script

The repo with the dataset is the `harvey-legal-agent-benchmark-service` (where the tasks live). Tokenize each task with the model's tokenizer to get a hard budget number.

`/home/ubuntu/tokcheck/tokenize_tasks.py` does this. Sketch:

```python
import json, pathlib
from anthropic import Anthropic                      # for Sonnet tokenizer
import tiktoken                                       # for OpenAI tokenizers
from pathlib import Path

DATASET_DIR = Path("/path/to/harvey-legal-agent-benchmark-service/tasks")

# Each task has prompt.md + working_files/* that get loaded into the agent's context.
def task_token_count(task_dir: Path, encoding) -> int:
    text = (task_dir / "prompt.md").read_text()
    for wf in (task_dir / "working_files").rglob("*"):
        if wf.is_file() and wf.suffix in (".md", ".txt", ".docx", ".pdf"):
            text += wf.read_text(errors="ignore")  # crude; for docx/pdf use a parser
    return len(encoding.encode(text))

enc = tiktoken.get_encoding("cl100k_base")  # close enough for cross-model estimates
out = []
for task_dir in DATASET_DIR.iterdir():
    if task_dir.is_dir():
        out.append({"task_id": task_dir.name, "tokens": task_token_count(task_dir, enc)})
out.sort(key=lambda x: -x["tokens"])
```

For docx / pdf reference files, use `python-docx` / `PyPDF2` to extract text first.

## Output format

CSV with columns: `task_id, total_tokens, prompt_tokens, working_files_tokens`. Sorted descending.

A real one is at `/home/ubuntu/run_logs/task_tokens.csv` from the running session — 1251 rows, ranging from 2 K to ~480 K tokens.

## Distribution we saw

| Bucket | # tasks |
|---|---|
| 0 – 32 K | 587 |
| 32 – 64 K | 312 |
| 64 – 128 K | 218 |
| 128 – 200 K | 92 |
| 200 – 250 K | 29 |
| **> 250 K** | **13** |

So at 250 K context: 13 tasks DQ. At 200 K context: 13 + 29 = 42 tasks DQ. At 128 K context: 134 tasks DQ.

## Picking models from the proxy registry by context window

```bash
# Browse the model proxy registry for context-window field
grep -rE 'context_window|max_tokens|MAX_INPUT' /home/ubuntu/repos/model-proxy/model_library/providers/ \
  | grep -E '\b(200|256|400|500|1000)\b' | head -20
```

A long-context shortlist for harvey-legal-agent (250 K threshold) is at `/home/ubuntu/run_logs/long_context_candidates.md`. Anthropic Sonnet/Opus, Google Gemini Pro/Flash, and a handful of OpenAI models clear it; most Mistral/Together/Fireworks-hosted models do not.

## Caveat — context window is necessary, not sufficient

A model with 1 M context can still:

- Hit max-output-tokens during deliverable drafting (Bug-L → judge `IndexError`)
- Hit a context-window error mid-conversation if the agent loop keeps appending tool outputs (the budget shrinks every turn)
- Refuse to comply on certain task content (legal disclaimers etc)

The token analysis is a *floor* check — does the model have any chance? — not a prediction of pass rate.
