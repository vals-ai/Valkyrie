# Docent ingestion

Optional post-hoc analysis: upload a finished run's transcripts to [Transluce Docent](https://docent.transluce.org) and auto-generate an error-analysis report.

## Enable

Declare the agent's analyzer Lambda in `contract.yaml`:

```yaml
ingest_lambda: analysis-model-library
```

The Lambda is resolved from the agent's **current** pushed contract, not the run's snapshot — so adding `ingest_lambda` and re-pushing makes past runs analyzable too.

## Trigger

```bash
valk run analyze <run_id>
```

Streams progress over SSE from the tracker, which invokes the analyzer Lambda using the run's stored AWS credentials. The Lambda converts each task's `agent_output.tar.gz` into a `docent.data_models.AgentRun`, uploads to a collection named after the benchmark, and submits a 2-step reading plan (`valkyrie-ingest:{benchmark}:{run_id}`):

1. **Per-run summary** — 2-3 sentences per AgentRun (`openai/gpt-5.4-mini`).
2. **Comprehensive error analysis** — markdown report aggregating all summaries (`openai/gpt-5.4`).

On success the command prints a Docent reading URL — open it to see the error-analysis report and drill into individual AgentRuns / transcripts.

`--no-cache` bypasses the stored URL and re-fires ingestion — useful after fixing an analyzer or after adding more tasks.

The reading-plan URL and status (`IDLE` / `RUNNING` / `ERROR` / `DONE`) live on the benchmark row, so repeated calls short-circuit to the cached URL. `valk run fetch <run_id>` surfaces the URL inline once available.

The CLI blocks for up to 15 min (the Lambda's hard ceiling). If the Lambda hits its timeout, the reading plan was already submitted and keeps running server-side — just open Docent. Reruns are additive at the collection level (sort by `metadata.ingested_at`).

## Going deeper

The auto-generated report is a **first pass**. For real investigation, paste reading URLs into Claude Code (with the Docent plugin):

```
Here's the error-analysis reading from our latest run — what are the
top failure modes, and which look like model issues vs. harness issues?
https://docent.transluce.org/.../readings/<reading_id>
```

Cross-run comparisons work the same way — paste multiple URLs and ask Claude to compare (different agents on the same benchmark, or one agent across model upgrades).

## Writing an analyzer Lambda

Each agent's output shape needs its own analyzer Lambda. To scaffold one with Claude Code:

1. **Install the Transluce Docent plugin** — see [quickstart](https://docs.transluce.org/quickstart#instructions).
2. **Install the Vals `docent-analyzer` plugin**:

   ```
   /plugin marketplace add vals-ai/claude-plugins
   /plugin install docent-analyzer@vals-plugins
   /reload-plugins
   ```

3. Ask Claude *"Set up Docent ingestion for run `<run_id>`"*. The skill resolves the agent + bucket from the run, inspects a sample task output, generates `handler.py` / `Dockerfile` / `deploy.sh`, deploys to AWS, and wires `ingest_lambda` into the contract.
