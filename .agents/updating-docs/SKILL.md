---
name: updating-docs
description: Use when adding or changing Valkyrie public documentation, Mintlify guides, generated CLI or Python SDK references, docs navigation, redirects, styling, or code that makes docs stale. Enforces the public/internal boundary, task-guide and reference separation, grouped reference architecture, deterministic generation, Devin visual review, and desktop/mobile validation.
---

# Updating Valkyrie documentation

Read this skill before editing `docs/`, `scripts/generate_reference.py`, documentation tests, or public behavior that changes documented commands, defaults, routes, SDK methods, or types.

## Canonical locations

`docs/` is the only public end-user documentation source.

| Subject | Canonical location |
| --- | --- |
| Public end-user documentation | `docs/` |
| Public CLI and Python SDK reference generator | `scripts/generate_reference.py` |
| Generated reference tests | `tests/unit/docs/test_generate_reference.py` |
| Contributor setup | `DEVELOPMENT.md` |
| Tracker operation | `services/tracker/README.md` and tracker-local docs |
| Database operation and migrations | `services/tracker/src/tracker/database/README.md` |
| Generic self-hosting | `docs/self-hosting/` |
| Vals-specific infrastructure operation | `infra/README.md` |
| SDK release procedure | `scripts/sdk/RELEASING.md` |
| Internal agent instructions | `.agents/` |

Never publish contributor procedures, Vals-specific operations, release activation, protected-environment instructions, incident procedures, break-glass access, or internal agent material in Mintlify.

Never reference a private repository, issue, URL, hostname, credential, or internal item from public documentation.

## Choose the document layer

Use each layer for one job:

| Layer | Job | Content |
| --- | --- | --- |
| Quickstart | Reach the first successful result | Shortest verified path only |
| Task guide | Complete a real workflow | Decisions, lifecycle behavior, safety, cost, consequences, recovery |
| Generated reference | Look up the complete public contract | Every argument, option, alias, default, constraint, signature, field, return, or enum value |
| Repository runbook | Maintain or operate Valkyrie | Contributor and environment-specific procedures |

Do not repeat exhaustive flags, parameters, signatures, or fields in handwritten guides. Link directly to the generated section anchor.

Do not turn quickstarts into feature surveys. Remove alternate workflows, optional branches, and kitchen-sink commands unless the shortest successful path needs them.

Do not remove unique lifecycle, safety, cost, consequence, or recovery guidance merely because a reference page exists.

## Write the smallest truthful change

- Read the affected code and current documentation before editing.
- Correct the smallest block that became false. Do not rewrite adjacent prose without a separate reason.
- Lead with the task outcome. Use concrete commands before abstract explanation.
- Use current repository vocabulary and real names.
- Verify every command shown to readers.
- Run safe local commands in the state readers will use.
- Use syntax checks or disposable environments when execution needs credentials.
- Obtain explicit approval before production, destructive, cost-incurring, or live commands.
- State prerequisites and recovery beside the failing step.
- Document current behavior only. Put plans in issues, not documentation.
- Search for stale names, routes, defaults, and links instead of relying on memory.

## Generated reference architecture

Always change `scripts/generate_reference.py` and its tests first. Never hand-edit generated files under `docs/reference/`.

The current source produces:

- 32 Click leaf commands in four top-level groups.
- 22 public SDK methods in four resources.
- 28 public SDK models and four enums in five families.
- 86 unique redirects from retired per-item routes to exact grouped-page anchors.

These counts describe the current source. They are not permanent limits. Derive future surfaces from inspected code, then update generator coverage and expected routes.

### CLI routes

- `/reference/cli`
- `/reference/cli/run`
- `/reference/cli/agent`
- `/reference/cli/benchmark`
- `/reference/cli/config`

### SDK resource routes

- `/reference/sdk`
- `/reference/sdk/client`
- `/reference/sdk/runs`
- `/reference/sdk/benchmarks`
- `/reference/sdk/agents`
- `/reference/sdk/services`
- `/reference/sdk/errors`

### SDK type routes

- `/reference/sdk/models`
- `/reference/sdk/models/agents`
- `/reference/sdk/models/runs`
- `/reference/sdk/models/benchmarks`
- `/reference/sdk/models/services`
- `/reference/sdk/models/config`

Do not restore one page per command, method, model, or enum.

Render each command, method, model, or enum once as a stable explicit H2 section:

```mdx
## `valkyrie run start` {#start}
## `client.runs.start` {#start}
## `FetchBenchmarksRequest` {#fetch-benchmarks-request}
```

Use Mintlify's native right-side table of contents as the section index. Keep Arguments, Options, Fields, Members, and Returns below H2 level so they do not crowd the right TOC.

## Reference content and presentation

Preserve source and registration order rather than alphabetizing.

Preserve every public option, alias, default, constraint, overload, return, field, enum member, description, and exact public-type link.

Use compact OpenAI-style rows:

- Plain monospace names without filled gray code-pill backgrounds.
- Inline type, requirement, default, alias, and constraint metadata.
- Descriptions below metadata only when needed.
- Thin dividers between rows.
- Natural wrapping without clipped code or horizontal document overflow.

Keep index cards short, fully clickable, and limited to routing. Do not put exhaustive command or type lists inside cards. Mintlify equalizes card heights within a row; long card lists leave large blank areas in neighboring cards.

Do not publish HTTP, cURL, base-URL, transport-header, or private wire-protocol examples in generated CLI or SDK references.

Do not use `Panel` or `CodeGroup` around grouped sections. Those components replace or suppress Mintlify's native right TOC.

Prefer native Mintlify components. Do not add custom JavaScript. Add CSS only when a verified native component cannot satisfy the layout.

## Generator invariants

`render_reference()` is the single manifest for pages, navigation, and redirects.

The generator must:

- Derive public surfaces from Click registration, SDK exports, signatures, models, and enums.
- Produce deterministic output with no runtime values.
- Work with an empty `HOME` and blocked networking.
- Keep `--check` non-mutating.
- Refuse to overwrite unmarked MDX.
- Remove only obsolete files bearing the generated marker, then prune empty directories.
- Emit navigation and redirects from the same manifest as the pages.
- Write deterministic UTF-8 files with LF newlines.

`check_reference()` must report missing, stale, and unexpected generated files without modifying the worktree.

Use JSX-safe generated text for literal names. Do not put Markdown backticks inside JSX strings. Escape literal braces so `{}` defaults do not become JSX. Confirm names contain no smart dashes, quotes, ellipses, or other typographic substitutions.

## Redirect contract

Generate every retired public route as a redirect to an exact stable anchor:

```text
/reference/cli/run/start -> /reference/cli/run#start
/reference/sdk/runs/start -> /reference/sdk/runs#start
/reference/sdk/models/runs/fetch-benchmarks-request -> /reference/sdk/models/runs#fetch-benchmarks-request
```

When regrouping or renaming:

1. Keep the old route as a generated redirect.
2. Generate the replacement anchor from stable public names.
3. Update handwritten links to the direct replacement anchor.
4. Validate redirects and anchors through Mintlify.
5. Browser-check representative old URLs and verify the matching element ID exists.

## Devin workflow

Use Devin for an independent browser audit when a change affects navigation, cards, grouped references, MDX components, CSS, typography, desktop/mobile behavior, or more than one public page.

Run Devin only in a disposable detached worktree. Disable GitHub tokens and SSH writes. Prohibit staging, commits, pushes, GitHub mutation, deployment, and credential access.

A useful pass has three phases:

1. Audit current rendered pages at desktop and mobile sizes.
2. Implement or recommend one bounded design change.
3. Report exact routes, measurements, console errors, validation, and residual risks.

Save prompts, reports, browser measurements, and recovery patches under the primary worktree's `.scratch/` directory. Temporary directories and disposable worktrees can disappear.

Verify a temporary worktree exists before every follow-up command. Before applying a patch, compare detached and primary worktrees by status, content hash, and file mode. Exclude `.scratch/` and `.pi-subagents/`.

Do not trust Devin's prose report alone. Inspect the diff, generated output, screenshots, and browser measurements. Use one fresh independent review after implementation. Do not repeat review unless the patch changes or a finding remains unresolved.

Discard a failed visual direction before starting the next iteration. Do not stack speculative fixes.

## Browser checks and Mintlify traps

Check at least 1440×900 and 390×844. Check light and dark mode when CSS or color changes.

Verify:

- HTTP 200 on representative index, grouped reference, guide, and retired-route pages.
- No document-level horizontal overflow.
- No console errors.
- Native right TOC entries match intended content H2 sections.
- Cards navigate from visible content after hydration.
- Retired URLs resolve to exact anchors.
- Public-type links land on exact type sections.
- Long code, names, and metadata wrap without clipping.

Mintlify's `On this page` control can itself appear as an H2. Count content headings by expected IDs or source headings rather than asserting on every `main h2`.

Rendered cards use a visible card surface plus an aria-hidden anchor with `display: contents`. Wait for hydration, click visible card text or surface, and verify the final pathname. Do not target the hidden anchor as the primary interaction.

Measure `document.documentElement.scrollWidth` and `clientWidth`. Ignore intentional screen-reader-only and sidebar truncation, but reject document-level overflow or clipped reference content.

For retired URLs, verify final pathname and hash, then resolve `document.getElementById(location.hash.slice(1))`. Do not rely only on `document.querySelector(':target')`; client-router timing can make it unreliable.

Restart `mint dev` after deleting or consolidating generated routes. Hot reload can retain stale routes and return HTTP 500 even when generated files are correct.

## Validation

Run the smallest applicable checks during development. Run the complete documentation set before committing:

```bash
make docs-reference
make docs-reference-check
uv run pytest tests/unit/docs -q
make typecheck
make format-check
uv run ruff check .
(cd docs && npx --yes mint@4.2.801 validate)
(cd docs && npx --yes mint@4.2.801 broken-links --check-anchors --check-redirects)
git diff --check
```

## Test and suite routing

A documentation-only change does not justify unrelated product tests. Existing `tests/unit/docs` coverage is sufficient when only generated output changes.

When documentation accompanies product code, run the smallest owning suites:

| Changed surface | Required proof |
| --- | --- |
| Root CLI or tracker client | `make test` |
| Python SDK or tracker SDK contract | `uv run pytest tests/unit/sdk tests/contract -q` |
| SDK package, build, version, or release tooling | Mirror the executable checks in `.github/workflows/sdk-package.yml`; final-head CI must pass |
| Tracker API, worker, or database | `(cd services/tracker && make test)` |
| Infrastructure or CDK | `(cd infra && make lint && make test && make typecheck)` |
| AWS, benchmark service, sandbox, or another external boundary | Run local proof first; live tests or smokes require explicit approval |

A cross-layer change owns the union of its rows. Reuse existing behavioral coverage instead of adding duplicate tests.

For local preview, use Node.js 20.17 or newer:

```bash
cd docs
npx mint dev
```

Basic preview needs no Mintlify credentials. Login is optional for search and assistant features.

`.github/workflows/cli-unit-tests.yaml` pins Mintlify validation and broken-link checks. Any workflow edit is a CI/CD change and requires explicit human attention.

## Before delivery

- Read the full diff and remove duplicate prose, debug artifacts, and stale generated files.
- Keep `.scratch/` and `.pi-subagents/` unstaged.
- Report changed routes, redirect counts, commands run, validation output, and residual risks.
- For a pull request, include the local preview command and truthful test/checklist state.
