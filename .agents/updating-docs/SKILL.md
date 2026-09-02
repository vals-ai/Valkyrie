---
name: updating-docs
description: Use when adding or changing Valkyrie public documentation, Mintlify guides, generated CLI or Python SDK references, docs navigation, redirects, styling, or code that makes docs stale. Enforces the public/internal boundary, task-guide and reference separation, grouped reference architecture, deterministic generation, and browser validation.
---

# Updating Valkyrie documentation

Read this skill before editing `docs/`, the reference generator, documentation tests, or public behavior that changes documented commands, defaults, routes, SDK methods, or types.

## Canonical locations

`docs/` is the only public end-user documentation source, published at `https://docs.valkyrie.vals.ai`. Repository files link readers to that site rather than restating or indexing its pages.

| Subject | Canonical location |
| --- | --- |
| Public end-user documentation | `docs/` |
| Public CLI and Python SDK reference generator | `scripts/generate_reference.py` entry point over the `scripts/reference_docs/` package: `collect` reads the public surface, `model` holds the manifest and `STATIC_REDIRECTS` for handwritten routes, `render` writes MDX, `generate` orchestrates |
| Generated reference tests | `tests/unit/docs/test_generate_reference.py` |
| Contributor setup, tracker operation, database and migrations | `docs/contributing/`. `DEVELOPMENT.md` and the service READMEs are pointers to those pages, plus the versioning table that CI links to |
| Generic self-hosting | `docs/self-hosting/` |
| Vals-specific infrastructure operation | `infra/README.md` |
| SDK release procedure | `scripts/sdk/RELEASING.md` |

Contributor setup and service operation belong in the `Contributing` group. Never publish Vals-specific operations, release activation, protected-environment instructions, incident procedures, break-glass access, or internal automation instructions in Mintlify.

Never reference a private repository, issue, URL, hostname, credential, or internal item from public documentation. This includes configuration an external reader cannot use: document the self-contained path instead, such as an inline allowlist rather than a Vals-internal catalog service.

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
- Trace every behavioral claim to the source that implements it, and reread that source when the claim changes. Treat a README as a claim to verify, not a source.
- Write no em dashes, in handwritten pages and in generator-rendered strings alike. Use a colon or a second sentence.
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

Always change the generator and its tests first. Never hand-edit generated files under `docs/reference/`. A redirect for a handwritten route belongs in `STATIC_REDIRECTS`, not in `docs/docs.json`.

Group the current public surface by top-level CLI command, SDK resource, and SDK type family. Derive groups, pages, anchors, and redirects from source through the generator; do not copy current counts or route inventories into instructions.

Do not create one page per command, method, model, or enum.

Render each command, method, model, or enum once as a stable explicit H2 section whose anchor comes from the public name.

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

Use one rendered manifest for pages, navigation, and redirects.

The generator must:

- Derive public surfaces from Click registration, SDK exports, signatures, models, and enums.
- Produce deterministic output with no runtime values.
- Work with an empty `HOME` and blocked networking.
- Keep `--check` non-mutating.
- Refuse to overwrite unmarked MDX.
- Remove only obsolete files bearing the generated marker, then prune empty directories.
- Emit navigation and redirects from the same manifest as the pages.
- Write deterministic UTF-8 files with LF newlines.

The check path must report missing, stale, and unexpected generated files without modifying the worktree.

Use JSX-safe generated text for literal names. Do not put Markdown backticks inside JSX strings. Escape literal braces so `{}` defaults do not become JSX. Confirm names contain no smart dashes, quotes, ellipses, or other typographic substitutions.

## Redirect contract

Generate every retired public route as a redirect to its exact stable replacement anchor.

When regrouping or renaming:

1. Keep the old route as a generated redirect.
2. Generate the replacement anchor from stable public names.
3. Update handwritten links to the direct replacement anchor.
4. Validate redirects and anchors through Mintlify.
5. Browser-check representative old URLs and verify the matching element ID exists.

## Visual review

Use an independent browser audit when a change affects navigation, cards, grouped references, MDX components, CSS, typography, responsive behavior, or several public pages.

Inspect the rendered pages, diff, screenshots, browser measurements, console output, and validation results. Do not accept a prose report that conflicts with direct evidence.

Test one bounded design direction at a time. Discard a failed direction before starting the next iteration rather than stacking speculative fixes.

## Browser checks and Mintlify traps

Check representative desktop and mobile viewports. Check light and dark mode when CSS or color changes.

Verify:

- Successful responses on representative index, grouped reference, guide, and retired-route pages.
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

Restart the preview server after deleting or consolidating generated routes. Hot reload can retain stale routes and return server errors even when generated files are correct.

## Validation

Read the current `Makefile`, `docs/contributing/local-development.mdx`, and documentation CI workflow before choosing commands. Do not copy tool versions or test counts into this skill.

Before committing, run the current checks for:

- Reference generation and freshness.
- Documentation unit tests.
- Type checking, formatting, and linting.
- Mintlify build validation.
- Links, anchors, and redirects.
- Whitespace errors.

A documentation-only change does not justify unrelated product tests. When documentation accompanies product code, run the smallest suites that own the changed behavior. Cross-layer changes need the union of their owning suites; live tests require explicit approval.

Use the local preview command and runtime requirement currently documented in `docs/contributing/local-development.mdx`. Basic preview needs no Mintlify credentials. Login is optional for account-backed features.

Any CI workflow edit requires explicit human attention.

## Before delivery

- Read the full diff and remove duplicate prose, debug artifacts, and stale generated files.
- After merging the base branch, reread the pages documenting whatever behavior it changed. Generated references go stale silently until `--check` runs.
- Report changed routes, commands run, validation output, and residual risks.
- For a pull request, include the local preview command and truthful test/checklist state.
