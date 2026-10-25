# Work on the documentation

`docs/` is the Mintlify site published at [docs.valkyrie.vals.ai](https://docs.valkyrie.vals.ai).
Follow the [writing rules](AGENTS.md) and the
[updating-docs skill](../.agents/updating-docs/SKILL.md). This file and `AGENTS.md` are
listed in `.mintignore`, so they stay off the site.

## Preview the site

From `docs/`, with Node.js 20.17 or later:

```bash
# Preview at http://localhost:3000
npx --yes mint@4.2.801 dev
```

Preview needs no Mintlify login. Check changed pages in light and dark mode.

## Regenerate the reference

`docs/reference/` is generated from the CLI and SDK source. After changing a public command,
SDK method or model, run this from the repository root:

```bash
make docs-reference
```

Never edit generated pages by hand. Redirects for handwritten pages live in
`STATIC_REDIRECTS` in `scripts/reference_docs/model.py`, and `make docs-reference` writes
them to `docs/reference/redirects.json`.

## Check before you send a change

These are the checks CI runs. From the repository root:

```bash
make docs-reference-check

uv run pytest tests/unit/docs -q
```

From `docs/`:

```bash
# Pages and navigation
npx --yes mint@4.2.801 validate

# Internal links, anchors and redirects
npx --yes mint@4.2.801 broken-links --check-anchors --check-redirects
```
