# Documentation rules

These rules are binding for every page in this directory. The updating-docs skill
(`.agents/updating-docs/SKILL.md`) has the procedures: checking facts against the code, the
generated reference, redirects, and the checks to run. For style these rules don't settle,
follow the [Google developer documentation style guide](https://developers.google.com/style).

The site is public, and so is every file the public repository sync copies. Write for
people outside Vals AI.

## Readers and page types

Each navigation group has one reader. Each page is one [Diátaxis](https://diataxis.fr) type
and doesn't mix in another: a tutorial, a how-to guide, reference, or an explanation.

| Group | Reader | Page types |
| --- | --- | --- |
| Get started | Someone new to Valkyrie | The introduction, the quickstart tutorial, and setup guides |
| Agents | People packaging an agent | How-to guides, and the contract reference |
| Runs | People running benchmarks from the CLI | How-to guides, and one explanation of time limits |
| Benchmarks | People building or connecting a benchmark service | How-to guides |
| Integrations | People processing finished runs in AWS Lambda | How-to guides |
| Python SDK | Developers driving Valkyrie from Python | How-to guides |
| CLI reference, Python SDK reference | Anyone looking up an option or a field | Generated reference |
| Self-hosting | People running their own Tracker | How-to guides, and settings reference |

- **Tutorial:** one path from nothing to a working result, with no choices. The quickstart
  is the only tutorial. Alternatives go in a guide.
- **How-to guide:** open with what the reader can do. Order sections as tasks, then this
  page's own reference tables, then a closing Limits list when something isn't supported.
  Say how the reader knows a task worked when it isn't obvious.
- **Reference:** complete and terse, ordered for lookup. The CLI and SDK reference is
  generated; never edit it by hand.
- **Explanation:** behavior and trade-offs that change a decision. Link to the guides that
  act on it.

## What stays out

Ask of every sentence: would this reader do or decide anything differently after reading
it? If not, cut it.

- **How it works inside:** locks, leases, transactions, queues, admission turns, payload
  encryption, retry mechanics, token minting, and class, function, module, table, column,
  and internal service names. Say what the reader sees. When a behavior changes a decision,
  say so in one sentence.
- **Vals AI operations:** deployment stages, Descope project setup, tenant roles, bucket
  tagging, release and rollout procedures, incident handling, internal hostnames, private
  repositories, credentials, and configuration only Vals AI can use. These go in
  `internal/`, which the public sync never copies. Document the path an outside reader can
  follow instead.
- **Another reader's details:** no HTTP routes or headers on CLI pages, and no Tracker
  environment variables outside Self-hosting. Link to the page for that reader.
- **Why it was built this way:** design history, alternatives, tests, CI, evidence, and
  hedges such as "this does not prove". These belong in the pull request.
- **Contributor chores:** setup, tests, migrations, regenerating files, and docs tooling go
  in `DEVELOPMENT.md`, the service READMEs and `docs/README.md`, outside site navigation.
  Those files are commands plus a few short bullets.
- **Plans and unshipped work:** describe shipped behavior only, without "now", "currently",
  or "no longer". Never document a feature that no deployment turns on.
- **Edge cases nobody hits,** and limits a reader can neither reach nor change.
- **Design documents.** Don't add them.

## Format

- The product is Valkyrie and the company is Vals AI. The Tracker is the server the CLI and
  SDK talk to; say "Valkyrie" when the reader doesn't need to know which component acts.
- Every page has a title, a one-sentence description, and a sidebarTitle when the title is
  longer than about four words.
- Use headings and numbered lists rather than Steps, Tabs, or Accordion. Keep callouts rare:
  a Warning for what can't be undone, a Note for a requirement that is easy to miss.
- Hard-wrap prose at about 90 columns. Keep code lines within 88 characters: break long
  commands with `\` and format Python as a formatter would. Code blocks scroll rather than
  wrap; add Mintlify's `wrap` option only to prose-like output. Give every code fence a
  language. Put runnable
  commands in code blocks, not inline in prose. In a block, put each comment on its own line
  above its command, and leave a blank line between commands.
- Show real commands and values. Use placeholders only for the reader's own values, written
  in uppercase like `RUN_ID` and explained on first use: after a code block with
  "Replace *`RUN_ID`* with ...", and inside a sentence with "where *`RUN_ID`* is ...".
- Put the goal or condition before the instruction: "To watch new log lines, add
  `--follow`", not "Add `--follow` to watch new log lines".
- Use tables for settings, fields, and limits. Put UI labels and keys in bold, and paths,
  variables, and values in code.
- Write no em dashes. Use a colon or a second sentence.
- Use the serial comma, and write "sign in" rather than "log in", as Google style does.

## Changes

- Update the affected pages in the same change as the behavior. Keep each fact on the page
  that owns it and link to it from elsewhere.
- Add new pages to `docs.json`. When you rename or remove a page, update every link to it and
  add a redirect to `STATIC_REDIRECTS` in `scripts/reference_docs/model.py`.
- Use root-relative internal links without extensions, such as `/runs/start#label-a-run`.
- Before handing off, run the checks in [docs/README.md](README.md).
