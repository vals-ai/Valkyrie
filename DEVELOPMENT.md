# Development

The local development guide is published at [docs.valkyrie.vals.ai/contributing/local-development](https://docs.valkyrie.vals.ai/contributing/local-development). Its source is [`docs/contributing/local-development.mdx`](docs/contributing/local-development.mdx).

- [Tracker service](https://docs.valkyrie.vals.ai/contributing/tracker-service)
- [Database and migrations](https://docs.valkyrie.vals.ai/contributing/database)
- [Infrastructure operations](infra/README.md)
- [Releasing the Valkyrie SDK](scripts/sdk/RELEASING.md)

## Versioning

Semantic versioning is used for the prod branch. Valkyrie uses [github-tag-action](https://github.com/anothrNick/github-tag-action) to handle release versions, so a pull request title for a deploy must contain one of these tags.

| Tag | Effect | Example |
| --- | --- | --- |
| `#patch` | Patch bump | v0.4.0 -> v0.4.1 |
| `#minor` | Minor bump | v0.4.1 -> v0.5.0 |
| `#major` | Major bump | v0.5.0 -> v1.0.0 |
