# WebhookNotifs

Slack webhook setup + the `-i` (interval) flag.

## What `-i` does

Per `valk run start --help`:

```
-i, --interval INTEGER       Progress percentage threshold for Slack
                             notification (e.g., -i 25 -i 75). Max 3,
                             must be divisible by 5, range 5-100.
```

Pass `-i N` and the tracker will fire a Slack message when the run hits N% finished. You get up to 3 thresholds. Standard set:

```bash
-i 25 -i 50 -i 75
```

This gives you "started", quarter, halfway, three-quarter pings — enough to see "is this run on track" without spamming.

## Constraints (the validator will reject otherwise)

- **Max 3** `-i` flags total (e.g. `-i 25 -i 50 -i 75 -i 90` will fail validation).
- **Must be divisible by 5** (`-i 27` fails).
- **Range 5–100** (`-i 0` and `-i 105` fail).

## When NOT to pass `-i`

**Subset / smoke tests (`--slice :10`):** *omit `-i`*. The user explicitly does not want Slack notifs from 10-task smoke tests. Save them for full runs.

**Retry passes:** `-i` is *not* honored on retries. If you launched with `-i 25 -i 50 -i 75`, the retry pass will not re-fire those notifications. Don't bother re-passing `-i` on retry — it'll just be ignored.

## Where the webhook URL lives

The webhook is configured at the tracker level (organization-wide, not per-run). Stored in AWS Secrets Manager as `localEvalInfra<...>` — common names:

- `localEvalInfraValkyrieWebhook`
- `ValkyrieWebhook` (alias)

The tracker reads it via env var (resolved at boot time) or via the `--webhook-secret-name` startup flag. To change the destination Slack channel, update the secret value (the webhook URL itself encodes the channel).

## Adding/changing the webhook for the tracker

1. Create the Slack App incoming webhook in Slack. Copy the URL (starts with `https://hooks.slack.com/services/...`).
2. Update the AWS secret:
   ```bash
   aws secretsmanager update-secret --region us-east-1 \
     --secret-id ValkyrieWebhook \
     --secret-string '<webhook_url>'
   ```
3. Restart the tracker pods (or wait for the next deploy — secrets are typically read at boot).

## Testing the webhook

The simplest test is to fire a real `--slice :10` run with `-i 25 -i 50 -i 75`:

```bash
valk run start --agent harvey-labs --benchmark harvey-legal-agent \
  --model openai/gpt-5.4-mini-2026-03-17 --slice :10 \
  -i 25 -i 50 -i 75
```

You'll see Slack pings as it crosses each threshold. **(But remember — for actual smoke tests of new models, omit `-i`. This is just a webhook plumbing test.)**

## What the Slack message contains

Approximately:

```
[Valkyrie] harvey-legal-agent / claude-sonnet-4-6 — 50% complete
Run: 71527464-...
Pending: 600 │ In Progress: 25 │ Finished: 626 │ Errors: 0
```

Plus a link back to the run-status page in the Vals webapp.

## Failures we've seen

- **Webhook URL revoked / 404 in Slack:** the tracker will log a warning but the run continues. Re-issue the webhook in Slack, update the secret.
- **Tracker can't reach Slack (egress firewall):** rare, but check tracker pod egress if pings stop firing. The tracker silently swallows the error.
- **Threshold > 100 or other validator failure:** caught at `valk run start` time; you get a clear error and the run isn't created. No silent failures.
