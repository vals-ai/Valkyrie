# Provider integration

Multiple sandbox providers will be available for usage, in order to setup the keys and use them with Valkyrie you need to ensure a few things are setup. You will need an AWS account that you can store the secrets in.

## Daytona

Create the daytona key with the correct permissions

1. [Sign up](https://app.daytona.io/)
2. Navigate to the [keys section](https://app.daytona.io/dashboard/keys)
3. Create the api key ensuring that it has full access to `Sandboxes` and `Snapshots` (Read,  write, delete)
4. Find what target you would like to use, options can be found under [shared regions](https://www.daytona.io/docs/regions#shared-regions)

Upload that key to AWS secrets manager using the following format in plain text

```json
{
"DAYTONA_API_KEY": "...",
"DAYTONA_API_URL":"https://app.daytona.io/api",
"DAYTONA_TARGET":"..."
}
```

Register the secret under a provider name:

```bash
valkyrie config provider set daytona DaytonaSecrets
```

You can configure multiple sandbox providers:

```bash
valkyrie config provider set daytona DaytonaSecrets
valkyrie config provider set modal ModalSecrets
```

The first configured provider is used by default. Select a provider for a single run with:

```bash
valkyrie run start --agent agents/claude_code --benchmark swebench --provider modal
```

Legacy flat config key `DAYTONA_SECRET_NAME` is still accepted when `sandbox_providers` is not configured.
