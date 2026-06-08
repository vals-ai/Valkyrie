# Provider integration

Multiple sandbox providers will be available for usage, in order to setup the keys and use them with Valkyrie you need to ensure a few things are setup. You will need an AWS account that you can store the secrets in.

## Daytona

Create the daytona key with the correct permissions

1. [Sign up](https://app.daytona.io/)
2. Navigate to the [keys section](https://app.daytona.io/dashboard/keys)
3. Create the api key ensuring that it has full access to `Sandboxes` and `Snapshots` (Read,  write, delete)
4. Find what target you would like to use, options can be found under [shared regions](https://www.daytona.io/docs/regions#shared-regions)

Upload that key to AWS Secrets Manager using the provider-neutral format:

```json
{
  "type": "daytona",
  "api_key": "...",
  "api_url": "https://app.daytona.io/api",
  "target": "..."
}
```

Legacy Daytona secrets with `DAYTONA_API_KEY`, `DAYTONA_API_URL`, and `DAYTONA_TARGET` are still accepted.

When using `valkyrie config init` or `valkyrie config set`, add `SANDBOX_PROVIDER_SECRET_NAME` with the name of the secret (e.g. `DaytonaSecrets`).
