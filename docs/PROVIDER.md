# Provider integration

Multiple sandbox providers will be available for usage, in order to setup the keys and use them with Valkyrie you need to ensure a few things are setup. You will need an AWS account that you can store the secrets in.

## Daytona

Create the daytona key with the correct permissions

1. [Sign up](https://app.daytona.io/)
2. Navigate to the [keys section](https://app.daytona.io/dashboard/keys)
3. Create the api key ensuring that it has full access to `Sandboxes` and `Snapshots` (Read,  write, delete)

Upload that key to AWS secrets manager using the following format in plain text

```json
{
"DAYTONA_API_KEY": "...",
"DAYTONA_API_URL":"https://app.daytona.io/api",
"DAYTONA_TARGET":"..."
}
```

When using `valkyrie config init` or `valkyrie config modify` add to the key `DAYTONA_SECRET_NAME` with the name of the secret (e.x, DaytonaSecrets)
