# Hosted vs Self-Hosted Mode

Valkyrie's deployment mode and AWS execution mode are separate choices:

- **Hosted or self-hosted** determines who operates Tracker and ExecutorHost.
- **Managed or access-key AWS** determines which AWS account and credentials a run uses.

Hosted mode supports both AWS execution modes. Eligible organizations default to managed AWS, but can configure access keys to run against their own AWS resources instead.

## Hosted mode

Use Vals-hosted compute infrastructure. Data is isolated per organization through Vals AI API key authentication.

### Prerequisites

- Vals AI API key (provided by Vals)
- Local AWS SDK credentials when uploading or downloading agents directly. `AWS_PROFILE` with AWS SSO is supported.
- API key for sandbox provider (Daytona). [Setup docs](PROVIDER.md)

### Setup

```bash
valkyrie config init
```

Choose **hosted** when prompted. Valkyrie asks for your Vals AI API key, validates it, and creates your organization. When the organization can use managed AWS, Valkyrie reads the deployment Region and S3 bucket from Tracker instead of asking for access keys.

```
$ valkyrie config init
Setup mode (hosted, self-hosted) [self-hosted]: hosted
API Key: <your-vals-ai-api-key>
Organization 'your-org' configured successfully.

Managed AWS execution is enabled. Local AWS operations will use the AWS SDK credential chain.
```

Your Vals AI API key is sent with every request to authenticate and scope data to your organization.

### AWS execution mode

The AWS credential fields in the selected Valkyrie config determine the execution mode for new runs:

| Configured fields | Run execution mode |
|---|---|
| No `AWS_ACCESS_KEY_ID` or `AWS_SECRET_ACCESS_KEY` | Managed AWS. Tracker and ExecutorHost use deployment task roles. |
| Both `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` | Access-key AWS. Tracker sends the configured AWS resources and credentials to the run. |
| Only one access-key field | Invalid. Valkyrie stops before sending the request. |

Managed execution does not prevent local AWS operations. Agent uploads and artifact downloads use the local AWS SDK credential chain, including `AWS_PROFILE` and AWS SSO.

### Use access-key AWS in hosted mode

Run hosted setup first, then add a complete access-key configuration. Replace each placeholder with the AWS resources and credentials the run should use:

```bash
valkyrie config set AWS_ACCESS_KEY_ID <access-key-id>
valkyrie config set AWS_SECRET_ACCESS_KEY <secret-access-key>
valkyrie config set AWS_DEFAULT_REGION <region>
valkyrie config set S3_BUCKET <bucket-name>
valkyrie config set LOG_GROUP <log-group-prefix>
valkyrie config set LOG_RETENTION_POLICY 365
```

For temporary AWS credentials, also set the session token:

```bash
valkyrie config set AWS_SESSION_TOKEN <session-token>
```

Complete both access-key fields before starting a run or using another command that needs AWS configuration. To return to managed execution, remove the credential fields:

```bash
valkyrie config remove AWS_ACCESS_KEY_ID
valkyrie config remove AWS_SECRET_ACCESS_KEY
valkyrie config remove AWS_SESSION_TOKEN
```

Removing a field that is not present is safe. The Region and bucket remain in the config for local AWS operations.

### Retry or resume an existing run

Tracker stores the AWS account, Region, S3 bucket, and CloudWatch log group used by each new run. Retry and resume can use managed AWS or access keys when the selected authority reaches those same resources. Tracker rejects a mismatch before changing the run or queueing work.

Runs created before resource binding was deployed do not have stored resource identity. These historical runs remain access-key-only. Keep a separate access-key config until they no longer need retry or resume.

#### Keep an access-key config for historical runs

These steps are CLI-only because Valkyrie selects a local config file before contacting Tracker. There is no web-console setting for a local fallback file.

1. Copy the current complete access-key config before running managed setup.

   **Why** -- Managed setup removes static credentials from the normal environment config. The copy preserves the credentials and resource settings required by historical runs.

   For dev:

   ```bash
   install -m 600 ~/.config/valkyrie/dev.yaml ~/.config/valkyrie/dev-access-key.yaml
   ```

   For prod:

   ```bash
   install -m 600 ~/.config/valkyrie/valkyrie.yaml ~/.config/valkyrie/prod-access-key.yaml
   ```

   **Done when** -- The fallback contains the Vals API key, both AWS access-key fields, Region, S3 bucket, log group, and log retention policy used by the historical runs. Include `AWS_SESSION_TOKEN` when required.

2. Initialize the normal environment config for managed AWS.

   **Why** -- New runs should use managed AWS by default without sending static credentials to Tracker.

   ```bash
   VALKYRIE_ENV=dev valkyrie config init  # dev
   VALKYRIE_ENV=prod valkyrie config init # prod
   ```

   Do not set `VALKYRIE_CONFIG_PATH` for this step.

   **Done when** -- Setup reports that managed AWS is enabled and the normal config contains no `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, or `AWS_SESSION_TOKEN`.

3. Select the fallback only when retrying or resuming a historical run.

   **Why** -- `VALKYRIE_ENV` selects Tracker. `VALKYRIE_CONFIG_PATH` supplies the credentials and AWS resources used by the historical run.

   For dev:

   ```bash
   VALKYRIE_ENV=dev \
   VALKYRIE_CONFIG_PATH=~/.config/valkyrie/dev-access-key.yaml \
   valkyrie run resume <run-id>
   ```

   For prod:

   ```bash
   VALKYRIE_ENV=prod \
   VALKYRIE_CONFIG_PATH=~/.config/valkyrie/prod-access-key.yaml \
   valkyrie run resume <run-id>
   ```

   Use `valkyrie run retry <run-id>` with the same environment variables when retrying.

   **Done when** -- Recovery starts for the intended historical run. Commands without `VALKYRIE_CONFIG_PATH` continue to use the normal managed config.

4. Remove the fallback after every historical run has finished and no longer needs recovery.

   **Why** -- The fallback contains long-lived static credentials and should exist only while it has an operational use.

   ```bash
   rm ~/.config/valkyrie/dev-access-key.yaml  # dev
   rm ~/.config/valkyrie/prod-access-key.yaml # prod
   ```

   If the key is no longer used elsewhere, disable or delete it through your organization's AWS process. For an IAM user, the console path is **IAM > Users > the user > Security credentials > Access keys**.

   **Done when** -- The fallback is absent and any otherwise-unused access key is disabled or deleted.

You can also set the API key manually:

```bash
valkyrie config set api_key <your-vals-ai-api-key>
```

## Self-hosted mode

Run your own infrastructure end-to-end, including your own tracker service deployment.

### Prerequisites

- Your own tracker service deployment (see [Infrastructure docs](../infra/README.md))
- AWS account with the [required permissions](#required-aws-permissions)
- S3 bucket for storing benchmark artifacts and agents
- API key for sandbox provider (Daytona). [Setup docs](PROVIDER.md)

### Setup

Set `TRACKER_SERVICE_URL` to point at your tracker instance:

```bash
export TRACKER_SERVICE_URL=https://your-tracker.example.com
```

Then run `config init` and choose **self-hosted**:

```
$ valkyrie config init
Setup mode (hosted, self-hosted) [self-hosted]: self-hosted
AWS_ACCESS_KEY_ID: ...
AWS_SECRET_ACCESS_KEY: ...
...
```

No API key or Descope authentication is used. All data belongs to a single default organization.

## Local AWS permissions

In managed execution, local AWS credentials are used only by local operations such as uploading an agent. Access-key execution also sends the configured credentials and resources to Tracker for the run. The credentials require:

| Service | Permissions | Used for |
|---------|------------|----------|
| **S3** | `s3:PutObject`, `s3:GetObject`, `s3:DeleteObject`, `s3:ListBucket` | Storing benchmark results, agent artifacts, and run outputs |
| **S3** | `s3:GetObject` (for presigned URLs) | Generating download links for results |
| **CloudWatch Logs** | `logs:CreateLogGroup`, `logs:CreateLogStream`, `logs:PutLogEvents` | Access-key runs only |
| **Secrets Manager** | `secretsmanager:GetSecretValue` | Access-key runs only |
| **Lambda** (optional) | `lambda:InvokeFunction` | Access-key runs using `--lambda` |

Scope local permissions to the configured S3 bucket. For access-key execution, also scope permissions to the configured CloudWatch log group, Secrets Manager secrets, and optional Lambda functions.
