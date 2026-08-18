# Hosted vs Self-Hosted Mode

Valkyrie supports hosted and self-hosted deployments. Hosted organizations with managed AWS enabled do not store AWS access keys in Valkyrie configuration. Tracker and ExecutorHost use deployment task roles for runs, while local S3 operations use the AWS SDK credential chain, including SSO-backed profiles.

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

Choose **hosted** when prompted. Valkyrie asks for your Vals AI API key, validates it, and creates your organization. When managed AWS is enabled, Valkyrie reads the deployment Region and S3 bucket from Tracker instead of asking for static AWS access keys.

```
$ valkyrie config init
Setup mode (hosted, self-hosted) [self-hosted]: hosted
API Key: <your-vals-ai-api-key>
Organization 'your-org' configured successfully.

Managed AWS execution is enabled. Local AWS operations will use the AWS SDK credential chain.
```

Your API key is sent with every request to authenticate and scope data to your organization. Managed starts do not send AWS credentials or a harness configuration. If both static access-key fields are present in an existing config, Valkyrie preserves the access-key execution path.

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

Local AWS credentials are used only by local operations such as uploading an agent. Managed benchmark execution uses deployment task roles instead. Local credentials require:

| Service | Permissions | Used for |
|---------|------------|----------|
| **S3** | `s3:PutObject`, `s3:GetObject`, `s3:DeleteObject`, `s3:ListBucket` | Storing benchmark results, agent artifacts, and run outputs |
| **S3** | `s3:GetObject` (for presigned URLs) | Generating download links for results |
| **CloudWatch Logs** | `logs:CreateLogGroup`, `logs:CreateLogStream`, `logs:PutLogEvents` | Legacy access-key runs only |
| **Secrets Manager** | `secretsmanager:GetSecretValue` | Legacy access-key runs only |
| **Lambda** (optional) | `lambda:InvokeFunction` | Legacy access-key runs using `--lambda` |

Scope local permissions to the configured S3 bucket. For legacy access-key execution, also scope permissions to the configured CloudWatch log group, Secrets Manager secrets, and optional Lambda functions.
