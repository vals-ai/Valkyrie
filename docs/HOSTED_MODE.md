# Hosted vs Self-Hosted Mode

Valkyrie supports two operational modes. Both require your own AWS credentials — hosted mode adds Vals AI API key authentication for multi-tenant data isolation.

## Hosted mode

Use Vals-hosted compute infrastructure with your own AWS storage. Data is isolated per organization via Vals AI API key authentication.

### Prerequisites

- Vals AI API key (provided by Vals)
- AWS account with the [required permissions](#required-aws-permissions)
- S3 bucket for storing benchmark artifacts and agents. This will need to be unique for the region and created before defining it inside of the config.
- API key for sandbox provider (Daytona). [Setup docs](PROVIDER.md)

### Setup

```bash
valkyrie config init
```

Choose **hosted** when prompted. You'll be asked for:
1. Your Vals AI API key — validates against the tracker and creates your organization
2. AWS credentials — same as self-hosted (you supply your own S3, CloudWatch, Daytona)

```
$ valkyrie config init
Setup mode (hosted, self-hosted) [self-hosted]: hosted
API Key: <your-vals-ai-api-key>
Organization 'your-org' configured successfully.

AWS_ACCESS_KEY_ID: ...
AWS_SECRET_ACCESS_KEY: ...
...
```

Your API key is sent with every request to authenticate and scope data to your organization. AWS credentials are sent via `X-Harness-*` headers.

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

## Required AWS permissions

Your AWS credentials must have the following permissions:

| Service | Permissions | Used for |
|---------|------------|----------|
| **S3** | `s3:PutObject`, `s3:GetObject`, `s3:DeleteObject`, `s3:ListBucket` | Storing benchmark results, agent artifacts, and agent outputs |
| **S3** | `s3:GetObject` (for presigned URLs) | Generating download links for results |
| **CloudWatch Logs** | `logs:CreateLogGroup`, `logs:CreateLogStream`, `logs:PutLogEvents` | Streaming task execution logs |
| **Secrets Manager** | `secretsmanager:GetSecretValue` | Retrieving sandbox provider credentials (Daytona) and webhook URLs |
| **Lambda** (optional) | `lambda:InvokeFunction` | Post-benchmark Lambda invocation (only if using `--lambda` flag) |

These permissions should be scoped to the S3 bucket, CloudWatch log group, and Secrets Manager secrets you configure during `valkyrie config init`.
