# Hosted vs Self-Hosted Mode

Valkyrie supports two operational modes. Hosted mode adds Vals AI API key authentication for multi-tenant data isolation.

## Hosted mode

Use Vals-hosted compute infrastructure with the hosted tracker storage. Data is isolated per organization via Vals AI API key authentication.

### Prerequisites

- Vals AI API key (provided by Vals)
- AWS credentials for client-side agent upload commands
- S3 bucket name used by the hosted tracker
- API key for sandbox provider (Daytona). [Setup docs](PROVIDER.md)

### Setup

```bash
valkyrie config init
```

Choose **hosted** when prompted. You'll be asked for:
1. Your Vals AI API key — validates against the tracker and creates your organization
2. S3 bucket and region used by the hosted tracker

```
$ valkyrie config init
Setup mode (hosted, self-hosted) [self-hosted]: hosted
API Key: <your-vals-ai-api-key>
Organization 'your-org' configured successfully.

AWS_ACCESS_KEY_ID: ...
AWS_SECRET_ACCESS_KEY: ...
...
```

Your API key is sent with every request to authenticate and scope data to your organization.
The hosted tracker uses its own IAM task role for S3 and CloudWatch uploads.
The legacy harness credentials remain available for user-owned Secrets Manager and Lambda integrations.

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

## Hosted tracker AWS permissions

The hosted tracker task role has the following S3 and CloudWatch permissions:

| Service | Permissions | Used for |
|---------|------------|----------|
| **S3** | `s3:GetObject`, `s3:PutObject`, multipart upload actions, and scoped list access | Storing benchmark results, agent artifacts, and run outputs |
| **S3** | `s3:GetObject` (for presigned URLs) | Generating download links for results |
| **CloudWatch Logs** | `logs:CreateLogGroup`, `logs:CreateLogStream`, `logs:PutLogEvents` | Streaming task execution logs |

These permissions are scoped to the configured S3 bucket and benchmark CloudWatch log group.
Secrets Manager and Lambda integrations continue to use the credentials in the harness configuration.
Local tracker development still uses the ambient AWS environment from Docker Compose.

Self-hosted deployments use the credentials supplied to their own tracker.
