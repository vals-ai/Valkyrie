"""Tests for the deployment workflow's account boundary."""

import unittest
from pathlib import Path

WORKFLOW = Path(__file__).parents[2] / ".github" / "workflows" / "deploy.yaml"


class DeployWorkflowTest(unittest.TestCase):
    def test_dev_push_deploys_all_through_protected_validated_path(self) -> None:
        workflow = WORKFLOW.read_text(encoding="utf-8")
        dev_job = workflow.split("  run-dev-operation:", maxsplit=1)[1]

        self.assertIn("branches: [dev, prod]", workflow)
        self.assertIn("uv run cdk deploy --all -c stage=prod --require-approval never", workflow)
        self.assertIn(
            "github.ref == 'refs/heads/dev' && (github.event_name == 'push' || "
            "github.event_name == 'workflow_dispatch')",
            dev_job,
        )
        self.assertIn("environment: dev", dev_job)
        self.assertIn("OPERATION: ${{ github.event_name == 'push' && 'deploy' || inputs.operation }}", dev_job)
        self.assertIn("SCOPE: ${{ github.event_name == 'push' && 'all' || inputs.scope }}", dev_job)
        self.assertIn("DESCOPE_PROJECT_ID: ${{ vars.DESCOPE_PROJECT_ID }}", dev_job)
        self.assertIn('PRODUCTION_ACCOUNT_ID: "613431292675"', dev_job)
        self.assertIn("SENTRY_DSN_SECRET_NAME: ${{ vars.SENTRY_DSN_SECRET_NAME }}", dev_job)
        self.assertIn('"$DEV_ACCOUNT_ID" == "$PRODUCTION_ACCOUNT_ID"', dev_job)
        self.assertIn('"$OPERATION" != "credentials-only" && -z "$DESCOPE_PROJECT_ID"', dev_job)
        self.assertIn("DESCOPE_PROJECT_ID before planning or deploying", dev_job)
        self.assertIn("allowed-account-ids: ${{ env.DEV_ACCOUNT_ID }}", dev_job)
        self.assertIn("AWS_REGION must be us-east-1", dev_job)
        self.assertLess(dev_job.index("Validate dev AWS identity"), dev_job.index("Checkout code"))
        self.assertIn("if: env.OPERATION != 'credentials-only'", dev_job)
        self.assertIn("if: env.OPERATION == 'plan'", dev_job)
        self.assertIn(
            'run: make plan STAGE=dev SCOPE="$SCOPE" DEV_ACCOUNT_ID="$DEV_ACCOUNT_ID" AWS_REGION="$AWS_REGION"',
            dev_job,
        )
        self.assertIn("if: env.OPERATION == 'deploy'", dev_job)
        self.assertIn(
            'run: make deploy STAGE=dev SCOPE="$SCOPE" DEV_ACCOUNT_ID="$DEV_ACCOUNT_ID" AWS_REGION="$AWS_REGION"',
            dev_job,
        )
        self.assertNotIn("submodules: recursive", workflow)
        self.assertNotIn("secrets.GH_PAT", workflow)


if __name__ == "__main__":
    unittest.main()
