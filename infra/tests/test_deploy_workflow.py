"""Tests for the deployment workflow's account boundary."""

import unittest
from pathlib import Path

WORKFLOW = Path(__file__).parents[2] / ".github" / "workflows" / "deploy.yaml"


class DeployWorkflowTest(unittest.TestCase):
    def test_dev_is_manual_protected_and_validated_before_tooling(self) -> None:
        workflow = WORKFLOW.read_text(encoding="utf-8")
        dev_job = workflow.split("  run-dev-operation:", maxsplit=1)[1]

        self.assertIn("branches: [prod]", workflow)
        self.assertIn("uv run cdk deploy --all -c stage=prod --require-approval never", workflow)
        self.assertIn("github.ref == 'refs/heads/dev'", dev_job)
        self.assertIn("environment: dev", dev_job)
        self.assertIn("allowed-account-ids: ${{ env.DEV_ACCOUNT_ID }}", dev_job)
        self.assertIn("AWS_REGION must be us-east-1", dev_job)
        self.assertLess(dev_job.index("Validate dev AWS identity"), dev_job.index("Checkout code"))
        self.assertIn("if: inputs.operation != 'credentials-only'", dev_job)
        self.assertIn("if: inputs.operation == 'plan'", dev_job)
        self.assertIn(
            'run: make plan STAGE=dev SCOPE=${{ inputs.scope }} DEV_ACCOUNT_ID="$DEV_ACCOUNT_ID" AWS_REGION="$AWS_REGION"',
            dev_job,
        )
        self.assertIn("if: inputs.operation == 'deploy'", dev_job)
        self.assertIn(
            'run: make deploy STAGE=dev SCOPE=${{ inputs.scope }} DEV_ACCOUNT_ID="$DEV_ACCOUNT_ID" AWS_REGION="$AWS_REGION"',
            dev_job,
        )


if __name__ == "__main__":
    unittest.main()
