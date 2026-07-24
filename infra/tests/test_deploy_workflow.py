"""Tests for the deployment workflow's account boundary."""

import unittest
from pathlib import Path

WORKFLOW = Path(__file__).parents[2] / ".github" / "workflows" / "deploy.yaml"
EXECUTOR_BUILD_WORKFLOW = Path(__file__).parents[2] / ".github" / "workflows" / "executor-build.yaml"


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

    def test_executor_release_jobs_require_successful_all_stack_deployments(self) -> None:
        workflow = WORKFLOW.read_text(encoding="utf-8")
        dev_release = workflow.split("  release-development:", maxsplit=1)[1].split(
            "  release-production:", maxsplit=1
        )[0]
        prod_release = workflow.split("  release-production:", maxsplit=1)[1]

        self.assertIn("needs: [build-executor-release, run-dev-operation]", dev_release)
        self.assertIn("inputs.operation == 'deploy' && inputs.scope == 'all'", dev_release)
        self.assertIn("environment: dev", dev_release)
        self.assertNotIn("always()", dev_release)
        self.assertIn("needs: [build-executor-release, deploy-production]", prod_release)
        self.assertIn("environment: production-release", prod_release)
        self.assertNotIn("always()", prod_release)
        self.assertLess(
            prod_release.index("Require configured production approval"),
            prod_release.index("Configure executor release credentials"),
        )
        self.assertLess(
            prod_release.index("PRODUCTION_RELEASE_APPROVAL_CONFIGURED"),
            prod_release.index("role-to-assume: arn:aws:iam::613431292675:role/ValkyrieExecutorRelease"),
        )
        self.assertEqual(workflow.count("uv run python build_executor_artifact.py"), 1)
        self.assertEqual(workflow.count("uv run python executor_release.py"), 2)

    def test_executor_build_check_covers_real_arm_images_and_pex(self) -> None:
        workflow = EXECUTOR_BUILD_WORKFLOW.read_text(encoding="utf-8")

        self.assertIn("runs-on: ubuntu-24.04-arm", workflow)
        self.assertEqual(workflow.count('"tests/unit/executor_host/**"'), 2)
        self.assertIn('"services/executor_host/**"', workflow)
        self.assertIn('"services/tracker/**"', workflow)
        self.assertIn('".dockerignore"', workflow)
        self.assertIn("uv run python build_executor_artifact.py", workflow)
        self.assertIn("uv run pytest tests/unit/executor_host", workflow)
        self.assertIn("-f services/tracker/Dockerfile", workflow)
        self.assertIn("-f services/executor_host/Dockerfile", workflow)
        self.assertEqual(workflow.count("build_executor_artifact.py"), 1)

    def test_deployment_tools_are_pinned_and_installed_from_the_lockfile(self) -> None:
        workflow = WORKFLOW.read_text(encoding="utf-8")
        production_job = workflow.split("  deploy-production:", maxsplit=1)[1].split(
            "  run-dev-operation:", maxsplit=1
        )[0]
        dev_job = workflow.split("  run-dev-operation:", maxsplit=1)[1].split("  release-development:", maxsplit=1)[0]

        self.assertIn("npm install -g aws-cdk@2.1132.0", production_job)
        self.assertIn("uv sync --frozen --python 3.12", production_job)
        self.assertIn("npm install -g aws-cdk@2.1132.0", dev_job)
        self.assertIn("uv sync --frozen --python 3.12", dev_job)


if __name__ == "__main__":
    unittest.main()
