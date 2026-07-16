import re
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
WORKFLOW_PATH = REPO_ROOT / ".github" / "workflows" / "deploy.yaml"


def mapping_block(document: str, key: str, indent: int) -> str:
    lines = document.splitlines()
    marker = f"{' ' * indent}{key}:"
    start = next(index for index, line in enumerate(lines) if line == marker)
    end = len(lines)
    for index in range(start + 1, len(lines)):
        line = lines[index]
        if line and not line.startswith(" " * (indent + 1)):
            end = index
            break
    return "\n".join(lines[start:end])


WORKFLOW = WORKFLOW_PATH.read_text()
PRODUCTION_JOB = mapping_block(WORKFLOW, "deploy-production", 2)
DEV_JOB = mapping_block(WORKFLOW, "run-dev-operation", 2)
OPERATION_INPUT = mapping_block(WORKFLOW, "operation", 6)
SCOPE_INPUT = mapping_block(WORKFLOW, "scope", 6)


class DeployWorkflowTests(unittest.TestCase):
    def test_push_deploys_production_only(self) -> None:
        self.assertRegex(WORKFLOW, r"push:\n    branches: \[prod\]")
        self.assertNotRegex(WORKFLOW, r"push:\n    branches: \[[^\]]*dev")
        self.assertIn("github.ref == 'refs/heads/prod'", PRODUCTION_JOB)
        self.assertIn('make deploy STAGE=prod SCOPE=all AWS_REGION="$AWS_REGION"', PRODUCTION_JOB)
        self.assertIn('CDK_DEFAULT_ACCOUNT: "613431292675"', PRODUCTION_JOB)
        self.assertIn("SLACK_WORKSPACE_ID", PRODUCTION_JOB)

    def test_manual_dev_run_is_branch_and_environment_protected(self) -> None:
        self.assertIn("github.event_name == 'workflow_dispatch'", DEV_JOB)
        self.assertIn("github.ref == 'refs/heads/dev'", DEV_JOB)
        self.assertIn("environment: dev", DEV_JOB)
        self.assertIn("required: true", OPERATION_INPUT)
        self.assertIn("required: true", SCOPE_INPUT)

    def test_manual_operations_and_scopes_are_explicit(self) -> None:
        for operation in ("credentials-only", "plan", "deploy"):
            self.assertIn(f"- {operation}", OPERATION_INPUT)
        for scope in (
            "deployment-access",
            "dns-zone",
            "shared",
            "tracker",
            "worker",
            "monitoring",
            "all",
        ):
            self.assertIn(f"- {scope}", SCOPE_INPUT)

    def test_credentials_are_validated_before_setup_or_synth(self) -> None:
        configure_index = DEV_JOB.index("name: Configure dev AWS credentials")
        target_validation_index = DEV_JOB.index("name: Validate dev AWS target")
        setup_index = DEV_JOB.index("name: Setup Node.js")
        self.assertLess(configure_index, target_validation_index)
        self.assertLess(target_validation_index, setup_index)
        self.assertIn("aws sts get-caller-identity", DEV_JOB)
        self.assertIn("allowed-account-ids: ${{ vars.DEV_ACCOUNT_ID }}", DEV_JOB)
        self.assertIn("AWS_REGION: ${{ vars.AWS_REGION }}", DEV_JOB)

    def test_credentials_only_does_not_install_or_synth(self) -> None:
        guarded_steps = re.findall(
            r"- name: (?:Checkout code|Setup Node.js|Setup Python|Install uv|Install infra dependencies)\n"
            r"        if: inputs\.operation != 'credentials-only'",
            DEV_JOB,
        )
        self.assertEqual(len(guarded_steps), 5)
        self.assertNotIn("uv run cdk", DEV_JOB)

    def test_plan_is_diff_only_and_deploy_uses_the_selected_scope(self) -> None:
        self.assertIn("if: inputs.operation == 'plan'", DEV_JOB)
        self.assertIn("make plan STAGE=dev SCOPE=${{ inputs.scope }}", DEV_JOB)
        self.assertIn("if: inputs.operation == 'deploy'", DEV_JOB)
        self.assertIn("make deploy STAGE=dev SCOPE=${{ inputs.scope }}", DEV_JOB)
        self.assertIn(
            '[[ "${{ inputs.operation }}" == "deploy" && "${{ inputs.scope }}" == "deployment-access" ]]',
            DEV_JOB,
        )


if __name__ == "__main__":
    unittest.main()
