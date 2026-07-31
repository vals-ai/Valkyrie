"""Tests for deployment workflow contracts."""

import unittest
from pathlib import Path

ROOT = Path(__file__).parents[2]
WORKFLOW = ROOT / ".github" / "workflows" / "deploy.yaml"
EXECUTOR_BUILD_WORKFLOW = ROOT / ".github" / "workflows" / "executor-build.yaml"


class DeployWorkflowTest(unittest.TestCase):
    def test_core_deployments_do_not_depend_on_executor_work(self) -> None:
        workflow = WORKFLOW.read_text(encoding="utf-8")
        production_core = workflow.split("  deploy-production-core:", maxsplit=1)[1].split(
            "  run-dev-operation:", maxsplit=1
        )[0]
        dev_core = workflow.split("  run-dev-operation:", maxsplit=1)[1].split("  executor-development:", maxsplit=1)[0]

        self.assertIn("branches: [dev, prod]", workflow)
        self.assertIn("workflow_dispatch:", workflow)
        manual_inputs = workflow.split("  workflow_dispatch:", maxsplit=1)[1].split("permissions:", maxsplit=1)[0]
        self.assertNotIn("- deploy", manual_inputs)
        self.assertIn("needs: classify-deployment", production_core)
        self.assertIn("group: valkyrie-prod-deploy", production_core)
        self.assertIn("core_maintenance_required != 'true'", production_core)
        self.assertIn("database_maintenance_required != 'true'", production_core)
        self.assertIn("SCOPE=core", production_core)
        self.assertNotIn("services/executor_artifact/build.py", production_core)
        self.assertNotIn("executor_release/main.py", production_core)
        self.assertNotIn("maintenance-operation", production_core)
        self.assertIn("needs: classify-deployment", dev_core)
        self.assertIn("group: valkyrie-dev-${{ github.event_name == 'push' && 'deploy' || github.run_id }}", dev_core)
        self.assertIn("core_maintenance_required != 'true'", dev_core)
        self.assertIn("database_maintenance_required != 'true'", dev_core)
        self.assertIn("environment: dev", dev_core)
        self.assertIn("SCOPE: ${{ github.event_name == 'push' && 'core' || inputs.scope }}", dev_core)
        self.assertIn("Deploy dev core stacks", dev_core)
        self.assertNotIn("services/executor_artifact/build.py", dev_core)
        self.assertNotIn("executor_release/main.py", dev_core)
        self.assertNotIn("maintenance-operation", dev_core)
        self.assertNotIn("submodules: recursive", workflow)
        self.assertNotIn("secrets.GH_PAT", workflow)

    def test_executor_keeps_the_deployed_worker_stack_identity(self) -> None:
        app = (ROOT / "infra" / "app.py").read_text(encoding="utf-8")
        makefile = (ROOT / "infra" / "Makefile").read_text(encoding="utf-8")
        classifier = (ROOT / "infra" / "classify_repository_change.py").read_text(encoding="utf-8")
        build_workflow = EXECUTOR_BUILD_WORKFLOW.read_text(encoding="utf-8")

        self.assertIn("from executor_stack import ExecutorStack", app)
        self.assertIn('stage.stack_id("WorkerStack")', app)
        self.assertIn("STACKS_executor = $(STACK_PREFIX)WorkerStack", makefile)
        self.assertIn('"infra/executor_stack.py"', classifier)
        self.assertIn('"infra/executor_stack.py"', build_workflow)
        self.assertNotIn("worker_stack.py", app + classifier + build_workflow)

    def test_executor_jobs_own_build_deploy_activation_and_maintenance(self) -> None:
        workflow = WORKFLOW.read_text(encoding="utf-8")
        dev_executor = workflow.split("  executor-development:", maxsplit=1)[1].split(
            "  production-executor-approval:", maxsplit=1
        )[0]
        production_approval = workflow.split("  production-executor-approval:", maxsplit=1)[1].split(
            "  executor-production:", maxsplit=1
        )[0]
        prod_executor = workflow.split("  executor-production:", maxsplit=1)[1]

        for executor_job, stage in ((dev_executor, "dev"), (prod_executor, "production")):
            with self.subTest(stage=stage):
                self.assertIn("executor_stack_deploy_required", executor_job)
                self.assertIn("executor_release_required", executor_job)
                self.assertIn("core_maintenance_required", executor_job)
                self.assertIn("database_maintenance_required", executor_job)
                self.assertIn(
                    "PYTHONPATH=services/tracker/src python services/executor_artifact/build.py",
                    executor_job,
                )
                self.assertIn("--maintenance-operation begin", executor_job)
                self.assertIn("SCOPE=executor", executor_job)
                self.assertIn("--maintenance-operation finish", executor_job)
                self.assertIn(
                    "PYTHONPATH=. uv run --project executor_release --frozen python executor_release/main.py",
                    executor_job,
                )
                self.assertLess(executor_job.index("Begin"), executor_job.index("SCOPE=executor"))
                self.assertLess(executor_job.index("SCOPE=executor"), executor_job.index("Publish and activate"))
                self.assertLess(executor_job.index("Publish and activate"), executor_job.index("Finish"))

        self.assertIn("needs: [classify-deployment, run-dev-operation]", dev_executor)
        self.assertIn("environment: dev", dev_executor)
        self.assertIn("environment: production-release", production_approval)
        self.assertNotIn("concurrency:", production_approval)
        self.assertIn(
            "needs: [classify-deployment, deploy-production-core, production-executor-approval]",
            prod_executor,
        )
        self.assertNotIn("environment: production-release", prod_executor)
        self.assertIn("needs.production-executor-approval.result == 'success'", prod_executor)
        self.assertEqual(
            workflow.count("PYTHONPATH=services/tracker/src python services/executor_artifact/build.py"),
            2,
        )
        self.assertEqual(workflow.count("--maintenance-operation begin"), 2)
        self.assertEqual(workflow.count("--maintenance-operation finish"), 2)
        self.assertNotIn("actions/upload-artifact", workflow)
        self.assertNotIn("actions/download-artifact", workflow)

    def test_maintenance_paths_bypass_the_skipped_core_job_and_preserve_failed_fences(self) -> None:
        workflow = WORKFLOW.read_text(encoding="utf-8")
        dev_executor = workflow.split("  executor-development:", maxsplit=1)[1].split(
            "  production-executor-approval:", maxsplit=1
        )[0]
        prod_executor = workflow.split("  executor-production:", maxsplit=1)[1]

        for executor_job in (dev_executor, prod_executor):
            job_condition = executor_job.split("    needs:", maxsplit=1)[0]
            self.assertIn("always()", job_condition)
            self.assertIn("core_maintenance_required == 'true'", job_condition)
            self.assertIn("database_maintenance_required == 'true'", job_condition)
            self.assertIn("result == 'success'", job_condition)

            core_under_maintenance = executor_job.split("Deploy ", maxsplit=1)[1].split(
                "      - name: Deploy", maxsplit=1
            )[0]
            self.assertIn("core_maintenance_required == 'true'", core_under_maintenance)
            self.assertIn("database_maintenance_required == 'true'", core_under_maintenance)

            finish = executor_job.rsplit("      - name: Finish", maxsplit=1)[1]
            self.assertNotIn("always()", finish)
            self.assertIn("--maintenance-operation finish", finish)

    def test_executor_bootstrap_fails_closed_until_release_control_exists(self) -> None:
        workflow = WORKFLOW.read_text(encoding="utf-8")
        dev_executor = workflow.split("  executor-development:", maxsplit=1)[1].split(
            "  production-executor-approval:", maxsplit=1
        )[0]
        prod_executor = workflow.split("  executor-production:", maxsplit=1)[1]

        stages = (
            (
                dev_executor,
                "dev",
                "arn:aws:iam::${{ env.DEV_ACCOUNT_ID }}:role/ValkyrieExecutorRelease-dev",
                "${{ env.AWS_DEPLOY_ROLE_ARN }}",
            ),
            (
                prod_executor,
                "production",
                "arn:aws:iam::613431292675:role/ValkyrieExecutorRelease",
                "arn:aws:iam::613431292675:role/github-actions-valkyrie-deploy",
            ),
        )
        for executor_job, stage, release_role, deployment_role in stages:
            with self.subTest(stage=stage):
                self.assertIn("aws ssm get-parameter", executor_job)
                self.assertIn("ParameterNotFound", executor_job)
                self.assertIn("physical WorkerStack bootstrap", executor_job)
                self.assertNotIn("describe-stacks", executor_job)
                self.assertNotIn("steps.executor-stack.outputs.exists", executor_job)

                preflight = f"Require deployed {stage} release control"
                maintenance = f"Begin {stage} maintenance"
                release_credentials = f"Configure {stage} release preflight credentials"
                restore_credentials = f"Restore {stage} deployment credentials"
                core_deploy = f"Deploy {stage} core stacks under maintenance"
                executor_deploy = f"Deploy {stage} executor stack"
                final_release_credentials = f"- name: Configure {stage} release credentials\n"
                activation = f"Publish and activate {stage} executor release"
                finish = f"Finish {stage} maintenance"

                self.assertIn(release_credentials, executor_job)
                self.assertLess(executor_job.index(release_credentials), executor_job.index(preflight))
                preflight_index = executor_job.index(preflight)
                preflight_credentials = executor_job[executor_job.index(release_credentials) : preflight_index]
                self.assertEqual(preflight_credentials.count("uses: aws-actions/configure-aws-credentials"), 1)
                self.assertIn(f"role-to-assume: {release_role}", preflight_credentials)
                self.assertNotIn(f"role-to-assume: {deployment_role}", preflight_credentials)
                preflight_step = executor_job[preflight_index : executor_job.index(maintenance)]
                self.assertIn("aws ssm get-parameter", preflight_step)
                self.assertLess(preflight_index, executor_job.index(maintenance))
                self.assertNotIn(f"Configure {stage} release credentials for maintenance", executor_job)

                restore_index = executor_job.index(restore_credentials)
                self.assertLess(restore_index, executor_job.index(core_deploy))
                self.assertLess(restore_index, executor_job.index(executor_deploy))
                deployment_credentials = executor_job[restore_index : executor_job.index(core_deploy)]
                self.assertIn(f"role-to-assume: {deployment_role}", deployment_credentials)

                final_release_index = executor_job.index(final_release_credentials)
                activation_index = executor_job.index(activation)
                self.assertLess(final_release_index, activation_index)
                self.assertLess(final_release_index, executor_job.index(finish))
                final_release = executor_job[final_release_index:activation_index]
                self.assertIn(f"role-to-assume: {release_role}", final_release)
                activation_step = executor_job[activation_index : executor_job.index(finish)]
                self.assertIn(
                    "needs.classify-deployment.outputs.executor_release_required == 'true'",
                    activation_step,
                )

        self.assertIn(
            "EXECUTOR_RELEASE_LAUNCH_PARAMETER: /valkyrie/dev/executor-release/launch-config",
            dev_executor,
        )
        self.assertIn(
            "EXECUTOR_RELEASE_LAUNCH_PARAMETER: /valkyrie/prod/executor-release/launch-config",
            prod_executor,
        )

    def test_mutations_share_stage_mutex_and_stale_executor_jobs_do_nothing(self) -> None:
        workflow = WORKFLOW.read_text(encoding="utf-8")
        dev_executor = workflow.split("  executor-development:", maxsplit=1)[1].split(
            "  production-executor-approval:", maxsplit=1
        )[0]
        prod_executor = workflow.split("  executor-production:", maxsplit=1)[1]

        self.assertEqual(workflow.count("group: valkyrie-prod-deploy"), 2)
        self.assertIn("group: valkyrie-dev-deploy", dev_executor)
        for executor_job in (dev_executor, prod_executor):
            self.assertIn("gh api", executor_job)
            self.assertIn("id: freshness", executor_job)
            self.assertGreaterEqual(executor_job.count("steps.freshness.outputs.current == 'true'"), 14)

    def test_deployment_reuses_the_pr_classifier(self) -> None:
        workflow = WORKFLOW.read_text(encoding="utf-8")
        classification_workflow = (
            Path(__file__).parents[2] / ".github" / "workflows" / "maintenance-classification.yaml"
        ).read_text(encoding="utf-8")

        command = "python3 infra/classify_repository_change.py"
        self.assertIn(command, workflow)
        self.assertIn(command, classification_workflow)
        for output in (
            "executor_stack_deploy_required",
            "executor_release_required",
            "core_maintenance_required",
            "database_maintenance_required",
        ):
            self.assertIn(output, workflow)
        self.assertIn("pull_request_target:", classification_workflow)
        trusted_checkout = classification_workflow.split("      - name: Checkout trusted classifier", maxsplit=1)[
            1
        ].split("      - name: Fetch base and candidate without executing them", maxsplit=1)[0]
        self.assertIn("ref: ${{ github.event.repository.default_branch }}", trusted_checkout)
        self.assertNotIn("github.event.pull_request.base.sha", trusted_checkout)
        self.assertNotIn("github.event.pull_request.head.sha", trusted_checkout)
        self.assertIn('git fetch --no-tags origin "$BASE_SHA"', classification_workflow)
        self.assertIn('test "$(git rev-parse FETCH_HEAD)" = "$BASE_SHA"', classification_workflow)
        self.assertIn('git fetch --no-tags origin "pull/$PR_NUMBER/head"', classification_workflow)
        self.assertIn('git fetch --no-tags origin "${MERGE_GROUP_HEAD_REF#refs/heads/}"', classification_workflow)
        self.assertIn('test "$(git rev-parse FETCH_HEAD)" = "$HEAD_SHA"', classification_workflow)
        self.assertNotIn("checks: write", classification_workflow)
        self.assertNotIn("actions/github-script", classification_workflow)

    def test_executor_build_check_covers_real_arm_images_and_pex(self) -> None:
        workflow = EXECUTOR_BUILD_WORKFLOW.read_text(encoding="utf-8")

        self.assertIn("runs-on: ubuntu-24.04-arm", workflow)
        self.assertEqual(workflow.count('"tests/unit/executor_host/**"'), 2)
        self.assertIn('"services/executor_artifact/**"', workflow)
        self.assertIn('"services/executor_host/**"', workflow)
        self.assertIn('"services/tracker/**"', workflow)
        self.assertIn('".dockerignore"', workflow)
        self.assertIn("PYTHONPATH=services/tracker/src python services/executor_artifact/build.py", workflow)
        self.assertIn("uv run pytest tests/unit/executor_host services/executor_artifact/tests", workflow)
        self.assertIn(
            "docker build --platform linux/arm64 -t valkyrie-tracker:ci services/tracker",
            workflow,
        )
        self.assertIn("-f services/executor_host/Dockerfile", workflow)
        self.assertEqual(workflow.count("services/executor_artifact/build.py"), 1)
        self.assertIn("services/executor_artifact/uv.lock", workflow)
        self.assertIn("uv lock --project services/executor_artifact --check", workflow)

    def test_deployment_tools_are_pinned_and_release_dependencies_are_isolated(self) -> None:
        workflow = WORKFLOW.read_text(encoding="utf-8")

        self.assertEqual(workflow.count("npm install -g aws-cdk@2.1132.0"), 4)
        self.assertIn("uv sync --frozen --python 3.12 --project infra", workflow)
        self.assertIn("infra/executor_release/uv.lock", workflow)
        self.assertIn("services/executor_artifact/uv.lock", workflow)
        self.assertNotIn("services/tracker/uv.lock", workflow)
        self.assertIn(
            "PYTHONPATH=. uv run --project executor_release --frozen python executor_release/main.py",
            workflow,
        )


if __name__ == "__main__":
    unittest.main()
