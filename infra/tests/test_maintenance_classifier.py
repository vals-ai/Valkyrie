import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

import classify_repository_change
from classify_executor_template_change import ExecutorHostTemplateEffect

_MIGRATION_DIRECTORY = "services/tracker/src/tracker/database/migrations/versions"
_NO_EXECUTOR_EFFECT = ExecutorHostTemplateEffect(redeploy_required=False, reasons=())


class MaintenanceClassifierTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.repository = Path(self.temporary_directory.name)
        self._git("init", "-q")
        self._git("config", "user.email", "test@example.com")
        self._git("config", "user.name", "Test")
        (self.repository / "README.md").write_text("base\n", encoding="utf-8")
        self._git("add", ".")
        self._git("commit", "-qm", "base")
        self.base_sha = self._git("rev-parse", "HEAD").strip()

    def tearDown(self) -> None:
        self.temporary_directory.cleanup()

    def _git(self, *arguments: str) -> str:
        return subprocess.run(
            ["git", *arguments],
            cwd=self.repository,
            check=True,
            capture_output=True,
            text=True,
        ).stdout

    def _commit_file(self, path: str, source: str | bytes) -> str:
        return self._commit_files({path: source})

    def _commit_files(self, sources: dict[str, str | bytes]) -> str:
        for path, source in sources.items():
            destination = self.repository / path
            destination.parent.mkdir(parents=True, exist_ok=True)
            if isinstance(source, bytes):
                destination.write_bytes(source)
            else:
                destination.write_text(source, encoding="utf-8")
        self._git("add", ".")
        self._git("commit", "-qm", "change")
        return self._git("rev-parse", "HEAD").strip()

    def _classify(
        self,
        head_sha: str,
        *,
        executor_effect: ExecutorHostTemplateEffect = _NO_EXECUTOR_EFFECT,
    ) -> classify_repository_change.Classification:
        return classify_repository_change.classify_repository_change(
            self.repository,
            base_sha=self.base_sha,
            head_sha=head_sha,
            executor_effect=executor_effect,
        )

    def test_initial_branch_push_uses_the_empty_tree(self) -> None:
        head_sha = self._commit_file("services/executor_host/supervisor.py", "changed = True\n")

        result = classify_repository_change.classify_repository_change(
            self.repository,
            base_sha="0" * 40,
            head_sha=head_sha,
            executor_effect=_NO_EXECUTOR_EFFECT,
        )

        self.assertEqual(result.classification, "safe")
        self.assertTrue(result.executor_stack_deploy_required)
        self.assertFalse(result.executor_host_redeploy_required)
        self.assertFalse(result.executor_release_required)
        self.assertFalse(result.database_maintenance_required)
        self.assertEqual(result.reasons, ["executor-core-change"])

    def test_new_table_is_safe(self) -> None:
        head_sha = self._commit_file(
            f"{_MIGRATION_DIRECTORY}/new_table.py",
            """
from alembic import op
import sqlalchemy as sa

def upgrade() -> None:
    op.create_table("maintenance_test", sa.Column("id", sa.Integer(), nullable=False))
""",
        )

        result = self._classify(head_sha)

        self.assertEqual(result.classification, "safe")
        self.assertEqual(result.findings, [])

    def test_nullable_column_without_default_is_safe(self) -> None:
        head_sha = self._commit_file(
            f"{_MIGRATION_DIRECTORY}/safe.py",
            """
from alembic import op
import sqlalchemy as sa

def upgrade() -> None:
    op.add_column("benchmark", sa.Column("note", sa.String(), nullable=True))
""",
        )

        result = self._classify(head_sha)

        self.assertEqual(result.classification, "safe")
        self.assertEqual(result.findings, [])

    def test_non_unique_index_is_safe(self) -> None:
        head_sha = self._commit_file(
            f"{_MIGRATION_DIRECTORY}/index.py",
            """
from alembic import op


def upgrade() -> None:
    op.create_index("ix_benchmark_label", "benchmark", ["label"], unique=False)
""",
        )

        result = self._classify(head_sha)

        self.assertEqual(result.classification, "safe")
        self.assertEqual(result.findings, [])

    def test_making_a_column_nullable_is_safe(self) -> None:
        head_sha = self._commit_file(
            f"{_MIGRATION_DIRECTORY}/nullable.py",
            """
from alembic import op
import sqlalchemy as sa


def upgrade() -> None:
    op.alter_column("evaluationresult", "instance_id", existing_type=sa.String(), nullable=True)
""",
        )

        result = self._classify(head_sha)

        self.assertEqual(result.classification, "safe")
        self.assertEqual(result.findings, [])

    def test_unique_index_requires_maintenance(self) -> None:
        head_sha = self._commit_file(
            f"{_MIGRATION_DIRECTORY}/unique_index.py",
            """
from alembic import op


def upgrade() -> None:
    op.create_index("ix_org_name", "org", ["name"], unique=True)
""",
        )

        result = self._classify(head_sha)

        self.assertEqual(result.classification, "maintenance-required")
        self.assertEqual(result.findings[0].operation, "op.create_index")

    def test_other_column_alterations_require_maintenance(self) -> None:
        head_sha = self._commit_file(
            f"{_MIGRATION_DIRECTORY}/alter_type.py",
            """
from alembic import op
import sqlalchemy as sa


def upgrade() -> None:
    op.alter_column("benchmark", "label", existing_type=sa.String(), type_=sa.Text())
""",
        )

        result = self._classify(head_sha)

        self.assertEqual(result.classification, "maintenance-required")
        self.assertEqual(result.findings[0].operation, "op.alter_column")

    def test_data_or_schema_rewrite_requires_maintenance(self) -> None:
        head_sha = self._commit_file(
            f"{_MIGRATION_DIRECTORY}/unsafe.py",
            """
from alembic import op

def upgrade() -> None:
    op.execute("UPDATE benchmark SET status = 'STOPPED'")
    op.drop_column("benchmark", "legacy")
""",
        )

        result = self._classify(head_sha)

        self.assertEqual(result.classification, "maintenance-required")
        self.assertFalse(result.executor_stack_deploy_required)
        self.assertFalse(result.executor_release_required)
        self.assertTrue(result.database_maintenance_required)
        self.assertEqual([finding.operation for finding in result.findings], ["op.execute", "op.drop_column"])

    def test_non_nullable_or_constrained_column_requires_maintenance(self) -> None:
        head_sha = self._commit_file(
            f"{_MIGRATION_DIRECTORY}/constrained.py",
            """
from alembic import op
import sqlalchemy as sa

def upgrade() -> None:
    op.add_column("benchmark", sa.Column("owner", sa.String(), nullable=False))
    op.add_column("benchmark", sa.Column("org", sa.Uuid(), sa.ForeignKey("org.id"), nullable=True))
""",
        )

        result = self._classify(head_sha)

        self.assertEqual(result.classification, "maintenance-required")
        self.assertEqual(len(result.findings), 2)

    def test_top_level_execution_requires_maintenance(self) -> None:
        head_sha = self._commit_file(
            f"{_MIGRATION_DIRECTORY}/top_level.py",
            """
from alembic import op
result = op.execute("SELECT 1")

def upgrade() -> None:
    pass
""",
        )

        result = self._classify(head_sha)

        self.assertEqual(result.classification, "maintenance-required")
        self.assertEqual(result.findings[0].operation, "module.Assign")

    def test_dynamic_migration_statement_requires_maintenance(self) -> None:
        head_sha = self._commit_file(
            f"{_MIGRATION_DIRECTORY}/dynamic.py",
            """
def upgrade() -> None:
    for statement in statements:
        statement()
""",
        )

        result = self._classify(head_sha)

        self.assertEqual(result.classification, "maintenance-required")
        self.assertEqual(result.findings[0].operation, "For")

    def test_classifier_change_requires_stack_without_assuming_maintenance(self) -> None:
        head_sha = self._commit_file("infra/classify_repository_change.py", "policy = True\n")

        result = self._classify(head_sha)

        self.assertEqual(result.classification, "safe")
        self.assertTrue(result.executor_stack_deploy_required)
        self.assertFalse(result.executor_host_redeploy_required)
        self.assertFalse(result.executor_release_required)
        self.assertFalse(result.core_maintenance_required)
        self.assertFalse(result.database_maintenance_required)
        self.assertEqual(result.reasons, ["executor-core-change"])

    def test_maintenance_classification_workflow_requires_stack_without_assuming_maintenance(self) -> None:
        head_sha = self._commit_file(
            ".github/workflows/maintenance-classification.yaml",
            "name: changed policy\n",
        )

        result = self._classify(head_sha)

        self.assertEqual(result.classification, "safe")
        self.assertTrue(result.executor_stack_deploy_required)
        self.assertFalse(result.executor_host_redeploy_required)
        self.assertFalse(result.executor_release_required)
        self.assertFalse(result.core_maintenance_required)
        self.assertFalse(result.database_maintenance_required)
        self.assertEqual(result.reasons, ["executor-core-change"])

    def test_cli_requires_template_effect_inputs_before_classification(self) -> None:
        output = self.repository / "classification.json"
        result = subprocess.run(
            [
                sys.executable,
                str(Path(classify_repository_change.__file__)),
                "--base-sha",
                self.base_sha,
                "--head-sha",
                self.base_sha,
                "--output",
                str(output),
            ],
            cwd=self.repository,
            check=False,
            capture_output=True,
            text=True,
        )

        self.assertEqual(result.returncode, 2)
        self.assertIn("--executor-base-template", result.stderr)
        self.assertIn("--executor-head-template", result.stderr)
        self.assertIn("--expected-stack-id", result.stderr)
        self.assertFalse(output.exists())

    def test_excluded_classifier_change_does_not_hide_template_effect(self) -> None:
        head_sha = self._commit_files(
            {
                "infra/classify_repository_change.py": "policy = True\n",
                "services/executor_host/supervisor.py": "runtime = True\n",
            }
        )
        effect = ExecutorHostTemplateEffect(
            redeploy_required=True,
            reasons=("executor-host-task-definition-changed",),
        )

        result = self._classify(head_sha, executor_effect=effect)

        self.assertEqual(result.classification, "maintenance-required")
        self.assertTrue(result.executor_stack_deploy_required)
        self.assertTrue(result.executor_host_redeploy_required)
        self.assertFalse(result.executor_release_required)
        self.assertFalse(result.core_maintenance_required)
        self.assertFalse(result.database_maintenance_required)
        self.assertEqual(
            result.reasons,
            ["executor-core-change", "executor-host-task-definition-changed"],
        )

    def test_executor_source_change_requires_stack_without_assuming_maintenance(self) -> None:
        head_sha = self._commit_file("services/executor_host/supervisor.py", "changed = True\n")

        result = self._classify(head_sha)

        self.assertEqual(result.classification, "safe")
        self.assertTrue(result.executor_stack_deploy_required)
        self.assertFalse(result.executor_host_redeploy_required)
        self.assertEqual(result.reasons, ["executor-core-change"])

    def test_executor_template_effect_requires_maintenance_and_stack_deploy(self) -> None:
        head_sha = self._commit_file("README.md", "unrelated source change\n")
        effect = ExecutorHostTemplateEffect(
            redeploy_required=True,
            reasons=("executor-host-task-definition-changed",),
        )

        result = self._classify(head_sha, executor_effect=effect)

        self.assertEqual(result.classification, "maintenance-required")
        self.assertTrue(result.executor_stack_deploy_required)
        self.assertTrue(result.executor_host_redeploy_required)
        self.assertIn("executor-host-task-definition-changed", result.reasons)

    def test_executor_host_context_change_requires_stack_without_assuming_maintenance(self) -> None:
        head_sha = self._commit_file(".dockerignore", "changed\n")

        result = self._classify(head_sha)

        self.assertEqual(result.classification, "safe")
        self.assertTrue(result.executor_stack_deploy_required)
        self.assertFalse(result.executor_host_redeploy_required)
        self.assertEqual(result.reasons, ["executor-core-change"])

    def test_deploy_workflow_change_requires_stack_without_release_or_maintenance(self) -> None:
        head_sha = self._commit_file(".github/workflows/deploy.yaml", "changed\n")

        result = self._classify(head_sha)

        self.assertEqual(result.classification, "safe")
        self.assertTrue(result.executor_stack_deploy_required)
        self.assertFalse(result.executor_host_redeploy_required)
        self.assertFalse(result.executor_release_required)
        self.assertFalse(result.core_maintenance_required)
        self.assertFalse(result.database_maintenance_required)

    def test_shared_executor_input_coordinates_core_without_assuming_maintenance(self) -> None:
        head_sha = self._commit_file("infra/shared.py", "changed = True\n")

        result = self._classify(head_sha)

        self.assertEqual(result.classification, "safe")
        self.assertTrue(result.executor_stack_deploy_required)
        self.assertFalse(result.executor_host_redeploy_required)
        self.assertFalse(result.executor_release_required)
        self.assertTrue(result.core_maintenance_required)
        self.assertFalse(result.database_maintenance_required)

    def test_tracker_stack_change_does_not_require_executor_work(self) -> None:
        head_sha = self._commit_file("infra/tracker_stack.py", "changed = True\n")

        result = self._classify(head_sha)

        self.assertEqual(result.classification, "safe")
        self.assertFalse(result.executor_stack_deploy_required)
        self.assertFalse(result.executor_release_required)
        self.assertFalse(result.database_maintenance_required)
        self.assertEqual(result.reasons, [])

    def test_executor_artifact_lock_requires_only_a_release(self) -> None:
        head_sha = self._commit_file("services/executor_artifact/uv.lock", "version = 1\n")

        result = self._classify(head_sha)

        self.assertEqual(result.classification, "safe")
        self.assertFalse(result.executor_stack_deploy_required)
        self.assertTrue(result.executor_release_required)
        self.assertFalse(result.database_maintenance_required)

    def test_executor_artifact_builder_requires_only_a_release(self) -> None:
        head_sha = self._commit_file("services/executor_artifact/build.py", "changed = True\n")

        result = self._classify(head_sha)

        self.assertEqual(result.classification, "safe")
        self.assertFalse(result.executor_stack_deploy_required)
        self.assertTrue(result.executor_release_required)
        self.assertFalse(result.core_maintenance_required)
        self.assertFalse(result.database_maintenance_required)

    def test_executor_release_helper_requires_stack_without_assuming_maintenance(self) -> None:
        head_sha = self._commit_file("infra/executor_release/main.py", "changed = True\n")

        result = self._classify(head_sha)

        self.assertEqual(result.classification, "safe")
        self.assertTrue(result.executor_stack_deploy_required)
        self.assertFalse(result.executor_host_redeploy_required)
        self.assertFalse(result.executor_release_required)
        self.assertFalse(result.core_maintenance_required)
        self.assertFalse(result.database_maintenance_required)

    def test_tracker_lock_does_not_trigger_an_executor_release(self) -> None:
        head_sha = self._commit_file("services/tracker/uv.lock", "version = 1\n")

        result = self._classify(head_sha)

        self.assertEqual(result.classification, "safe")
        self.assertFalse(result.executor_stack_deploy_required)
        self.assertFalse(result.executor_release_required)
        self.assertFalse(result.database_maintenance_required)

    def test_executor_runtime_dependency_requires_only_a_release(self) -> None:
        head_sha = self._commit_file("services/tracker/src/tracker/auth.py", "changed = True\n")

        result = self._classify(head_sha)

        self.assertEqual(result.classification, "safe")
        self.assertFalse(result.executor_stack_deploy_required)
        self.assertTrue(result.executor_release_required)
        self.assertFalse(result.database_maintenance_required)

    def test_tracker_api_only_change_does_not_require_executor_work(self) -> None:
        head_sha = self._commit_file("services/tracker/src/tracker/api/health.py", "changed = True\n")

        result = self._classify(head_sha)

        self.assertEqual(result.classification, "safe")
        self.assertFalse(result.executor_stack_deploy_required)
        self.assertFalse(result.executor_release_required)
        self.assertFalse(result.database_maintenance_required)

    def test_executor_runtime_change_requires_only_a_release(self) -> None:
        head_sha = self._commit_file(
            "services/tracker/src/tracker/executor/entrypoint.py",
            "changed = True\n",
        )

        result = self._classify(head_sha)

        self.assertEqual(result.classification, "safe")
        self.assertFalse(result.executor_stack_deploy_required)
        self.assertTrue(result.executor_release_required)
        self.assertFalse(result.database_maintenance_required)
        self.assertEqual(result.reasons, ["executor-release-change"])

    def test_existing_migration_history_cannot_be_changed(self) -> None:
        path = f"{_MIGRATION_DIRECTORY}/existing.py"
        first_sha = self._commit_file(path, "def upgrade() -> None:\n    pass\n")
        self.base_sha = first_sha
        head_sha = self._commit_file(path, "def upgrade() -> None:\n    raise RuntimeError\n")

        with self.assertRaisesRegex(RuntimeError, "immutable"):
            self._classify(head_sha)


if __name__ == "__main__":
    unittest.main()
