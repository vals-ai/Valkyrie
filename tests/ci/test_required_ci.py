"""Behavior tests for the required-ci relevance selector and aggregate decision.

These guard the two real failure modes the gate exists to prevent:
* an irrelevant subsystem being required (or a relevant one not being required) for a diff, and
* the aggregate passing when a required leaf failed / was cancelled / unexpectedly skipped, or
  failing when only non-required leaves were skipped.
"""

from __future__ import annotations

import importlib.util
import json
import os
import unittest
from pathlib import Path

_SCRIPTS = Path(__file__).resolve().parents[2] / ".github" / "scripts"


def _load(name: str):
    spec = importlib.util.spec_from_file_location(name, _SCRIPTS / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(module)
    return module


select_mod = _load("required_ci_select")
aggregate_mod = _load("required_ci_aggregate")


def _select(paths, base_ref="dev", is_fork=False):
    return select_mod.select(list(paths), base_ref=base_ref, is_fork=is_fork)


class SelectorTest(unittest.TestCase):
    def test_docs_only_change_requires_no_code_validation(self) -> None:
        out = _select(["docs/guide.md", "README.md"])
        for flag in ("run_lint", "run_typecheck", "run_cli", "run_tracker_unit",
                     "run_executor", "run_infra", "run_sdk"):
            self.assertEqual(out[flag], "false", flag)

    def test_infra_only_change_does_not_require_tracker_or_sdk_or_cli(self) -> None:
        out = _select(["infra/executor_stack.py"])
        self.assertEqual(out["run_infra"], "true")
        self.assertEqual(out["run_executor"], "true")  # executor_stack.py is an executor path
        self.assertEqual(out["run_lint"], "true")
        self.assertEqual(out["run_tracker_unit"], "false")
        self.assertEqual(out["run_sdk"], "false")
        self.assertEqual(out["run_cli"], "false")

    def test_pure_infra_change_stays_out_of_executor(self) -> None:
        out = _select(["infra/README.md"])
        self.assertEqual(out["run_infra"], "true")
        self.assertEqual(out["run_executor"], "false")

    def test_same_repo_tracker_change_requires_unit_and_live(self) -> None:
        out = _select(["services/tracker/src/tracker/api/health.py"], is_fork=False)
        self.assertEqual(out["run_tracker_unit"], "true")
        self.assertEqual(out["run_tracker_live"], "true")
        self.assertEqual(out["run_tracker_live_fork_blocked"], "false")

    def test_fork_tracker_change_is_blocked_not_live_tested(self) -> None:
        out = _select(["services/tracker/src/tracker/api/health.py"], is_fork=True)
        self.assertEqual(out["run_tracker_unit"], "true")
        self.assertEqual(out["run_tracker_live"], "false")
        self.assertEqual(out["run_tracker_live_fork_blocked"], "true")

    def test_cbs_is_prod_only(self) -> None:
        self.assertEqual(_select(["src/valkyrie/app.py"], base_ref="dev")["run_cbs"], "false")
        self.assertEqual(_select(["src/valkyrie/app.py"], base_ref="prod")["run_cbs"], "true")

    def test_lockfile_scopes_are_independent(self) -> None:
        out = _select(["services/tracker/uv.lock"])
        self.assertEqual(out["run_lockfile_tracker"], "true")
        self.assertEqual(out["run_lockfile_root"], "false")
        self.assertEqual(out["run_lockfile_infra"], "false")

    def test_self_modification_forces_full_validation(self) -> None:
        out = _select([".github/workflows/required-ci.yaml"])
        self.assertEqual(out["force_all"], "true")
        for flag in ("run_lint", "run_typecheck", "run_cli", "run_tracker_unit",
                     "run_executor", "run_infra", "run_sdk", "run_lockfile_root"):
            self.assertEqual(out[flag], "true", flag)


class AggregateTest(unittest.TestCase):
    def _run(self, select_result, jobs):
        os.environ["SELECT_RESULT"] = select_result
        os.environ["JOBS_JSON"] = json.dumps(jobs)
        os.environ.pop("GITHUB_STEP_SUMMARY", None)
        return aggregate_mod.main()

    def test_passes_when_required_leaves_succeed_and_others_skip(self) -> None:
        jobs = [
            {"name": "infra", "required": "true", "result": "success"},
            {"name": "tracker-unit", "required": "false", "result": "skipped"},
        ]
        self.assertEqual(self._run("success", jobs), 0)

    def test_fails_when_a_required_leaf_fails(self) -> None:
        jobs = [{"name": "infra", "required": "true", "result": "failure"}]
        self.assertEqual(self._run("success", jobs), 1)

    def test_fails_when_a_required_leaf_is_cancelled(self) -> None:
        jobs = [{"name": "infra", "required": "true", "result": "cancelled"}]
        self.assertEqual(self._run("success", jobs), 1)

    def test_fails_when_a_required_leaf_is_unexpectedly_skipped(self) -> None:
        # e.g. sdk compatibility skipped because its package prerequisite failed
        jobs = [{"name": "sdk", "required": "true", "result": "skipped"}]
        self.assertEqual(self._run("success", jobs), 1)

    def test_fails_when_selector_did_not_succeed(self) -> None:
        jobs = [{"name": "infra", "required": "false", "result": "skipped"}]
        self.assertEqual(self._run("failure", jobs), 1)

    def test_fails_when_non_required_leaf_errors(self) -> None:
        jobs = [{"name": "infra", "required": "false", "result": "failure"}]
        self.assertEqual(self._run("success", jobs), 1)


if __name__ == "__main__":
    unittest.main()
