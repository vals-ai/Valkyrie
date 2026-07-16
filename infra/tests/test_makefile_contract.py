"""Tests for the operator-facing infrastructure targets."""

import unittest
from pathlib import Path

MAKEFILE = (Path(__file__).resolve().parents[1] / "Makefile").read_text()


class MakefileContractTest(unittest.TestCase):
    def test_runbook_component_targets_exist(self) -> None:
        for target in (
            "bootstrap",
            "diff-deployment-access",
            "deploy-deployment-access",
            "diff-dns-zone",
            "deploy-dns-zone",
            "diff-shared",
            "deploy-shared",
            "diff-tracker",
            "deploy-tracker",
            "diff-worker",
            "deploy-worker",
            "diff-monitoring",
            "deploy-monitoring",
        ):
            with self.subTest(target=target):
                self.assertIn(f"{target}:", MAKEFILE)

    def test_all_scope_contains_application_stacks_only(self) -> None:
        self.assertIn(
            "STACKS_all = $(STACKS_shared) $(STACKS_tracker) $(STACKS_worker) $(STACKS_monitoring)",
            MAKEFILE,
        )
        self.assertIn('if [ "$(SCOPE)" = "deployment-access" ]', MAKEFILE)

    def test_bootstrap_and_cdk_cli_are_pinned_to_dev(self) -> None:
        self.assertRegex(MAKEFILE, r"CDK_VERSION := [0-9]+\.[0-9]+\.[0-9]+")
        self.assertIn("bootstrap: STAGE := dev", MAKEFILE)


if __name__ == "__main__":
    unittest.main()
