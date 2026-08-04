"""Read-only drift check: compare the live dev/prod ruleset required contexts against the
version-controlled manifest (.github/required-contexts.json).

This never mutates a ruleset. It fails (non-zero) when the live required contexts differ
from the manifest so a human can reconcile them; ruleset changes are always applied by a
human, never by CI.
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

_MANIFEST = Path(".github/required-contexts.json")


def _ruleset_required_contexts(repo: str, ruleset_id: int) -> list[str]:
    raw = subprocess.run(
        ["gh", "api", f"repos/{repo}/rulesets/{ruleset_id}"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    ruleset = json.loads(raw)
    contexts: list[str] = []
    for rule in ruleset.get("rules", []):
        if rule.get("type") != "required_status_checks":
            continue
        for check in rule["parameters"]["required_status_checks"]:
            contexts.append(check["context"])
    return sorted(contexts)


def main() -> int:
    repo = subprocess.run(
        ["gh", "repo", "view", "--json", "nameWithOwner", "--jq", ".nameWithOwner"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()

    manifest = json.loads(_MANIFEST.read_text(encoding="utf-8"))

    drift = False
    for name, spec in manifest["rulesets"].items():
        expected = sorted(spec["required_contexts"])
        try:
            actual = _ruleset_required_contexts(repo, spec["ruleset_id"])
        except subprocess.CalledProcessError as error:
            print(f"::error::Could not read ruleset {spec['ruleset_id']} ({name}): {error.stderr.strip()}")
            print("::error::A token with repository administration read access is required.")
            return 2
        if actual != expected:
            drift = True
            print(f"::error::Ruleset drift for {name} (id={spec['ruleset_id']}).")
            print(f"  expected required contexts: {expected}")
            print(f"  live required contexts:     {actual}")
        else:
            print(f"OK: {name} ruleset required contexts match the manifest: {actual}")

    if drift:
        print("::error::Reconcile the rulesets with .github/required-contexts.json (apply manually).")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
