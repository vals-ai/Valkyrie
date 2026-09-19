"""Select the private database environment before importing tracker engines."""

import argparse
import json
import os
import re
import sys
from pathlib import Path


def report_notes(error: BaseException) -> None:
    for note in getattr(error, "__notes__", []):
        print(note, file=sys.stderr)


def main() -> int:
    parser = argparse.ArgumentParser(description="Verify and relocate held terminal runs in one AWS account")
    parser.add_argument("--request", required=True, type=Path)
    parser.add_argument("--report", required=True, type=Path)
    parser.add_argument("--database-url-env", required=True)
    parser.add_argument("--expected-database-target", required=True)
    parser.add_argument("--apply", action="store_true")
    options = parser.parse_args()
    try:
        payload = options.request.read_bytes()
        action = json.loads(payload).get("action")
        if action in {"prepare", "relocate", "release"} and not options.apply:
            print("Mutation actions require --apply", file=sys.stderr)
            return 2
        if action in {"inventory", "inspect"} and options.apply:
            print("Read-only actions do not accept --apply", file=sys.stderr)
            return 2
        if re.fullmatch(r"[A-Z][A-Z0-9_]*", options.database_url_env) is None:
            raise ValueError("Invalid database environment variable")
        os.environ["DATABASE_URL"] = os.environ[options.database_url_env]
        sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
        from tracker.lifecycle import LifecycleConflict
        from tracker.run_relocation.cli import execute

        try:
            return execute(payload, options.request, options.report, options.expected_database_target)
        except LifecycleConflict as conflict:
            # Every refusal message in this tool is an authored constant with no provider payload.
            print(f"Relocation remains incomplete (LifecycleConflict: {conflict})", file=sys.stderr)
            report_notes(conflict)
            return 2
    except Exception as error:
        print(f"Relocation remains incomplete ({type(error).__name__})", file=sys.stderr)
        report_notes(error)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
