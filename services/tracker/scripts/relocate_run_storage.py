"""Select the private database environment before importing tracker engines."""

import argparse
import json
import os
import re
import sys
from pathlib import Path


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
        from tracker.run_relocation.cli import execute

        return execute(payload, options.request, options.report, options.expected_database_target)
    except Exception as error:
        print(f"Relocation remains incomplete ({type(error).__name__})", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
