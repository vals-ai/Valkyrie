"""Select both private databases before importing Tracker configuration."""

import argparse
import json
import os
import re
import sys
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser(description="Transfer held historical runs between two explicit Tracker databases")
    parser.add_argument("--request", required=True, type=Path)
    parser.add_argument("--report", required=True, type=Path)
    parser.add_argument("--source-database-url-env", required=True)
    parser.add_argument("--destination-database-url-env", required=True)
    parser.add_argument("--expected-source-database-target", required=True)
    parser.add_argument("--expected-destination-database-target", required=True)
    parser.add_argument("--source-aws-profile-env", required=True)
    parser.add_argument("--destination-aws-profile-env", required=True)
    parser.add_argument("--journal-directory", required=True, type=Path)
    parser.add_argument("--apply", action="store_true")
    options = parser.parse_args()
    try:
        payload = options.request.read_bytes()
        action = json.loads(payload).get("action", "plan")
        if action in {"prepare", "import", "cleanup", "finalize"} and not options.apply:
            print("Mutation actions require --apply", file=sys.stderr)
            return 2
        if action in {"plan", "inspect"} and options.apply:
            raise ValueError("Read-only action cannot apply")
        names = (
            options.source_database_url_env,
            options.destination_database_url_env,
            options.source_aws_profile_env,
            options.destination_aws_profile_env,
        )
        if any(re.fullmatch(r"[A-Z][A-Z0-9_]*", name) is None for name in names) or len(set(names)) != len(names):
            raise ValueError("Separate named environment variables are required")
        source_url, destination_url, source_profile, destination_profile = (os.environ[name] for name in names)
        os.environ["DATABASE_URL"] = source_url
        sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
        from tracker.run_transfer.cli import execute

        return execute(
            payload,
            options.request,
            options.report,
            source_url,
            destination_url,
            options.expected_source_database_target,
            options.expected_destination_database_target,
            source_profile,
            destination_profile,
            options.journal_directory,
        )
    except Exception as error:
        print(f"Transfer remains incomplete ({type(error).__name__})", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
