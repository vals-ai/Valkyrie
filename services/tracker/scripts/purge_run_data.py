"""Bootstrap the purge CLI after selecting its explicit database environment."""

import argparse
import os
import sys
from pathlib import Path


def main() -> int:
    arguments = sys.argv[1:]
    bootstrap = argparse.ArgumentParser(add_help=False)
    bootstrap.add_argument("--database-url-env")
    options, _ = bootstrap.parse_known_args(arguments)
    if options.database_url_env is not None:
        if options.database_url_env not in os.environ:
            print("Named database environment variable is missing", file=sys.stderr)
            return 2
        os.environ["DATABASE_URL"] = os.environ[options.database_url_env]
    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
    from tracker.run_purge.cli import main as run

    return run(arguments)


if __name__ == "__main__":
    raise SystemExit(main())
