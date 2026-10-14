import os
import sys


def main():
    # Before any model is defined: Logfire's pydantic plugin would load Logfire and OpenTelemetry, unused by the CLI.
    os.environ.setdefault("PYDANTIC_DISABLE_PLUGINS", "__all__")
    try:
        from valkyrie.cli.main import cli

        cli()
    except KeyboardInterrupt:
        sys.exit(130)
