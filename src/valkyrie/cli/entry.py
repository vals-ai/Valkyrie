import sys


def main():
    try:
        from valkyrie.cli.main import cli

        cli()
    except KeyboardInterrupt:
        sys.exit(130)
