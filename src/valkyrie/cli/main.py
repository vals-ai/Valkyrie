"""CLI views/commands for Valkyrie."""

import sys
from typing import Any

import click
from dotenv import load_dotenv

from valkyrie.cli.lazy import LazyGroup
from valkyrie.cli.logging import configure_cli_logging


class _ValkyrieCLI(LazyGroup):
    def invoke(self, ctx: click.Context) -> Any:
        try:
            return super().invoke(ctx)
        except Exception as error:
            # Looked up, not imported: the SDK takes 0.3s to import, and until it's loaded no error can be one of its own.
            errors = sys.modules.get("valkyrie.sdk.errors")
            if errors is not None and isinstance(error, errors.ValkyrieConfigError):
                raise click.ClickException(str(error)) from error
            raise


@click.group(
    cls=_ValkyrieCLI,
    lazy_commands={
        "run": ("valkyrie.cli.run:run", "Start, monitor, manage, and retrieve runs."),
        "agent": ("valkyrie.cli.agent:agent", "Install, upload, list, download, and remove agents."),
        "benchmark": (
            "valkyrie.cli.benchmark:benchmark",
            "Export the task IDs in a benchmark dataset, and save benchmark services.",
        ),
        "queue": ("valkyrie.cli.queue:queue", "Inspect your organization's sandbox scheduler."),
        "project": ("valkyrie.cli.project:project", "Manage the projects runs are started in"),
        "secret": ("valkyrie.cli.secret:secret", "Store secrets that your runs reference"),
        "provider": (
            "valkyrie.cli.provider:provider",
            "Choose the secret that configures each sandbox provider, and your default provider.",
        ),
        "login": (
            "valkyrie.cli.login:login",
            "Sign in with your browser, save the login, and use this Tracker by default.",
        ),
        "logout": (
            "valkyrie.cli.login:logout",
            "Forget the saved login for this Tracker, and stop using it by default.",
        ),
        "whoami": ("valkyrie.cli.login:whoami", "Show which Tracker, organization and account your requests use."),
        "token": ("valkyrie.cli.login:token", "Create and revoke access keys for CI and other headless use."),
        "org": ("valkyrie.cli.org:org", "Create, join and switch organizations, and manage their members."),
        "ui": ("valkyrie.cli.ui:ui", "Browse runs, tasks and logs in a terminal UI."),
    },
)
def cli():
    """Valkyrie CLI."""
    configure_cli_logging()
    load_dotenv()
    configure_cli_logging()


__all__ = ["cli"]


if __name__ == "__main__":
    cli()
