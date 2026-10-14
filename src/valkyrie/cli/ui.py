"""Defines `valk ui`, which opens the terminal UI."""

import click


@click.command()
@click.option("--fake", is_flag=True, help="Use built-in sample data instead of the tracker.")
def ui(fake: bool) -> None:
    """Browse runs, tasks and logs in a terminal UI."""
    # Textual loads when the UI opens, not for `valk ui --help` or the reference docs.
    from valkyrie.sdk.errors import ValkyrieConfigError
    from valkyrie.tui.app import ValkyrieApp
    from valkyrie.tui.fake import FakeSource
    from valkyrie.tui.source import RunSource, TrackerSource, TrackerSwitcher

    source: RunSource
    if fake:
        source = FakeSource()
    else:
        try:
            source = TrackerSource.from_cli_config()
        except ValkyrieConfigError as error:
            raise click.ClickException(f"{error}\nOr try `valk ui --fake` for sample data.") from error
    ValkyrieApp(source, switcher=None if fake else TrackerSwitcher()).run()
