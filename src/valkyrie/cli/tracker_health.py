"""Tracker health helpers for CLI commands."""

import json

import click

from valkyrie.cli.tracker_client import TrackerService


def check_tracker_service_health(tracker: TrackerService) -> bool:
    """Check the tracker service and print a CLI-friendly error when unhealthy."""
    response = tracker.health_check()
    if response.status_code != 200:
        click.echo(click.style("Tracker service failed to respond!", fg="red", bold=True))
        click.echo(json.dumps(response.json(), indent=4, default=str))
        return False

    return True
