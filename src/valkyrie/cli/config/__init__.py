import click


@click.group()
def config():
    """Config command group"""
    pass


__all__ = ["config"]
