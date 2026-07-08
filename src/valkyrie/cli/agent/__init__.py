import click


@click.group()
def agent():
    """Agent command group"""
    pass


__all__ = ["agent"]
