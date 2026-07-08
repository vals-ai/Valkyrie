import click


@click.group()
def benchmark():
    """Benchmark command group"""
    pass


__all__ = ["benchmark"]
