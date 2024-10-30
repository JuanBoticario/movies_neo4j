import click
from dump import dump
from clear import clear

@click.group()
def cli():
    pass

cli.add_command(dump)
cli.add_command(clear)

if __name__ == '__main__':
    cli()

