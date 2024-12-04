import click
from dump import dump
from clear import clear
from demo import demo

@click.group()
def cli():
    pass

cli.add_command(dump)
cli.add_command(clear)
cli.add_command(demo)

if __name__ == '__main__':
    cli()

