import click
from queries import DELETE_NAMES_QUERY
from queries import DELETE_TITLES_QUERY
from utils import execute_query

@click.command()
def clear():
    """
    clears all data from the database.
    usage: python3 main.py clear
    """
    execute_query(DELETE_NAMES_QUERY)
    click.echo("All Names have been cleared from database.")
    execute_query(DELETE_TITLES_QUERY)
    click.echo("All Titles have been cleared from database.")
