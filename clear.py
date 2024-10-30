import click
from queries import DELETE_DB_QUERY
from utils import execute_query

@click.command()
def clear():
    """
    clears all data from the database.
    usage: python3 main.py clear
    """
    execute_query(DELETE_DB_QUERY)
    click.echo("All data has been cleared from database.")
