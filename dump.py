import click
import csv
import os
from tqdm import tqdm
from queries import *
from concurrent.futures import ThreadPoolExecutor
from utils import execute_query


# Define paths for the TSV files
DATA_DIR = 'data'
DEFAULT_FILES = {
    'actors': os.path.join(DATA_DIR, 'name.basics.tsv'),
    'movies': os.path.join(DATA_DIR, 'title.basics.tsv'),
    'principals': os.path.join(DATA_DIR, 'title.principals.tsv')
}

@click.command()
@click.argument('file_type', type=click.Choice(['actors', 'movies', 'principals', 'all']))
@click.option('--threads', default=4, help='Number of threads to use for import')
@click.option('--percentage', default=2, type=int, help='Percentage of the file to process')
def dump(file_type, threads, percentage):
    """
    Dumps data into database. Use `all` to dump all datasets at once.
    Usage: python3 main.py dump all --threads 4
    """
    # Parameter mappers
    def person_params(row):
        return {
            "nconst": row["nconst"],
            "primaryName": row["primaryName"],
            "birthYear": row["birthYear"],
            "deathYear": row["deathYear"]
        }

    def movie_params(row):
        return {
            "tconst": row["tconst"],
            "primaryTitle": row["primaryTitle"],
            "startYear": row["startYear"],
            "endYear": row["endYear"],
            "genres": row["genres"]
        }

    def relationship_params(row):
        return {
            "tconst": row["tconst"],
            "ordering": row["ordering"],
            "nconst": row["nconst"],
            "category": row["category"],
            "job": row["job"],
            "characters": row["characters"]
        }

    # Load data function
    def load_data(file_path, delimiter='\t'):
        with open(file_path, 'r', encoding='utf-8') as file:
            reader = list(csv.DictReader(file, delimiter=delimiter))
            total_rows = len(reader)
            row_count = int(total_rows * percentage/100)
            click.echo(f"{row_count} rows of a total of {total_rows} will be dumped.")
            return reader[:row_count]
       
    def import_actors():
            click.echo("Loading Actors into memory...")
            name_data = load_data(DEFAULT_FILES['actors'])
            click.echo("Dumping Actors into database...")
            execute_in_batches(name_data, INSERT_ACTOR_QUERY, person_params, len(name_data))

    def import_movies():
            click.echo("Loading movies into memory...")
            title_data = load_data(DEFAULT_FILES['movies'])
            click.echo("Dumping Movies into database...")
            execute_in_batches(title_data, INSERT_MOVIE_QUERY, movie_params, len(title_data))

    def create_relation_actors_movies():
            click.echo("Loading principals into memory...")
            principals_data = load_data(DEFAULT_FILES['principals'])
            click.echo("Dumping principals into database...")
            execute_in_batches(principals_data, RELATION_ACTOR_MOVIE_QUERY, relationship_params, len(principals_data))

    # Insert batch function
    def insert_batch(data, query, params_mapper, progress_bar):
        for row in data:
            execute_query(query, params_mapper(row))
            progress_bar.update(1)

    # Multithreading function
    def execute_in_batches(data, query, params_mapper, total):
        batch_size = 1000
        with tqdm(total=total, desc="Progress", unit="records") as progress_bar:
            batches = [data[i:i + batch_size] for i in range(0, len(data), batch_size)]
            with ThreadPoolExecutor(max_workers=threads) as executor:
                for batch in batches:
                    executor.submit(insert_batch, batch, query, params_mapper, progress_bar)

    if file_type == 'all':
        click.echo("Dumping all data...")

        import_actors()
        import_movies()
        create_relation_actors_movies()

    elif file_type == 'actors':
        import_actors()

    elif file_type == 'movies':
        import_movies()

    elif file_type == 'principals':
        create_relation_actors_movies()

    click.echo("Finished.")
