import click
import csv
from tqdm import tqdm
from queries import *
from concurrent.futures import ThreadPoolExecutor
from utils import execute_query, get_unique_genres

@click.command()
@click.option('--threads', default=4, help='Number of threads to use for import')
@click.option('--percentage', default=2, type=float, help='Percentage of the file to process')
def dump(threads, percentage):
    """
    Dumps data into database.
    Usage: python3 main.py dump --threads 4 --percentage 2
    """

    def name_params(row):
        return {
            "nconst": row["nconst"],
            "primaryName": row["primaryName"],
            "birthYear": row["birthYear"],
            "deathYear": row["deathYear"]
        }

    def title_params(row):
        return {
            "tconst": row["tconst"],
            "primaryTitle": row["primaryTitle"],
            "startYear": row["startYear"],
            "endYear": row["endYear"]
        }

    def acted_in_params(row):
        return {
            "nconst": row['nconst'],
            "tconst": row['tconst']
        }     

     
    def season_params(row):
        return {
            "seasonId": row['seasonId'],
            "seasonNumber": row['seasonNumber']
        }

    def season_to_title_params(row):
        return {
            "seasonId": row['seasonId'],
            "parentTconst": row['parentTconst']
        }

    def episode_to_season_params(row):
        return {
            "seasonId": row['seasonId'],
            "tconst": row['tconst']
        }


    def episode_params(row):
        return {
            "tconst": row['tconst'],
            "episodeNumber": row.get('episodeNumber')
        }

    def get_names_and_titles():

        file_path = "data/name.basics.tsv"
        names = []
        titles_set = set()  
        acted_in_relation = []

        click.echo("Loading Names and Titles into memory...")
        with open(file_path, 'r', encoding='utf-8') as file:
            reader = list(csv.DictReader(file, delimiter='\t'))
            for i, row in enumerate(reader):

                if i >= percentage/100*len(reader):  # Stop after the first 1000 names
                    break

                kft = row.get('knownForTitles', '')
                nconst = row.get('nconst')

                names.append(name_params(row))

                if kft:
                    for tconst in kft.split(','):
                        titles_set.add(tconst)
                        acted_in_relation.append({'nconst': nconst, 'tconst': tconst})

        return names, titles_set, acted_in_relation

    def load_episodes(titles_set):

        file_path = "data/title.episode.tsv"

        episodes = []

        with open(file_path, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file, delimiter='\t')

            for row in reader:
                if row['parentTconst'] in titles_set:
                    season_id = f"{row['parentTconst']}_S{row['seasonNumber']}"
                    episodes.append({
                                    "tconst": row['tconst'],
                                    "parentTconst": row['parentTconst'],
                                    "seasonNumber": row.get('seasonNumber', '\\N'),
                                    "episodeNumber": row.get('episodeNumber', '\\N'),
                                    "seasonId": season_id
                })

        return episodes

    def filter_titles_by_ids(titles_set):
        file_path = "data/title.basics.tsv"
        filtered_titles = []

        with open(file_path, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file, delimiter='\t')
            for row in reader:
                if row['tconst'] in titles_set:
                    filtered_titles.append(row)

        return filtered_titles

    genre_name_to_id = {}

    def import_genres(genres):
        click.echo("Dumping Genres into database...")
        for i, genre in enumerate(genres):
            execute_query(INSERT_GENRE_QUERY, {"gconst": i,
                          "name": genre})
            genre_name_to_id[genre] = i 

    def insert_batch(data, query, params_mapper, progress_bar, dump_title_genres):
        for row in data:
            execute_query(query, params_mapper(row))
            if dump_title_genres:
                genres = row.get('genres').split(',')
                for genre in genres:
                    genre_id = genre_name_to_id.get(genre)
                    execute_query(INSERT_TITLE_GENRES_QUERY, {"tconst": row.get('tconst'),
                                                              "gconst": genre_id})

            progress_bar.update(1)

    def execute_in_batches(data, query, params_mapper, total, dump_title_genres=False):
        batch_size = 1000
        with tqdm(total=total, desc="Progress", unit="records") as progress_bar:
            batches = [data[i:i + batch_size] for i in range(0, len(data), batch_size)]
            with ThreadPoolExecutor(max_workers=threads) as executor:
                for batch in batches:
                    executor.submit(insert_batch, batch, query, params_mapper, progress_bar, dump_title_genres)

    names, titles_set, acted_in_relation = get_names_and_titles()

    titles = filter_titles_by_ids(titles_set)

    episodes = load_episodes(titles_set)

    click.echo("Dumping Names into database...")
    execute_in_batches(names, CREATE_NAME_QUERY, name_params, len(names))

    unique_genres = get_unique_genres(titles)
    import_genres(unique_genres)

    click.echo("Dumping Titles into database...")
    execute_in_batches(titles, INSERT_TITLE_QUERY, title_params, len(titles), dump_title_genres=True)

    click.echo("Creating Season nodes...")
    seasons = list({e['seasonId']: e for e in episodes}.values())
    execute_in_batches(seasons, CREATE_SEASON_QUERY, season_params, len(seasons))

    click.echo("Linking Seasons to Titles...")
    execute_in_batches(seasons, LINK_SEASON_TO_TITLE_QUERY, season_to_title_params, len(seasons))

    click.echo("Creating Episode nodes...")
    execute_in_batches(episodes, CREATE_EPISODE_QUERY, episode_params, len(episodes))

    click.echo("Linking Episodes to Seasons...")
    execute_in_batches(episodes, LINK_EPISODE_TO_SEASON_QUERY, episode_to_season_params, len(episodes))

    click.echo("Linking Names to Titles...")
    execute_in_batches(acted_in_relation, INSERT_ACTED_IN_QUERY, acted_in_params, len(acted_in_relation))

    click.echo("Finished.")
