from neo4j import GraphDatabase


def execute_query(query: str, params=None):
    uri = "neo4j://localhost:7687"
    username = "neo4j"
    password = "password"
    database = "neo4j"

    with GraphDatabase.driver(uri, auth=(username, password)) as driver:
        records, summary, keys = driver.execute_query(query, params, database=database)
        return records, summary, keys


def get_unique_genres(data):
    unique_genres = set()

    for row in data:
        genres = row.get("genres")
        if genres and genres != "\\N":
            unique_genres.update(genres.split(","))

    return unique_genres
