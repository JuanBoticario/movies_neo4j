from neo4j import GraphDatabase, basic_auth

def execute_query(query: str, params = None):
    
    uri = "neo4j://localhost:7687"
    username = "neo4j"
    password = "password"
    database = "neo4j"
    
    with GraphDatabase.driver(uri, auth=(username, password)) as driver:
        records, summary, keys = driver.execute_query(query, params, database=database)
        return records, summary, keys

def close_driver():
    """Closes the Neo4j driver."""
    driver.close()

def get_unique_genres(data):
    unique_genres = set()  # Use a set to store unique genres

    # Iterate over each row and extract genres
    for row in data:
        genres = row.get('genres')
        if genres and genres != '\\N':  # Check if 'genres' is not empty or null placeholder
            # Split genres by comma and add to set
            unique_genres.update(genres.split(','))

    return unique_genres
