DELETE_DB_QUERY = "MATCH (n) DETACH DELETE n"

INSERT_ACTOR_QUERY = """
    CREATE (p:Person { 
        id: $nconst, 
        name: $primaryName, 
        birth_year: toInteger($birthYear), 
        death_year: CASE WHEN $deathYear <> '\\N' THEN toInteger($deathYear) ELSE NULL END 
    });
    """

INSERT_MOVIE_QUERY = """
    CREATE (m:Movie { 
        id: $tconst, 
        title: $primaryTitle, 
        start_year: toInteger($startYear), 
        end_year: CASE WHEN $endYear <> '\\N' THEN toInteger($endYear) ELSE NULL END, 
        genres: split($genres, ',') 
    });
    """

RELATION_ACTOR_MOVIE_QUERY = """
    MATCH (p:Person {id: $nconst}), (m:Movie {id: $tconst})
    CREATE (p)-[:ACTED_IN {category: $category, job: $job, characters: split($characters, ',')}]->(m);
    """

