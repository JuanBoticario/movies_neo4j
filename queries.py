DELETE_DB_QUERY = "MATCH (n) DETACH DELETE n"

INSERT_NAME_QUERY = """
    CREATE (n: Name{ 
        id: $nconst, 
        name: $primaryName, 
        birth_year: toInteger($birthYear), 
        death_year: CASE WHEN $deathYear <> '\\N' THEN toInteger($deathYear) ELSE NULL END 
    });
    """

INSERT_TITLE_QUERY = """
    CREATE (t:Title { 
        id: $tconst, 
        title: $primaryTitle, 
        start_year: toInteger($startYear), 
        end_year: CASE WHEN $endYear <> '\\N' THEN toInteger($endYear) ELSE NULL END, 
        genres: split($genres, ',') 
    });
    """

INSERT_PRINCIPALS_QUERY = """
    MATCH (n:Name {id: $nconst}), (t:Title {id: $tconst})
    CREATE (n)-[:ACTED_IN]->(t);
    """

INDEX_NAME_ID = """
    CREATE INDEX idx_name_id FOR (n:Name) ON (n.id)
    """

INDEX_TITLE_ID = """
    CREATE INDEX idx_title_id FOR (t:Title) ON (t.id)
    """
