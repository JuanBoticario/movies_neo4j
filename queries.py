DELETE_NAMES_QUERY = "MATCH (n: Name) DETACH DELETE n"

DELETE_TITLES_QUERY = "MATCH (t: Title) DETACH DELETE t"

DELETE_GENRES_QUERY = "MATCH (g: Genre) DETACH DELETE g"

DELETE_EPISODES_QUERY = "MATCH (e: Episode) DETACH DELETE e"

DELETE_SEASONS_QUERY = "MATCH (s: Season) DETACH DELETE s"

COUNT_GENRES_QUERY = "MATCH (g: Genre) RETURN COUNT(g)"

INSERT_NAME_QUERY = """
    CREATE (n: Name{ 
        id: $nconst, 
        name: $primaryName, 
        birth_year: toInteger($birthYear), 
        death_year: CASE WHEN $deathYear <> '\\N' THEN toInteger($deathYear) ELSE NULL END 
    });
    """


CREATE_NAME_QUERY = """
    MERGE (n: Name { id: $nconst })
    ON CREATE
    SET n.name = $primaryName,
        n.birth_year = toInteger($birthYear),
        n.death_year = CASE 
            WHEN $deathYear <> '\\N' THEN toInteger($deathYear) 
            ELSE NULL 
        END
    ON MATCH 
    SET n.last_dump = timestamp()
    RETURN n.last_dump;
    """

INSERT_TITLE_QUERY = """
    CREATE (t:Title { 
        id: $tconst, 
        title: $primaryTitle, 
        start_year: toInteger($startYear), 
        end_year: CASE WHEN $endYear <> '\\N' THEN toInteger($endYear) ELSE NULL END 
    });
    """

CREATE_EPISODE_QUERY = """
    CREATE (e:Episode {
        id: $tconst,
        episode_number: CASE WHEN $episodeNumber <> '\\N' THEN toInteger($episodeNumber) ELSE NULL END
    });
"""

INSERT_GENRE_QUERY = """
    CREATE (g: Genre {
        id: $gconst,
        name: $name
    });
    """

CREATE_SEASON_QUERY = """
    CREATE (s:Season {
        id: $seasonId,
        season_number: $seasonNumber
})
RETURN s;
"""

LINK_SEASON_TO_TITLE_QUERY = """
MATCH (t:Title {id: $parentTconst}), (s:Season {id: $seasonId})
MERGE (t)-[:HAS_SEASON]->(s);
"""

LINK_EPISODE_TO_SEASON_QUERY = """
MATCH (s:Season {id: $seasonId}), (e:Episode {id: $tconst})
MERGE (s)-[:HAS_EPISODE]->(e);
"""

INSERT_ACTED_IN_QUERY = """
    MATCH (n:Name {id: $nconst}), (t:Title {id: $tconst})
    MERGE (n)-[:ACTED_IN]->(t);
    """

INSERT_TITLE_GENRES_QUERY = """
    MATCH(t:Title {id: $tconst}), (g:Genre {id: $gconst})
    MERGE (t)-[:IS_GENRE]->(g);
    """

INSERT_TITLE_EPISODES_QUERY = """
    MATCH(e: Episode {id: $tconst}), (t: Title {id: $ttconst})
    MERGE (e)-[:BELONGS_TO]->(t);
    """
DEMO_DIRECTED_INSERT = """
    MATCH(n: Name {name: $primaryName}), (t:Title {title: $primaryTitle} 
    CREATE (n)-[d:DIRECTED]->(t)
    RETURN d, n.id, t.id;
    """

DEMO_DIRECTED_REMOVE = """
    MATCH(n: Name {id: $name_id}), (t:Title {id: $title_id})

                """
