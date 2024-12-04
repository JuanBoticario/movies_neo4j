import click
import random
from utils import execute_query
from queries import DEMO_GET_NAMES 

@click.command()
def demo():
    records, summary, keys = execute_query(DEMO_GET_NAMES)

    if records:
        random_name = random.choice(records)
        name_id = random_name["n.id"]  
        name = random_name["n.name"]  

        print(f"Selected Random Name: {name} (ID: {name_id})")

        non_existent_title_id = "tt999999999"  

        NON_EXISTENT_RELATION_QUERY = """
        MATCH (n:Name {id: $name_id}),
        (t:Title {id: $title_id})  
        CREATE (n)-[d:DIRECTED]->(t);
        """
        records, summary, keys = execute_query(
            NON_EXISTENT_RELATION_QUERY, {"name_id": name_id,
                                          "title_id": non_existent_title_id}
        )
        
        if not summary.counters.relationships_created:
            print(f"No relationship created between {name} and Title with non existend id {non_existent_title_id}")

        year1 = 1900
        year2 = 2000
        
        GET_NAME_TITLES_BETWEEN_YEARS = """
            MATCH (n:Name {id: $name_id})-[d:ACTED_IN]->(t:Title)
            WHERE $start_year < t.start_year < $start_year_2 
            RETURN t.title AS name, t.id AS id, t.title AS title, t.start_year AS year;
        """

        records, summary, keys = execute_query(
            GET_NAME_TITLES_BETWEEN_YEARS, {"name_id": name_id,
                                            "start_year": year1,
                                            "start_year_2": year2})

        if not records: 
            print(f"{name} has not acted in Titles that were initiated between {year1} and {year2}.")
            return 
        
        print(f"Titles acted in by {name} between {year1} and {year2}:")
        for record in records:
            print(f"- {record['title']} ({record['year']})")

        
        random_title = random.choice(records)
        title_id = random_title['id']
        title_name = random_title['name']

        print(f"Selected Random Title: {title_name} (ID: {title_id})")

        print(f"Creating relation {title_name} DIRECTED_BY {name}.")

        TITLE_DIRECTED_BY = """
            MERGE (t: Title {id: $title_id})<-[d:DIRECTED_BY]-(n: Name {id: $name_id})
            ON CREATE 
            SET d.create_timestamp = timestamp()
            ON MATCH
            SET d.update_timestamp = timestamp()
            RETURN d.create_timestamp AS create_timestamp, d.update_timestamp AS update_timestamp;
            """

        for i in range(0,2):
            records, summary, keys = execute_query(
                    TITLE_DIRECTED_BY, {"title_id": title_id,
                                        "name_id": name_id})
            if summary.counters.relationships_created:
                print("Relation created for the first time.")
            else:
                print("Relation updated.")

            for record in records:
                print(f"CREATION DATE: {record[keys[0]]}\nLAST_UPDATED: {record[keys[1]]}")

        print("Cleaning up...")
        CLEAN_DIRECTED_BY = """
            MATCH (t: Title)<-[d:DIRECTED_BY]-(n: Name {id: $name_id}) DELETE d;
            """

        records, summary, keys = execute_query(CLEAN_DIRECTED_BY, {"name_id": name_id})
        if summary.counters.relationships_deleted:
            print("Relation deleted.")
            print(f"Time to Consume Result: {summary.result_consumed_after}")
            
    else:
        print("No Names in database")
