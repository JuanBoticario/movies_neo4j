from neo4j import GraphDatabase, basic_auth

# Initialize Neo4j driver (import this where needed)
driver = GraphDatabase.driver("bolt://localhost:7687", auth=basic_auth("neo4j", "password"))

def execute_query(query, parameters=None):
    """
    Executes a Cypher query with the provided parameters.
    Manages session creation and error handling.
    """
    parameters = parameters or {}
    try:
        with driver.session() as session:
            session.run(query, parameters)
    except Exception as e:
        print(f"Error executing query: {e}")

def close_driver():
    """Closes the Neo4j driver."""
    driver.close()
