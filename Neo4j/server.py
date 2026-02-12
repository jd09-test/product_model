import sys
import os

from neo4j import GraphDatabase
from mcp.server.fastmcp import FastMCP
import logging

logging.basicConfig(
    level=logging.ERROR,
    stream=sys.stderr,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

mcp = FastMCP(
    name="neo4jTools",
    host="0.0.0.0",  # only used for SSE transport (localhost)
    port=8000,  # only used for SSE transport (set this to any port))
)

# Neo4j connection details
uri = "neo4j://localhost:7687"
user = "neo4j" 
password = "Asdfg@12345" 

try:
    driver = GraphDatabase.driver(uri, auth=(user, password))
except Exception as e:
    print(f"Failed to connect to Neo4j: {e}", file=sys.stderr)
    sys.exit(1)

# --- Helper Functions ---
def get_node_types_and_properties(tx):
    query = """
    CALL db.schema.nodeTypeProperties()
    YIELD nodeType, propertyName, propertyTypes
    RETURN nodeType, collect({property: propertyName, type: propertyTypes}) AS properties
    ORDER BY nodeType
    """
    return [dict(record) for record in tx.run(query)]

def get_relationship_types_and_properties(tx):
    query = """
    CALL db.schema.relTypeProperties()
    YIELD relType, propertyName, propertyTypes
    RETURN relType, collect({property: propertyName, type: propertyTypes}) AS properties
    ORDER BY relType
    """
    return [dict(record) for record in tx.run(query)]

def get_result(tx, query):
    return [dict(record) for record in tx.run(query)]

# --- Tools ---
# @mcp.tool()
# def write_data(query: str) -> list:
#     try:
#         with driver.session(database="neo4j") as session:
#             result = session.execute_write(get_result, query)
#             return result
#     except Exception as e:
#         return [{"error": str(e)}]

@mcp.tool()
def get_data(query: str) -> list:
    try:
        with driver.session(database="neo4j") as session:
            result = session.execute_read(get_result, query)
            return result
    except Exception as e:
        return [{"error": str(e)}]

@mcp.tool()
def get_nodetypes_properties() -> list:
    try:
        with driver.session(database="neo4j") as session:
            result = session.execute_read(get_node_types_and_properties)
            return result
    except Exception as e:
        return [{"error": str(e)}]

@mcp.tool()
def get_relationshiptypes_properties() -> list:
    try:
        with driver.session(database="neo4j") as session:
            result = session.execute_read(get_relationship_types_and_properties)
            return result
    except Exception as e:
        return [{"error": str(e)}]

if __name__ == "__main__":
    mcp.run(transport="stdio")