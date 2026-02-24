"""
Oracle Property Graph MCP Server
=================================
Exposes Oracle property graph (PGQL) capabilities to LLMs via the MCP protocol.

Tools available:
  1. query               — Run any SQL/PGQL query against a named graph
  2. schema_vertices      — Get all vertex labels and their properties for a graph
  3. schema_vertices_filter — Get properties for a specific subset of vertex labels
  4. schema_edges         — Get all edge labels with their source/target join columns
  5. schema_edges_filter  — Get join metadata for a specific subset of edge labels

Setup:
  - Requires config.json in the same directory with Oracle DB credentials
  - Run:  python3 oracle_pgql_mcp.py
"""

import json
import time
import traceback
from collections import OrderedDict
from pathlib import Path
from typing import Any

from mcp.server.fastmcp import FastMCP

# ── Config ────────────────────────────────────────────────────────────────────

CONFIG_PATH = Path(__file__).resolve().parent / "config.json"
with CONFIG_PATH.open() as f:
    CONFIG = json.load(f)

# ── MCP Server ────────────────────────────────────────────────────────────────

mcp = FastMCP("OracleGraph")

# ── DB Connection ─────────────────────────────────────────────────────────────

def _get_conn():
    """Open and return a fresh Oracle DB connection using config.json credentials."""
    import oracledb
    return oracledb.connect(
        user            = CONFIG["26AI_USER"],
        password        = CONFIG["26AI_PASSWORD"],
        dsn             = CONFIG["26AI_DSN"],
        config_dir      = CONFIG["26AI_CONFIG_DIR"],
        wallet_location = CONFIG["26AI_WALLET_LOCATION"],
        wallet_password = CONFIG["26AI_WALLET_PASSWORD"],
    )


def _run_sql(sql: str, params: dict = None) -> list[dict[str, Any]]:
    """
    Internal helper: execute a SQL statement and return rows as a list of dicts.
    Handles connection lifecycle and error reporting.
    """
    conn = cursor = None
    try:
        conn = _get_conn()
        cursor = conn.cursor()
        t0 = time.time()
        cursor.execute(sql, params or {})
        elapsed = round(time.time() - t0, 2)
        cols = [d[0] for d in cursor.description]
        rows = [dict(zip(cols, [str(c) if c is not None else None for c in row]))
                for row in cursor.fetchall()]
        return [{"elapsed": elapsed, "columns": cols, "rows": rows}]
    except Exception as exc:
        return [{"error": str(exc), "traceback": traceback.format_exc()}]
    finally:
        if cursor: cursor.close()
        if conn:   conn.close()


def _group_by_label(results: list[dict], label_key: str, drop_key: str) -> list[dict]:
    """
    Internal helper: group a flat list of {label, property} rows into
    [{label: ..., properties: [...]}, ...] format for compact LLM consumption.
    """
    grouped: OrderedDict[str, list] = OrderedDict()
    for record in results:
        label = record.pop(label_key)
        grouped.setdefault(label, []).append(record)
    return [{drop_key: label, "properties": props} for label, props in grouped.items()]


# ── Tools ─────────────────────────────────────────────────────────────────────

@mcp.tool()
def query(graph_name: str, sql: str) -> list[dict[str, Any]]:
    """
    Execute a SQL/PGQL query against a named Oracle property graph.

    Use this tool to query graph data using Oracle's GRAPH_TABLE syntax.
    The query must follow Oracle SQL/PGQL conventions and reference the correct
    graph name inside the GRAPH_TABLE(...) clause.

    Parameters:
        graph_name : The name of the graph to query, e.g. "product_graph".
                     This is substituted into the query if you use {graph_name}
                     as a placeholder inside the GRAPH_TABLE clause.
        sql        : A complete SQL statement using GRAPH_TABLE syntax.
                     Example:
                       SELECT *
                       FROM GRAPH_TABLE("product_graph"
                           MATCH (p IS PRODUCTVOD)
                           COLUMNS(p.VOD_NAME AS name)
                       )

    Returns:
        A list with one dict containing:
          - elapsed  : Query execution time in seconds
          - columns  : List of returned column names
          - rows     : List of row dicts, each mapping column name → value
        On error, returns a dict with "error" and "traceback" keys.

    Important rules when writing queries:
        - Always use IS to apply labels:  (n IS SOMELABEL)
        - Always use a COLUMNS(...) clause — it is required
        - Use CURRENT_VERSION_FLAG = 'Y' to target the latest version of any VOD
        - Apply version range filters on child nodes:
            FIRST_VERSION <= version_number AND (LAST_VERSION IS NULL OR version_number <= LAST_VERSION)
        - For ObjectRelationship, always filter SUB_OBJECT_TYPE_CODE:
            'Product'  → product-to-product links  (join on SUB_OBJECT_PRODUCT_ID)
            'Port'     → class port links           (join on SUB_OBJECT_CLASS_ID)
            'DynPort'  → dynamic class links        (join on SUB_OBJECT_CLASS_ID)
        - Domain traversal (via OBJECTRELATIONSHIPDOMAIN) is only valid for 'Port' or 'DynPort'
        - For usage/inclusion queries (who uses product X), always use UNION of two
          separate queries — one for direct relationships, one for domain relationships.
          Never use OPTIONAL MATCH or combine them in a single query branch.
        - Never invent property names. Use schema_vertices or schema_edges to confirm
          property names before writing a query.
        - Column aliases declared in COLUMNS(...) must exactly match any alias
          referenced in the outer WHERE clause to avoid ORA-00904 errors.
    """
    formatted = sql.format(graph_name=graph_name)
    return _run_sql(formatted)


@mcp.tool()
def schema_vertices(graph_name: str) -> list[dict[str, Any]]:
    """
    Return all vertex labels and their property names for a given graph.

    Use this tool before writing any query to discover what vertex types exist
    in the graph and what properties are available on each. This prevents
    ORA-00904 errors caused by referencing non-existent property names.

    Parameters:
        graph_name : The name of the graph, e.g. "product_graph".

    Returns:
        A list of dicts, one per vertex label, each in the form:
          {
            "vertex_label": "PRODUCTVOD",
            "properties": [
              {"PROPERTY_NAME": "ROW_ID"},
              {"PROPERTY_NAME": "VOD_NAME"},
              ...
            ]
          }

    Tip: If you only need a subset of labels, use schema_vertices_filter instead
    to keep the response compact.
    """
    sql = """
        SELECT l.label_name AS vertex_label, lp.property_name
        FROM   user_pg_labels l
        JOIN   user_pg_label_properties lp
               ON  l.graph_name = lp.graph_name
               AND l.label_name = lp.label_name
        WHERE  l.graph_name = :graph_name
        ORDER  BY l.label_name, lp.property_name
    """
    result = _run_sql(sql, {"graph_name": graph_name})
    if "error" in result[0]:
        return result
    rows = result[0]["rows"]
    return _group_by_label(rows, "VERTEX_LABEL", "vertex_label")


@mcp.tool()
def schema_vertices_filter(graph_name: str, vertex_labels: list[str]) -> list[dict[str, Any]]:
    """
    Return property names for a specific subset of vertex labels in a graph.

    Use this tool when you already know which vertex types you need and want
    a compact response instead of fetching the entire graph schema.

    Parameters:
        graph_name    : The name of the graph, e.g. "product_graph".
        vertex_labels : A list of vertex label names to look up, e.g.
                        ["PRODUCTVOD", "VODVERSION", "OBJECTRELATIONSHIP"].
                        Labels are case-sensitive and must match exactly.

    Returns:
        Same structure as schema_vertices but limited to the requested labels:
          [
            {
              "vertex_label": "PRODUCTVOD",
              "properties": [{"PROPERTY_NAME": "ROW_ID"}, ...]
            },
            ...
          ]
        Labels not found in the graph are silently omitted.
    """
    if not vertex_labels:
        return [{"error": "vertex_labels must not be empty", "traceback": ""}]

    placeholders = ", ".join(f":label_{i}" for i in range(len(vertex_labels)))
    sql = f"""
        SELECT l.label_name AS vertex_label, lp.property_name
        FROM   user_pg_labels l
        JOIN   user_pg_label_properties lp
               ON  l.graph_name = lp.graph_name
               AND l.label_name = lp.label_name
        WHERE  l.graph_name = :graph_name
          AND  l.label_name IN ({placeholders})
        ORDER  BY l.label_name, lp.property_name
    """
    params = {"graph_name": graph_name}
    params.update({f"label_{i}": v for i, v in enumerate(vertex_labels)})

    result = _run_sql(sql, params)
    if "error" in result[0]:
        return result
    return _group_by_label(result[0]["rows"], "VERTEX_LABEL", "vertex_label")


@mcp.tool()
def schema_edges(graph_name: str) -> list[dict[str, Any]]:
    """
    Return all edge labels with their source and target vertex join columns for a graph.

    Use this tool to discover how vertex types are connected and — critically —
    which property columns must be used to join source and target vertices across
    each edge. Always consult this before writing MATCH patterns to avoid
    using wrong join keys.

    Parameters:
        graph_name : The name of the graph, e.g. "product_graph".

    Returns:
        A list of dicts, one per edge, each in the form:
          {
            "EDGE_TABLE"           : "PRODUCTVOD_HAS_VERSION_VODVERSION",
            "SOURCE_VERTEX_TABLE"  : "PRODUCTVOD",
            "SOURCE_VERTEX_COLUMN" : "ROW_ID",
            "TARGET_VERTEX_TABLE"  : "VODVERSION",
            "TARGET_VERTEX_COLUMN" : "VOD_ID"
          }

    Critical rule: Always use the exact SOURCE_VERTEX_COLUMN and TARGET_VERTEX_COLUMN
    values shown here when writing join conditions in PGQL. Never guess join keys.

    Tip: If you only need metadata for specific edges, use schema_edges_filter
    to keep the response compact.
    """
    sql = """
        SELECT
            edge_tab_name AS edge_table,
            MAX(CASE WHEN edge_end = 'SOURCE'      THEN vertex_tab_name END) AS source_vertex_table,
            MAX(CASE WHEN edge_end = 'SOURCE'      THEN vertex_col_name END) AS source_vertex_column,
            MAX(CASE WHEN edge_end = 'DESTINATION' THEN vertex_tab_name END) AS target_vertex_table,
            MAX(CASE WHEN edge_end = 'DESTINATION' THEN vertex_col_name END) AS target_vertex_column
        FROM   user_pg_edge_relationships
        WHERE  graph_name = :graph_name
        GROUP  BY edge_tab_name
        ORDER  BY edge_tab_name
    """
    return _run_sql(sql, {"graph_name": graph_name})


@mcp.tool()
def schema_edges_filter(graph_name: str, edge_tables: list[str]) -> list[dict[str, Any]]:
    """
    Return source/target join metadata for a specific subset of edge labels in a graph.

    Use this tool when you already know which edges you need and want a compact
    response. Especially useful when writing multi-hop MATCH patterns and you
    need to verify join columns for just a few edges.

    Parameters:
        graph_name  : The name of the graph, e.g. "product_graph".
        edge_tables : A list of edge table names to look up, e.g.
                      ["PRODUCTVOD_HAS_RELATIONSHIP_OBJECTRELATIONSHIP",
                       "OBJECTRELATIONSHIP_HAS_RELATIONSHIP_DOMAIN_OBJECTRELATIONSHIPDOMAIN"].
                      Names are case-sensitive and must match exactly.

    Returns:
        Same structure as schema_edges but limited to the requested edge tables:
          [
            {
              "EDGE_TABLE"           : "PRODUCTVOD_HAS_RELATIONSHIP_OBJECTRELATIONSHIP",
              "SOURCE_VERTEX_TABLE"  : "PRODUCTVOD",
              "SOURCE_VERTEX_COLUMN" : "ROW_ID",
              "TARGET_VERTEX_TABLE"  : "OBJECTRELATIONSHIP",
              "TARGET_VERTEX_COLUMN" : "VOD_ID"
            },
            ...
          ]
        Edge tables not found in the graph are silently omitted.
    """
    if not edge_tables:
        return [{"error": "edge_tables must not be empty", "traceback": ""}]

    placeholders = ", ".join(f":edge_{i}" for i in range(len(edge_tables)))
    sql = f"""
        SELECT
            edge_tab_name AS edge_table,
            MAX(CASE WHEN edge_end = 'SOURCE'      THEN vertex_tab_name END) AS source_vertex_table,
            MAX(CASE WHEN edge_end = 'SOURCE'      THEN vertex_col_name END) AS source_vertex_column,
            MAX(CASE WHEN edge_end = 'DESTINATION' THEN vertex_tab_name END) AS target_vertex_table,
            MAX(CASE WHEN edge_end = 'DESTINATION' THEN vertex_col_name END) AS target_vertex_column
        FROM   user_pg_edge_relationships
        WHERE  graph_name = :graph_name
          AND  edge_tab_name IN ({placeholders})
        GROUP  BY edge_tab_name
        ORDER  BY edge_tab_name
    """
    params = {"graph_name": graph_name}
    params.update({f"edge_{i}": e for i, e in enumerate(edge_tables)})
    return _run_sql(sql, params)


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    mcp.run()