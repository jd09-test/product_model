"""
MCP server exposing direct Oracle DB PGQL/SQL queries via oracledb.

Usage:
  python3 oracle_pgql_mcp.py

Exposes one tool: run_pgql_query
Inputs:
    - graph_ref: The graph name/reference (string) to use in GRAPH_TABLE queries
    - query:     The SQL/PGQL query to execute (string)

Returns:
    - List of rows (as dicts) or error message(s)
"""

import json
import time
import traceback
from collections import OrderedDict
from pathlib import Path
from typing import List, Dict, Any

from mcp.server.fastmcp import FastMCP

CONFIG_PATH = Path(__file__).resolve().parent / "config.json"
with CONFIG_PATH.open() as config_file:
    CONFIG = json.load(config_file)

mcp = FastMCP("oraclePgqlTools")

def get_conn():
    import oracledb
    return oracledb.connect(
        user            = CONFIG["PGQL_USER"],
        password        = CONFIG["PGQL_PASSWORD"],
        dsn             = CONFIG["PGQL_DSN"],
        config_dir      = CONFIG["PGQL_CONFIG_DIR"],
        wallet_location = CONFIG["PGQL_WALLET_LOCATION"],
        wallet_password = CONFIG["PGQL_WALLET_PASSWORD"],
    )

@mcp.tool()
def run_pgql_query(graph_ref: str, query: str) -> List[Dict[str, Any]]:
    """
    Run an arbitrary PGQL/SQL query against the Oracle property graph using GRAPH_TABLE.
    Inputs:
        - graph_ref: e.g. '"catalog_graph"' or other graph names
        - query:     The SQL/PGQL statement (should use {graph_ref} in GRAPH_TABLE)
    Returns:
        - List of row dicts, or error explanation(s)
    """
    conn = None
    cursor = None
    try:
        conn = get_conn()
        cursor = conn.cursor()
        # If the query is templated, substitute the graph_ref in
        formatted_query = query.format(graph_ref=graph_ref)
        t0 = time.time()
        cursor.execute(formatted_query)
        elapsed = round(time.time() - t0, 2)
        cols = [d[0] for d in cursor.description]
        rows = cursor.fetchall()
        result = []
        for row in rows:
            result.append(dict(zip(cols, [str(c) for c in row])))
        return [{"elapsed": elapsed, "columns": cols, "rows": result}]
    except Exception as exc:
        tb = traceback.format_exc()
        return [{"error": str(exc), "traceback": tb}]
    finally:
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()


@mcp.tool()
def get_vertex_label_details(graph_name: str) -> List[Dict[str, Any]]:
    """Return catalog metadata for every vertex label in the given graph.

    The output groups properties under each vertex label so the payload stays compact
    when sending to an LLM.
    """

    sql = """
        SELECT
            l.label_name AS vertex_label,
            lp.property_name
        FROM user_pg_labels l
        JOIN user_pg_label_properties lp
          ON l.graph_name = lp.graph_name
         AND l.label_name = lp.label_name
        WHERE l.graph_name = :graph_name
        ORDER BY l.label_name, lp.property_name
    """

    conn = None
    cursor = None
    try:
        conn = get_conn()
        cursor = conn.cursor()
        cursor.execute(sql, graph_name=graph_name)

        grouped: "OrderedDict[str, List[Dict[str, Any]]]" = OrderedDict()
        cols = [d[0] for d in cursor.description]

        for row in cursor:
            record = dict(zip(cols, row))
            label = record.pop("VERTEX_LABEL")
            grouped.setdefault(label, []).append(record)

        return [
            {"vertex_label": label, "properties": props}
            for label, props in grouped.items()
        ]
    except Exception as exc:
        tb = traceback.format_exc()
        return [{"error": str(exc), "traceback": tb}]
    finally:
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()


@mcp.tool()
def get_filtered_vertex_label_details(graph_name: str, vertex_labels: List[str]) -> List[Dict[str, Any]]:
    """Return catalog metadata for a selected list of vertex labels in the given graph."""

    if not vertex_labels:
        return [{"error": "vertex_labels list must not be empty", "traceback": ""}]

    placeholders = ", ".join([f":label_{i}" for i in range(len(vertex_labels))])
    sql = f"""
        SELECT
            l.label_name AS vertex_label,
            lp.property_name
        FROM user_pg_labels l
        JOIN user_pg_label_properties lp
          ON l.graph_name = lp.graph_name
         AND l.label_name = lp.label_name
        WHERE l.graph_name = :graph_name
          AND l.label_name IN ({placeholders})
        ORDER BY l.label_name, lp.property_name
    """

    params = {"graph_name": graph_name}
    params.update({f"label_{i}": label for i, label in enumerate(vertex_labels)})

    conn = None
    cursor = None
    try:
        conn = get_conn()
        cursor = conn.cursor()
        cursor.execute(sql, params)

        grouped: "OrderedDict[str, List[Dict[str, Any]]]" = OrderedDict()
        cols = [d[0] for d in cursor.description]

        for row in cursor:
            record = dict(zip(cols, row))
            label = record.pop("VERTEX_LABEL")
            grouped.setdefault(label, []).append(record)

        return [
            {"vertex_label": label, "properties": props}
            for label, props in grouped.items()
        ]
    except Exception as exc:
        tb = traceback.format_exc()
        return [{"error": str(exc), "traceback": tb}]
    finally:
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()


@mcp.tool()
def get_edge_relationship_details(graph_name: str) -> List[Dict[str, Any]]:
    """Return join metadata for every edge table in the given graph."""

    sql = """
        SELECT
            edge_tab_name AS edge_table,
            MAX(CASE WHEN edge_end = 'SOURCE' THEN vertex_tab_name END) AS source_vertex_table,
            MAX(CASE WHEN edge_end = 'SOURCE' THEN vertex_col_name END) AS source_vertex_column,
            MAX(CASE WHEN edge_end = 'DESTINATION' THEN vertex_tab_name END) AS target_vertex_table,
            MAX(CASE WHEN edge_end = 'DESTINATION' THEN vertex_col_name END) AS target_vertex_column
        FROM user_pg_edge_relationships
        WHERE graph_name = :graph_name
        GROUP BY edge_tab_name
        ORDER BY edge_tab_name
    """

    conn = None
    cursor = None
    try:
        conn = get_conn()
        cursor = conn.cursor()
        cursor.execute(sql, graph_name=graph_name)

        cols = [d[0] for d in cursor.description]
        rows = cursor.fetchall()
        return [dict(zip(cols, row)) for row in rows]
    except Exception as exc:
        tb = traceback.format_exc()
        return [{"error": str(exc), "traceback": tb}]
    finally:
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()


@mcp.tool()
def get_filtered_edge_relationship_details(graph_name: str, edge_tables: List[str]) -> List[Dict[str, Any]]:
    """Return join metadata for a specific list of edge tables in the given graph."""

    if not edge_tables:
        return [{"error": "edge_tables list must not be empty", "traceback": ""}]

    placeholders = ", ".join([f":edge_{i}" for i in range(len(edge_tables))])
    sql = f"""
        SELECT
            edge_tab_name AS edge_table,
            MAX(CASE WHEN edge_end = 'SOURCE' THEN vertex_tab_name END) AS source_vertex_table,
            MAX(CASE WHEN edge_end = 'SOURCE' THEN vertex_col_name END) AS source_vertex_column,
            MAX(CASE WHEN edge_end = 'DESTINATION' THEN vertex_tab_name END) AS target_vertex_table,
            MAX(CASE WHEN edge_end = 'DESTINATION' THEN vertex_col_name END) AS target_vertex_column
        FROM user_pg_edge_relationships
        WHERE graph_name = :graph_name
          AND edge_tab_name IN ({placeholders})
        GROUP BY edge_tab_name
        ORDER BY edge_tab_name
    """

    params = {"graph_name": graph_name}
    params.update({f"edge_{i}": name for i, name in enumerate(edge_tables)})

    conn = None
    cursor = None
    try:
        conn = get_conn()
        cursor = conn.cursor()
        cursor.execute(sql, params)

        cols = [d[0] for d in cursor.description]
        rows = cursor.fetchall()
        return [dict(zip(cols, row)) for row in rows]
    except Exception as exc:
        tb = traceback.format_exc()
        return [{"error": str(exc), "traceback": tb}]
    finally:
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()

if __name__ == "__main__":
    mcp.run()