"""
Oracle Property Graph DDL Generator
=====================================
Reads a graph model JSON file and generates an Oracle 26ai
CREATE PROPERTY GRAPH DDL statement. Optionally executes the DDL
against a target Oracle database after user confirmation.

Usage:
    python3 oracle_graph_ddl_generator.py \\
        --graph_model graph_model.json \\
        --ddl_output    property_graph_schema.sql \\
        --graph_name  my_graph \\
        --config      config.json

Expected graph_model.json structure:
    {
        "nodes": [
            {
                "name": "PRODUCTVOD",
                "label": "PRODUCTVOD",           # optional, falls back to name
                "properties": {"alias": "COL"},  # map of display-name → DB column
                "filter": "...",                  # optional WHERE clause
                "table": ["TABLE_NAME"]           # underlying DB table(s)
            }
        ],
        "relationships": [
            {
                "type":     "PRODUCTVOD_HAS_VERSION_VODVERSION",
                "from":     "PRODUCTVOD",
                "to":       "VODVERSION",
                "from_key": "ROW_ID",
                "to_key":   "VOD_ID"
            }
        ]
    }

Expected config.json keys:
    26AI_USER, 26AI_PASSWORD, 26AI_DSN,
    26AI_CONFIG_DIR, 26AI_WALLET_LOCATION, 26AI_WALLET_PASSWORD
"""

import json
import logging
import re
from typing import Dict, List, Tuple
from pathlib import Path

import oracledb

SCRIPT_DIR = Path(__file__).parent

# ── Logging ───────────────────────────────────────────────────────────────────

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(message)s",
    level=logging.INFO,
    handlers=[logging.StreamHandler()],
)
log = logging.getLogger(__name__)



def resolve_path(path_value: str) -> Path:
    """
    Resolve a path string to an absolute Path.

    If the given path is already absolute, it is returned as-is.
    If it is relative, it is resolved relative to the directory containing
    this script (SCRIPT_DIR), not the current working directory.

    Parameters:
        path_value : A path string (relative or absolute).

    Returns:
        An absolute Path object.
    """
    path = Path(path_value)
    if not path.is_absolute():
        path = (SCRIPT_DIR / path).resolve()
    return path


# ── Config helpers ────────────────────────────────────────────────────────────

def load_config(config_path: str) -> Dict:
    """
    Load Oracle database connection credentials from a JSON config file.

    Parameters:
        config_path : Path to the config JSON file.

    Returns:
        A dict containing the raw config values (credentials, DSN, wallet paths).
    """
    with open(config_path, "r", encoding="utf-8") as f:
        return json.load(f)


# ── Graph model helpers ───────────────────────────────────────────────────────
def load_graph_model(json_path: str) -> Dict:
    """
    Load the graph model definition from a JSON file.

    The graph model describes the vertices (nodes) and edges (relationships)
    that make up the property graph, including their underlying DB tables,
    property columns, and join keys.

    Parameters:
        json_path : Path to the graph model JSON file.

    Returns:
        The raw graph model as a dict with "nodes" and "relationships" keys.
    """
    with open(json_path, "r", encoding="utf-8") as f:
        return json.load(f)

def extract_vertices_edges(graph_model: Dict) -> Tuple[List[Dict], List[Dict]]:
    """
    Parse the raw graph model into structured vertex and edge lists.

    Each vertex entry captures the label, property map, optional filter,
    and underlying table names needed to generate DDL.
    Each edge entry captures the type, source/target labels, and join keys.

    Parameters:
        graph_model : The raw graph model dict loaded from JSON.

    Returns:
        A tuple of (vertices, edges), where:
          vertices — list of dicts with keys:
                       type, label, properties, filter, tables
          edges    — list of dicts with keys:
                       type, from, to, from_key, to_key
    """
    vertices = []
    for node in graph_model.get("nodes", []):
        vertices.append({
            "type":       node["name"],
            "label":      node.get("label") or node.get("name"),
            "properties": node.get("properties", {}),
            "filter":     node.get("filter"),
            "tables":     node.get("table", []),
        })

    edges = []
    for rel in graph_model.get("relationships", []):
        edges.append({
            "type":     rel["type"],
            "from":     rel["from"],
            "to":       rel["to"],
            "from_key": rel["from_key"],
            "to_key":   rel["to_key"],
        })

    return vertices, edges


# ── Name normalisation helpers ────────────────────────────────────────────────

def _edge_view_name(rel_type: str) -> str:
    """
    Convert a relationship type string into a safe, uppercase Oracle identifier.

    Replaces any character that is not alphanumeric or underscore with '_'
    so the result is always a valid Oracle object name.

    Example:
        "ProductVod Has Version" → "PRODUCTVOD_HAS_VERSION"

    Parameters:
        rel_type : The raw relationship type string from the graph model.

    Returns:
        A sanitised uppercase string suitable for use as an Oracle edge view name.
    """
    return re.sub(r"[^A-Za-z0-9_]", "_", rel_type).upper()


def _vertex_view_name(label: str) -> str:
    """
    Convert a vertex label string into a safe, uppercase Oracle identifier.

    Replaces any character that is not alphanumeric or underscore with '_'
    so the result is always a valid Oracle object name.

    Example:
        "Product Vod" → "PRODUCT_VOD"

    Parameters:
        label : The raw vertex label string from the graph model.

    Returns:
        A sanitised uppercase string suitable for use as an Oracle vertex view name.
    """
    return re.sub(r"[^A-Za-z0-9_]", "_", label).upper()


# ── DDL generation ────────────────────────────────────────────────────────────

def write_pgql_schema(
    vertices:   List[Dict],
    edges:      List[Dict],
    filename:   str,
    graph_name: str,
) -> None:
    """
    Generate an Oracle CREATE PROPERTY GRAPH DDL statement and write it to a file.

    The DDL defines:
      - VERTEX TABLES: one entry per vertex label, with property columns and a
        primary key (prefers ROW_ID, then ID, then omits KEY clause).
      - EDGE TABLES: one entry per relationship, with SOURCE KEY and DESTINATION KEY
        clauses pointing to the correct vertex tables and join columns.

    Parameters:
        vertices   : Parsed vertex list from extract_vertices_edges().
        edges      : Parsed edge list from extract_vertices_edges().
        filename   : Output file path for the generated SQL DDL.
        graph_name : Name to use in the CREATE PROPERTY GRAPH statement.
    """
    with open(filename, "w", encoding="utf-8") as f:

        # ── Header ────────────────────────────────────────────────────────────
        f.write(f'CREATE PROPERTY GRAPH "{graph_name}"\n')

        # ── Vertex tables ─────────────────────────────────────────────────────
        f.write("VERTEX TABLES (\n")
        for i, v in enumerate(vertices):
            label      = v.get("label") or v["type"]
            table_name = f'"{_vertex_view_name(label)}"'
            prop_list  = ", ".join(f'"{p}"' for p in v.get("properties", {}).values())

            # Prefer ROW_ID as the primary key, fall back to ID, or omit KEY clause
            all_props = v.get("properties", {}).values()
            if "ROW_ID" in all_props:
                key_clause = '"ROW_ID"'
            elif "ID" in all_props:
                key_clause = '"ID"'
            else:
                key_clause = None

            if key_clause:
                f.write(f"  {table_name}\n  KEY ({key_clause})\n  PROPERTIES ({prop_list})")
            else:
                f.write(f"  {table_name} LABEL {label} PROPERTIES ({prop_list})")

            f.write(",\n" if i < len(vertices) - 1 else "\n")

        f.write(")\n")

        # ── Edge tables ───────────────────────────────────────────────────────
        f.write("EDGE TABLES (\n")
        
        for i, rel in enumerate(edges):
            edge_view  = _edge_view_name(rel["type"])
            from_table = rel["from"].upper()
            to_table   = rel["to"].upper()
            from_key   = rel["from_key"]
            to_key     = rel["to_key"]

            f.write(
                f'  "{from_table}" AS "{edge_view}"\n'
                f'  KEY ("{from_key}")\n'
                f'  SOURCE KEY ("{from_key}") REFERENCES "{from_table}" ("{from_key}")\n'
                f'  DESTINATION KEY ("{from_key}") REFERENCES "{to_table}" ("{to_key}")\n'
                f'  NO PROPERTIES'
            )
            f.write(",\n" if i < len(edges) - 1 else "\n")

        f.write(")\n")


# ── Orchestration ─────────────────────────────────────────────────────────────

def generate_pgql_graph(
    json_path:    str,
    config_path:  str,
    ddl_output: str,
    graph_name:   str,
) -> None:
    """
    End-to-end orchestration: load graph model → generate DDL → optionally execute.

    Steps:
      1. Load Oracle credentials from config_path.
      2. Load and parse the graph model from json_path.
      3. Generate the CREATE PROPERTY GRAPH DDL and write it to ddl_output.
      4. Prompt the user interactively whether to execute the DDL.
         If confirmed, execute it against the target Oracle database.

    Parameters:
        json_path     : Path to the graph model JSON file.
        config_path   : Path to the Oracle credentials config JSON file.
        ddl_output : Output path for the generated DDL SQL file.
        graph_name    : Name to assign to the property graph in the DDL.
    """
    cfg    = load_config(config_path)

    target_config = {
        "user":            cfg["26AI_USER"],
        "password":        cfg["26AI_PASSWORD"],
        "dsn":             cfg["26AI_DSN"],
        "config_dir":      cfg["26AI_CONFIG_DIR"],
        "wallet_location": cfg["26AI_WALLET_LOCATION"],
        "wallet_password": cfg["26AI_WALLET_PASSWORD"],
    }

    log.info("Loading graph model from %s ...", json_path)
    model             = load_graph_model(json_path)
    vertices, edges   = extract_vertices_edges(model)

    log.info("Generating Oracle CREATE PROPERTY GRAPH DDL ...")
    write_pgql_schema(
        vertices   = vertices,
        edges      = edges,
        filename   = ddl_output,
        graph_name = graph_name,
    )
    log.info("DDL written to %s", ddl_output)

    # Interactive confirmation gate before touching the database
    try:
        choice = input(
            "\nApply (execute) this DDL against the target database? "
            "Type 'yes' to proceed: "
        ).strip().lower()
    except EOFError:
        choice = ""

    if choice == "yes":
        log.info("Executing DDL on Oracle 26ai database ...")
        execute_pgql_ddl(ddl_output, target_config)
        log.info("DDL execution complete.")
    else:
        log.info("DDL execution skipped by user.")


def execute_pgql_ddl(ddl_output: str, target_config: Dict) -> None:
    """
    Read a DDL file and execute it against the target Oracle database.

    Connects using the provided credentials (thin mode via python-oracledb),
    executes the full DDL text as a single statement, commits, and closes.
    Errors during connection or execution are logged and re-raised.

    Parameters:
        ddl_output : Path to the SQL DDL file to execute.
        target_config : Dict of Oracle connection kwargs:
                        user, password, dsn, config_dir,
                        wallet_location, wallet_password.
    """
    with open(ddl_output, "r", encoding="utf-8") as f:
        ddl = f.read()

    log.info("Connecting to Oracle 26ai (thin mode) ...")
    try:
        conn = oracledb.connect(**target_config)
        log.info("Connected. Oracle version: %s", conn.version)
    except Exception as exc:
        log.error("Connection failed: %s", exc)
        raise

    try:
        with conn.cursor() as cur:
            log.info("Executing CREATE PROPERTY GRAPH DDL ...")
            cur.execute(ddl)
            conn.commit()
            log.info("DDL committed successfully.")
    finally:
        conn.close()


# ── CLI entry point ───────────────────────────────────────────────────────────

def main() -> None:
    """
    Parse command-line arguments and invoke the DDL generation pipeline.

    Arguments:
        --graph_model : Path to the input graph model JSON file.
                        Default: graph_model.json
        --ddl_output    : Path for the output DDL SQL file.
                        Default: property_graph_schema.sql
        --graph_name  : Name of the property graph to create in the DDL.
                        Default: test_graph
        --config      : Path to the Oracle credentials config JSON file.
                        Default: config.json
    """
    import argparse

    parser = argparse.ArgumentParser(
        description="Generate an Oracle 26ai CREATE PROPERTY GRAPH DDL from a graph model JSON."
    )
    parser.add_argument(
        "--graph_model", default="graph_model.json",
        help="Input graph model JSON file (default: graph_model.json)"
    )
    parser.add_argument(
        "--ddl_output", default="property_graph_schema.sql",
        help="Output DDL SQL file path (default: property_graph_schema.sql)"
    )
    parser.add_argument(
        "--graph_name", default="test_graph",
        help="Name of the property graph to create (default: test_graph)"
    )
    parser.add_argument(
        "--config", default="config.json",
        help="Path to Oracle credentials config JSON (default: config.json)"
    )

    args = parser.parse_args()

    config_path = resolve_path(args.config)
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    graph_model_path = resolve_path(
        args.graph_model or "graph_model.json"
    )
    ddl_output_path = resolve_path(
        args.ddl_output or "create_26ai_schema.sql"
    )

    generate_pgql_graph(
        json_path     = graph_model_path,
        config_path   = config_path,
        ddl_output = ddl_output_path,
        graph_name    = args.graph_name
    )


if __name__ == "__main__":
    main()