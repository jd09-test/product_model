import json
import os
import re
from typing import Dict, List, Tuple, Optional
import oracledb as cx_Oracle  # use python-oracledb with cx_Oracle-compatible API


pgql_graph = False
# -----------------------------
# Config helpers (loaded at runtime via --config)
# -----------------------------
def load_config(config_path: str) -> Dict:
    with open(config_path, "r", encoding="utf-8") as f:
        return json.load(f)

# -----------------------------
# Load model & filter helpers
# -----------------------------
def load_graph_model(json_path: str) -> Dict:
    with open(json_path, 'r', encoding='utf-8') as f:
        return json.load(f)

def extract_vertices_edges(graph_model: Dict) -> Tuple[List[Dict], List[Dict]]:
    vertices = []
    for node in graph_model.get("nodes", []):
        vertices.append({
            "type": node["name"],
            "label": node.get("label") or node.get("name"),
            "properties": node.get("properties", {}),
            "filter": node.get("filter"),
            "tables": node.get("table", [])
        })
    edges = []
    for rel in graph_model.get("relationships", []):
        edges.append({
            "type": rel["type"],
            "from": rel["from"],
            "to": rel["to"],
            "from_key": rel["from_key"],
            "to_key": rel["to_key"]
        })
    return vertices, edges

def _edge_view_name(rel_type: str) -> str:
    return f"{re.sub(r'[^A-Za-z0-9_]', '_', rel_type)}".upper()

def _vertex_view_name(label: str) -> str:
    return f"{re.sub(r'[^A-Za-z0-9_]', '_', label)}".upper()


def write_pgql_schema(vertices: List[Dict], edges: List[Dict], filename: str, full_model: Dict, schema: str, graph_name: str):
    with open(filename, "w", encoding="utf-8") as f:
        f.write(f"CREATE PROPERTY GRAPH \"{graph_name}\" \n")
        f.write(" VERTEX TABLES (\n")
        for i, v in enumerate(vertices):
            # Use the vertex view name so each label can filter a single table differently
            table_name = f"\"{_vertex_view_name(v.get('label') or v.get('type'))}\""
            label = v.get("label") or v["type"]
            prop_list = ", ".join(f'"{p}"' for p in v.get("properties", {}).values())
            # Use ROW_ID or ID as key if present
            if "ROW_ID" in v.get("properties", {}).values():
                key = '"ROW_ID"'
            elif "ID" in v.get("properties", {}).values():
                key = '"ID"'
            else:
                key = None
            if key:
                f.write(f"{table_name} \n  KEY ({key}) \n  PROPERTIES ({prop_list})")
            else:
                f.write(f"{table_name} LABEL {label} PROPERTIES ({prop_list})")
            if i < len(vertices) - 1:
                f.write(",\n")
            else:
                f.write("\n")

        f.write("  )\n")
        f.write("EDGE TABLES (\n")
        # Use the same relationship list from the full model to ensure exact key names
        rels = full_model.get("relationships", [])
        for i, rel in enumerate(rels):
            edge_table = f"\"{rel['from'].upper()}\" AS \"{_edge_view_name(rel['type'])}\""
            label = rel["type"]
            from_label = rel["from"].upper()
            to_label = rel["to"].upper()
            from_key = rel["from_key"]
            to_key = rel["to_key"]
            f.write(
                f"{edge_table} \nKEY (\"{from_key}\")"
                f" \nSOURCE KEY (\"{from_key}\") REFERENCES \"{from_label}\" (\"{from_key}\")"
                f" \nDESTINATION  KEY (\"{from_key}\") REFERENCES \"{to_label}\" (\"{to_key}\")"
                f" \nNO PROPERTIES"
            )
            if i < len(rels) - 1:
                f.write(",\n")
            else:
                f.write("\n")
        f.write(")\n")
        if pgql_graph:
             f.write("OPTIONS (PG_PGQL);")



def generate_pgqlgraph(
        json_path: str,
        config_path: str,
        pgql_filename: str,
        graph_name: str,
        execute_ddl: bool = False):  # execute_ddl is deprecated; user will be prompted instead
    import logging

    logging.basicConfig(
        format="%(asctime)s | %(levelname)s | %(message)s",
        level=logging.INFO, handlers=[logging.StreamHandler()]
    )

    cfg = load_config(config_path)
    schema = "ADMIN"

    TARGET_CONFIG = {
    "user": cfg["PGQL_USER"],
    "password": cfg["PGQL_PASSWORD"],
    "dsn": cfg["PGQL_DSN"],
    "config_dir": cfg["PGQL_CONFIG_DIR"],
    "wallet_location": cfg["PGQL_WALLET_LOCATION"],
    "wallet_password": cfg["PGQL_WALLET_PASSWORD"]
}

    model = load_graph_model(json_path)
    vertices, edges = extract_vertices_edges(model)
    logging.info("Generating Oracle PGQL CREATE PROPERTY GRAPH DDL...")
    write_pgql_schema(vertices, edges, pgql_filename, full_model=model, schema=schema, graph_name=graph_name)
    logging.info(f"PGQL schema written to {pgql_filename}")

    # Always ask the user whether to execute the DDL now (interactive prompt)
    try:
        choice = input("Apply (execute) this DDL in the target database? Type 'yes' to approve: ").strip().lower()
    except EOFError:
        choice = ""
    if choice == "yes":
        logging.info("Executing DDL on Oracle 26AI database via oracledb...")
        execute_pgql_ddl(pgql_filename, TARGET_CONFIG)
        logging.info("DDL execution complete.")
    else:
        logging.info("DDL execution skipped by user.")


def execute_pgql_ddl(pgql_filename: str, TARGET_CONFIG):
    import logging
    with open(pgql_filename, "r", encoding="utf-8") as f:
        ddl = f.read()

    # Initialize Oracle client if needed
    print("\nConnecting to target database (Oracle 23ai/Graph, thin mode)...")
    try:
        conn = cx_Oracle.connect(**TARGET_CONFIG)
        print(f"Connected to Oracle 23ai (Graph): {conn.version}")
    except Exception as e:
        print(f"Failed to connect to Oracle 23ai DB: {e}")
        return
    try:
        with conn.cursor() as cur:
            logging.info("Executing CREATE PROPERTY GRAPH DDL...")
            cur.execute(ddl)
            conn.commit()
            logging.info("DDL executed successfully.")
    finally:
        conn.close()


# No data export pipeline; only schema DDL generation and optional execution


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Oracle 26AI Property Graph DDL generator")
    parser.add_argument("--graph_model", default="graph_model.json", help="Input graph model JSON file")
    parser.add_argument("--pgql_out", default="pgql_schema.sql", help="Output PGQL DDL file")
    parser.add_argument("--graph_name", default="product_graph", help="Name of the property graph to create")
    parser.add_argument("--config", default="config.json", help="Path to config JSON with Oracle connection and schema")
    # --apply is deprecated; script now prompts interactively after DDL generation
    parser.add_argument("--apply", action="store_true", help="[Deprecated] Execute PGQL DDL without prompt (will be ignored)")

    args = parser.parse_args()

    generate_pgqlgraph(
        json_path=args.graph_model,
        config_path=args.config,
        pgql_filename=args.pgql_out,
        graph_name=args.graph_name,
        execute_ddl=False,
    )


if __name__ == "__main__":
    main()
