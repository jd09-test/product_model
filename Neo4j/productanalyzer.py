import json
import oracledb
from neo4j import GraphDatabase
import time
import re

with open("config.json", "r") as f:
    config = json.load(f)

ORACLE_DSN = config["ORACLE_DSN"]
ORACLE_USER = config["ORACLE_USER"]
ORACLE_PASS = config["ORACLE_PASS"]
SCHEMA = config["SCHEMA"]
NEO4J_URI = config["NEO4J_URI"]
NEO4J_USER = config["NEO4J_USER"]
NEO4J_PASS = config["NEO4J_PASS"]
GRAPH_JSON_PATH = config["GRAPH_JSON_PATH"]
DATE = config["DATE"]
BATCH_SIZE = config["BATCH_SIZE"]
ORACLE_CLIENT_PATH = config["ORACLE_CLIENT_PATH"]

_VALID_PROP_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

def print_progress(batch_num, batch_size, total, start, entity):
    elapsed = time.time() - start
    rate = total / elapsed if elapsed > 0 else 0
    print(f"  Batch {batch_num}: {batch_size:,} {entity} | Total: {total:,} | Rate: {rate:.0f}/sec | {elapsed/60:.1f}m")

def is_valid_prop(prop):
    return _VALID_PROP_RE.match(prop or "")

def create_constraints_for_rowid(session, graph):
    print("\nCreating unique constraints for ROW_ID properties...")
    cyphers = [
        f"CREATE CONSTRAINT IF NOT EXISTS FOR (n:{node['label']}) REQUIRE n.ROW_ID IS UNIQUE"
        for node in graph.get("nodes", [])
        if "ROW_ID" in node.get("properties", {}).values()
    ]
    with session.begin_transaction() as tx:
        for query in cyphers:
            try:
                tx.run(query)
                print(f"Constraint created: {query}")
            except Exception as e:
                print(f"Constraint failed or already exists: {e}")
    print("ROW_ID constraints created.\n")

def create_indexes(session, graph):
    print("\nCreating indexes for involved labels...")
    index_cyphers = set()
    for node in graph['nodes']:
        label = node['label']
        for prop in node['properties'].values():
            if is_valid_prop(prop):
                index_cyphers.add(f"CREATE INDEX IF NOT EXISTS FOR (n:{label}) ON (n.{prop})")
    for rel in graph.get('relationships', []):
        from_label = next((n['label'] for n in graph['nodes'] if n['name'] == rel['from']), None)
        to_label = next((n['label'] for n in graph['nodes'] if n['name'] == rel['to']), None)
        if from_label and is_valid_prop(rel['from_key']):
            index_cyphers.add(f"CREATE INDEX IF NOT EXISTS FOR (n:{from_label}) ON (n.{rel['from_key']})")
        if to_label and is_valid_prop(rel['to_key']):
            index_cyphers.add(f"CREATE INDEX IF NOT EXISTS FOR (n:{to_label}) ON (n.{rel['to_key']})")
    with session.begin_transaction() as tx:
        for idx_query in index_cyphers:
            try:
                tx.run(idx_query)
            except Exception as e:
                print("Index creation failed or already exists:", e)
    print("Indexes created.\n")

def parse_filter(filter_obj):
    if not isinstance(filter_obj, dict):
        raise ValueError("Filter must be a dictionary")
    clauses = []
    for key, value in filter_obj.items():
        if key == "AND":
            clauses.append(f"({' AND '.join(map(parse_filter, value))})")
        elif key == "OR":
            clauses.append(f"({' OR '.join(map(parse_filter, value))})")
        elif key == "NOT":
            clauses.append(f"(NOT {parse_filter(value)})")
        else:
            if isinstance(value, dict):
                op_map = {"gt": ">", "gte": ">=", "lt": "<", "lte": "<=", "ne": "<>", "eq": "="}
                for op, v in value.items():
                    if op not in op_map:
                        raise ValueError(f"Unsupported operator {op}")
                    if v is None:
                        if op == "eq":
                            clauses.append(f"{key} IS NULL")
                        elif op == "ne":
                            clauses.append(f"{key} IS NOT NULL")
                        else:
                            raise ValueError("Cannot use comparison operator with NULL")
                    else:
                        v_str = f"'{v}'" if isinstance(v, str) else str(v)
                        clauses.append(f"{key} {op_map[op]} {v_str}")
            elif isinstance(value, str) and value.strip().upper() in ("IS NULL", "IS NOT NULL"):
                clauses.append(f"{key} {value.strip().upper()}")
            else:
                v_str = f"'{value}'" if isinstance(value, str) else str(value)
                clauses.append(f"{key}={v_str}")
    return " AND ".join(clauses)

def build_select_sql(node, schema):
    def col_alias_pairs(properties):
        return [f'{col} AS "{alias}"' for col, alias in properties.items()]
    if len(node["table"]) == 1:
        tbl = node["table"][0]
        cols = ", ".join(col_alias_pairs(node["properties"]))
        sql = f"SELECT {cols} FROM {schema}.{tbl}"
        if "filter" in node:
            where_clause = parse_filter(node["filter"])
            sql += f" WHERE {where_clause}"
        print(sql)
        return sql
    elif len(node["table"]) == 2 and "join_on" in node:
        tbl1, tbl2 = node["table"]
        cols = ", ".join(col_alias_pairs(node["properties"]))
        left, right = list(node["join_on"].items())[0]
        sql = f"SELECT {cols} FROM {schema}.{tbl1} JOIN {schema}.{tbl2} ON {left}={right}"
        if "filter" in node:
            where_clause = parse_filter(node["filter"])
            sql += f" WHERE {where_clause}"
        print(sql)
        return sql
    raise Exception("Unsupported node structure (multi-table join with >2 tables not implemented)")

def fetch_data(cursor, sql, props):
    cursor.execute(sql)
    dbcolumns = [d[0] for d in cursor.description]
    out = []
    for row in cursor:
        r = {col: row[idx] for idx, col in enumerate(dbcolumns)}
        out.append(r)
    return out

def get_node_label(node):
    return node["label"]

def get_property_cypher(record):
    return ", ".join(f"{k}: ${k}" for k in record.keys())

def create_neo4j_nodes(session, node, records):
    print("*"*50)
    print(f"Creating Node: {node['label']}")
    label = get_node_label(node)
    def cleaned_data(k):
        return re.sub(r'\W+', '_', k).upper()
    unique_property = None
    cleaned_properties = [cleaned_data(k) for k in node["properties"].values()]
    for candidate in ["ROW_ID", "row_id"]:
        if candidate in cleaned_properties:
            unique_property = candidate
            break
    if not unique_property and cleaned_properties:
        unique_property = cleaned_properties[0]
    if not unique_property:
        print(f"ERROR: No unique property found for node {node['label']}. Skipping.")
        return
    start = time.time()
    batch = []
    total = 0
    batch_num = 0
    for rec in records:
        filtered_rec = {cleaned_data(k): v for k, v in rec.items() if v is not None}
        if not filtered_rec or unique_property not in filtered_rec or filtered_rec[unique_property] is None:
            continue
        batch.append(filtered_rec)
        if len(batch) >= BATCH_SIZE:
            batch_num += 1
            cypher = (
                f"UNWIND $batch AS row "
                f"MERGE (n:{label} {{ {unique_property}: row.{unique_property} }}) "
                f"SET n += row"
            )
            session.run(cypher, batch=batch)
            total += len(batch)
            print_progress(batch_num, len(batch), total, start, label + 's')
            batch = []
    if batch:
        cypher = (
            f"UNWIND $batch AS row "
            f"MERGE (n:{label} {{ {unique_property}: row.{unique_property} }}) "
            f"SET n += row"
        )
        session.run(cypher, batch=batch)
        total += len(batch)
        print_progress(batch_num + 1, len(batch), total, start, label + 's')
    print(f"Done Creating Node: {node['label']}. Total: {total}")

def get_or_fetch_node_records(node_name, node_data, session, properties="*"):
    records = node_data.get(node_name, [])
    if not records:
        print(f"Querying Neo4j for missing node: {node_name}")
        label_check = f"CALL db.labels() YIELD label WHERE label = '{node_name}' RETURN label"
        result = session.run(label_check)
        labels = [record["label"] for record in result]
        if not labels:
            print(f"Warning: Label {node_name} does not exist in Neo4j. Skipping MATCH for this label.")
            return []
        return_clause = "n"
        query = f"MATCH (n:{node_name}) RETURN {return_clause}"
        try:
            result = session.run(query)
            records = [record["n"] for record in result]
            node_data[node_name] = records
            print(f"Found {len(records)} records for {node_name}")
        except Exception as e:
            print(f"Error querying {node_name}: {e}")
            records = []
    else:
        print(f"Using cached data for {node_name}: {len(records)} records")
    return records

def create_neo4j_relationships(session, graph, node_data):
    for rel in graph['relationships']:
        rel_type = rel['type']
        from_name = rel['from']
        to_name = rel['to']
        from_key = rel['from_key']
        to_key = rel['to_key']
        print("*" * 50)
        print(f"Creating Relationship: {rel['from']} -> [{rel['type']}] -> {rel['to']}")
        from_records = get_or_fetch_node_records(from_name, node_data, session)
        to_records = get_or_fetch_node_records(to_name, node_data, session)
        if not from_records or not to_records:
            print(f"Skipping relationship {from_name} -> {rel_type} -> {to_name} due to missing data.")
            continue
        node_label_from = None
        node_label_to = None
        for n in graph['nodes']:
            if n['name'] == from_name:
                node_label_from = n['label']
            if n['name'] == to_name:
                node_label_to = n['label']
        to_index = {r[to_key]: r for r in to_records if to_key in r}
        start = time.time()
        batch = []
        total = 0
        batch_num = 0
        for fr in from_records:
            fr_val = fr.get(from_key)
            to_node = to_index.get(fr_val)
            if fr_val is None or to_node is None:
                continue
            batch.append({"from_val": fr_val, "to_val": fr_val})
            if len(batch) >= BATCH_SIZE:
                batch_num += 1
                cypher = (
                    f"UNWIND $batch AS row\n"
                    f"MATCH (a:{node_label_from} {{{from_key}: row.from_val}})\n"
                    f"MATCH (b:{node_label_to} {{{to_key}: row.to_val}})\n"
                    f"MERGE (a)-[r:{rel_type}]->(b)\n"
                    f"SET r.{from_key} = row.from_val, r.{to_key} = row.to_val, "
                    f"r.source_field = '{from_key}', r.target_field = '{to_key}'"
                )
                session.run(cypher, batch=batch)
                total += len(batch)
                print_progress(batch_num, len(batch), total, start, "relationships")
                batch = []
        if batch:
            cypher = (
                f"UNWIND $batch AS row\n"
                f"MATCH (a:{node_label_from} {{{from_key}: row.from_val}})\n"
                f"MATCH (b:{node_label_to} {{{to_key}: row.to_val}})\n"
                f"MERGE (a)-[r:{rel_type}]->(b)\n"
                f"SET r.source_field = '{from_key}', r.target_field = '{to_key}'"
            )
            session.run(cypher, batch=batch)
            total += len(batch)
            print_progress(batch_num + 1, len(batch), total, start, "relationships")
        print(f"Done Creating Relation: {rel['from']} -> [{rel['type']}] -> {rel['to']} (Total: {total} relationships)")

def load_data_to_neo4j(
    oracle_user, oracle_pass, oracle_dsn, schema,
    neo4j_uri, neo4j_user, neo4j_pass,
    graph_json_path
):
    with open(graph_json_path, "r") as f:
        graph = json.load(f)
    oracledb.init_oracle_client(lib_dir="/Users/darshanjaju/instantclient_19_8")
    ora_conn = oracledb.connect(user=oracle_user, password=oracle_pass, dsn=oracle_dsn)
    print("Connected to Oracle:", ora_conn.version)
    ora_cur = ora_conn.cursor()
    ora_cur.arraysize = 10000
    node_data = {}
    with GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_pass)).session() as session:
        total_start = time.time()
        node_load_start = time.time()
        for node in graph['nodes']:
            print("_"*50)
            print(f"Processing node: {node['name']}")
            label = node["label"]
            import sys
            import io
            exists_query = f"MATCH (n:{label}) RETURN n LIMIT 1"
            try:
                orig_stderr = sys.stderr
                sys.stderr = io.StringIO()
                try:
                    neo4j_result = session.run(exists_query)
                    exists = False
                finally:
                    sys.stderr = orig_stderr
            except Exception as e:
                exists = False
            if exists:
                print(f"Node of type {label} already exists in Neo4j. Skipping Oracle fetch and creation for {node['name']}.")
                continue
            sql = build_select_sql(node, schema)
            records = fetch_data(ora_cur, sql, node['properties'])
            node_data[node['name']] = records
            print(f"Fetched {len(records)} record(s) for {node['name']}")
            create_neo4j_nodes(session, node, records)
        node_load_end = time.time()
        print(
            "\n=== TIMING REPORT ===",
            f"\nNode creation phase: {(node_load_end - node_load_start):.2f} seconds ({(node_load_end - node_load_start)/60:.2f} min) [SKIPPED]"
        )
        rel_load_start = time.time()
        create_neo4j_relationships(session, graph, node_data)
        rel_load_end = time.time()
        print(
            f"Relationship creation phase: {(rel_load_end - rel_load_start):.2f} seconds ({(rel_load_end - rel_load_start)/60:.2f} min)"
        )
        # Add constraints BEFORE indexes for correct schema setup
        constraint_start = time.time()
        create_constraints_for_rowid(session, graph)
        constraint_end = time.time()
        print(
            f"Constraint creation phase: {(constraint_end - constraint_start):.2f} seconds ({(constraint_end - constraint_start)/60:.2f} min)"
        )
        index_start = time.time()
        create_indexes(session, graph)
        index_end = time.time()
        print(
            f"Index creation phase: {(index_end - index_start):.2f} seconds ({(index_end - index_start)/60:.2f} min)"
        )
        print(
            f"=== TOTAL ELAPSED TIME: {(index_end - total_start):.2f} seconds ({(index_end - total_start)/60:.2f} min) ==="
        )
    ora_cur.close()
    ora_conn.close()

def main():
    load_data_to_neo4j(
        ORACLE_USER,
        ORACLE_PASS,
        ORACLE_DSN,
        SCHEMA,
        NEO4J_URI,
        NEO4J_USER,
        NEO4J_PASS,
        GRAPH_JSON_PATH
    )

if __name__ == "__main__":
    main()