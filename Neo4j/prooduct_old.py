import json
import oracledb
from neo4j import GraphDatabase

ORACLE_DSN = "phoenix610241.appsdev.fusionappsdphx1.oraclevcn.com:1551/qa241"
ORACLE_USER = "sadmin"
ORACLE_PASS = "sadmin"
SCHEMA = "ORA241125"
NEO4J_URI = "neo4j://127.0.0.1:7687"
NEO4J_USER = "neo4j"
NEO4J_PASS = "Asdfg@12345"
GRAPH_JSON_PATH = "graph_model.json"
DATE = "21-01-26"

def parse_filter(filter_obj):
    """
    Recursively parse the filter object into a SQL WHERE clause.
    Supports nested AND, OR, and NOT, as well as basic equality/comparison.
    """
    if not isinstance(filter_obj, dict):
        raise ValueError("Filter must be a dictionary")

    clauses = []

    for key, value in filter_obj.items():
        if key == "AND":
            and_clauses = [parse_filter(v) for v in value]
            clauses.append(f"({' AND '.join(and_clauses)})")
        elif key == "OR":
            or_clauses = [parse_filter(v) for v in value]
            clauses.append(f"({' OR '.join(or_clauses)})")
        elif key == "NOT":
            not_clause = parse_filter(value)
            clauses.append(f"(NOT {not_clause})")
        else:
            # Comparison / equality: e.g. {'VOD_TYPE_CD': 'ISS_ATTR_DEF'}
            if isinstance(value, dict):
                # Advanced: e.g., {'FIELD': {'gt': 10}}
                for op, v in value.items():
                    op_map = {
                        "gt": ">",
                        "gte": ">=",
                        "lt": "<",
                        "lte": "<=",
                        "ne": "<>",
                        "eq": "="
                    }
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
                        # Quote string
                        v_str = f"'{v}'" if isinstance(v, str) else str(v)
                        clauses.append(f"{key} {op_map[op]} {v_str}")
            elif isinstance(value, str) and value.strip().upper() in ("IS NULL", "IS NOT NULL"):
                # Custom literal SQL for IS NULL/IS NOT NULL, ex: "PAR_REL_ID": "IS NULL"
                clauses.append(f"{key} {value.strip().upper()}")
            else:
                v_str = f"'{value}'" if isinstance(value, str) else str(value)
                clauses.append(f"{key}={v_str}")

    return " AND ".join(clauses)


def build_select_sql(node, schema):
    # Compose SELECT statement using aliases
    def col_alias_pairs(properties):
        # properties is a {col: alias, ...} dict 
        pairs = []
        for col, alias in properties.items():
            pairs.append(f'{col} AS "{alias}"')
        return pairs

    if len(node["table"]) == 1:
        tbl = node["table"][0]
        cols = ", ".join(col_alias_pairs(node["properties"]))
        sql = f"SELECT {cols} FROM {schema}.{tbl}"
        if "filter" in node:
            where_clause = parse_filter(node["filter"])+ f" AND LAST_UPD >= TO_DATE('{DATE}', 'DD-MM-YY')"
            sql += f" WHERE {where_clause}"
        else:
            sql += f" WHERE LAST_UPD >= TO_DATE('{DATE}', 'DD-MM-YY')"
        print(sql)
        return sql
    elif len(node["table"]) == 2 and "join_on" in node:
        tbl1, tbl2 = node["table"]
        cols = ", ".join(col_alias_pairs(node["properties"]))
        left, right = list(node["join_on"].items())[0]
        sql = f"SELECT {cols} FROM {schema}.{tbl1} JOIN {schema}.{tbl2} ON {left}={right}"
        if "filter" in node:
            where_clause = parse_filter(node["filter"])+ f" AND LAST_UPD >= TO_DATE('{DATE}', 'DD-MM-YY')"
            sql += f" WHERE {where_clause}"
        else:
            sql += f" WHERE LAST_UPD >= TO_DATE('{DATE}', 'DD-MM-YY')"
        print(sql)
        return sql
    else:
        raise Exception("Unsupported node structure (multi-table join with >2 tables not implemented)")

def fetch_data(cursor, sql, props):
    cursor.execute(sql)
    dbcolumns = [d[0] for d in cursor.description]
    out = []
    for row in cursor:
        r = {}
        # Map columns exactly as they come from Oracle result (should match aliases)
        for idx, col in enumerate(dbcolumns):
            r[col] = row[idx]
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
    import re
    def cleaned_data(k):
        return re.sub(r'\W+', '_', k).upper()
    for rec in records:
        # Remove any properties with None values, and remap keys for Neo4j property compatibility
        filtered_rec = {cleaned_data(k): v for k, v in rec.items() if v is not None}
        if not filtered_rec:
            continue  # skip if all properties are None
        cypher = f"MERGE (n:{label} {{ {', '.join(f'{k}: ${k}' for k in filtered_rec.keys())} }})"
        session.run(cypher, **filtered_rec)
    print(f"Done Creating Node: {node['label']}")

def get_or_fetch_node_records(node_name, node_data, session, properties="*"):
    """
    Get node records from cache or fetch from Neo4j if not available.
    
    Args:
        node_name: Name of the node label
        node_data: Dictionary cache of node data
        session: Neo4j session
        properties: Properties to return ("*" for all or list of property names)
        limit: Maximum number of records to fetch
    
    Returns:
        List of node records
    """
    records = node_data.get(node_name, [])
    
    if not records:
        print(f"Querying Neo4j for missing node: {node_name}")
        
        # First, check if the label exists
        label_check = f"CALL db.labels() YIELD label WHERE label = '{node_name}' RETURN label"
        result = session.run(label_check)
        labels = [record["label"] for record in result]
        if not labels:
            print(f"Warning: Label {node_name} does not exist in Neo4j. Skipping MATCH for this label.")
            return []

        # Build RETURN clause
        if properties == "*":
            return_clause = "n"
        else:
            return_clause = "n"  # Still return the full node for consistency
        
        query = f"""
        MATCH (n:{node_name})
        RETURN {return_clause}
        
        """
        
        try:
            result = session.run(query)
            records = [record["n"] for record in result]
            # Store in cache for future use
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
        # Build an index for quick lookup of to_records by key
        to_index = {r[to_key]: r for r in to_records if to_key in r}

        for fr in from_records:
            fr_val = fr.get(from_key)
            to_node = to_index.get(fr_val)
            # Ensure both nodes exist with the required property
            
            print(
                    f"Creating relationship creation for {node_label_from}({from_key}={fr_val}) -> [{rel_type}] -> {node_label_to}({to_key}={fr_val})"
                )
            

            cypher = (
                f"MATCH (a:{node_label_from}), (b:{node_label_to}) "
                f"WHERE a.{from_key} = $from_val AND b.{to_key} = $to_val "
                f"MERGE (a)-[r:{rel_type}]->(b)"
            )
            print("Cypher Query:", cypher)
            print("Parameters:", {"from_val": fr_val, "to_val": fr_val})
            session.run(
                cypher,
                from_val=fr_val,
                to_val=fr_val
            )

        print(f"Done Creating Relation: {rel['from']} -> [{rel['type']}] -> {rel['to']}")

def load_data_to_neo4j(
    oracle_user, oracle_pass, oracle_dsn,schema,
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
        for node in graph['nodes']:
            print("_"*50)
            print(f"Processing node: {node['name']}")
            # Check if node already exists in Neo4j
            label = node["label"]
            import sys
            import io
            exists_query = f"MATCH (n:{label}) RETURN n LIMIT 1"
            try:
                orig_stderr = sys.stderr
                sys.stderr = io.StringIO()
                try:
                    neo4j_result = session.run(exists_query)
                    exists = False#any(True for _ in neo4j_result)
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
        create_neo4j_relationships(session, graph, node_data)
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
