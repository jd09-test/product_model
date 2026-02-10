import oracledb
import cx_Oracle
import json

def log(msg):
    print(msg)

def generate_ddl(graph_model):
    node_map = {n['name']: n for n in graph_model['nodes']}
    table_to_node = {tuple(n.get('table',[]))[0]: n['name'] for n in graph_model['nodes'] if n.get('table')}
    node_pk_cols = {}
    ddls = []
    fks_by_table = {}
    # Pass 1: scan relationships to determine which columns must be unique in referenced tables
    must_be_unique = dict()  # {table_name: set(column_names)}
    for rel in graph_model["relationships"]:
        to_table = rel["to"]
        to_key_orig = rel["to_key"]
        to_node = node_map[to_table]
        to_key = to_node["properties"].get(to_key_orig)
        if not to_key:
            continue
        if to_table not in must_be_unique:
            must_be_unique[to_table] = set()
        must_be_unique[to_table].add(to_key)

    for node in graph_model['nodes']:
        table_name = node['name']
        column_map = node['properties']
        columns = [f"{col} VARCHAR2(4000)" for col in column_map.values()]
        pk_col = column_map.get('ROW_ID')
        node_pk_cols[table_name] = pk_col

        # Find what columns need unique constraints for FKs
        extra_uniques = []
        for col_name in must_be_unique.get(table_name, set()):
            if (not pk_col) or (pk_col != col_name):
                extra_uniques.append(col_name)
        # Create the column and constraint block with proper commas
        create_lines = []
        for idx, col in enumerate(columns):
            line = f"  {col}"
            if (
                idx < len(columns) - 1
                or pk_col
                or (extra_uniques and idx == len(columns) - 1)
            ):
                line += ","
            create_lines.append(line)
        if pk_col:
            create_lines.append(f"  CONSTRAINT PK_{table_name} PRIMARY KEY ({pk_col})" + ("," if extra_uniques else ""))
        for uniq_idx, uniq_col in enumerate(extra_uniques):
            tail = "," if uniq_idx < len(extra_uniques) - 1 else ""
            create_lines.append(f"  CONSTRAINT UQ_{table_name}_{uniq_col} UNIQUE ({uniq_col}){tail}")
        table_ddl = [f"CREATE TABLE {table_name} ("]
        table_ddl += create_lines
        table_ddl += [");"]
        ddls.append('\n'.join(table_ddl))

    for rel in graph_model['relationships']:
        from_table = rel['from']
        to_table = rel['to']
        from_key_orig = rel['from_key']
        to_key_orig = rel['to_key']
        from_node = node_map[from_table]
        to_node = node_map[to_table]
        from_key = from_node['properties'].get(from_key_orig)
        to_key = to_node['properties'].get(to_key_orig)
        if not from_key or not to_key:
            continue
        fk_name = f"FK_{from_table}_{from_key}_TO_{to_table}_{to_key}"
        if from_table not in fks_by_table:
            fks_by_table[from_table] = []
        fks_by_table[from_table].append(
            f"ALTER TABLE {from_table} ADD CONSTRAINT {fk_name} FOREIGN KEY ({from_key}) REFERENCES {to_table} ({to_key});"
        )

    full_ddl = []
    for ddl in ddls:
        full_ddl.append(ddl)
    for table, fks in fks_by_table.items():
        full_ddl += fks
    return '\n'.join(full_ddl)

def execute_ddl_file_on_target_db(sqlfile, conn):
    log("\n-- Executing DDL in target database (26ai DB) --")
    with open(sqlfile, "r") as f:
        ddl_sql = f.read()
    # Split on semicolons outside quotes, remove empty fragments and comments
    stmts = [stmt.strip() for stmt in ddl_sql.split(';') if stmt.strip()]
    success = True
    with conn.cursor() as cur:
        for stmt in stmts:
            if not stmt or stmt.startswith('--') or stmt.upper().startswith('REM '):
                continue
            try:
                cur.execute(stmt)
                log(f"[OK] Executed: {stmt.splitlines()[0][:80]}")
            except Exception as e:
                log(f"[FAIL] {stmt.splitlines()[0][:80]}: {e}")
                success = False
    return success

# --- Source Configuration (Legacy Oracle DB) ---
SOURCE_CONFIG = {
    "user": "sadmin",
    "password": "sadmin",
    "dsn": "phoenix610241.appsdev.fusionappsdphx1.oraclevcn.com:1551/qa241"
}
SOURCE_SCHEMA = "ORA241122"

# --- Target Configuration (Oracle 23ai Autonomous Graph DB) ---
TARGET_CONFIG = {
    "user": "admin",
    "password": "Cxpassword@123",
    "dsn": "(description= (retry_count=20)(retry_delay=3)(address=(protocol=tcps)(port=1522)(host=adb.us-phoenix-1.oraclecloud.com))(connect_data=(service_name=zo9pvg9f3zd89xb_graphstudiocheck1_high.adb.oraclecloud.com))(security=(ssl_server_dn_match=yes)))",
    "config_dir": "/Users/darshanjaju/Wallet_graphstudiocheck1",
    "wallet_location": "/Users/darshanjaju/Wallet_graphstudiocheck1",
    "wallet_password": "Asdfg@12345"
}

def drop_tables_in_target_db(table_list, conn):
    log("\n-- Dropping all auto-created tables in 26ai DB before schema CREATE --")
    with conn.cursor() as cur:
        for tbl in table_list:
            try:
                cur.execute(f'DROP TABLE {tbl} CASCADE CONSTRAINTS PURGE')
                log(f"[OK] Dropped table: {tbl}")
            except Exception as e:
                log(f"[WARN] Could not drop {tbl} (may not exist or already dropped): {e}")

def main():
    log("=" * 70)
    log("Legacy Oracle to Oracle 23ai Property Graph Migration (using PGQL or relational API)")
    log("=" * 70)

    with open("graph_model.json") as f:
        graph_model = json.load(f)
    ddl_out = generate_ddl(graph_model)
    log("\n26ai (target) DB - Suggested DDL:\n")
    print(ddl_out)
    with open("create_26ai_schema.sql", "w") as outf:
        outf.write(ddl_out)
    log("\nDDL written to create_26ai_schema.sql\n")

    # Get all table names that will be (re)created
    table_list = [node['name'] for node in graph_model['nodes']]

    # Optional DROP statement
    drop_input = input("Drop ALL created tables in 26ai DB before CREATE? Type 'drop' to confirm: ").strip().lower()
    drop_before_create = drop_input == 'drop'

    source_conn = None
    if drop_before_create:
        log("\nConnecting to target database to drop tables ...")
        try:
            target_conn = oracledb.connect(**TARGET_CONFIG)
            log(f"Connected to Oracle 23ai (Graph): {target_conn.version}")
            drop_tables_in_target_db(table_list, target_conn)
        except Exception as e:
            log(f"Failed to connect to Oracle 23ai DB for dropping tables: {e}")
            return
        finally:
            try:
                target_conn.close()
            except Exception:
                pass

    # Prompt user to approve/reject applying the DDL
    user_input = input("Apply (execute) this DDL in the target database? Type 'yes' to approve: ").strip().lower()
    apply_ddl = user_input == 'yes'

    if apply_ddl:
        log("\nConnecting to target database (Oracle 23ai/Graph, thin mode)...")
        try:
            target_conn = oracledb.connect(**TARGET_CONFIG)
            log(f"Connected to Oracle 23ai (Graph): {target_conn.version}")
        except Exception as e:
            log(f"Failed to connect to Oracle 23ai DB: {e}")
            return
        ok = execute_ddl_file_on_target_db("create_26ai_schema.sql", target_conn)
        if ok:
            log("\nAll DDL successfully executed on 26ai DB.")
        else:
            log("\nSome DDL statements failed. Check output for details.")
        target_conn.close()
    else:
        log("\nDDL execution on 26ai DB REJECTED by user.")

    # After schema creation, connect to legacy DB as before (optional: proceed to migration phase)
    # Keeping this here for full migration scripting context
    log("\nConnecting to source database (legacy Oracle, thick mode)...")
    try:
        cx_Oracle.init_oracle_client(lib_dir="/Users/darshanjaju/instantclient_19_8")
        source_conn = cx_Oracle.connect(**SOURCE_CONFIG)
        log(f"Connected to source Oracle: {source_conn.version}")
    except Exception as e:
        log(f"Failed to connect to legacy Oracle DB: {e}")
        source_conn = None

    # Placeholder for later data migration steps...

if __name__ == "__main__":
    main()