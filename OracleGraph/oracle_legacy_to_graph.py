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
    # No UNIQUE constraints apart from PK
    for node in graph_model['nodes']:
        table_name = node['name']
        column_map = node['properties']
        columns = [f"{col} VARCHAR2(4000)" for col in column_map.values()]
        pk_col = column_map.get('ROW_ID')
        node_pk_cols[table_name] = pk_col

        # Only create the PK constraint if defined
        create_lines = []
        for idx, col in enumerate(columns):
            line = f"  {col}"
            if idx < len(columns) - 1 or pk_col:
                line += ","
            create_lines.append(line)
        if pk_col:
            create_lines.append(f"  CONSTRAINT PK_{table_name} PRIMARY KEY ({pk_col})")
        table_ddl = [f"CREATE TABLE {table_name} ("]
        table_ddl += create_lines
        table_ddl += [");"]
        ddls.append('\n'.join(table_ddl))

    for rel in graph_model['relationships']:
        from_table = rel['from']
        to_table = rel['to']
        from_key = rel['from_key']
        to_key = rel['to_key']
        
        fk_name = f"FK_{to_table}_{to_key}_TO_{from_table}_{from_key}"
        if to_table not in fks_by_table:
            fks_by_table[to_table] = []
        fks_by_table[to_table].append(
            f"ALTER TABLE {to_table} ADD CONSTRAINT {fk_name} FOREIGN KEY ({to_key}) REFERENCES {from_table} ({from_key});"
        )

    full_ddl = []
    fks_flat = []
    for ddl in ddls:
        full_ddl.append(ddl)
    for table, fks in fks_by_table.items():
        fks_flat += fks
    return '\n'.join(full_ddl), fks_flat

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
    ddl_out, fk_alter_stmts = generate_ddl(graph_model)
    log("\n26ai (target) DB - Suggested DDL (NO FK CONSTRAINTS YET):\n")
    print(ddl_out)
    with open("create_26ai_schema.sql", "w") as outf:
        outf.write(ddl_out)
    with open("add_fk_constraints.sql", "w") as outf:
        for stmt in fk_alter_stmts:
            outf.write(stmt + "\n")
    log("\nDDL written to create_26ai_schema.sql (tables only), FK ALTERs written to add_fk_constraints.sql\n")

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
    log("\nConnecting to source databasde (legacy Oracle, thick mode)...")
    try:
        cx_Oracle.init_oracle_client(lib_dir="/Users/darshanjaju/instantclient_19_8")
        source_conn = cx_Oracle.connect(**SOURCE_CONFIG)
        log(f"Connected to source Oracle: {source_conn.version}")
    except Exception as e:
        log(f"Failed to connect to legacy Oracle DB: {e}")
        source_conn = None

    # ----- DATA MIGRATION SECTION -----

    # === Enhanced parse_filter from productanalyzer.py for legal SQL string WHERE clause ===
    def parse_filter(filter_obj):
        # Recursively builds SQL WHERE (as a string) from the filter dict per Neo4j loader
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

    def migrate_node_data(node, src_conn, tgt_conn, schema=SOURCE_SCHEMA, batch_size=1000):
        tgt_cols = list(node['properties'].values())
        sql = build_select_sql(node, schema)
        src_cur = src_conn.cursor()
        src_cur.execute(sql)
        rows = src_cur.fetchall()
        log(f"Migrating {len(rows)} row(s) from node {node['name']} ...")
        tgt_cur = tgt_conn.cursor()
        insert_cols = ', '.join(tgt_cols)
        pholders = ', '.join([':' + str(i+1) for i in range(len(tgt_cols))])
        isql = f"INSERT INTO {node['name']} ({insert_cols}) VALUES ({pholders})"
        n_total = len(rows)
        n_batches = (n_total + batch_size - 1) // batch_size
        for b in range(n_batches):
            batch_rows = rows[b*batch_size:(b+1)*batch_size]
            try:
                tgt_cur.executemany(isql, batch_rows)
                log(f"[BATCH] Loaded {len(batch_rows)} rows into {node['name']}")
            except Exception as e:
                log(f"[WARN] Batch insert failed in {node['name']} (rows {b*batch_size}-{b*batch_size+len(batch_rows)}): {e}")
        tgt_conn.commit()
        src_cur.close()
        tgt_cur.close()

    # User prompt for data load
    load_input = input("Load/migrate DATA from source to target? Type 'migrate' to approve: ").strip().lower()
    do_load = load_input == "migrate"

    if do_load:
        log("\nConnecting to both databases for data migration ...")
        try:
            src_conn = cx_Oracle.connect(**SOURCE_CONFIG)
            tgt_conn = oracledb.connect(**TARGET_CONFIG)
            for node in graph_model['nodes']:
                migrate_node_data(node, src_conn, tgt_conn)
            log("\nAll data migration complete.")
            # # Prompt user to apply FK constraints after data load
            # fk_apply = input("Apply FK ALTER constraints after data load? Type 'fk' to approve: ").strip().lower()
            # if fk_apply == "fk":
            #     log("Applying FK ALTER TABLE statements ...")
            #     for stmt in fk_alter_stmts:
            #         try:
            #             tgt_cur = tgt_conn.cursor()
            #             tgt_cur.execute(stmt)
            #             tgt_cur.close()
            #             log(f"[OK] {stmt[:80]}")
            #         except Exception as e:
            #             log(f"[FAIL] FK {stmt[:80]}: {e}")
            #     tgt_conn.commit()
            #     log("All FK constraints attempted.")
            src_conn.close()
            tgt_conn.close()
        except Exception as e:
            log(f"[FAIL] Data migration failed: {e}")
    else:
        log("\nDATA MIGRATION REJECTED or skipped by user.")

if __name__ == "__main__":
    main()