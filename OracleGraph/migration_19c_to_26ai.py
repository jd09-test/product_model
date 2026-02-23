import oracledb
import cx_Oracle
import json
from pathlib import Path

CONFIG_PATH = Path(__file__).with_name("config.json")

def log(msg):
    print(msg)

def generate_ddl(graph_model):
    node_pk_cols = {}
    ddls = []
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


    full_ddl = []
    for ddl in ddls:
        full_ddl.append(ddl)
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

def load_config(config_path: Path = CONFIG_PATH):
    with config_path.open() as f:
        return json.load(f)

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

    config = load_config()
    schema = config.get("19C_SCHEMA")
    graph_model_path = Path(__file__).with_name(config.get("GRAPH_JSON_PATH", "graph_model.json"))
    last_updated_date = config.get("QUERY_DATE")
    last_updated_format = config.get("DATE_FORMAT")
    batch_size = int(config.get("BATCH_SIZE"))
    oracle_client_path = config.get("19C_CLIENT_PATH")

    source_config = {
        "user": config.get("19C_USER"),
        "password": config.get("19C_PASS"),
        "dsn": config.get("19C_DSN"),
    }
    target_config = {
        "user": config.get("26AI_USER"),
        "password": config.get("26AI_PASSWORD"),
        "dsn": config.get("26AI_DSN"),
        "config_dir": config.get("26AI_CONFIG_DIR"),
        "wallet_location": config.get("26AI_WALLET_LOCATION"),
        "wallet_password": config.get("26AI_WALLET_PASSWORD"),
    }

    with graph_model_path.open() as f:
        graph_model = json.load(f)
    ddl_out = generate_ddl(graph_model)
    print(ddl_out)
    with open("create_26ai_schema.sql", "w") as outf:
        outf.write(ddl_out)
    # Get all table names that will be (re)created
    table_list = [node['name'] for node in graph_model['nodes']]

    # Optional DROP statement
    drop_input = input("Drop ALL created tables in 26ai DB before CREATE? Type 'drop' to confirm: ").strip().lower()
    drop_before_create = drop_input == 'drop'

    source_conn = None
    if drop_before_create:
        log("\nConnecting to target database to drop tables ...")
        try:
            target_conn = oracledb.connect(**target_config)
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
            target_conn = oracledb.connect(**target_config)
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
    log("\nConnecting to source database ...")
    try:
        if oracle_client_path:
            cx_Oracle.init_oracle_client(lib_dir=oracle_client_path)
        source_conn = cx_Oracle.connect(**source_config)
        log(f"Connected to source Oracle: {source_conn.version}")
    except Exception as e:
        log(f"Failed to connect to source Oracle DB: {e}")
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

        def apply_last_updated_filter(where_clause, date_column):
            if not date_column:
                return where_clause
            date_filter = f"{date_column} >= TO_DATE('{last_updated_date}', '{last_updated_format}')"
            if where_clause:
                return f"{where_clause} AND {date_filter}"
            return date_filter

        def col_alias_pairs(properties):
            return [f'{col} AS "{alias}"' for col, alias in properties.items()]
        if len(node["table"]) == 1:
            tbl = node["table"][0]
            cols = ", ".join(col_alias_pairs(node["properties"]))
            sql = f"SELECT {cols} FROM {schema}.{tbl}"
            if "filter" in node:
                where_clause = parse_filter(node["filter"])
                where_clause = apply_last_updated_filter(where_clause, "LAST_UPD")
            else:
                where_clause = apply_last_updated_filter('', "LAST_UPD")
            if where_clause:
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
                where_clause = apply_last_updated_filter(where_clause, "LAST_UPD")
            else:
                where_clause = apply_last_updated_filter('', "LAST_UPD")
            if where_clause:
                sql += f" WHERE {where_clause}"
            print(sql)
            return sql
        raise Exception("Unsupported node structure (multi-table join with >2 tables not implemented)")

    def migrate_node_data(node, src_conn, tgt_conn, schema, batch_size):
        tgt_cols = list(node['properties'].values())
        pk_col = node['properties'].get('ROW_ID')
        sql = build_select_sql(node, schema)
        src_cur = src_conn.cursor()
        src_cur.execute(sql)
        rows = src_cur.fetchall()
        log(f"Migrating {len(rows)} row(s) from node {node['name']} ...")
        tgt_cur = tgt_conn.cursor()
        insert_cols = ', '.join(tgt_cols)
        pholders = [':' + str(i+1) for i in range(len(tgt_cols))]

        merge_sql = None
        if pk_col:
            src_select = ', '.join([f"{ph} AS {col}" for ph, col in zip(pholders, tgt_cols)])
            on_clause = f"tgt.{pk_col} = src.{pk_col}"
            update_cols = [col for col in tgt_cols if col != pk_col]
            update_clause = ''
            if update_cols:
                set_exprs = ', '.join([f"tgt.{col} = src.{col}" for col in update_cols])
                update_clause = f" WHEN MATCHED THEN UPDATE SET {set_exprs}"
            merge_sql = (
                f"MERGE INTO {node['name']} tgt "
                f"USING (SELECT {src_select} FROM dual) src "
                f"ON ({on_clause})"
                f"{update_clause} "
                f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({', '.join(['src.' + col for col in tgt_cols])})"
            )
        else:
            isql = f"INSERT INTO {node['name']} ({insert_cols}) VALUES ({', '.join(pholders)})"
        n_total = len(rows)
        n_batches = (n_total + batch_size - 1) // batch_size
        for b in range(n_batches):
            batch_rows = rows[b*batch_size:(b+1)*batch_size]
            try:
                if merge_sql:
                    tgt_cur.executemany(merge_sql, batch_rows)
                else:
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
            src_conn = cx_Oracle.connect(**source_config)
            tgt_conn = oracledb.connect(**target_config)
            for node in graph_model['nodes']:
                migrate_node_data(node, src_conn, tgt_conn, schema, batch_size)
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
