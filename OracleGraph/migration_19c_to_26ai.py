"""
Oracle 19c-to-26ai Property Graph Migration Tool
=====================================================
Migrates schema and data from a Oracle 19c database to an Oracle 26ai
property graph database. Driven entirely by a graph model JSON file that
describes the target vertex tables, their source tables, property columns,
filter conditions, and join keys.

Pipeline (three interactive steps):
  1. DDL Generation  — Reads the graph model and writes a CREATE TABLE SQL file.
  2. Schema Apply    — Optionally drops existing tables, then executes the DDL
                       against the 26ai target database.
  3. Data Migration  — Reads rows from the 19c source DB and upserts them into
                       the 26ai target DB in configurable batch sizes.

Each step requires explicit user confirmation before touching any database.

Usage:
    python3 oracle_graph_migration.py \\
        --config      config.json \\
        --graph_model graph_model.json \\
        --ddl_output  create_26ai_schema.sql

Expected config.json keys:
    19C_USER, 19C_PASS, 19C_DSN, 19C_SCHEMA, 19C_CLIENT_PATH
    26AI_USER, 26AI_PASSWORD, 26AI_DSN,
    26AI_CONFIG_DIR, 26AI_WALLET_LOCATION, 26AI_WALLET_PASSWORD
    GRAPH_JSON_PATH  (optional, overridden by --graph_model)
    DDL_OUTPUT_PATH  (optional, overridden by --ddl_output)
    QUERY_DATE       (used as incremental cut-off for LAST_UPD filter)
    DATE_FORMAT      (Oracle TO_DATE format string, e.g. 'YYYY-MM-DD')
    BATCH_SIZE       (number of rows per insert/merge batch)

Expected graph_model.json structure:
    {
        "nodes": [
            {
                "name": "PRODUCTVOD",
                "properties": {
                    "ROW_ID": "ROW_ID",       # key → DB column name
                    "ALIAS":  "VOD_NAME"
                },
                "table":  ["S_PROD_INT"],     # one or two source tables
                "join_on": {"T1.KEY": "T2.KEY"},  # required only for two-table join
                "filter": { "ACTIVE_FLG": "Y" }   # optional; see parse_filter()
            }
        ],
        "relationships": [
            {
                "type":     "PRODUCTVOD_HAS_VERSION",
                "from":     "PRODUCTVOD",
                "to":       "VODVERSION",
                "from_key": "ROW_ID",
                "to_key":   "VOD_ID"
            }
        ]
    }
"""

import argparse
import json
from pathlib import Path
from typing import Dict, List, Optional

import cx_Oracle       # Oracle 19c client (thick mode)
import oracledb        # Oracle 26ai target client (thin mode)

# ── Constants ─────────────────────────────────────────────────────────────────

SCRIPT_DIR = Path(__file__).parent


# ── Logging ───────────────────────────────────────────────────────────────────

def log(msg: str) -> None:
    """Print a timestamped message to stdout. Used throughout for consistent output."""
    print(msg)


# ── Config & path helpers ─────────────────────────────────────────────────────

def load_config(config_path: Path) -> Dict:
    """
    Load the migration configuration from a JSON file.

    Parameters:
        config_path : Absolute path to the config JSON file.

    Returns:
        A dict containing all config values (credentials, paths, batch settings).
    """
    with config_path.open() as f:
        return json.load(f)


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


# ── CLI ───────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    """
    Define and parse command-line arguments for the migration tool.

    Returns:
        A Namespace object with attributes:
          config      — path to the JSON config file
          graph_model — path to the graph model JSON (overrides config value)
          ddl_output  — path for the generated DDL SQL file (overrides config value)
    """
    parser = argparse.ArgumentParser(
        description="Migrate schema and data from Oracle 19c to Oracle 26ai property graph"
    )
    parser.add_argument(
        "--config",
        default="config.json",
        help="Path to the JSON config file (default: config.json)",
    )
    parser.add_argument(
        "--graph_model",
        default=None,
        help="Path to the graph model JSON file (overrides GRAPH_JSON_PATH in config)",
    )
    parser.add_argument(
        "--ddl_output",
        default=None,
        help="Output path for the generated 26ai schema SQL (overrides DDL_OUTPUT_PATH in config)",
    )
    return parser.parse_args()


# ── DDL generation ────────────────────────────────────────────────────────────

def generate_ddl(graph_model: Dict) -> str:
    """
    Generate CREATE TABLE SQL statements for every node in the graph model.

    For each node:
      - Creates one VARCHAR2(4000) column per property.
      - Adds a PRIMARY KEY constraint if the node has a 'ROW_ID' property.

    Parameters:
        graph_model : The full graph model dict with a "nodes" list.

    Returns:
        A single string containing all CREATE TABLE statements separated
        by newlines, ready to write to a SQL file or execute directly.
    """
    ddl_blocks = []

    for node in graph_model["nodes"]:
        table_name = node["name"]
        column_map = node["properties"]          # { display_alias → DB_column_name }
        columns    = [f"{col} VARCHAR2(4000)" for col in column_map.values()]
        pk_col     = column_map.get("ROW_ID")   # Use ROW_ID as PK if present

        create_lines = []
        for idx, col_def in enumerate(columns):
            # Trailing comma needed if more lines follow (more columns or PK line)
            needs_comma = idx < len(columns) - 1 or pk_col
            create_lines.append(f"  {col_def}{',' if needs_comma else ''}")

        if pk_col:
            create_lines.append(f"  CONSTRAINT PK_{table_name} PRIMARY KEY ({pk_col})")

        block = "\n".join(
            [f"CREATE TABLE {table_name} ("] + create_lines + [");"]
        )
        ddl_blocks.append(block)

    return "\n\n".join(ddl_blocks)


# ── Schema operations (target DB) ─────────────────────────────────────────────

def drop_tables_in_target_db(table_list: List[str], conn) -> None:
    """
    Drop a list of tables from the 26ai target database before schema recreation.

    Uses CASCADE CONSTRAINTS PURGE to handle foreign key dependencies and
    skip the recycle bin. Tables that do not exist are silently warned and skipped.

    Parameters:
        table_list : List of table names to drop.
        conn       : An active oracledb connection to the target database.
    """
    log("\n-- Dropping tables in 26ai target DB before CREATE --")
    with conn.cursor() as cur:
        for tbl in table_list:
            try:
                cur.execute(f"DROP TABLE {tbl} CASCADE CONSTRAINTS PURGE")
                log(f"  [OK]   Dropped: {tbl}")
            except Exception as e:
                log(f"  [WARN] Could not drop {tbl} (may not exist): {e}")


def execute_ddl_file_on_target_db(sql_file: str, conn) -> bool:
    """
    Read a SQL DDL file and execute each statement against the target database.

    Splits the file content on semicolons, skips blank lines and SQL comments
    (-- and REM), and executes each remaining statement individually.
    Logs success or failure per statement.

    Parameters:
        sql_file : Path to the SQL file to execute.
        conn     : An active oracledb connection to the target database.

    Returns:
        True if all statements succeeded, False if any statement failed.
    """
    log("\n-- Executing DDL in 26ai target database --")
    with open(sql_file, "r") as f:
        raw_sql = f.read()

    # Split on semicolons; strip whitespace; drop empty or comment-only fragments
    statements = [s.strip() for s in raw_sql.split(";") if s.strip()]
    all_ok = True

    with conn.cursor() as cur:
        for stmt in statements:
            first_line = stmt.splitlines()[0]
            if not stmt or first_line.startswith("--") or first_line.upper().startswith("REM "):
                continue
            try:
                cur.execute(stmt)
                log(f"  [OK]   {first_line[:80]}")
            except Exception as e:
                log(f"  [FAIL] {first_line[:80]}\n         Error: {e}")
                all_ok = False

    return all_ok


# ── Filter & SQL builder ──────────────────────────────────────────────────────

def parse_filter(filter_obj) -> str:
    """
    Recursively convert a structured filter dict into a SQL WHERE clause string.

    Supports logical operators (AND, OR, NOT) and column-level comparisons
    using named operators (gt, gte, lt, lte, ne, eq) or direct value equality.
    Also handles IS NULL / IS NOT NULL shorthand values.

    Operator mapping:
        gt  → >     gte → >=
        lt  → <     lte → <=
        ne  → <>    eq  → =

    Examples:
        {"ACTIVE_FLG": "Y"}
            → "ACTIVE_FLG='Y'"

        {"AND": [{"STATUS": {"ne": "D"}}, {"TYPE": "PROD"}]}
            → "(STATUS <> 'D' AND TYPE='PROD')"

        {"LAST_UPD": "IS NOT NULL"}
            → "LAST_UPD IS NOT NULL"

    Parameters:
        filter_obj : A dict following the structured filter schema, or a
                     nested list of such dicts under AND/OR/NOT keys.

    Returns:
        A SQL WHERE clause fragment as a string (without the WHERE keyword).

    Raises:
        ValueError : If filter_obj is not a dict, an unknown operator is used,
                     or a comparison operator is applied against NULL.
    """
    if not isinstance(filter_obj, dict):
        raise ValueError(f"Filter must be a dict, got: {type(filter_obj)}")

    op_map = {"gt": ">", "gte": ">=", "lt": "<", "lte": "<=", "ne": "<>", "eq": "="}
    clauses = []

    for key, value in filter_obj.items():
        if key == "AND":
            clauses.append(f"({' AND '.join(parse_filter(c) for c in value)})")
        elif key == "OR":
            clauses.append(f"({' OR '.join(parse_filter(c) for c in value)})")
        elif key == "NOT":
            clauses.append(f"(NOT {parse_filter(value)})")
        elif isinstance(value, dict):
            # Column-level operator dict, e.g. {"gt": 5}
            for op, v in value.items():
                if op not in op_map:
                    raise ValueError(f"Unsupported filter operator: '{op}'")
                if v is None:
                    if op == "eq":
                        clauses.append(f"{key} IS NULL")
                    elif op == "ne":
                        clauses.append(f"{key} IS NOT NULL")
                    else:
                        raise ValueError(f"Cannot apply operator '{op}' against NULL")
                else:
                    v_str = f"'{v}'" if isinstance(v, str) else str(v)
                    clauses.append(f"{key} {op_map[op]} {v_str}")
        elif isinstance(value, str) and value.strip().upper() in ("IS NULL", "IS NOT NULL"):
            # Shorthand: {"COL": "IS NULL"} or {"COL": "IS NOT NULL"}
            clauses.append(f"{key} {value.strip().upper()}")
        else:
            # Direct equality: {"COL": "value"} or {"COL": 123}
            v_str = f"'{value}'" if isinstance(value, str) else str(value)
            clauses.append(f"{key}={v_str}")

    return " AND ".join(clauses)


def build_select_sql(
    node:               Dict,
    schema:             str,
    last_updated_date:  Optional[str],
    last_updated_format: Optional[str],
) -> str:
    """
    Build a SELECT statement to extract data for one graph node from the source DB.

    Supports two node shapes:
      - Single-table node: SELECT cols FROM schema.table [WHERE ...]
      - Two-table join node: SELECT cols FROM schema.t1 JOIN schema.t2 ON key=key [WHERE ...]

    Appends an incremental cut-off filter on LAST_UPD if last_updated_date is set.
    Any structural filter defined in node["filter"] is also applied.

    Parameters:
        node               : A single node dict from graph_model["nodes"].
        schema             : Oracle schema name to prefix source table references.
        last_updated_date  : Date string used as the incremental lower bound
                             for LAST_UPD (e.g. '2024-01-01'). None to skip.
        last_updated_format : Oracle TO_DATE format for last_updated_date
                              (e.g. 'YYYY-MM-DD'). None to skip.

    Returns:
        A SQL SELECT string ready to execute against the 19c source database.

    Raises:
        Exception : If the node has more than two tables (not supported).
    """

    def col_alias_pairs(properties: Dict) -> List[str]:
        """Format property map as 'DB_COL AS "ALIAS"' expressions for SELECT."""
        return [f'{col} AS "{alias}"' for col, alias in properties.items()]

    def apply_date_filter(where_clause: str, date_col: str) -> str:
        """Append the incremental LAST_UPD filter to an existing WHERE clause."""
        if not last_updated_date or not last_updated_format:
            return where_clause
        date_filter = f"{date_col} >= TO_DATE('{last_updated_date}', '{last_updated_format}')"
        return f"{where_clause} AND {date_filter}" if where_clause else date_filter

    cols = ", ".join(col_alias_pairs(node["properties"]))

    # Build the base WHERE clause from the node's structural filter (if any)
    base_where = parse_filter(node["filter"]) if "filter" in node else ""
    where_clause = apply_date_filter(base_where, "LAST_UPD")
    where_sql = f" WHERE {where_clause}" if where_clause else ""

    tables = node["table"]

    if len(tables) == 1:
        sql = f"SELECT {cols} FROM {schema}.{tables[0]}{where_sql}"

    elif len(tables) == 2 and "join_on" in node:
        tbl1, tbl2 = tables
        join_col, ref_col = next(iter(node["join_on"].items()))
        sql = (
            f"SELECT {cols} "
            f"FROM {schema}.{tbl1} "
            f"JOIN {schema}.{tbl2} ON {join_col}={ref_col}"
            f"{where_sql}"
        )

    else:
        raise Exception(
            f"Node '{node['name']}' has {len(tables)} tables — "
            f"only 1 or 2 tables (with join_on) are supported."
        )

    log(f"  [SQL] {sql}")
    return sql


# ── Data migration ────────────────────────────────────────────────────────────

def migrate_node_data(
    node:       Dict,
    src_conn,
    tgt_conn,
    schema:     str,
    batch_size: int,
    last_updated_date:   Optional[str],
    last_updated_format: Optional[str],
) -> None:
    """
    Extract all rows for one node from the 19c source DB and upsert into the 26ai target.

    Fetch strategy:
      - Executes the SELECT built by build_select_sql() on the source connection.
      - Fetches all rows into memory before writing (suitable for dataset sizes
        that fit in memory; for very large tables, refactor to use fetchmany).

    Write strategy:
      - If the node has a ROW_ID primary key → uses MERGE (upsert) to avoid
        duplicate key errors on re-runs.
      - Otherwise → uses plain INSERT (no conflict handling).
    Both strategies use executemany() in configurable batches for performance.

    Parameters:
        node               : A single node dict from graph_model["nodes"].
        src_conn           : Active cx_Oracle connection to the 19c source DB.
        tgt_conn           : Active oracledb connection to the 26ai target DB.
        schema             : Oracle schema name for source table references.
        batch_size         : Number of rows per executemany() call.
        last_updated_date  : Incremental cut-off date string (or None).
        last_updated_format : Oracle date format for last_updated_date (or None).
    """
    tgt_cols = list(node["properties"].values())
    pk_col   = node["properties"].get("ROW_ID")
    sql      = build_select_sql(node, schema, last_updated_date, last_updated_format)

    # ── Fetch from source ──────────────────────────────────────────────────
    src_cur = src_conn.cursor()
    src_cur.execute(sql)
    rows = src_cur.fetchall()
    src_cur.close()
    log(f"  Fetched {len(rows):,} row(s) from source for node '{node['name']}'")

    # ── Prepare write statement ────────────────────────────────────────────
    tgt_cur     = tgt_conn.cursor()
    insert_cols = ", ".join(tgt_cols)
    placeholders = [f":{i+1}" for i in range(len(tgt_cols))]

    if pk_col:
        # MERGE: update non-PK columns when key matches, insert when it does not
        src_select    = ", ".join(f"{ph} AS {col}" for ph, col in zip(placeholders, tgt_cols))
        update_cols   = [col for col in tgt_cols if col != pk_col]
        update_clause = ""
        if update_cols:
            set_exprs     = ", ".join(f"tgt.{col} = src.{col}" for col in update_cols)
            update_clause = f" WHEN MATCHED THEN UPDATE SET {set_exprs}"
        insert_vals  = ", ".join(f"src.{col}" for col in tgt_cols)
        write_sql = (
            f"MERGE INTO {node['name']} tgt "
            f"USING (SELECT {src_select} FROM dual) src "
            f"ON (tgt.{pk_col} = src.{pk_col})"
            f"{update_clause} "
            f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})"
        )
    else:
        # Plain INSERT — no PK to detect duplicates
        write_sql = (
            f"INSERT INTO {node['name']} ({insert_cols}) "
            f"VALUES ({', '.join(placeholders)})"
        )

    # ── Execute in batches ─────────────────────────────────────────────────
    total    = len(rows)
    n_batches = (total + batch_size - 1) // batch_size

    for b in range(n_batches):
        batch = rows[b * batch_size:(b + 1) * batch_size]
        try:
            tgt_cur.executemany(write_sql, batch)
            log(f"  [BATCH {b+1}/{n_batches}] Loaded {len(batch):,} rows into '{node['name']}'")
        except Exception as e:
            start = b * batch_size
            log(f"  [WARN] Batch {b+1} failed for '{node['name']}' "
                f"(rows {start}–{start + len(batch)}): {e}")

    tgt_conn.commit()
    tgt_cur.close()


# ── Main orchestration ────────────────────────────────────────────────────────

def main() -> None:
    """
    Orchestrate the full  -to-26ai property graph migration in three steps.

    Step 1 — DDL Generation:
        Reads the graph model and generates CREATE TABLE SQL, then writes it
        to the configured output file.

    Step 2 — Schema Apply (interactive):
        a) Prompts whether to DROP all target tables first (type 'drop').
        b) Prompts whether to execute the generated DDL (type 'yes').
        Both prompts require explicit confirmation before touching the database.

    Step 3 — Data Migration (interactive):
        a) Connects to the 19c source DB using cx_Oracle (thick mode).
        b) Prompts whether to run data migration (type 'migrate').
        c) If confirmed, iterates every node in the graph model, extracts rows
           from the source DB, and upserts them into the 26ai target DB.

    All database-touching steps are gated behind interactive prompts and can
    be skipped safely without affecting the other steps.
    """
    log("=" * 70)
    log("Oracle 19c → 26ai Property Graph Migration")
    log("=" * 70)

    # ── CLI & config ───────────────────────────────────────────────────────
    args        = parse_args()
    config_path = resolve_path(args.config)
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    config = load_config(config_path)
    schema = config.get("19C_SCHEMA")

    graph_model_path = resolve_path(
        args.graph_model or config.get("GRAPH_JSON_PATH") or "graph_model.json"
    )
    ddl_output_path = resolve_path(
        args.ddl_output or config.get("DDL_OUTPUT_PATH") or "create_26ai_schema.sql"
    )

    last_updated_date   = config.get("QUERY_DATE")
    last_updated_format = config.get("DATE_FORMAT")
    batch_size          = int(config.get("BATCH_SIZE", 500))
    oracle_client_path  = config.get("19C_CLIENT_PATH")

    source_config = {
        "user":     config.get("19C_USER"),
        "password": config.get("19C_PASS"),
        "dsn":      config.get("19C_DSN"),
    }
    target_config = {
        "user":            config.get("26AI_USER"),
        "password":        config.get("26AI_PASSWORD"),
        "dsn":             config.get("26AI_DSN"),
        "config_dir":      config.get("26AI_CONFIG_DIR"),
        "wallet_location": config.get("26AI_WALLET_LOCATION"),
        "wallet_password": config.get("26AI_WALLET_PASSWORD"),
    }

    # ── Step 1: DDL Generation ─────────────────────────────────────────────
    log("\n[STEP 1] Generating DDL from graph model ...")
    with graph_model_path.open() as f:
        graph_model = json.load(f)

    ddl_text = generate_ddl(graph_model)
    log(ddl_text)

    with ddl_output_path.open("w") as f:
        f.write(ddl_text)
    log(f"DDL written to: {ddl_output_path}")

    table_list = [node["name"] for node in graph_model["nodes"]]

    # ── Step 2a: Optional DROP ─────────────────────────────────────────────
    log("\n[STEP 2] Schema apply ...")
    drop_choice = input(
        "Drop ALL target tables in 26ai DB before CREATE? Type 'drop' to confirm: "
    ).strip().lower()

    if drop_choice == "drop":
        log("Connecting to 26ai target DB to drop tables ...")
        try:
            tgt_conn = oracledb.connect(**target_config)
            log(f"Connected. Oracle version: {tgt_conn.version}")
            drop_tables_in_target_db(table_list, tgt_conn)
        except Exception as e:
            log(f"[FAIL] Could not connect to 26ai DB for DROP: {e}")
            return
        finally:
            try:
                tgt_conn.close()
            except Exception:
                pass

    # ── Step 2b: Apply DDL ─────────────────────────────────────────────────
    apply_choice = input(
        "Execute the generated DDL in the 26ai target database? Type 'yes' to approve: "
    ).strip().lower()

    if apply_choice == "yes":
        log("Connecting to 26ai target DB to apply DDL ...")
        try:
            tgt_conn = oracledb.connect(**target_config)
            log(f"Connected. Oracle version: {tgt_conn.version}")
        except Exception as e:
            log(f"[FAIL] Could not connect to 26ai DB: {e}")
            return

        ok = execute_ddl_file_on_target_db(str(ddl_output_path), tgt_conn)
        tgt_conn.close()

        if ok:
            log("\n[OK] All DDL statements executed successfully.")
        else:
            log("\n[WARN] Some DDL statements failed — check output above for details.")
    else:
        log("DDL execution skipped by user.")

    # ── Step 3: Data Migration ─────────────────────────────────────────────
    log("\n[STEP 3] Data migration ...")
    migrate_choice = input(
        "Migrate data from 19c source to 26ai target? Type 'migrate' to approve: "
    ).strip().lower()

    if migrate_choice != "migrate":
        log("Data migration skipped by user.")
        return

    log("Connecting to 19c source DB ...")
    try:
        if oracle_client_path:
            oracledb.init_oracle_client(lib_dir=oracle_client_path)
        src_conn = oracledb.connect(
    user="sadmin",
    password="sadmin",
    dsn="phoenix610241.appsdev.fusionappsdphx1.oraclevcn.com:1551/qa241" # The alias defined in tnsnames.ora
)
        log(f"Connected to source Oracle. Version: {src_conn.version}")
    except Exception as e:
        log(f"[FAIL] Could not connect to 19c source DB: {e}")
        return

    log("Connecting to 26ai target DB ...")
    try:
        tgt_conn = oracledb.connect(**target_config)
        log(f"Connected to 26ai target Oracle. Version: {tgt_conn.version}")
    except Exception as e:
        log(f"[FAIL] Could not connect to 26ai target DB: {e}")
        src_conn.close()
        return

    try:
        for node in graph_model["nodes"]:
            log(f"\nMigrating node: {node['name']}")
            migrate_node_data(
                node               = node,
                src_conn           = src_conn,
                tgt_conn           = tgt_conn,
                schema             = schema,
                batch_size         = batch_size,
                last_updated_date  = last_updated_date,
                last_updated_format = last_updated_format,
            )
        log("\n[OK] All data migration complete.")
    except Exception as e:
        log(f"[FAIL] Data migration error: {e}")
    finally:
        src_conn.close()
        tgt_conn.close()


if __name__ == "__main__":
    main()