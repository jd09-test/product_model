"""
Oracle 19c -> 26ai Property Graph Migration Tool
=====================================================
Migrates schema and data from an Oracle 19c database to an Oracle 26ai
property graph database. Driven entirely by a graph model JSON file that
describes the target vertex tables, their source tables, property columns,
filter conditions, and join keys.

Architecture:
  - Source DB (19c) requires thick mode
  - Target DB (26ai) uses thin mode with wallet.
  - These cannot coexist in the same process, so all source queries run in
    a dedicated child process that streams row chunks back via a Queue.
  - The main process stays in thin mode and handles the 26ai target only.

Scalability:
  - Rows are NEVER fully loaded into memory.
  - The child process fetches CHUNK_SIZE rows at a time via cursor.fetchmany()
    and puts each chunk onto the Queue immediately.
  - The main process inserts each chunk into 26ai and discards it.
  - Peak memory = ~2 x CHUNK_SIZE rows, regardless of table size.

Queue message protocol:
  ("data",     node_name, chunk)   — list of rows, len <= CHUNK_SIZE
  ("done",     node_name, total)   — all chunks for this node sent
  ("error",    node_name, str(e))  — query failed, node skipped
  ("fatal",    None,      str(e))  — source connection failed, abort all
  ("sentinel", None,      None)    — child finished all queries, main loop exits

Pipeline (three interactive steps):
  1. DDL Generation  — Reads the graph model, writes CREATE TABLE SQL to file.
  2. Schema Apply    — Optionally drops existing tables, then executes the DDL
                       against the 26ai target database.
  3. Data Migration  — Streams rows from the 19c source and upserts them into
                       the 26ai target in configurable chunk sizes.

Each step requires explicit user confirmation before touching any database.

Usage:
    python3 migrate.py \\
        --config      config.json \\
        --graph_model graph_model.json \\
        --ddl_output  create_26ai_schema.sql

Expected config.json keys:
    19C_USER, 19C_PASS, 19C_DSN, 19C_SCHEMA, 19C_CLIENT_PATH
    26AI_USER, 26AI_PASSWORD, 26AI_DSN,
    26AI_CONFIG_DIR, 26AI_WALLET_LOCATION, 26AI_WALLET_PASSWORD
    GRAPH_JSON_PATH  (optional, overridden by --graph_model)
    DDL_OUTPUT_PATH  (optional, overridden by --ddl_output)
    QUERY_DATE       (incremental cut-off date for LAST_UPD filter)
    DATE_FORMAT      (Oracle TO_DATE format string, e.g. 'YYYY-MM-DD')
    BATCH_SIZE       (number of rows per insert/merge chunk)

Expected graph_model.json structure:
    {
        "nodes": [
            {
                "name": "PRODUCTVOD",
                "properties": {
                    "ROW_ID": "ROW_ID",       # source col -> target col
                    "ALIAS":  "VOD_NAME"
                },
                "table":   ["S_PROD_INT"],     # one or two source tables
                "join_on": {"T1.KEY": "T2.KEY"},  # required only for 2-table join
                "filter":  {"ACTIVE_FLG": "Y"}    # optional; see parse_filter()
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
import multiprocessing
import oracledb
from pathlib import Path
from typing import Dict, List, Optional

SCRIPT_DIR = Path(__file__).parent

# Default chunk size: 10_000 rows x ~200 bytes/row ~ 2 MB per queue message.
# Tune via BATCH_SIZE in config.json.
DEFAULT_CHUNK_SIZE = 10_000


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def log(msg: str) -> None:
    """Print a message to stdout with immediate flush for real-time progress."""
    print(msg, flush=True)


# ---------------------------------------------------------------------------
# Config & path helpers
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    """
    Define and parse command-line arguments for the migration tool.

    Returns:
        A Namespace with attributes:
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
        help="Path to graph model JSON (overrides GRAPH_JSON_PATH in config)",
    )
    parser.add_argument(
        "--ddl_output",
        default=None,
        help="Output path for generated 26ai schema SQL (overrides DDL_OUTPUT_PATH in config)",
    )
    return parser.parse_args()


# ---------------------------------------------------------------------------
# DDL generation
# ---------------------------------------------------------------------------

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
        by blank lines, ready to write to a SQL file or execute directly.
    """
    ddl_blocks = []

    for node in graph_model["nodes"]:
        table_name = node["name"]
        column_map = node["properties"]        # { display_alias: DB_column_name }
        pk_col     = column_map.get("ROW_ID")
        columns    = [f"  {col} VARCHAR2(4000)" for col in column_map.values()]

        lines = []
        for i, col_def in enumerate(columns):
            needs_comma = i < len(columns) - 1 or pk_col
            lines.append(f"{col_def}{',' if needs_comma else ''}")
        if pk_col:
            lines.append(f"  CONSTRAINT PK_{table_name} PRIMARY KEY ({pk_col})")

        ddl_blocks.append(
            f"CREATE TABLE {table_name} (\n" + "\n".join(lines) + "\n);"
        )

    return "\n\n".join(ddl_blocks)


# ---------------------------------------------------------------------------
# SQL helpers
# ---------------------------------------------------------------------------

def parse_filter(filter_obj: Dict) -> str:
    """
    Recursively convert a structured filter dict into a SQL WHERE clause string.

    Supports logical operators (AND, OR, NOT) and column-level comparisons
    using named operators (gt, gte, lt, lte, ne, eq) or direct value equality.
    Also handles IS NULL / IS NOT NULL shorthand values.

    Operator mapping:
        gt  -> >     gte -> >=
        lt  -> <     lte -> <=
        ne  -> <>    eq  -> =

    Examples:
        {"ACTIVE_FLG": "Y"}
            -> "ACTIVE_FLG='Y'"

        {"AND": [{"STATUS": {"ne": "D"}}, {"TYPE": "PROD"}]}
            -> "(STATUS <> 'D' AND TYPE='PROD')"

        {"LAST_UPD": "IS NOT NULL"}
            -> "LAST_UPD IS NOT NULL"

    Parameters:
        filter_obj : A dict following the structured filter schema.

    Returns:
        A SQL WHERE clause fragment (without the WHERE keyword).

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
            clauses.append(f"{key} {value.strip().upper()}")
        else:
            v_str = f"'{value}'" if isinstance(value, str) else str(value)
            clauses.append(f"{key}={v_str}")

    return " AND ".join(clauses)


def build_select_sql(
    node:                Dict,
    schema:              str,
    last_updated_date:   Optional[str],
    last_updated_format: Optional[str],
) -> str:
    """
    Build a SELECT statement to extract data for one graph node from the source DB.

    Supports two node shapes:
      - Single-table : SELECT cols FROM schema.table [WHERE ...]
      - Two-table join: SELECT cols FROM schema.t1 JOIN schema.t2 ON key=key [WHERE ...]

    Appends an incremental LAST_UPD cut-off filter if last_updated_date is set.
    Any structural filter defined in node["filter"] is also applied.

    Parameters:
        node               : A single node dict from graph_model["nodes"].
        schema             : Oracle schema name to prefix source table references.
        last_updated_date  : Date string for incremental lower bound on LAST_UPD.
                             Pass None or empty string to skip.
        last_updated_format : Oracle TO_DATE format (e.g. 'YYYY-MM-DD'). None to skip.

    Returns:
        A SQL SELECT string ready to execute against the 19c source database.

    Raises:
        NotImplementedError : If the node has more than two source tables.
    """

    def col_alias_pairs(properties: Dict) -> List[str]:
        """Format property map as 'DB_COL AS "ALIAS"' expressions for SELECT."""
        return [f'{col} AS "{alias}"' for col, alias in properties.items()]

    def apply_date_filter(where_clause: str, date_col: str) -> str:
        """Append the incremental LAST_UPD filter to an existing WHERE clause."""
        if not last_updated_date or not last_updated_format:
            return where_clause
        date_filter = (
            f"{date_col} >= TO_DATE('{last_updated_date}', '{last_updated_format}')"
        )
        return f"{where_clause} AND {date_filter}" if where_clause else date_filter

    tables    = node["table"]
    cols      = ", ".join(col_alias_pairs(node["properties"]))
    where     = parse_filter(node["filter"]) if "filter" in node else ""
    where     = apply_date_filter(where, "LAST_UPD")
    where_sql = f" WHERE {where}" if where else ""
    print(f"SELECT {cols} FROM {schema}.{tables[0]}{where_sql}")
    if len(tables) == 1:
        return f"SELECT {cols} FROM {schema}.{tables[0]}{where_sql}"

    if len(tables) == 2 and "join_on" in node:
        tbl1, tbl2 = tables
        left, right = next(iter(node["join_on"].items()))
        return (
            f"SELECT {cols} "
            f"FROM {schema}.{tbl1} "
            f"JOIN {schema}.{tbl2} ON {left}={right}"
            f"{where_sql}"
        )

    raise NotImplementedError(
        f"Node '{node['name']}' has {len(tables)} tables — "
        "only 1 or 2 tables (with join_on) are supported."
    )


# ---------------------------------------------------------------------------
# Target DB — schema operations (thin mode, main process only)
# ---------------------------------------------------------------------------

def connect_target(target_config: Dict):
    """
    Open a thin-mode oracledb connection to the 26ai target database.

    Parameters:
        target_config : Dict of oracledb.connect() keyword arguments.

    Returns:
        An active oracledb connection.
    """
    conn = oracledb.connect(**target_config)
    log(f"[TARGET] Connected to Oracle 26ai: {conn.version}")
    return conn


def drop_tables(table_names: List[str], conn) -> None:
    """
    Drop a list of tables from the 26ai target database.

    Uses CASCADE CONSTRAINTS PURGE to handle FK dependencies and bypass the
    recycle bin. Tables that do not exist are warned and skipped gracefully.

    Parameters:
        table_names : List of table names to drop.
        conn        : Active oracledb connection to the target database.
    """
    log("\n-- Dropping tables in 26ai target DB before CREATE --")
    with conn.cursor() as cur:
        for tbl in table_names:
            try:
                cur.execute(f"DROP TABLE {tbl} CASCADE CONSTRAINTS PURGE")
                log(f"  [OK]   Dropped: {tbl}")
            except Exception as e:
                log(f"  [WARN] Could not drop {tbl} (may not exist): {e}")


def execute_ddl_on_target(ddl_sql: str, conn) -> bool:
    """
    Execute a DDL string statement-by-statement against the 26ai target database.

    Splits on semicolons, skips blank lines and SQL comments (-- and REM),
    and executes each remaining statement individually. Logs success or failure
    per statement.

    Parameters:
        ddl_sql : Full DDL string (may contain multiple statements).
        conn    : Active oracledb connection to the target database.

    Returns:
        True if all statements succeeded, False if any statement failed.
    """
    log("\n-- Executing DDL in 26ai target database --")
    all_ok = True

    with conn.cursor() as cur:
        for stmt in (s.strip() for s in ddl_sql.split(";") if s.strip()):
            first_line = stmt.splitlines()[0]
            if first_line.startswith("--") or first_line.upper().startswith("REM "):
                continue
            try:
                cur.execute(stmt)
                log(f"  [OK]   {first_line[:80]}")
            except Exception as e:
                log(f"  [FAIL] {first_line[:80]}\n         Error: {e}")
                all_ok = False

    return all_ok


# ---------------------------------------------------------------------------
# Target DB — data load helpers (thin mode, main process only)
# ---------------------------------------------------------------------------

def _build_dml(node: Dict) -> str:
    """
    Build the INSERT or MERGE DML string for a node. Called once per node and
    reused for every chunk, avoiding repeated string construction.

    Strategy:
      - With ROW_ID (PK present) -> MERGE: updates existing rows, inserts new ones.
        Makes migration idempotent — safe to re-run without duplicate key errors.
      - Without ROW_ID           -> plain INSERT (no conflict detection).

    Parameters:
        node : A single node dict from graph_model["nodes"].

    Returns:
        A parameterised DML string suitable for cursor.executemany().
    """
    tgt_cols    = list(node["properties"].values())
    pk_col      = node["properties"].get("ROW_ID")
    table_name  = node["name"]
    insert_cols = ", ".join(tgt_cols)
    pholders    = [f":{i+1}" for i in range(len(tgt_cols))]

    if pk_col:
        # MERGE using SELECT ... FROM dual so executemany() works correctly.
        # Each bind variable becomes a named column; Oracle resolves per-row.
        src_cols      = ", ".join(f":{i+1} AS {col}" for i, col in enumerate(tgt_cols))
        on_clause     = f"tgt.{pk_col} = src.{pk_col}"
        update_cols   = [c for c in tgt_cols if c != pk_col]
        update_clause = ""
        if update_cols:
            set_exprs     = ", ".join(f"tgt.{c} = src.{c}" for c in update_cols)
            update_clause = f" WHEN MATCHED THEN UPDATE SET {set_exprs}"
        ins_vals = ", ".join(f"src.{c}" for c in tgt_cols)
        return (
            f"MERGE INTO {table_name} tgt "
            f"USING (SELECT {src_cols} FROM dual) src "
            f"ON ({on_clause})"
            f"{update_clause} "
            f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({ins_vals})"
        )

    return (
        f"INSERT INTO {table_name} ({insert_cols}) "
        f"VALUES ({', '.join(pholders)})"
    )


def insert_chunk(
    dml:        str,
    chunk:      list,
    node_name:  str,
    conn,
    chunk_num:  int,
) -> None:
    """
    Insert one chunk of rows into the target table using executemany().

    Each chunk is committed immediately after insertion. If the chunk fails,
    the transaction is rolled back and a warning is logged — other chunks
    for the same node continue.

    Parameters:
        dml       : Parameterised INSERT or MERGE statement.
        chunk     : List of row tuples to insert.
        node_name : Table name, used only for log messages.
        conn      : Active oracledb connection to the target database.
        chunk_num : Chunk sequence number, used only for log messages.
    """
    with conn.cursor() as cur:
        try:
            cur.executemany(dml, chunk)
            conn.commit()
            log(f"  [CHUNK {chunk_num}] Inserted {len(chunk):,} rows -> {node_name}")
        except Exception as e:
            conn.rollback()
            log(f"  [WARN]  Chunk {chunk_num} failed for '{node_name}': {e}")


# ---------------------------------------------------------------------------
# Source DB streaming — child process only (thick mode)
# ---------------------------------------------------------------------------

def _stream_worker(
    source_config:      Dict,
    oracle_client_path: str,
    queries:            List,
    queue:              multiprocessing.Queue,
    chunk_size:         int,
) -> None:
    """
    Child process entry point: initialises thick mode, connects to the 19c
    source DB, and streams all query results to the main process in chunks.

    Runs entirely in a separate process so that thick mode (needed for the
    legacy 0x939 password verifier) does not conflict with the thin-mode
    oracledb connection used by the main process for the 26ai target.

    For each query, fetches rows CHUNK_SIZE at a time using cursor.fetchmany()
    with arraysize tuned to match, minimising network round-trips while keeping
    per-message queue payload small.

    Queue message protocol (sent in order):
      ("data",     node_name, chunk)  — one chunk of rows
      ("done",     node_name, total)  — signals end of this node's data
      ("error",    node_name, str(e)) — query failed; node is skipped
      ("fatal",    None,      str(e)) — DB connection failed; abort everything
      ("sentinel", None,      None)   — all queries done; main loop must exit

    Parameters:
        source_config      : Dict of oracledb.connect() kwargs for the 19c DB.
        oracle_client_path : Path to Oracle Instant Client libs for thick mode.
        queries            : List of (node_name, sql) tuples to execute.
        queue              : Shared multiprocessing.Queue for result streaming.
        chunk_size         : Number of rows per fetchmany() call and queue message.
    """
    try:
        oracledb.init_oracle_client(lib_dir=oracle_client_path)
        conn = oracledb.connect(**source_config)
        log(f"[SOURCE] Connected to Oracle 19c: {conn.version}")
    except Exception as e:
        queue.put(("fatal", None, str(e)))
        return

    for node_name, sql in queries:
        total = 0
        try:
            with conn.cursor() as cur:
                # arraysize = chunk_size reduces network round-trips from
                # (total_rows / 100) to (total_rows / chunk_size)
                cur.arraysize = chunk_size
                cur.execute(sql)
                while True:
                    chunk = cur.fetchmany(chunk_size)
                    if not chunk:
                        break
                    total += len(chunk)
                    queue.put(("data", node_name, chunk))
            queue.put(("done", node_name, total))
        except Exception as e:
            queue.put(("error", node_name, str(e)))

    conn.close()
    queue.put(("sentinel", None, None))   # always the last message; unblocks main loop


# ---------------------------------------------------------------------------
# Stream orchestration — main process
# ---------------------------------------------------------------------------

def stream_and_load(
    source_config:      Dict,
    oracle_client_path: str,
    queries:            List,
    graph_model:        Dict,
    target_config:      Dict,
    chunk_size:         int,
) -> None:
    """
    Spawn the source-fetch child process and consume its output stream,
    writing each chunk to the 26ai target immediately.

    Memory profile: at most two chunks exist simultaneously — one in the queue
    and one being inserted — regardless of total row count.

    Back-pressure: Queue(maxsize=20) causes the child to block once 20 chunks
    are queued, preventing it from racing too far ahead of the main process and
    accumulating unbounded memory.

    Parameters:
        source_config      : Connection kwargs for the 19c source DB.
        oracle_client_path : Path to Oracle Instant Client for thick mode.
        queries            : List of (node_name, sql) tuples to stream.
        graph_model        : Full graph model dict (used to build DML per node).
        target_config      : Connection kwargs for the 26ai target DB.
        chunk_size         : Rows per chunk (controls memory and throughput).
    """
    node_lookup = {node["name"]: node for node in graph_model["nodes"]}

    # maxsize=20 applies back-pressure so the child cannot run unboundedly ahead
    queue = multiprocessing.Queue(maxsize=20)
    proc  = multiprocessing.Process(
        target=_stream_worker,
        args=(source_config, oracle_client_path, queries, queue, chunk_size),
    )
    proc.start()

    tgt_conn       = connect_target(target_config)
    dml_cache      = {}   # node_name -> pre-built DML (built once, reused per chunk)
    chunk_counters = {}   # node_name -> chunk sequence number for logging

    try:
        while True:
            msg  = queue.get()   # blocks until child sends a message
            kind = msg[0]

            if kind == "sentinel":
                # Child has finished all queries — safe to exit the loop
                break

            if kind == "fatal":
                log(f"[FATAL] Source connection failed: {msg[2]}")
                break

            node_name = msg[1]

            if kind == "data":
                chunk = msg[2]
                if node_name not in dml_cache:
                    dml_cache[node_name]      = _build_dml(node_lookup[node_name])
                    chunk_counters[node_name] = 0
                chunk_counters[node_name] += 1
                insert_chunk(
                    dml_cache[node_name],
                    chunk,
                    node_name,
                    tgt_conn,
                    chunk_counters[node_name],
                )

            elif kind == "done":
                log(f"  [DONE] {node_name}: {msg[2]:,} total rows migrated.")

            elif kind == "error":
                log(f"  [ERROR] {node_name} fetch failed: {msg[2]}")

    finally:
        tgt_conn.close()
        proc.join()


# ---------------------------------------------------------------------------
# Main orchestration
# ---------------------------------------------------------------------------

def main() -> None:
    """
    Orchestrate the full 19c-to-26ai property graph migration in three steps.

    Step 1 — DDL Generation:
        Reads the graph model and generates CREATE TABLE SQL, then writes it
        to the configured output file. No database connection required.

    Step 2 — Schema Apply (interactive):
        a) Prompts whether to DROP all target tables first (type 'drop').
        b) Prompts whether to execute the generated DDL (type 'yes').
        Both sub-steps require explicit confirmation before touching the DB.

    Step 3 — Data Migration (interactive):
        Prompts whether to run data migration (type 'migrate').
        If confirmed, builds SELECT queries from the graph model, spawns a
        child process to stream rows from the 19c source, and upserts each
        chunk into the 26ai target as it arrives.

    All database-touching steps are gated behind interactive prompts and can
    be skipped safely without affecting other steps.
    """
    log("=" * 70)
    log("Oracle 19c -> 26ai Property Graph Migration")
    log("=" * 70)

    # -- CLI & config --------------------------------------------------------
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
    chunk_size          = int(config.get("BATCH_SIZE", DEFAULT_CHUNK_SIZE))
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

    # -- Step 1: DDL generation ----------------------------------------------
    log("\n[STEP 1] Generating DDL from graph model ...")
    if not graph_model_path.exists():
        raise FileNotFoundError(f"Graph model not found: {graph_model_path}")

    with graph_model_path.open() as f:
        graph_model = json.load(f)

    ddl_sql = generate_ddl(graph_model)
    ddl_output_path.write_text(ddl_sql)
    #log(ddl_sql)
    log(f"\n  DDL written to: {ddl_output_path}")

    table_names = [node["name"] for node in graph_model["nodes"]]

    # -- Step 2a: Optional DROP ----------------------------------------------
    log("\n[STEP 2] Schema apply ...")
    if input(
        "Drop ALL target tables in 26ai DB before CREATE? Type 'drop' to confirm: "
    ).strip().lower() == "drop":
        log("  Connecting to 26ai target DB to drop tables ...")
        try:
            tgt_conn = connect_target(target_config)
            drop_tables(table_names, tgt_conn)
        except Exception as e:
            log(f"  [FAIL] Could not connect to 26ai DB for DROP: {e}")
            return
        finally:
            try:
                tgt_conn.close()
            except Exception:
                pass

    # -- Step 2b: Apply DDL --------------------------------------------------
    if input(
        "Execute the generated DDL in the 26ai target database? Type 'yes' to approve: "
    ).strip().lower() == "yes":
        log("  Connecting to 26ai target DB to apply DDL ...")
        try:
            tgt_conn = connect_target(target_config)
        except Exception as e:
            log(f"  [FAIL] Could not connect to 26ai DB: {e}")
            return

        ok = execute_ddl_on_target(ddl_sql, tgt_conn)
        tgt_conn.close()

        if ok:
            log("\n  [OK] All DDL statements executed successfully.")
        else:
            log("\n  [WARN] Some DDL statements failed — check output above before migrating data.")
    else:
        log("  DDL execution skipped.")

    # -- Step 3: Data migration ----------------------------------------------
    log("\n[STEP 3] Data migration ...")
    if input(
        "Migrate data from 19c source to 26ai target? Type 'migrate' to approve: "
    ).strip().lower() != "migrate":
        log("  Data migration skipped.")
        return

    # Build all SELECT queries in main process — no DB connection needed
    queries = []
    for node in graph_model["nodes"]:
        try:
            sql = build_select_sql(node, schema, last_updated_date, last_updated_format)
            queries.append((node["name"], sql))
            #log(f"  [SQL] {node['name']}: {sql[:120]}")
        except Exception as e:
            log(f"  [WARN] Could not build SQL for '{node['name']}': {e}")

    log(f"\n  Streaming in chunks of {chunk_size:,} rows ...")
    stream_and_load(
        source_config      = source_config,
        oracle_client_path = oracle_client_path,
        queries            = queries,
        graph_model        = graph_model,
        target_config      = target_config,
        chunk_size         = chunk_size,
    )

    log("\n[DONE] Migration complete.")


if __name__ == "__main__":
    multiprocessing.freeze_support()
    main()