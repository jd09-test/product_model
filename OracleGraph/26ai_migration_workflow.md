# 26ai DB Migration Process – Oracle Legacy to Property Graph

This Confluence-ready guide documents the end-to-end automation flow that powers our Oracle legacy-to-26ai property graph migration. The workflow is orchestrated by three companion scripts inside the `OracleGraph/` folder:

1. `oracle_legacy_to_graph.py` – Generates relational schema DDL and migrates table data from the legacy Oracle source into 26ai.
2. `createpropertygraph.py` – Projects the freshly loaded relational tables into an Oracle PGQL property graph.
3. `oracle_pgql_mcp.py` – Hosts an MCP server that issues PGQL queries (and graph catalog introspection calls) against the deployed property graph.

Together they deliver a repeatable, idempotent pipeline: **Load ➜ Create Graph ➜ Query**.

---

## 1. Migration Script (`oracle_legacy_to_graph.py`)

### Purpose
Interactively apply schema DDL and migrate data from the legacy Oracle database into the Oracle Autonomous Database (26ai) based on `graph_model.json`.

### Prerequisites
- Network + credentials for both databases (configured inside the script constants)
- Oracle Instant Client available for thick connections (`cx_Oracle.init_oracle_client`)
- Python env with `oracledb`, `cx_Oracle`, and other dependencies installed
- `graph_model.json` validated with the desired logical schema definition

### High-Level Flow
1. **Read graph model** → Build node metadata and relationships.
2. **Generate DDL**
   - Table & PK DDL emitted to `create_26ai_schema.sql`
   - FK `ALTER TABLE` statements emitted to `add_fk_constraints.sql`
3. **Prompt: Drop tables**
   - If the user types `drop`, connects to 26ai and issues `DROP TABLE ... CASCADE CONSTRAINTS PURGE` for every node table.
4. **Prompt: Apply DDL**
   - If the user types `yes`, opens a thin connection to 26ai and executes every statement in `create_26ai_schema.sql` (logging success/failure per statement).
5. **Connect to legacy Oracle (thick mode)**
   - Initializes Instant Client and authenticates with the source DSN.
6. **Prompt: Data migration**
   - When the user types `migrate`, both source and target connections are opened.
   - For each node in `graph_model.json`:
     - Build `SELECT` statements (including optional filters, last-updated guardrails, and simple joins when `table` specifies two entries).
     - Fetch rows from the source.
     - Execute MERGE (preferred when PK exists) or INSERT batches (default batch size 1,000) into the 26ai tables.
     - Log per-batch row counts and warnings, then commit.

### Interactive Prompts & Sample Session
```text
$ python oracle_legacy_to_graph.py
======================================================================
Legacy Oracle to Oracle 23ai Property Graph Migration
======================================================================

26ai (target) DB - Suggested DDL:
CREATE TABLE ProductVod (...);

DDL written to create_26ai_schema.sql (tables only)
FK ALTERs written to add_fk_constraints.sql

Drop ALL created tables in 26ai DB before CREATE? Type 'drop' to confirm: drop
[OK] Dropped table: ProductVod

Apply (execute) this DDL in the target database? Type 'yes' to approve: yes
[OK] Executed: CREATE TABLE ProductVod ...

Connecting to source database (legacy Oracle, thick mode)...
Connected to source Oracle: <version info>

Load/migrate DATA from source to target? Type 'migrate' to approve: migrate
Migrating 2340 row(s) from node ProductVod ...
[BATCH] Loaded 1000 rows into ProductVod
...
All data migration complete.
```

### Key Features
- Safe reset via optional `drop`
- Generated DDL is human-readable and version controlled
- Batch MERGE/INSERT per node with commit boundaries
- Structured logging for auditing and troubleshooting

---

## 2. Property Graph Promotion (`createpropertygraph.py`)

### Purpose
Transforms the relational tables (now populated in 26ai) into a property graph by emitting PGQL `CREATE PROPERTY GRAPH` DDL. Optionally executes the DDL in 26ai.

### Inputs
- `graph_model.json` – same logical model used by the migration step
- `config.json` – stores PGQL connection credentials and wallet paths

### Generated Artifacts
- `pgql_schema.sql` – Oracle PGQL DDL defining vertex tables and edge tables

### Execution Flow
1. **Load configuration** via `--config` (defaults to `config.json`).
2. **Parse graph model** to extract node (vertex) and relationship (edge) metadata.
3. **Write PGQL schema**
   - Vertex section declares a view per label, includes `KEY` (prefers `ROW_ID` → `ID`) and property projections.
   - Edge section enumerates relationships from the model, wiring up `SOURCE KEY`/`DESTINATION KEY` columns so the PG respects join rules.
4. **Prompt: Execute PGQL DDL**
   - If the user types `yes`, the script connects to 26ai using python-oracledb and executes the generated DDL, creating or replacing the property graph object (e.g., `CREATE PROPERTY GRAPH "product_graph" ...`).

### Typical Command
```bash
python createpropertygraph.py \
  --graph_model graph_model.json \
  --pgql_out pgql_schema.sql \
  --graph_name product_graph \
  --config config.json
```

### Notes
- `--apply` flag remains for backward compatibility but the script always prompts interactively.
- Set `pgql_graph = True` if you need the `OPTIONS (PG_PGQL)` clause appended.
- Make sure the tables from Step 1 are in the same schema specified in `config.json` (default `ADMIN`).

---

## 3. PGQL Query Access via MCP (`oracle_pgql_mcp.py`)

### Purpose
Expose the property graph through a FastMCP server so downstream tools (including Cline) can run PGQL queries and retrieve catalog metadata.

### Available Tools
After launching `python oracle_pgql_mcp.py`, the MCP server registers the following tools:

| Tool | Description |
| --- | --- |
| `run_pgql_query(graph_ref, query)` | Executes arbitrary PGQL/SQL via `GRAPH_TABLE`, returning elapsed time, column headers, and rows. |
| `get_vertex_label_details(graph_name)` | Lists every vertex label plus its properties using `USER_PG_LABELS` metadata. |
| `get_filtered_vertex_label_details(graph_name, vertex_labels)` | Same as above but limited to selected labels. |
| `get_edge_relationship_details(graph_name)` | Returns edge-to-vertex join metadata (source/target columns) for all edge tables. |
| `get_filtered_edge_relationship_details(graph_name, edge_tables)` | Edge metadata filtered to a subset of edge tables. |

### Configuration
All connection attributes live in `OracleGraph/config.json`:
```json
{
  "PGQL_USER": "admin",
  "PGQL_PASSWORD": "***",
  "PGQL_DSN": "...",
  "PGQL_CONFIG_DIR": "/path/to/wallet",
  "PGQL_WALLET_LOCATION": "/path/to/wallet",
  "PGQL_WALLET_PASSWORD": "***"
}
```

### Usage Example
Within Cline or any MCP-aware client:
```python
result = run_pgql_query(
    graph_ref='"product_graph"',
    query='''
        SELECT DISTINCT *
        FROM GRAPH_TABLE({graph_ref}
             MATCH (c IS CLASSVOD)-[r IS CLASSVOD_HAS_VERSION_VODVERSION]->(v IS VODVERSION)
             COLUMNS(c.ROW_ID, v.VERSION_NUMBER)
        )
    '''
)
```
The server formats timing, column headers, and row payloads for downstream consumption.

### Safety & Governance
- All join keys come from catalog metadata (`user_pg_edge_relationships`) to enforce the “no guessed join columns” rule.
- Sensitive credentials stay inside `config.json`; clients only invoke tools via MCP.

---

## End-to-End Checklist
1. **Define graph model** in `graph_model.json`.
2. **Run migration** (`oracle_legacy_to_graph.py`) and respond to prompts:
   - Optionally drop previous tables
   - Apply generated DDL
   - Approve data migration
3. **Generate + apply property graph DDL** using `createpropertygraph.py`.
4. **Start MCP server** (`oracle_pgql_mcp.py`) to enable PGQL analytics and Confluence demos.

Following this sequence keeps environments idempotent, aligns relational + graph schemas, and gives the team a single source-of-truth for querying the 26ai property graph.