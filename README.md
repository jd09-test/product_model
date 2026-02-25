# OracleGraph — Oracle Property Graph Toolkit

A toolkit for modeling, migrating, and querying Oracle 26ai Property Graphs from a Siebel CRM product catalog. It takes your existing Oracle 19c relational data, rebuilds it as a property graph on Oracle 26ai, and exposes the graph to AI assistants via a purpose-built MCP server so you can query it using natural language.

---

## What Does It Do?

```
Oracle 19c (relational)  →  Oracle 26ai (property graph)  →  AI / PGQL queries
```

The toolkit has three main jobs:

1. **Migrate your data** — Connects to your Oracle 19c source database, extracts the data, and loads it into the 26ai property graph in batches.
2. **Build the graph schema** — Reads a `graph_model.json` file and generates the Oracle `CREATE PROPERTY GRAPH` DDL automatically.
3. **Query with AI** — Runs an MCP server that lets AI assistants introspect the graph schema and execute safe, validated PGQL queries against it.

---

## Project Structure

```
OracleGraph/
├── config.json                   # Your database credentials (not committed to git)
├── graph_model.json              # Defines your graph — nodes, edges, properties
│
├── create_property_graph.py      # Script 2: Generates CREATE PROPERTY GRAPH DDL
├── migration_19c_to_26ai.py      # Script 1: Migrates schema + data from 19c → 26ai
├── property_graph_mcp.py         # Script 3: MCP server for AI-assisted PGQL queries
│
├── create_26ai_schema.sql        # Generated output: relational staging table DDL
├── property_graph_schema.sql     # Generated output: CREATE PROPERTY GRAPH statement
│
├── use_case.txt                  # Ready-to-use PGQL query examples and prompts
└── rules.md                      # Domain rules that guide safe PGQL query generation
```

> **`config.json` is never committed to source control.** Copy the template below, fill in your credentials, and keep it local.

---

## Prerequisites

| Requirement | Details |
|---|---|
| Python | 3.10 or higher |
| `python-oracledb` | Thin-mode driver for Oracle 26ai (target database) |
| `cx_Oracle` | Thick-mode driver for Oracle 19c (source database) |
| Oracle Instant Client | Required by `cx_Oracle`; path set via `19C_CLIENT_PATH` in config |
| Oracle Wallet / TLS | Required for 26ai connections; paths set in config |
| `fastmcp` | Required for the MCP server |

Install all Python dependencies:

```bash
pip install -r requirements.txt
```

---

## Configuration

All three scripts share a single `config.json` file. Create it in the `OracleGraph/` directory:

```json
{
    "19C_USER":          "your_19c_username",
    "19C_PASS":          "your_19c_password",
    "19C_DSN":           "your_19c_dsn",
    "19C_SCHEMA":        "your_19c_schema",
    "19C_CLIENT_PATH":   "/path/to/oracle/instant/client",

    "26AI_USER":          "your_23ai_username",
    "26AI_PASSWORD":      "your_23ai_password",
    "26AI_DSN":           "your_23ai_dsn",
    "26AI_CONFIG_DIR":    "/path/to/wallet/dir",
    "26AI_WALLET_LOCATION": "/path/to/wallet/dir",
    "26AI_WALLET_PASSWORD": "your_wallet_password",

    "GRAPH_JSON_PATH":   "graph_model.json",
    "DDL_OUTPUT_PATH":   "create_26ai_schema.sql",

    "QUERY_DATE":        "2024-01-01",
    "DATE_FORMAT":       "YYYY-MM-DD",
    "BATCH_SIZE":        "500"
}
```

| Key | What It's For |
|---|---|
| `19C_USER / PASS / DSN / SCHEMA` | Oracle 19c source database credentials |
| `19C_CLIENT_PATH` | Path to Oracle Instant Client library (required by cx_Oracle) |
| `26AI_USER / PASSWORD / DSN` | Oracle 26ai target database credentials |
| `26AI_CONFIG_DIR / WALLET_LOCATION / WALLET_PASSWORD` | Oracle Wallet for 26ai TLS connection |
| `GRAPH_JSON_PATH` | Default path to graph model JSON (can be overridden via CLI) |
| `DDL_OUTPUT_PATH` | Default path for generated SQL output (can be overridden via CLI) |
| `QUERY_DATE` | Incremental extract cut-off — only rows updated on or after this date are migrated |
| `DATE_FORMAT` | Oracle `TO_DATE` format string for `QUERY_DATE` |
| `BATCH_SIZE` | Number of rows per database write batch during migration |

---

## The Graph Model

Everything is driven by `graph_model.json`. This file describes your graph — what the nodes are, what properties they have, where the data comes from in 19c, and how nodes are connected to each other.

```json
{
    "nodes": [
        {
            "name":       "PRODUCTVOD",
            "label":      "PRODUCTVOD",
            "properties": {
                "ROW_ID":   "ROW_ID",
                "VOD_NAME": "VOD_NAME"
            },
            "table":  ["S_PROD_INT"],
            "filter": { "ACTIVE_FLG": "Y" }
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
```

| Field | Description |
|---|---|
| `name` | Target table name in the 26ai database |
| `label` | Graph vertex label (defaults to `name` if omitted) |
| `properties` | Map of `DB_column -> property_key` for the columns to include |
| `table` | One or two source table names in the 19c database |
| `join_on` | Join condition for two-table nodes, e.g. `{"T1.KEY": "T2.KEY"}` |
| `filter` | Optional filter applied when extracting from 19c (supports AND / OR / NOT) |
| `type` | Edge label — also becomes the Oracle edge view name |
| `from / to` | Source and target vertex labels for this edge |
| `from_key / to_key` | Join columns that connect the two vertex tables |

---

## Script 1 — Migrate Data from Oracle 19c to 26ai

`migration_19c_to_26ai.py` handles the full three-step migration pipeline. Every step that touches a database requires your explicit confirmation before proceeding.

```bash 
python OracleGraph/migration_19c_to_26ai.py \
  --config      OracleGraph/config.json \
  --graph_model OracleGraph/graph_model.json \
  --ddl_output  OracleGraph/create_26ai_schema.sql
```

### Step 1 — DDL Generation
Reads the graph model and generates `CREATE TABLE` SQL for each node. Writes the output to `--ddl_output`. No database connection needed at this stage.

### Step 2 — Schema Apply
Prompts you twice:

- *"Drop all target tables first? Type 'drop' to confirm."*
  Drops existing staging tables in 26ai (useful for a clean re-run).
- *"Execute the DDL now? Type 'yes' to approve."*
  Creates the staging tables in 26ai.

### Step 3 — Data Migration
Prompts you once:

- *"Migrate data now? Type 'migrate' to approve."*

If confirmed, it connects to both databases and for each node:
- Runs a `SELECT` on the 19c source (with optional `LAST_UPD` date filter for incremental loads).
- Writes rows to 26ai in batches using `MERGE` (when a `ROW_ID` primary key exists) or `INSERT`.

> **Re-run safe:** The `MERGE` strategy means you can run migration multiple times without creating duplicate rows.

---

## Script 2 — Generate the Property Graph DDL

`create_property_graph.py` reads your `graph_model.json` and generates a `CREATE PROPERTY GRAPH` SQL statement. It then asks whether you want to execute it against the 26ai database.

```bash
python3 OracleGraph/create_property_graph.py \
  --graph_model OracleGraph/graph_model.json \
  --pgql_out    OracleGraph/property_graph_schema.sql \
  --graph_name  product_graph \
  --config      OracleGraph/config.json
```

What happens:
1. Reads the graph model and identifies all vertex labels and edge relationships.
2. Writes the `CREATE PROPERTY GRAPH` DDL to `--pgql_out`.
3. Prompts you: *"Apply this DDL to the database? Type 'yes' to proceed."*
   — The graph is only created if you explicitly confirm.

---

## Script 3 — Run the MCP Server for AI Queries

`property_graph_mcp.py` starts an MCP server that exposes five tools to AI assistants. This lets an AI agent safely inspect the graph schema and run PGQL queries without guessing property names or join keys.

```bash
python3 OracleGraph/property_graph_mcp.py
```

Once running, connect it to your AI assistant's MCP configuration. The assistant can then use:

| Tool | What It Does |
|---|---|
| `query` | Executes a SQL/PGQL query against a named graph using `GRAPH_TABLE(...)` syntax |
| `schema_vertices` | Lists all vertex labels and their property columns for a graph |
| `schema_vertices_filter` | Same as above but for a specific subset of vertex labels |
| `schema_edges` | Lists all edge labels with their source and target join columns |
| `schema_edges_filter` | Same as above but for a specific subset of edge labels |

The schema tools exist specifically to prevent the AI from inventing property names or using wrong join keys — it looks up the real schema before writing any query.

---

## Reference Files

### `use_case.txt`
Contains ready-to-use natural language prompts for common graph analysis tasks including:
- Domain product counts per class VOD relationship
- Finding products with inactive child components
- Recursive ancestor traversal
- Promotion coverage checks

Use these as starting prompts when chatting with the MCP-connected AI assistant.

### `rules.md`
Contains the domain-specific PGQL rules that keep queries correct — version filtering conventions, `SUB_OBJECT_TYPE_CODE` handling, when to use UNION vs OPTIONAL MATCH, and promotion path patterns. The MCP server references these rules to guide query generation.

---

## Typical Workflow

```
1.  Edit graph_model.json        → define your nodes and relationships
2.  Run migration_19c_to_26ai.py → migrate schema and data from 19c
3.  Run create_property_graph.py → generate and apply the property graph DDL
4.  Run property_graph_mcp.py    → start the MCP server
5.  Chat with your AI assistant  → use use_case.txt prompts to explore the graph
```

---

## Troubleshooting

| Problem | Likely Cause | Fix |
|---|---|---|
| `FileNotFoundError: config.json` | Config file missing or wrong path | Create `config.json` from the template above |
| `Failed to connect to Oracle 19c` | cx_Oracle thick client not initialised | Set `19C_CLIENT_PATH` in config to your Instant Client directory |
| `ORA-00904: invalid identifier` | Query references a non-existent column | Use `schema_vertices` tool to check real column names before querying |
| `Failed to connect to Oracle 26ai` | Wallet path or credentials incorrect | Verify `26AI_CONFIG_DIR`, `26AI_WALLET_LOCATION`, and `26AI_WALLET_PASSWORD` in config |
| MCP server not found by assistant | Server not running or endpoint not configured | Ensure `property_graph_mcp.py` is running and the MCP endpoint is registered in your assistant config |