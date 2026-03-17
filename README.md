# Property Graph Based Product Analyzer Toolkit

A toolkit for querying Siebel CRM product catalog. It takes your existing Oracle 19c relational data, rebuilds it as a property graph on Oracle 26ai, and exposes the graph to AI assistants via a purpose-built MCP server so you can query it using natural language.

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
  ├── migration_19c_to_26ai.py      # Script 1: Migrates schema + data from 19c → 26ai
  ├── create_property_graph.py      # Script 2: Generates CREATE PROPERTY GRAPH DDL
  ├── property_graph_mcp.py         # Script 3: MCP server for AI-assisted PGQL queries
  ├── use_case.txt                  # Ready-to-use PGQL query examples and prompts
  └── rules.md                      # Domain rules that guide safe PGQL query generation
├── requirements.txt                # Python dependencies
```

> **`config.json` is never committed to source control.** Copy the template in the Configuration section below, fill in your credentials, and keep it local.

---

## Step-by-Step Implementation

Follow these steps in order before running any scripts.

### Step 1 — Set Up the Environment

Download and install **Python 3.10 or higher** from [python.org](https://www.python.org/downloads/).

Verify your installation:

```bash
python3 --version
```

Create and activate a Python virtual environment, then install all required packages:

```bash
# Create the virtual environment
python3 -m venv venv

# Activate it
# macOS / Linux:
source venv/bin/activate

# Windows:
venv\Scripts\activate

# Install dependencies
pip3 install -r requirements.txt
```

---

### Step 2 — Install Oracle Instant Client and Configure `config.json`

#### Install Oracle Instant Client

Oracle Instant Client is required to connect to the legacy Oracle source and target database.

Download and install the version for your platform:

- **Windows** — [Oracle Instant Client for Windows x64](https://www.oracle.com/in/database/technologies/instant-client/winx64-64-downloads.html)
- **macOS (Apple Silicon)** — [Oracle Instant Client for macOS ARM64](https://www.oracle.com/database/technologies/instant-client/macos-arm64-downloads.html)

Note the directory where you install it — you will need to provide this path as `19C_CLIENT_PATH` in `config.json`.

#### Create the Configuration File

Create a `config.json` file in the project root. This file holds all database credentials and runtime settings. It is **never committed to source control**.

```json
{
    "19C_DSN":              "your_19c_dsn",
    "19C_USER":             "your_19c_username",
    "19C_PASS":             "your_19c_password",
    "19C_SCHEMA":           "your_19c_schema",
    "19C_CLIENT_PATH":      "/path/to/oracle/instant/client",

    "26AI_USER":            "your_26ai_username",
    "26AI_PASSWORD":        "your_26ai_password",
    "26AI_DSN":             "your_26ai_dsn",
    "26AI_CONFIG_DIR":      "/path/to/wallet/dir",
    "26AI_WALLET_LOCATION": "/path/to/wallet/dir",
    "26AI_WALLET_PASSWORD": "your_wallet_password",

    "QUERY_DATE":           "2024-01-01",
    "DATE_FORMAT":          "YYYY-MM-DD",
    "BATCH_SIZE":           "500"
}
```

Fill in the following fields:

| Key | What It's For |
|---|---|
| `19C_USER / PASS / DSN / SCHEMA` | Oracle 19c source database credentials |
| `19C_CLIENT_PATH` | Path to Oracle Instant Client library directory |
| `26AI_USER / PASSWORD / DSN` | Oracle 26ai target database credentials |
| `26AI_CONFIG_DIR / WALLET_LOCATION / WALLET_PASSWORD` | Oracle Wallet files for 26ai TLS connection |
| `QUERY_DATE` | Incremental extract cut-off — only rows updated on or after this date are migrated |
| `DATE_FORMAT` | Oracle `TO_DATE` format string for `QUERY_DATE` (e.g. `YYYY-MM-DD`) |
| `BATCH_SIZE` | Number of rows per database write batch during migration |

---

### Step 3 — Define and Validate the Graph Model

Everything is driven by `graph_model.json`. This file describes your graph — which node types exist, which Oracle 19c tables they are sourced from, which columns become vertex properties, and how nodes connect to each other as edges.

The default model covers the core Siebel product catalog entities and is ready to use out of the box. Review it to understand the existing structure, and update it to map any additional source tables into new vertex or edge definitions, including optional row-level filters such as `ACTIVE_FLG = 'Y'`.

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
| `properties` | Map of `DB_column → property_key` for the columns to include |
| `table` | One or two source table names in the 19c database |
| `join_on` | Join condition for two-table nodes, e.g. `{"T1.KEY": "T2.KEY"}` |
| `filter` | Optional filter applied when extracting from 19c (supports AND / OR / NOT) |
| `type` | Edge label — also becomes the Oracle edge view name |
| `from / to` | Source and target vertex labels for this edge |
| `from_key / to_key` | Join columns that connect the two vertex tables |

---

### Step 4 — Run Script 1: Migrate Data from Oracle 19c to 26ai

`migration_19c_to_26ai.py` streams relational data from Oracle 19c to Oracle 26ai in configurable batch sizes. It generates DDL for the target schema from the graph model, prompts for explicit confirmation before touching the 26ai database, and upserts data idempotently so migrations can be re-run safely.

```bash
python3 OracleGraph/migration_19c_to_26ai.py \
  --config      config.json \
  --graph_model graph_model.json \
  --ddl_output  create_26ai_schema.sql
```

The script runs through three internal stages:

**Stage 1 — DDL Generation**
Reads the graph model and generates `CREATE TABLE` SQL for each node. Writes the output to `--ddl_output`. No database connection is needed at this stage.

**Stage 2 — Schema Apply**
Prompts you twice:

- *"Drop all target tables first? Type 'drop' to confirm."*
  Drops existing staging tables in 26ai (useful for a clean re-run).
- *"Execute the DDL now? Type 'yes' to approve."*
  Creates the staging tables in 26ai.

**Stage 3 — Data Migration**
Prompts you once:

- *"Migrate data now? Type 'migrate' to approve."*

If confirmed, it connects to both databases and for each node:
- Runs a `SELECT` on the 19c source (with optional `LAST_UPD` date filter for incremental loads).
- Writes rows to 26ai in batches using `MERGE` (when a `ROW_ID` primary key exists) or plain `INSERT`.

> **Re-run safe:** The `MERGE` strategy means you can run migration multiple times without creating duplicate rows.

---

### Step 5 — Run Script 2: Generate the Property Graph DDL

`create_property_graph.py` reads your `graph_model.json` and generates a `CREATE PROPERTY GRAPH` SQL statement that defines vertex and edge tables, key columns, and label mappings in Oracle 26ai. It outputs a SQL file and optionally executes it against the target database after explicit confirmation.

```bash
python3 OracleGraph/create_property_graph.py \
  --graph_model graph_model.json \
  --ddl_output  property_graph_schema.sql \
  --graph_name  catalog_graph \
  --config      config.json
```

What happens:
1. Reads the graph model and identifies all vertex labels and edge relationships.
2. Writes the `CREATE PROPERTY GRAPH` DDL to `--ddl_output`.
3. Prompts you: *"Apply this DDL to the database? Type 'yes' to proceed."*
   — The graph is only created if you explicitly confirm.

---

### Step 6 — Script 3: Start the MCP Server

`property_graph_mcp.py` starts an MCP-compliant server that exposes tools for AI-assisted analysis — including `query`, `schema_vertices`, and `schema_edges`. This lets an AI agent safely inspect the graph schema and run PGQL queries without guessing property names or join keys.

```bash
python3 OracleGraph/property_graph_mcp.py
```

Once started, the server exposes the following tools:

| Tool | What It Does |
|---|---|
| `query` | Executes a SQL/PGQL query against a named graph using `GRAPH_TABLE(...)` syntax |
| `schema_vertices` | Lists all vertex labels and their property columns for a graph |
| `schema_vertices_filter` | Same as above but for a specific subset of vertex labels |
| `schema_edges` | Lists all edge labels with their source and target join columns |
| `schema_edges_filter` | Same as above but for a specific subset of edge labels |

The schema tools exist specifically to prevent the AI from inventing property names or using wrong join keys — it always looks up the real schema before writing any query.

---

### Step 7 — Configure the MCP Server and Add the Rules Skill

#### Register the MCP Server in Your MCP Client

The MCP server uses the `stdio` transport and works with any MCP-compatible client — including Cline, Claude Desktop, Cursor, Windsurf, or any other agent that supports the MCP protocol.

Register the server by adding the following block to your client's MCP server configuration. Replace `<Path of project>` with the absolute path to your local project directory.
```json
{
  "mcpServers": {
    "OracleGraphMCP": {
      "autoApprove": [
        "query",
        "schema_vertices",
        "schema_vertices_filter",
        "schema_edges",
        "schema_edges_filter"
      ],
      "disabled": false,
      "timeout": 60,
      "type": "stdio",
      "command": "<Path of project>/product_model/venv/bin/python3",
      "args": [
        "<Path of project>/product_model/OracleGraph/property_graph_mcp.py"
      ]
    }
  }
}
```

Where to add this config in common clients:

| MCP Client | Where to Register |
|---|---|
| **Cline** | Cline → Manage MCP Servers |
| **Claude Desktop** | `claude_desktop_config.json` (Settings → Developer → Edit Config) |
| **Cursor** | Cursor Settings → MCP Servers → Add Server |
| **Windsurf** | Windsurf Settings → Cascade → MCP Servers |
| **Other clients** | Refer to your client's documentation for `mcpServers` config location |

#### Add `rules.md` as a Skill

`rules.md` contains the domain-specific PGQL rules that keep queries correct — version filtering conventions, `SUB_OBJECT_TYPE_CODE` handling, when to use UNION vs OPTIONAL MATCH, and promotion path patterns. Adding it as a skill ensures the AI generates correct PGQL for Siebel-specific version filtering and traversal patterns on every conversation.

How to add the skill in common clients:

| MCP Client | How to Add the Skill |
|---|---|
| **Cline** | Cline → Manage Rules → create `rules.md` → paste contents |
| **Claude Desktop** | Not natively supported — include `rules.md` content in your system prompt |
| **Cursor** | `.cursor/rules/rules.md` in your project root |
| **Windsurf** | `.windsurf/rules/rules.md` in your project root, or via Cascade Rules settings |
| **Other clients** | Add the contents of `rules.md` to the system prompt or rules file your client supports |

Regardless of client, the content to add is always the full text of `OracleGraph/rules.md` from this project.

---

### Step 8 — Query the Graph

Choose your preferred LLM and start a conversation in natural language. The toolkit ships with documented use-case prompts that are ready to use as starting points.

| Sr No | Use Case | Prompt |
|---|---|---|
| 1 | Domain product count per Class relationship | For each Class, show me how many distinct domain products are present in its Class (Port) or Dynamic Class (DynPort) relationships. Group the results by Class name and then by relationship name, considering only the latest version of each Class. |
| 2 | Domain product count per Product relationship | For each product, show me how many distinct domain products are present in its Class (Port) or Dynamic Class (DynPort) relationships. Group the results by product name and then by relationship name, considering only the latest version of each product. Show results only where domain count is more than N. |
| 3 | Classes with unspecified target class in Port/DynPort relationships | Show me all Classes that have Class (Port) or Dynamic Class (DynPort) type relationships where no target class has been specified — meaning the relationship was created without filling in the Sub Object Class field. |
| 4 | Products with exactly one domain in a Port/DynPort relationship | Show me all Products that have a Class (Port) or Dynamic Class (DynPort) relationship which contains exactly one domain entry, considering only the latest version of each product. |
| 5 | Products with both a sub-product and sub-class on the same relationship | List all products in their latest version that have a relationship of type Product where both a product and a class are populated on the same relationship. |
| 6 | Products with unspecified target class in Port/DynPort relationships | Show me all Products that have Class (Port) or Dynamic Class (DynPort) type relationships where no target class has been specified. |
| 7 | Products with duplicate active constraint rules | Find all products where, in their latest version, there are multiple active constraint rules that share the exact same Rule Specification. Show me the product details, the duplicated rule specification, and the list of duplicate rule IDs. |
| 8 | Products that include a specific product in their relationships | Show me all products in their latest version that include the product ACT_IC_SP5 either directly in their relationships or inside a relationship domain. |
| 9 | Products containing inactive child products | Show me all products in their latest version that contain at least one child product which is currently inactive. Include both direct relationships and relationship domain entries, and indicate which path each result came from. |
| 10 | Inactive products used in active promotions | I want to check if any inactive products are part of active promotions. Step 1 — Find all products that are inactive in their latest version. Step 2 — For each inactive product found, get the list of all their ancestors. Step 3 — Take the complete list of products from Steps 1 and 2 and check whether any of them appear in any active promotions. |

Use these as starting prompts when chatting with the MCP-connected AI assistant.

---

## Typical Workflow

```
1.  Edit graph_model.json            → define and validate your nodes and relationships
2.  Run migration_19c_to_26ai.py     → migrate schema and data from 19c to 26ai
3.  Run create_property_graph.py     → generate and apply the property graph DDL
4.  Run property_graph_mcp.py        → start the MCP server
5.  Configure MCP in your client     → register the server and add rules.md as a skill
6.  Choose your LLM and chat         → use use_case.txt prompts to explore the graph
```

---

## Troubleshooting

| Problem | Likely Cause | Fix |
|---|---|---|
| `FileNotFoundError: config.json` | Config file missing or wrong path | Create `config.json` from the template above |
| `Failed to connect to Oracle 19c` | cx_Oracle thick client not initialised | Set `19C_CLIENT_PATH` in config to your Instant Client directory |
| `Failed to connect to Oracle 26ai` | Wallet path or credentials incorrect | Verify `26AI_CONFIG_DIR`, `26AI_WALLET_LOCATION`, and `26AI_WALLET_PASSWORD` in config |
| `ORA-00904: invalid identifier` | Query references a non-existent column | Use `schema_vertices` tool to check real column names before querying |
| MCP server not found by client | Server not running or path incorrect | Check that `command` and `args` paths in the MCP config point to your venv Python and the correct script |
| AI not following query rules | `rules.md` not added as a skill | Add the contents of `OracleGraph/rules.md` to your client's rules or system prompt (see Step 7) |
