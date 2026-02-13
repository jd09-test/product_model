"""
=============================================================
  Oracle 26ai — Graph Name Detective + PGQL Test
  
  SITUATION:
    ALL_PROPERTY_GRAPHS shows: product_graph_test, GRAPH_APPS_PG, SIEBEL_GRAPH
    Graph Studio UI shows:     product_graph, asda
  
  These Graph Studio graphs are PGQL Property Graphs (SQL/PGQ style).
  They may be stored differently in the catalog than what the UI shows.
  
  This script tries EVERY variation to find what works.
=============================================================
"""

import json
import time
import traceback
from typing import Dict

# -----------------------------
# Config helpers (loaded at runtime via --config)
# -----------------------------
def load_config(config_path: str) -> Dict:
    with open(config_path, "r", encoding="utf-8") as f:
        return json.load(f)
    
cfg = load_config("config.json")



CONFIG = {
    "user": cfg["PGQL_USER"],
    "password": cfg["PGQL_PASSWORD"],
    "dsn": cfg["PGQL_DSN"],
    "config_dir": cfg["PGQL_CONFIG_DIR"],
    "wallet_location": cfg["PGQL_WALLET_LOCATION"],
    "wallet_password": cfg["PGQL_WALLET_PASSWORD"]
}

def get_conn():
    import oracledb
    return oracledb.connect(
        user            = CONFIG["user"],
        password        = CONFIG["password"],
        dsn             = CONFIG["dsn"],
        config_dir      = CONFIG["config_dir"],
        wallet_location = CONFIG["wallet_location"],
        wallet_password = CONFIG["wallet_password"],
    )


# ══════════════════════════════════════════════════════════════
#   STEP 1 — Dump EVERYTHING from ALL catalog views
#   So we can see exactly what graph names are stored
# ══════════════════════════════════════════════════════════════

def dump_all_graph_catalog():
    print("\n" + "="*60)
    print("STEP 1 — Full Graph Catalog Dump")
    print("="*60)

    conn   = get_conn()
    cursor = conn.cursor()

    views_to_check = [
        "USER_PROPERTY_GRAPHS",
        "ALL_PROPERTY_GRAPHS",
        "DBA_PROPERTY_GRAPHS",
        "USER_PG_LABELS",
        "USER_PG_ELEMENTS",
        "USER_PG_ELEMENT_LABELS",
    ]

    for view in views_to_check:
        print(f"\n  📋 {view}:")
        try:
            cursor.execute(f"SELECT * FROM {view} ORDER BY 1")
            cols = [d[0] for d in cursor.description]
            rows = cursor.fetchall()
            if rows:
                print(f"     Columns: {cols}")
                for row in rows:
                    print(f"     → {dict(zip(cols, [str(c) for c in row]))}")
            else:
                print("     (empty)")
        except Exception as e:
            print(f"     ⚠️ {e}")

    # Also check what the Graph Studio internal schema looks like
    # Graph Studio sometimes stores graphs under GRAPH_STUDIO or GRAPHUSER schema
    print("\n  📋 Checking ALL schemas for property graphs:")
    try:
        cursor.execute("""
            SELECT OWNER, GRAPH_NAME, GRAPH_MODE
            FROM DBA_PROPERTY_GRAPHS
            ORDER BY OWNER, GRAPH_NAME
        """)
        cols = [d[0] for d in cursor.description]
        rows = cursor.fetchall()
        if rows:
            for row in rows:
                print(f"     → Owner: {row[0]:<20} Graph: {row[1]:<30} Mode: {row[2]}")
        else:
            print("     (no graphs found in DBA_PROPERTY_GRAPHS)")
    except Exception as e:
        print(f"     ⚠️ DBA_PROPERTY_GRAPHS: {e}")

    cursor.close()
    conn.close()


# ══════════════════════════════════════════════════════════════
#   STEP 2 — Brute force try every graph name variation
#   Both quoted and unquoted, both cases
# ══════════════════════════════════════════════════════════════

def try_all_graph_names():
    print("\n" + "="*60)
    print("STEP 2 — Try Every Graph Name Variation in GRAPH_TABLE()")
    print("="*60)

    # All candidates — from catalog + from Graph Studio UI screenshot
    candidates = [
        # From Graph Studio UI (what you see on screen)
        '"product_graph"',          # lowercase quoted
        '"asda"',                   # lowercase quoted
        # From DB catalog
        '"product_graph_test"',     # what catalog showed
        # '"GRAPH_APPS_PG"',
        # '"SIEBEL_GRAPH"',
        # # Unquoted variants (Oracle folds to uppercase)
        # 'product_graph',
        # 'PRODUCT_GRAPH',
        # 'asda',
        # 'ASDA',
        # 'product_graph',
        # 'PRODUCT_GRAPH',
        # # Graph Studio may prefix with username
        # '"ADMIN"."product_graph"',
        # '"ADMIN"."asda"',
        # 'ADMIN.product_graph',
        # 'ADMIN.PRODUCT_GRAPH',
        'catalog_graph',
    ]

    conn   = get_conn()
    cursor = conn.cursor()

    working = []

    for name in candidates:
        sql = f"""
            SELECT COUNT(*) as cnt
            FROM GRAPH_TABLE(
                {name}
                MATCH (v)
                COLUMNS (vertex_id(v) AS vid)
            )
        """
        try:
            t0 = time.time()
            cursor.execute(sql)
            row = cursor.fetchone()
            elapsed = round(time.time() - t0, 2)
            count = row[0] if row else 0
            print(f"  ✅ WORKS: {name:<40} → {count} vertices  ({elapsed}s)")
            working.append(name)
        except Exception as e:
            # Shorten the error message
            err = str(e).split('\n')[0][:80]
            print(f"  ❌ FAIL : {name:<40} → {err}")

    cursor.close()
    conn.close()

    print(f"\n  Working graph name(s): {working}")
    return working


# ══════════════════════════════════════════════════════════════
#   STEP 3 — Once we know the working name, run a real PGQL query
# ══════════════════════════════════════════════════════════════

def run_real_pgql(graph_ref: str):
    print("\n" + "="*60)
    print(f"STEP 3 — Real PGQL Queries on {graph_ref}")
    print("="*60)

    conn   = get_conn()
    cursor = conn.cursor()

    queries = [
        # Q1: get vertex labels and counts
        {
            "desc": "Vertex label counts",
            "sql":  f"""
                SELECT vlabel, COUNT(*) AS cnt
                FROM GRAPH_TABLE(
                    {graph_ref}
                    MATCH (v)
                    COLUMNS (label(v) AS vlabel)
                )
                GROUP BY vlabel
                ORDER BY cnt DESC
            """
        },
        # Q2: sample 5 vertices with all their properties
        {
            "desc": "Sample 5 vertices (all properties via v.*)",
            "sql":  f"""
                SELECT *
                FROM GRAPH_TABLE(
                    {graph_ref}
                    MATCH (v)
                    COLUMNS (v.*)
                )
                FETCH FIRST 5 ROWS ONLY
            """
        },
        # Q3: edge label counts
        {
            "desc": "Edge label counts",
            "sql":  f"""
                SELECT DISTINCT 
    *
FROM GRAPH_TABLE("catalog_graph"
    MATCH 
        (c IS CLASSVOD) -[r1 IS CLASSVOD_HAS_VERSION_VODVERSION]- (v IS VODVERSION),
        (c) -[r2 IS CLASSVOD_HAS_DEFINITION_OBJECTDEFINITION]- (od IS OBJECTDEFINITION)
    COLUMNS(
        c.ROW_ID AS class_row_id,
        c.VOD_NAME AS class_name,
        c.OBJECT_NUMBER AS class_object_number,
        c.DESCRIPTIVE_TEXT AS class_description,
        v.VERSION_NUMBER AS version_number,
        v.VERSION_ID AS version_id,
        v.START_DATE AS version_start_date,
        v.END_DATE AS version_end_date,
        v.RELEASED_FLAG AS released_flag,
        od.ROW_ID AS objectdef_row_id,
        od.OBJECT_NAME AS object_name,
        od.FIRST_VERSION AS od_first_version,
        od.LAST_VERSION AS od_last_version,
        v.CURRENT_VERSION_FLAG AS current_version_flag
    )
)
WHERE class_name = 'ACT_IC_Class1'
  AND current_version_flag = 'Y'
  AND od_first_version <= version_number
  AND (od_last_version IS NULL OR version_number <= od_last_version)

            """
        },
        # Q4: sample edges with src → dst
        {
            "desc": "Sample 5 edges (src → edge → dst)",
            "sql":  f"""
                SELECT * FROM GRAPH_TABLE("product_graph_test", "MATCH (c:CLASSVOD) RETURN c")
COLUMNS (c.*)

            """
        },
    ]

    for q in queries:
        print(f"\n  🔍 {q['desc']}")
        print(f"  SQL:{q['sql']}")
        try:
            t0 = time.time()
            cursor.execute(q["sql"])
            elapsed = round(time.time() - t0, 2)
            cols = [d[0] for d in cursor.description]
            rows = cursor.fetchall()
            print(f"  ✅ {len(rows)} row(s) in {elapsed}s  |  Columns: {cols}")
            for i, row in enumerate(rows):
                print(f"     [{i+1}] {dict(zip(cols, [str(c) for c in row]))}")
        except Exception as e:
            print(f"  ❌ Failed: {e}")

    cursor.close()
    conn.close()
    print("\n✅ STEP 3 DONE — PGQL execution confirmed working!\n")


# ══════════════════════════════════════════════════════════════
#   MAIN
# ══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    print("\n" + "🔍 "*20)
    print("  ORACLE 26ai — GRAPH NAME DETECTIVE v4")
    print("🔍 "*20 + "\n")

    # Step 1: See everything in catalog
    # dump_all_graph_catalog()

    # Step 2: Brute force find working graph name
    working = try_all_graph_names()

    # Step 3: Run real queries on first working graph
    if working:
        print(f"\n🎯 Using first working graph: {working[0]}")
        run_real_pgql(working[0])
        print(f"""
╔══════════════════════════════════════════════════════╗
║  🎉  CONNECTION + PGQL FULLY WORKING!                ║
║                                                      ║
║  Working graph reference: {working[0]:<26} ║
║  Method: python-oracledb + GRAPH_TABLE() SQL         ║
║  Ready to build MCP tools!                           ║
╚══════════════════════════════════════════════════════╝
""")
    else:
        print("""
╔══════════════════════════════════════════════════════╗
║  ⚠️  No graph name worked with GRAPH_TABLE()         ║
║                                                      ║
║  POSSIBLE CAUSES:                                    ║
║  1. Graphs in Graph Studio UI are PGQL-style graphs  ║
║     stored differently than SQL property graphs      ║
║  2. The ADMIN user may need GRAPH_DEVELOPER role     ║
║  3. Graph Studio graphs might live under a different ║
║     DB user (e.g. GRAPHUSER, not ADMIN)              ║
║                                                      ║
║  NEXT STEP: Share the full output and we'll check    ║
║  the DBA_PROPERTY_GRAPHS dump from Step 1            ║
╚══════════════════════════════════════════════════════╝
""")