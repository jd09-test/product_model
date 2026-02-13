"""Inactive product + promotion usage audit (bounded parent hops).

This script connects to the same Oracle DB used by the pgqlTools MCP server
and runs a small set of GRAPH_TABLE queries to:

1) Find products that are inactive in their *latest* version
2) Walk upward through Product->Product inclusion relationships up to N hops
   using BOTH paths (direct relationship + relationship domain)
3) Check whether any product in the discovered set is used in promotions via:
   A) PROMOTIONITEM.SUB_OBJECT_VOD_NUMBER -> PRODUCTVOD.OBJECT_NUMBER
   B) PROMOTIONITEM.PRODUCT_ID = PROMOTIONDEFINITION.ROW_ID   (promotion-as-product)

Notes / constraints:
- We intentionally avoid correlating outer SQL variables into GRAPH_TABLE
  because this environment raises ORA-40996 for correlated GRAPH_TABLE usage.
- Instead, each hop is executed as its own query with an IN (...) list.

Usage:
  python OracleGraph/inactive_promotion_audit.py --graph product_graph --hops 3

Outputs:
  - inactive products (object numbers)
  - products included in the inactive hierarchy (inactive + ancestors)
  - promotion usages (via A and B)
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Iterable, List, Dict, Any, Set


CONFIG_PATH = Path(__file__).resolve().parent / "config.json"


def get_conn():
    import oracledb

    with CONFIG_PATH.open() as f:
        cfg = json.load(f)

    return oracledb.connect(
        user=cfg["PGQL_USER"],
        password=cfg["PGQL_PASSWORD"],
        dsn=cfg["PGQL_DSN"],
        config_dir=cfg["PGQL_CONFIG_DIR"],
        wallet_location=cfg["PGQL_WALLET_LOCATION"],
        wallet_password=cfg["PGQL_WALLET_PASSWORD"],
    )


def run_query(conn, sql: str) -> List[Dict[str, Any]]:
    cur = conn.cursor()
    try:
        cur.execute(sql)
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]
    finally:
        cur.close()


def quote_list(values: Iterable[str]) -> str:
    # Oracle SQL string literal quoting
    return ",".join("'" + v.replace("'", "''") + "'" for v in values)


def fetch_inactive_products(conn, graph: str) -> List[str]:
    sql = f"""
SELECT inactive_product_object_number
FROM GRAPH_TABLE(
  \"{graph}\"
  MATCH (p IS PRODUCTVOD) -[hv IS PRODUCTVOD_HAS_VERSION_VODVERSION]-> (v IS VODVERSION)
  WHERE v.CURRENT_VERSION_FLAG = 'Y'
    AND v.ACTIVE_FLAG <> 'Y'
  COLUMNS(
    p.OBJECT_NUMBER AS inactive_product_object_number
  )
)
ORDER BY inactive_product_object_number
"""
    rows = run_query(conn, sql)
    return [r["INACTIVE_PRODUCT_OBJECT_NUMBER"] for r in rows]


def fetch_parents(conn, graph: str, child_object_numbers: List[str]) -> List[str]:
    if not child_object_numbers:
        return []

    in_list = quote_list(child_object_numbers)

    sql = f"""
SELECT DISTINCT parent_product_object_number
FROM (
  /* Path 1: Direct product relationship */
  SELECT *
  FROM GRAPH_TABLE(
    \"{graph}\"
    MATCH (child IS PRODUCTVOD),
          (parent IS PRODUCTVOD) -[hasRel IS PRODUCTVOD_HAS_RELATIONSHIP_OBJECTRELATIONSHIP]-> (rel IS OBJECTRELATIONSHIP)
                   -[refProd IS PRODUCTRELATION_REFERS_TO_PRODUCTVOD]-> (child)
    WHERE child.OBJECT_NUMBER IN ({in_list})
      AND rel.SUB_OBJECT_TYPE_CODE = 'Product'
    COLUMNS(
      parent.OBJECT_NUMBER AS parent_product_object_number
    )
  )

  UNION

  /* Path 2: Relationship domain */
  SELECT *
  FROM GRAPH_TABLE(
    \"{graph}\"
    MATCH (child IS PRODUCTVOD),
          (parent IS PRODUCTVOD) -[hasRel IS PRODUCTVOD_HAS_RELATIONSHIP_OBJECTRELATIONSHIP]-> (rel IS OBJECTRELATIONSHIP)
                   -[hasDom IS OBJECTRELATIONSHIP_HAS_RELATIONSHIP_DOMAIN_OBJECTRELATIONSHIPDOMAIN]-> (dom IS OBJECTRELATIONSHIPDOMAIN)
                   -[domRef IS OBJECTRELATIONSHIPDOMAIN_REFERS_TO_PRODUCTVOD]-> (child)
    WHERE child.OBJECT_NUMBER IN ({in_list})
      AND dom.SUB_OBJECT_TYPE_CODE = 'Product'
    COLUMNS(
      parent.OBJECT_NUMBER AS parent_product_object_number
    )
  )
)
ORDER BY parent_product_object_number
"""
    rows = run_query(conn, sql)
    return [r["PARENT_PRODUCT_OBJECT_NUMBER"] for r in rows]


def fetch_promotion_usage(conn, graph: str, object_numbers: List[str]) -> List[Dict[str, Any]]:
    if not object_numbers:
        return []

    in_list = quote_list(object_numbers)

    # NOTE:
    # Promotion usage can happen via either:
    #   A) PROMOTIONITEM.SUB_OBJECT_VOD_NUMBER -> PRODUCTVOD.OBJECT_NUMBER
    #   B) PROMOTIONITEM.PRODUCT_ID -> PRODUCTDEFINITION.ROW_ID
    #      (then map ProductDefinition.CONFIGURATION_MODEL_ID -> ProductVod.OBJECT_NUMBER)
    #
    # The relationship PROMOTIONDEFINITION_HAS_ITEMS_PROMOTIONITEM is joined by:
    #   PROMOTIONDEFINITION.ROW_ID -> PROMOTIONITEM.PROMOTION_ID
    # (the MATCH traversal already enforces that join; do not add an extra join condition.)
    sql = f"""
SELECT DISTINCT promo_name,
                promo_row_id,
                used_via,
                used_product_object_number
FROM (
  /* Usage path A: PromotionItem.SUB_OBJECT_VOD_NUMBER -> ProductVod.OBJECT_NUMBER */
  SELECT *
  FROM GRAPH_TABLE(
    \"{graph}\"
    MATCH (promo IS PROMOTIONDEFINITION)
          -[hasItem IS PROMOTIONDEFINITION_HAS_ITEMS_PROMOTIONITEM]-> (item IS PROMOTIONITEM)
          -[domain IS PROMOTIONITEM_DOMAIN_PRODUCTVOD]-> (pvod IS PRODUCTVOD)
    WHERE pvod.OBJECT_NUMBER IN ({in_list})
    COLUMNS(
      promo.NAME AS promo_name,
      promo.ROW_ID AS promo_row_id,
      'SUB_OBJECT_VOD_NUMBER' AS used_via,
      pvod.OBJECT_NUMBER AS used_product_object_number
    )
  )

  UNION

  /* Usage path B1: PromotionItem.PRODUCT_ID -> ProductDefinition.ROW_ID -> ProductVod */
  SELECT *
  FROM GRAPH_TABLE(
    \"{graph}\"
    MATCH (promo IS PROMOTIONDEFINITION)
          -[hasItem IS PROMOTIONDEFINITION_HAS_ITEMS_PROMOTIONITEM]-> (item IS PROMOTIONITEM)
          -[refPd IS PROMOTIONITEM_REFERS_TO_PRODUCTDEFINITION]-> (pd IS PRODUCTDEFINITION),
          (pvod IS PRODUCTVOD) -[hasDef IS PRODUCTVOD_HAS_DEFINITION_PRODUCTDEFINITION]-> (pd)
    WHERE pvod.OBJECT_NUMBER IN ({in_list})
    COLUMNS(
      promo.NAME AS promo_name,
      promo.ROW_ID AS promo_row_id,
      'PRODUCT_ID_TO_PRODUCTDEFINITION' AS used_via,
      pvod.OBJECT_NUMBER AS used_product_object_number
    )
  )

  UNION

  /* Usage path B2: PromotionItem.PRODUCT_ID directly equals ProductVod.OBJECT_NUMBER.
     Uses explicit edge PROMOTIONITEM_POINTS_TO_PRODUCTVOD:
       PROMOTIONITEM.PRODUCT_ID -> PRODUCTVOD.OBJECT_NUMBER
  */
  SELECT *
  FROM GRAPH_TABLE(
    \"{graph}\"
    MATCH (promo IS PROMOTIONDEFINITION)
          -[hasItem IS PROMOTIONDEFINITION_HAS_ITEMS_PROMOTIONITEM]-> (item IS PROMOTIONITEM)
          -[pointsTo IS PROMOTIONITEM_POINTS_TO_PRODUCTVOD]-> (pvod IS PRODUCTVOD)
    WHERE pvod.OBJECT_NUMBER IN ({in_list})
    COLUMNS(
      promo.NAME AS promo_name,
      promo.ROW_ID AS promo_row_id,
      'PRODUCT_ID_EQUALS_PRODUCTVOD_OBJECT_NUMBER' AS used_via,
      pvod.OBJECT_NUMBER AS used_product_object_number
    )
  )
)
ORDER BY promo_name, used_via, used_product_object_number
"""
    rows = run_query(conn, sql)

    # De-duplicate: prefer the more "structural" ProductDefinition-based match when
    # both match the same (promotion, product).
    # Priority: PRODUCT_ID_TO_PRODUCTDEFINITION (best) > SUB_OBJECT_VOD_NUMBER > PRODUCT_ID_EQUALS_PRODUCTVOD_OBJECT_NUMBER
    priority = {
        "PRODUCT_ID_TO_PRODUCTDEFINITION": 1,
        "SUB_OBJECT_VOD_NUMBER": 2,
        "PRODUCT_ID_EQUALS_PRODUCTVOD_OBJECT_NUMBER": 3,
    }

    best_by_key: dict[tuple[str, str], Dict[str, Any]] = {}
    for r in rows:
        promo_row_id = str(r.get("PROMO_ROW_ID"))
        used_product = str(r.get("USED_PRODUCT_OBJECT_NUMBER"))
        used_via = str(r.get("USED_VIA"))
        key = (promo_row_id, used_product)

        existing = best_by_key.get(key)
        if existing is None:
            best_by_key[key] = r
            continue

        if priority.get(used_via, 99) < priority.get(str(existing.get("USED_VIA")), 99):
            best_by_key[key] = r

        # If same priority (or existing kept), but existing name is null and new has it, take new.
        elif str(existing.get("USED_PRODUCT_NAME")) in ("None", "", "NULL") and str(r.get("USED_PRODUCT_NAME")) not in (
            "None",
            "",
            "NULL",
        ):
            best_by_key[key] = r

    # Keep deterministic ordering
    return sorted(
        best_by_key.values(),
        key=lambda x: (
            str(x.get("PROMO_NAME")),
            str(x.get("USED_PRODUCT_OBJECT_NUMBER")),
            str(x.get("USED_VIA")),
        ),
    )


def fetch_product_names(conn, graph: str, object_numbers: List[str]) -> Dict[str, str]:
    """Map ProductVod.OBJECT_NUMBER -> ProductDefinition.NAME when available.

    This is intentionally a separate step so we don't turn promotion-usage paths into
    inner-joins that can drop rows (e.g., when a ProductVod has no ProductDefinition).
    """

    if not object_numbers:
        return {}

    in_list = quote_list(object_numbers)
    # Two strategies combined:
    # 1) Prefer PRODUCTDEFINITION.NAME when mapping exists
    # 2) Fallback to PRODUCTVOD.VOD_NAME if no ProductDefinition mapping
    sql = f"""
SELECT product_object_number,
       product_name
FROM (
  /* Preferred: ProductDefinition name */
  SELECT *
  FROM GRAPH_TABLE(
    \"{graph}\"
    MATCH (pvod IS PRODUCTVOD)
          -[hasDef IS PRODUCTVOD_HAS_DEFINITION_PRODUCTDEFINITION]-> (pd IS PRODUCTDEFINITION)
    WHERE pvod.OBJECT_NUMBER IN ({in_list})
    COLUMNS(
      pvod.OBJECT_NUMBER AS product_object_number,
      pd.NAME AS product_name
    )
  )

  UNION

  /* Fallback: ProductVod name */
  SELECT *
  FROM GRAPH_TABLE(
    \"{graph}\"
    MATCH (pvod IS PRODUCTVOD)
    WHERE pvod.OBJECT_NUMBER IN ({in_list})
    COLUMNS(
      pvod.OBJECT_NUMBER AS product_object_number,
      pvod.VOD_NAME AS product_name
    )
  )
)
"""

    rows = run_query(conn, sql)

    # De-dupe: prefer non-null/non-empty names. If both exist, ProductDefinition.NAME
    # is typically more business-friendly, but either way both are strings.
    result: Dict[str, str] = {}
    for r in rows:
        obj = str(r["PRODUCT_OBJECT_NUMBER"])
        name = r.get("PRODUCT_NAME")
        if name is None:
            continue
        name_str = str(name)
        if not name_str or name_str == "None":
            continue
        # Keep first good name (ProductDefinition branch tends to come first)
        result.setdefault(obj, name_str)

    return result


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--graph", default="product_graph")
    ap.add_argument("--hops", type=int, default=3)
    args = ap.parse_args()

    conn = get_conn()
    try:
        inactive = fetch_inactive_products(conn, args.graph)

        all_nodes: Set[str] = set(inactive)
        frontier = inactive
        levels: List[List[str]] = []

        for _ in range(args.hops):
            parents = fetch_parents(conn, args.graph, frontier)
            # remove already-seen to prevent infinite loops
            parents = [p for p in parents if p not in all_nodes]
            if not parents:
                break
            levels.append(parents)
            all_nodes.update(parents)
            frontier = parents

        promo_usage = fetch_promotion_usage(conn, args.graph, sorted(all_nodes))

        # Enrich promo usage with product names (best-effort)
        used_products = sorted({str(r.get("USED_PRODUCT_OBJECT_NUMBER")) for r in promo_usage if r.get("USED_PRODUCT_OBJECT_NUMBER") not in (None, "None")})
        name_map = fetch_product_names(conn, args.graph, used_products)
        for r in promo_usage:
            obj = str(r.get("USED_PRODUCT_OBJECT_NUMBER"))
            r["USED_PRODUCT_NAME"] = name_map.get(obj)

        print("inactive_product_object_numbers:")
        print(json.dumps(inactive, indent=2))

        print("\nancestor_levels (each list is one hop upwards):")
        print(json.dumps(levels, indent=2))

        print("\nall_related_product_object_numbers (inactive + ancestors):")
        print(json.dumps(sorted(all_nodes), indent=2))

        print("\npromotion_usage:")
        print(json.dumps(promo_usage, indent=2, default=str))

        return 0
    finally:
        conn.close()


## Usage inactive_promotion_audit.py --graph product_graph --hops 3

if __name__ == "__main__":
    raise SystemExit(main())
