# Oracle PGQL Complete Reference Guide

> **Graph Name:** Always ask user what is graph_name  
> **Query Wrapper:** Always use `SELECT … FROM GRAPH_TABLE("{graph_name}" MATCH … COLUMNS (…))`  
> **Critical Rules:** Only use verified property names which you get from schema (mcp tool). Never guess join keys — use the exact `source_vertex_column → target_vertex_column` pairs listed per edge.

---

## VERSIONING RULES (Apply to Every Versioned Query)

1. **Get current version:** Filter `VODVERSION` with `CURRENT_VERSION_FLAG = 'Y'`  
2. **Get version by date:** Filter `START_DATE <= :date AND (END_DATE IS NULL OR :date <= END_DATE)` , (Use datetime if needed, always as `YYYY-MM-DD HH:mm:ss`.)
3. **Filter child nodes by version range:** `FIRST_VERSION <= version_number AND (LAST_VERSION IS NULL OR version_number <= LAST_VERSION)` — apply at **every hop**

---

## OBJECTRELATIONSHIP — SUB_OBJECT_TYPE_CODE Rules

| Code | Meaning | Key Field for Join |
|---|---|---|
| `'Product'` | Product-to-Product link | `SUB_OBJECT_PRODUCT_ID` → `PRODUCTVOD.OBJECT_NUMBER` |
| `'Port'` | Product/Class-to-Class port | `SUB_OBJECT_CLASS_ID` → `CLASSVOD.OBJECT_NUMBER` |
| `'DynPort'` | Dynamic class relationship | `SUB_OBJECT_CLASS_ID` → `CLASSVOD.OBJECT_NUMBER` |

- Domain traversal (`OBJECTRELATIONSHIP → OBJECTRELATIONSHIPDOMAIN`) is only valid for `'Port'` or `'DynPort'`  
- **Always filter `SUB_OBJECT_TYPE_CODE`** whenever using `OBJECTRELATIONSHIP`

---

## RECURSIVE ANCESTRY TRAVERSAL RULES

### Purpose
To find **all ancestor products** of a given product (i.e., who uses this product, and who uses those, and so on up the chain until no more parents are found), execute **iterative step-by-step queries** — never try to do this in a single recursive SQL.

### Rule: Always Check BOTH Paths at Every Level
At every recursion level, run two separate queries (UNION-style logic):
1. **Direct Path** — `OBJECTRELATIONSHIP.SUB_OBJECT_PRODUCT_ID` with `SUB_OBJECT_TYPE_CODE = 'Product'`
2. **Domain Path** — `OBJECTRELATIONSHIPDOMAIN.SUB_OBJECT_PRODUCT_ID` with `SUB_OBJECT_TYPE_CODE IN ('Port', 'DynPort')`

Collect all unique parent `OBJECT_NUMBER` values from both paths, deduplicate, then use them as input to the next level.

### Step-by-Step Algorithm

```
Level 0:  START with target product OBJECT_NUMBER = 'X'
Level 1:  Query Path 1 (direct) WHERE SUB_OBJECT_PRODUCT_ID = 'X'  → collect parent OBJECT_NUMBERs
          Query Path 2 (domain) WHERE d.SUB_OBJECT_PRODUCT_ID = 'X' → collect parent OBJECT_NUMBERs
          Merge & deduplicate → Level 1 parents set
Level 2:  Query Path 1 WHERE SUB_OBJECT_PRODUCT_ID IN (level1_set)
          Query Path 2 WHERE d.SUB_OBJECT_PRODUCT_ID IN (level1_set)
          Merge & deduplicate, subtract already-seen → Level 2 parents set
Level N:  Repeat until both queries return 0 rows → STOP
```

### Critical: Avoid Infinite Loops
Track **all seen OBJECT_NUMBERs** across levels. If Level N parents are all already in the seen set, stop immediately. This prevents circular reference loops.

### Promotion Usage Summary Table

In `product_graph`, promotions live under the `PROMOTIONDEFINITION` label (not `PRODUCTVOD`). Use the following edge patterns to find product participation in active promotions:

| Path | Edge Used | Join Key (Source → Target) | What It Captures |
|---|---|---|---|
| A | `PROMOTIONDEFINITION_HAS_ITEMS_PROMOTIONITEM` → `PROMOTIONITEM_DOMAIN_PRODUCTVOD` | `PROMOTIONITEM.SUB_OBJECT_VOD_NUMBER` → `PRODUCTVOD.OBJECT_NUMBER` | Product appears as a domain/scope entry in a promotion item |
| B | `PROMOTIONDEFINITION_HAS_ITEMS_PROMOTIONITEM` → `PROMOTIONITEM_REFERS_TO_PRODUCTDEFINITION` + `PRODUCTVOD_HAS_DEFINITION_PRODUCTDEFINITION` | `PROMOTIONITEM.PRODUCT_ID` → `PRODUCTDEFINITION.ROW_ID` and `PRODUCTDEFINITION.CONFIGURATION_MODEL_ID` → `PRODUCTVOD.OBJECT_NUMBER` | Product included via ProductDefinition bridge |

> **Rule:** To get complete promotion coverage for a product, always run **both paths** (A,B) using the labels above and combine results. Never assume one path is sufficient.


## KEY RULES SUMMARY

| Rule | Detail |
|---|---|
| **No guessed properties** | Only use property names listed above per vertex/edge |
| **Always filter SUB_OBJECT_TYPE_CODE** | `'Product'`, `'Port'`, or `'DynPort'` on every ObjectRelationship query |
| **Domain traversal** | Only for `'Port'` or `'DynPort'`, not `'Product'` |
| **Version filter at every hop** | `FIRST_VERSION <= ver AND (LAST_VERSION IS NULL OR ver <= LAST_VERSION)` |
| **Join keys from schema** | Always use `source_vertex_column → target_vertex_column` as listed in Edge table above |
| **Usage queries use UNION** | Never use OPTIONAL MATCH — always UNION separate paths |
| **Edge direction in Oracle PGQL** | Use `-[IS EDGE_LABEL]->` (undirected `-[IS EDGE_LABEL]-` also accepted) |