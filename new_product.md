# Neo4j Product Model Rule Summary

## 1. Cypher Node Property Safety

- Whenever writing Cypher, only use property names you have explicitly validated on that node/relationship type.
- Never invent property names. Always confirm keys using schema/tools if unsure.

### Relationship Join Field Enforcement

- For every relationship (as defined in schema or graph metadata, e.g. `AttributeVod_HAS_VERSION_VodVersion`), whenever a join (MATCH/WHERE/ON) is needed in Cypher, always use the property names given in the relationship's definition:
  - Use `source_field` as the property on the source node
  - Use `target_field` as the property on the target node
- Do NOT infer or guess join keys – ALWAYS extract and apply the correct `source_field`/`target_field` from the relationship definition.
- **Example:**
  - If `source_field: "ROW_ID"` and `target_field: "VERSIONED_OBJECT_DEFINITION_ID"`, your MATCH/WHERE must join these exact properties:
    ```
    ...WHERE source.ROW_ID = rel.VOD_ID...
    ```
- This applies to ALL relationship traversals, including multi-hop paths or domain traversals.

## General Join Field Mapping Rule

- Whenever a node property gets its value from another node’s property and the connection is established via a REFERS_TO (or equivalent) relationship, always use the explicit source_field and target_field as declared in the relationship type for Cypher joins.
- Never connect properties by guessing or by using similarly-named fields—enforce only schema-defined join paths.
- For example: If ObjectRelationship.SUB_OBJECT_PRODUCT_ID is connected by ObjectRelationship_REFERS_TO_ProductVod (source_field: SUB_OBJECT_PRODUCT_ID, target_field: OBJECT_NUMBER), always join SUB_OBJECT_PRODUCT_ID to the OBJECT_NUMBER of ProductVod, not to ROW_ID or any other field.

- This rule applies whenever property values “flow” from source to target node via an explicit, schema-defined REFERS_TO (or other mapping) relationship.

## 2. Versioned Entity Value Retrieval

- To find versioned values (product, class, attribute, etc.) for a given VOD_NAME:
  1. Match ROW_ID (ID) of the target node:  
     ```
     MATCH (v:Vod_*) WHERE v.VOD_NAME = $name RETURN v.ROW_ID AS VOD_ID
     ```
  2. Determine **effective version**:
     - If a version number is given: use directly.
     - If a date (or no version/date specified):  
       ```
       WITH '<date>' AS target_date
       MATCH (ver:VodVersion)
       WHERE ver.VOD_ID = $ID
         AND ver.START_DATE <= target_date
         AND (ver.END_DATE IS NULL OR target_date <= ver.END_DATE)
       RETURN ver.VERSION_NUMBER AS version
       ```
       (Use current datetime if needed, always as `YYYY-MM-DD HH:mm:ss`.)
     - If user requests "latest":  
       ```
       MATCH (ver:VodVersion)
       WHERE ver.VOD_ID = $ID AND ver.CURRENT_VERSION_FLAG = 'Y'
       RETURN ver.VERSION_NUMBER AS version
       ```
  3. To retrieve child entities at that version:
     - Always filter on [FIRST_VERSION, LAST_VERSION] at **every hop** (mapping, intermediate, child).
     - Follow relationships;
     - At each step, include only nodes valid for the version (`toFloat(FIRST_VERSION) <= version <= toFloat(LAST_VERSION)`).
     - Return identifiers and value fields as needed.

<!-- ## 3. Versioned Relationship & Mapping Rules

- When joining relationship nodes to their targets, always use the correct join keys as defined in the schema (e.g., SUB_OBJECT_PRODUCT_ID, OBJECT_NUMBER), and filter on [FIRST_VERSION, LAST_VERSION].
- Always filter ObjectRelationship by SUB_OBJECT_TYPE_CODE:
  - 'Product' for product-product links (SUB_OBJECT_PRODUCT_ID)
  - 'Port' / 'DynPort' for class-port (class-level) links (SUB_OBJECT_CLASS_ID)
- For relationship domains: traverse only relationships with SUB_OBJECT_TYPE_CODE in ['Port','DynPort'], and use the correct join fields. -->

## 3. ObjectRelationship Type Rule

The `ObjectRelationship` entity has a property called `SUB_OBJECT_TYPE_CODE` that determines the type of relationship it represents. There are three supported relationship types:

1. **Product-Product Relationship**
   - "SUB_OBJECT_PRODUCT_ID" contains reference to Product
   - Identified by: `SUB_OBJECT_TYPE_CODE = "Product"`
   - Represents a relationship between two product entities.

2. **Port-Class Relationship**
   - "SUB_OBJECT_CLASS_ID" contains reference to class
   - Identified by: `SUB_OBJECT_TYPE_CODE = "Port"`
   - Represents a relationship between a product and a class (port or class-level association).

3. **DynPort - Dynamic Class Relationship**
   - "SUB_OBJECT_CLASS_ID" contains reference to class
   - Identified by: `SUB_OBJECT_TYPE_CODE = "DynPort"`
   - Represents a dynamic (runtime or configurable) relationship between class entities.

**Rule:**  
Whenever an operation, query, or report requires the selection of a particular relationship type, it must filter `ObjectRelationship` nodes according to this property.  
For example, to fetch all `Port-Class` relationships for a given class, use:
```cypher
MATCH (c:ClassVod)-[:ClassVod_HAS_RELATIONSHIP_ObjectRelationship]->(r:ObjectRelationship)
WHERE r.SUB_OBJECT_TYPE_CODE = 'Port'
RETURN r
```
Replace `'Port'` with `'Product'` or `'DynPort'` to search respectively for product-product or dynamic class relationships.

Queries dealing with relationships **must always check and filter using `SUB_OBJECT_TYPE_CODE`** to ensure only relevant relationship types are included for the user's intent.

---

**Usage Query Rule (UNION for relationship or domain usage):**  
Whenever a query is required to find all usages/inclusions of a particular product in ObjectRelationship or ObjectRelationshipDomain, the Cypher MUST use `UNION` to combine the results of separate `MATCH` queries for each path (direct relationship and relationship domain), rather than using `OPTIONAL MATCH` or combining paths in a single query branch. This ensures clarity, strictness, and avoids false positives from outer joins.

_Example:_
```cypher
WITH 'product_key' AS object_number

// Path 1: Direct product relationship
MATCH (p:ProductVod)-[:ProductVod_HAS_RELATIONSHIP_ObjectRelationship]->(rel:ObjectRelationship)
WHERE rel.SUB_OBJECT_PRODUCT_ID = object_number
  AND rel.SUB_OBJECT_TYPE_CODE = 'Product'
RETURN DISTINCT p.VOD_NAME AS product_name

UNION

// Path 2: Relationship domain
MATCH (p:ProductVod)-[:ProductVod_HAS_RELATIONSHIP_ObjectRelationship]->(rel:ObjectRelationship)
      -[:ObjectRelationship_HAS_RELATIONSHIP_DOMAIN_ObjectRelationshipDomain]->(domain:ObjectRelationshipDomain)
      -[:ObjectRelationshipDomain_REFERS_TO_ProductVod]->(target:ProductVod)
WHERE domain.SUB_OBJECT_PRODUCT_ID = object_number
  AND target.OBJECT_NUMBER = object_number
RETURN DISTINCT p.VOD_NAME AS product_name

ORDER BY product_name
```
**Do NOT use OPTIONAL MATCH for these usage queries—always split the queries and combine results with UNION.**