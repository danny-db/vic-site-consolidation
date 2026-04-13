# Databricks notebook source
# MAGIC %md
# MAGIC # 06 — Lakebase MVT (Vector Tile) Setup
# MAGIC
# MAGIC This notebook creates the **`candidates_mvt`** table in Lakebase with native
# MAGIC PostGIS geometry columns and spatial indexes, enabling the Databricks App to
# MAGIC serve **Mapbox Vector Tiles (MVT)** for high-performance map rendering.
# MAGIC
# MAGIC ## What this does
# MAGIC 1. Connects to the Lakebase Autoscaling endpoint
# MAGIC 2. Creates `candidates_mvt` from `consolidation_candidates_sync` with:
# MAGIC    - `geom` — native geometry in EPSG:4326 (for GeoJSON endpoints)
# MAGIC    - `geom_3857` — pre-transformed EPSG:3857 (for fast MVT tile generation)
# MAGIC 3. Adds GIST spatial indexes + attribute indexes for fast tile queries
# MAGIC 4. Grants `SELECT` to `PUBLIC` so the App service principal can read the data
# MAGIC 5. Scales up the Lakebase endpoint for better tile serving performance
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Run **05_lakebase_sync.py** first (populates `consolidation_candidates_sync`)
# MAGIC - Lakebase Autoscaling project must exist
# MAGIC
# MAGIC ## When to run
# MAGIC - Once after initial data sync
# MAGIC - After any re-sync that recreates `consolidation_candidates_sync`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Lakebase connection — update these for your environment
LAKEBASE_PROJECT = "vic-site-consolidation"
LAKEBASE_DATABASE = "vic_consolidation_db"
LAKEBASE_SCHEMA = "dtp_hackathon"

# Lakebase endpoint scaling for tile serving performance
# Increase these if tile loading is slow under concurrent use
LAKEBASE_MIN_CU = 2
LAKEBASE_MAX_CU = 4

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Lakebase credentials

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# Generate a short-lived OAuth token for Lakebase
token_resp = w.api_client.do(
    "POST",
    "/api/2.0/postgres/generate-database-credential",
    body={},
)
pg_token = token_resp["token"]

# Get the endpoint host from the project
endpoints = w.api_client.do(
    "GET",
    f"/api/2.0/postgres/projects/{LAKEBASE_PROJECT}/branches/production/endpoints",
)
endpoint = endpoints["endpoints"][0]
pg_host = endpoint["status"]["hosts"]["host"]
endpoint_name = endpoint["name"]
pg_user = w.current_user.me().user_name

print(f"Host: {pg_host}")
print(f"User: {pg_user}")
print(f"Database: {LAKEBASE_DATABASE}")
print(f"Endpoint: {endpoint_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scale up Lakebase endpoint

# COMMAND ----------

print(f"Scaling endpoint to {LAKEBASE_MIN_CU}-{LAKEBASE_MAX_CU} CU...")
try:
    w.api_client.do(
        "PATCH",
        f"/api/2.0/postgres/{endpoint_name}",
        body={
            "settings": {
                "autoscaling_limit_min_cu": LAKEBASE_MIN_CU,
                "autoscaling_limit_max_cu": LAKEBASE_MAX_CU,
            }
        },
    )
    print(f"Endpoint scaled to {LAKEBASE_MIN_CU}-{LAKEBASE_MAX_CU} CU")
except Exception as e:
    print(f"Warning: Could not scale endpoint: {e}")
    print("You may need to scale manually in the Lakebase UI")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create MVT table with native geometry columns

# COMMAND ----------

import psycopg2

conn = psycopg2.connect(
    host=pg_host,
    port=5432,
    dbname=LAKEBASE_DATABASE,
    user=pg_user,
    password=pg_token,
    sslmode="require",
)
conn.autocommit = True
cur = conn.cursor()

# Set schema search path
cur.execute(f"SET search_path TO {LAKEBASE_SCHEMA}, public;")

# Drop and recreate the MVT table with both 4326 and 3857 geometry columns.
# Pre-computing geom_3857 eliminates per-tile ST_Transform calls (~70% speedup).
# ST_Buffer(geom, 0) rebuilds clean topology to prevent MVT quantization artifacts.
print("Creating candidates_mvt table (this takes a few minutes for 4M+ rows)...")
cur.execute("DROP TABLE IF EXISTS candidates_mvt;")
cur.execute("""
    CREATE TABLE candidates_mvt AS
    SELECT parcel_id, plan_number, lot_number, zone_code, lga_name,
           centroid_lon, centroid_lat, area_sqm, compactness_index,
           num_adjacent_parcels, nearest_any_pt_m, is_growth_area_lga,
           lots_in_plan, opportunity_score, constraint_score,
           suitability_score, suitability_tier, rank,
           ST_GeomFromText(geometry_wkt, 4326) AS geom,
           ST_Buffer(ST_MakeValid(ST_Transform(ST_GeomFromText(geometry_wkt, 4326), 3857)), 0) AS geom_3857
    FROM consolidation_candidates_sync
    WHERE geometry_wkt IS NOT NULL;
""")

# Count rows
cur.execute("SELECT count(*) FROM candidates_mvt;")
row_count = cur.fetchone()[0]
print(f"Created candidates_mvt with {row_count:,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create indexes

# COMMAND ----------

print("Creating GIST spatial index on geom (4326)...")
cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_candidates_mvt_geom
    ON candidates_mvt USING GIST (geom);
""")

print("Creating GIST spatial index on geom_3857 (used by tile endpoint)...")
cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_candidates_mvt_geom3857
    ON candidates_mvt USING GIST (geom_3857);
""")

print("Creating attribute indexes...")
cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_candidates_mvt_tier
    ON candidates_mvt (suitability_tier);
""")
cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_candidates_mvt_lga
    ON candidates_mvt (lga_name);
""")

print("All indexes created.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Grant permissions

# COMMAND ----------

cur.execute("GRANT SELECT ON candidates_mvt TO PUBLIC;")
print("Granted SELECT on candidates_mvt to PUBLIC")

# Also ensure all tables in schema are accessible to the App SP
cur.execute(f"GRANT USAGE ON SCHEMA {LAKEBASE_SCHEMA} TO PUBLIC;")
cur.execute(f"GRANT SELECT ON ALL TABLES IN SCHEMA {LAKEBASE_SCHEMA} TO PUBLIC;")
cur.execute(f"ALTER DEFAULT PRIVILEGES IN SCHEMA {LAKEBASE_SCHEMA} GRANT SELECT ON TABLES TO PUBLIC;")
print("Granted schema-wide permissions")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify

# COMMAND ----------

cur.execute("""
    SELECT count(*) AS total,
           count(geom) AS with_geom_4326,
           count(geom_3857) AS with_geom_3857,
           count(DISTINCT suitability_tier) AS tiers
    FROM candidates_mvt;
""")
row = cur.fetchone()
print(f"Total rows:       {row[0]:,}")
print(f"With geom (4326): {row[1]:,}")
print(f"With geom (3857): {row[2]:,}")
print(f"Distinct tiers:   {row[3]}")

# Test MVT generation for a sample tile (Melbourne suburbs, z=14)
# Using geom_3857 with ST_TileEnvelope (both in EPSG:3857)
cur.execute("""
    SELECT length(ST_AsMVT(tile, 'parcels', 4096, 'geom')) AS tile_bytes,
           count(*) AS features
    FROM (
        SELECT ST_AsMVTGeom(
            ST_Buffer(ST_SimplifyPreserveTopology(geom_3857, 0.5), 0),
            ST_TileEnvelope(14, 14804, 10068),
            4096, 256, true
        ) AS geom,
        parcel_id, suitability_tier
        FROM candidates_mvt
        WHERE geom_3857 && ST_TileEnvelope(14, 14804, 10068)
    ) AS tile
    WHERE geom IS NOT NULL;
""")
result = cur.fetchone()
print(f"\nSample tile (z=14): {result[0]:,} bytes, {result[1]:,} features")

cur.close()
conn.close()
print("\nMVT setup complete!")
