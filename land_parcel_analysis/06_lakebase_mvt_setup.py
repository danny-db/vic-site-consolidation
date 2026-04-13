# Databricks notebook source
# MAGIC %md
# MAGIC # 06 — Lakebase MVT (Vector Tile) Setup
# MAGIC
# MAGIC This notebook creates the **`candidates_mvt`** table in Lakebase with a native
# MAGIC PostGIS geometry column and spatial index, enabling the Databricks App to serve
# MAGIC **Mapbox Vector Tiles (MVT)** for high-performance map rendering.
# MAGIC
# MAGIC ## What this does
# MAGIC 1. Connects to the Lakebase Autoscaling endpoint
# MAGIC 2. Creates `candidates_mvt` from `consolidation_candidates_sync` with a native `geometry` column
# MAGIC 3. Adds GIST spatial index + attribute indexes for fast tile queries
# MAGIC 4. Grants `SELECT` to `PUBLIC` so the App service principal can read the data
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Run **05_lakebase_sync.py** first (populates `consolidation_candidates_sync`)
# MAGIC - Lakebase Autoscaling project `vic-site-consolidation` must exist
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

# The Lakebase endpoint host is auto-resolved from the project.
# If running outside the workspace, set this explicitly:
# LAKEBASE_HOST = "ep-icy-sound-e1pdqoh3.database.eastus2.azuredatabricks.net"

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
pg_user = w.current_user.me().user_name

print(f"Host: {pg_host}")
print(f"User: {pg_user}")
print(f"Database: {LAKEBASE_DATABASE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create MVT table with native geometry + spatial index

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

# Drop and recreate the MVT table
print("Creating candidates_mvt table...")
cur.execute("DROP TABLE IF EXISTS candidates_mvt;")
cur.execute("""
    CREATE TABLE candidates_mvt AS
    SELECT parcel_id, plan_number, lot_number, zone_code, lga_name,
           centroid_lon, centroid_lat, area_sqm, compactness_index,
           num_adjacent_parcels, nearest_any_pt_m, is_growth_area_lga,
           lots_in_plan, opportunity_score, constraint_score,
           suitability_score, suitability_tier, rank,
           ST_GeomFromText(geometry_wkt, 4326) AS geom
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

print("Creating GIST spatial index on geom...")
cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_candidates_mvt_geom
    ON candidates_mvt USING GIST (geom);
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

# Also ensure the source table is accessible
cur.execute("GRANT USAGE ON SCHEMA dtp_hackathon TO PUBLIC;")
cur.execute("GRANT SELECT ON ALL TABLES IN SCHEMA dtp_hackathon TO PUBLIC;")
cur.execute("ALTER DEFAULT PRIVILEGES IN SCHEMA dtp_hackathon GRANT SELECT ON TABLES TO PUBLIC;")
print("Granted schema-wide permissions")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify

# COMMAND ----------

cur.execute("""
    SELECT count(*) AS total,
           count(geom) AS with_geom,
           count(DISTINCT suitability_tier) AS tiers
    FROM candidates_mvt;
""")
row = cur.fetchone()
print(f"Total rows: {row[0]:,}")
print(f"With geometry: {row[1]:,}")
print(f"Distinct tiers: {row[2]}")

# Test MVT generation for a sample tile (Melbourne CBD area, z=12)
cur.execute("""
    SELECT length(ST_AsMVT(tile, 'parcels', 4096, 'geom')) AS tile_bytes
    FROM (
        SELECT ST_AsMVTGeom(geom, ST_TileEnvelope(12, 3657, 2516), 4096, 64, true) AS geom,
               parcel_id, suitability_tier
        FROM candidates_mvt
        WHERE ST_Intersects(geom, ST_TileEnvelope(12, 3657, 2516))
        LIMIT 10000
    ) AS tile
    WHERE geom IS NOT NULL;
""")
tile_size = cur.fetchone()[0]
print(f"Sample tile size: {tile_size:,} bytes")

cur.close()
conn.close()
print("\nMVT setup complete!")
