# Databricks notebook source
# MAGIC %md
# MAGIC # Stage 5: Lakebase (PostGIS) Sync
# MAGIC
# MAGIC This notebook syncs consolidation candidate data from Unity Catalog to **Lakebase** (managed PostGIS)
# MAGIC for serving via the Databricks App visualization layer.
# MAGIC
# MAGIC ## What it does:
# MAGIC 1. Creates a Lakebase Autoscaling project (if not exists)
# MAGIC 2. Creates a UC catalog for Lakebase sync
# MAGIC 3. Syncs `consolidation_candidates` table to Lakebase
# MAGIC 4. Enables PostGIS and creates spatial views/indexes
# MAGIC 5. Creates a native Postgres role for the app

# COMMAND ----------

# MAGIC %pip install databricks-sdk>=0.90.0 psycopg2-binary --quiet

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# Create widgets
dbutils.widgets.text("catalog_name", "danny_catalog", "Catalog Name")
dbutils.widgets.text("schema_name", "dtp_hackathon", "Schema Name")
dbutils.widgets.text("lakebase_project_name", "vic-site-consolidation", "Lakebase Project Name")

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")
lakebase_project_name = dbutils.widgets.get("lakebase_project_name")

print(f"Using: {catalog_name}.{schema_name}")
print(f"Lakebase project: {lakebase_project_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Create Lakebase Project

# COMMAND ----------

from databricks.sdk import WorkspaceClient
import time

w = WorkspaceClient()

# Check if project already exists
existing_projects = list(w.lakebase.list_projects())
project = None
for p in existing_projects:
    if p.name == lakebase_project_name:
        project = p
        print(f"Lakebase project '{lakebase_project_name}' already exists: {p.uid}")
        break

if project is None:
    print(f"Creating Lakebase project: {lakebase_project_name}")
    project = w.lakebase.create_project(
        name=lakebase_project_name,
        catalog_name=catalog_name,
        min_capacity_units=0.5,
        max_capacity_units=2,
    )
    print(f"Created project: {project.uid}")

# Wait for project to be ready
print("Waiting for Lakebase project to be ACTIVE...")
for i in range(60):
    p = w.lakebase.get_project(project.uid)
    if p.status and p.status.value == "ACTIVE":
        print(f"Project is ACTIVE after {i*10}s")
        break
    time.sleep(10)
else:
    print(f"WARNING: Project status: {p.status}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Create Sync Table

# COMMAND ----------

# Prepare data for sync: create a flat view without geometry column (use lat/lon instead)
# Lakebase sync requires standard column types
spark.sql(f"""
    CREATE OR REPLACE VIEW {catalog_name}.{schema_name}.consolidation_candidates_for_sync AS
    SELECT
        parcel_id,
        plan_number,
        lot_number,
        zone_code,
        zone_description,
        lga_code,
        lga_name,
        centroid_lon,
        centroid_lat,
        ROUND(area_sqm, 2) AS area_sqm,
        ROUND(perimeter_m, 2) AS perimeter_m,
        ROUND(compactness_index, 4) AS compactness_index,
        ROUND(aspect_ratio, 4) AS aspect_ratio,
        ROUND(elongation_index, 4) AS elongation_index,
        num_adjacent_parcels,
        ROUND(longest_shared_boundary_m, 2) AS longest_shared_boundary_m,
        adjacent_same_zone_count,
        ROUND(nearest_any_pt_m, 2) AS nearest_any_pt_m,
        is_growth_area_lga,
        lots_in_plan,
        score_growth_area_lga,
        score_recent_subdivision,
        score_mass_subdivision,
        opportunity_score,
        constraint_score,
        suitability_score,
        suitability_tier,
        rank,
        -- WKT geometry for PostGIS reconstruction
        ST_AsText(ST_Transform(geometry, 4326)) AS geometry_wkt
    FROM {catalog_name}.{schema_name}.consolidation_candidates
    WHERE centroid_lon IS NOT NULL
      AND centroid_lat IS NOT NULL
""")

print("Created sync view: consolidation_candidates_for_sync")
display(spark.sql(f"SELECT COUNT(*) AS total FROM {catalog_name}.{schema_name}.consolidation_candidates_for_sync"))

# COMMAND ----------

# Create the sync table
try:
    sync = w.lakebase.create_sync_table(
        project_uid=project.uid,
        source_catalog=catalog_name,
        source_schema=schema_name,
        source_table="consolidation_candidates_for_sync",
    )
    print(f"Created sync table: {sync.uid}")
except Exception as e:
    if "already exists" in str(e).lower():
        print("Sync table already exists, continuing...")
    else:
        raise

# Wait for initial sync
print("Waiting for initial sync to complete...")
time.sleep(30)
print("Sync initiated. Data should be available shortly.")

# COMMAND ----------

# Also sync the adjacency pairs for the consolidation pairs endpoint
spark.sql(f"""
    CREATE OR REPLACE VIEW {catalog_name}.{schema_name}.adjacency_pairs_for_sync AS
    SELECT
        a.parcel_1,
        a.parcel_2,
        ROUND(a.shared_boundary_m, 2) AS shared_boundary_m,
        s1.suitability_score AS score_1,
        s2.suitability_score AS score_2,
        s1.suitability_score + s2.suitability_score AS combined_score,
        s1.zone_code AS zone_1,
        s2.zone_code AS zone_2,
        s1.lga_name AS lga_name,
        s1.centroid_lon AS lon_1,
        s1.centroid_lat AS lat_1,
        s2.centroid_lon AS lon_2,
        s2.centroid_lat AS lat_2,
        ROUND(s1.area_sqm + s2.area_sqm, 2) AS combined_area_sqm
    FROM {catalog_name}.{schema_name}.parcel_adjacency a
    JOIN {catalog_name}.{schema_name}.consolidation_candidates s1 ON a.parcel_1 = s1.parcel_id
    JOIN {catalog_name}.{schema_name}.consolidation_candidates s2 ON a.parcel_2 = s2.parcel_id
    WHERE a.shared_boundary_m > 5
      AND s1.suitability_score >= 40
      AND s2.suitability_score >= 40
      AND s1.zone_code = s2.zone_code
""")

print("Created adjacency pairs sync view")

try:
    sync2 = w.lakebase.create_sync_table(
        project_uid=project.uid,
        source_catalog=catalog_name,
        source_schema=schema_name,
        source_table="adjacency_pairs_for_sync",
    )
    print(f"Created adjacency sync table: {sync2.uid}")
except Exception as e:
    if "already exists" in str(e).lower():
        print("Adjacency sync table already exists, continuing...")
    else:
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Enable PostGIS and Create Spatial Indexes

# COMMAND ----------

# Get Lakebase connection details
project_info = w.lakebase.get_project(project.uid)
print(f"Lakebase project: {project_info.name}")
print(f"Status: {project_info.status}")

# COMMAND ----------

import psycopg2

# Connect to Lakebase using SDK-provided credentials
conn_info = w.lakebase.get_project_connection_info(project.uid)

conn = psycopg2.connect(
    host=conn_info.host,
    port=conn_info.port,
    dbname=conn_info.database,
    user=conn_info.user,
    password=conn_info.password,
    sslmode="require"
)
conn.autocommit = True
cur = conn.cursor()

# Enable PostGIS
cur.execute("CREATE EXTENSION IF NOT EXISTS postgis;")
print("PostGIS enabled")

# Create spatial view with proper geometry from WKT
cur.execute(f"""
    CREATE OR REPLACE VIEW candidates_geo AS
    SELECT
        *,
        ST_SetSRID(ST_GeomFromText(geometry_wkt), 4326)::geometry AS geom,
        ST_SetSRID(ST_MakePoint(centroid_lon, centroid_lat), 4326)::geography AS centroid_geog
    FROM "{catalog_name}"."{schema_name}"."consolidation_candidates_for_sync"
    WHERE geometry_wkt IS NOT NULL;
""")
print("Created candidates_geo spatial view")

# Create a materialized view for the centroid points (for fast proximity queries)
cur.execute(f"""
    CREATE MATERIALIZED VIEW IF NOT EXISTS candidates_points AS
    SELECT
        parcel_id,
        plan_number,
        lot_number,
        zone_code,
        lga_name,
        centroid_lon,
        centroid_lat,
        area_sqm,
        compactness_index,
        num_adjacent_parcels,
        nearest_any_pt_m,
        is_growth_area_lga,
        lots_in_plan,
        opportunity_score,
        constraint_score,
        suitability_score,
        suitability_tier,
        rank,
        ST_SetSRID(ST_MakePoint(centroid_lon, centroid_lat), 4326)::geography AS centroid_geog
    FROM "{catalog_name}"."{schema_name}"."consolidation_candidates_for_sync"
    WHERE centroid_lon IS NOT NULL;
""")
print("Created candidates_points materialized view")

# Create spatial indexes
cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_candidates_points_geog
    ON candidates_points USING GIST (centroid_geog);
""")
print("Created spatial index on candidates_points")

# Refresh materialized view
cur.execute("REFRESH MATERIALIZED VIEW candidates_points;")
print("Refreshed candidates_points materialized view")

# Verify data
cur.execute("SELECT COUNT(*) FROM candidates_points;")
count = cur.fetchone()[0]
print(f"\nTotal candidates in Lakebase: {count:,}")

cur.execute("""
    SELECT suitability_tier, COUNT(*) AS cnt
    FROM candidates_points
    GROUP BY suitability_tier
    ORDER BY cnt DESC;
""")
print("\nTier distribution:")
for row in cur.fetchall():
    print(f"  {row[0]}: {row[1]:,}")

cur.close()
conn.close()
print("\nLakebase sync complete!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC This notebook:
# MAGIC 1. Created/verified Lakebase project: `vic-site-consolidation`
# MAGIC 2. Synced `consolidation_candidates` to Lakebase with WKT geometry
# MAGIC 3. Synced `adjacency_pairs` for consolidation pair lookups
# MAGIC 4. Enabled PostGIS and created spatial views with proper geometry types
# MAGIC 5. Created spatial GIST indexes for fast proximity queries
# MAGIC
# MAGIC The Databricks App (`app/`) connects to Lakebase to serve interactive maps.
