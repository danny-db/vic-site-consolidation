# Databricks notebook source
# MAGIC %md
# MAGIC # Stage 5: Lakebase (PostGIS) Sync
# MAGIC
# MAGIC Creates flat sync-ready tables in Delta (without GEOMETRY column) that are
# MAGIC automatically replicated to Lakebase PostGIS via Sync Tables.
# MAGIC
# MAGIC **Lakebase infrastructure** (project, branch, endpoint, UC catalog, sync tables,
# MAGIC native Postgres role, PostGIS views) is managed outside this notebook.
# MAGIC
# MAGIC This notebook only handles:
# MAGIC 1. Creating `consolidation_candidates_sync` (flat, with `geometry_wkt` TEXT)
# MAGIC 2. Creating `consolidation_pairs_sync` (pre-computed best pairs)
# MAGIC 3. Triggering a sync refresh
# MAGIC 4. Verifying the sync completed

# COMMAND ----------

# Create widgets for pipeline parameters
dbutils.widgets.text("catalog_name", "danny_catalog", "Catalog Name")
dbutils.widgets.text("schema_name", "dtp_hackathon", "Schema Name")

CATALOG = dbutils.widgets.get("catalog_name")
SCHEMA = dbutils.widgets.get("schema_name")
LAKEBASE_CATALOG = "vic_consolidation"

print(f"Delta source: {CATALOG}.{SCHEMA}")
print(f"Lakebase catalog: {LAKEBASE_CATALOG}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Create flat candidates table
# MAGIC
# MAGIC The `consolidation_candidates` table has a native GEOMETRY column that can't
# MAGIC be synced to Lakebase. We create a flat copy with `geometry_wkt` (TEXT) instead.

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.consolidation_candidates_sync AS
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
        ST_AsText(ST_Transform(geometry, 4326)) AS geometry_wkt
    FROM {CATALOG}.{SCHEMA}.consolidation_candidates
    WHERE centroid_lon IS NOT NULL AND centroid_lat IS NOT NULL
""")

candidates_count = spark.sql(f"SELECT COUNT(*) AS cnt FROM {CATALOG}.{SCHEMA}.consolidation_candidates_sync").first()["cnt"]
print(f"Created consolidation_candidates_sync: {candidates_count:,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Create flat pairs table

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.consolidation_pairs_sync AS
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
    FROM {CATALOG}.{SCHEMA}.parcel_adjacency a
    JOIN {CATALOG}.{SCHEMA}.consolidation_candidates s1 ON a.parcel_1 = s1.parcel_id
    JOIN {CATALOG}.{SCHEMA}.consolidation_candidates s2 ON a.parcel_2 = s2.parcel_id
    WHERE a.shared_boundary_m > 5
      AND s1.suitability_score >= 40
      AND s2.suitability_score >= 40
      AND s1.zone_code = s2.zone_code
""")

pairs_count = spark.sql(f"SELECT COUNT(*) AS cnt FROM {CATALOG}.{SCHEMA}.consolidation_pairs_sync").first()["cnt"]
print(f"Created consolidation_pairs_sync: {pairs_count:,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Trigger sync refresh & verify

# COMMAND ----------

import time
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

candidates_synced = f"{LAKEBASE_CATALOG}.{SCHEMA}.consolidation_candidates_sync"
pairs_synced = f"{LAKEBASE_CATALOG}.{SCHEMA}.consolidation_pairs_sync"

# Trigger refresh on both sync tables
for table_name in [candidates_synced, pairs_synced]:
    try:
        encoded = table_name.replace(".", "%2E")
        result = w.api_client.do("GET", f"/api/2.0/database/synced_tables/{encoded}")
        state = result.get("data_synchronization_status", {}).get("detailed_state", "?")
        print(f"{table_name}: {state}")

        # Trigger a sync refresh if the table is online
        if "ONLINE" in str(state):
            pipeline_id = result.get("data_synchronization_status", {}).get("pipeline_id")
            if pipeline_id:
                try:
                    w.api_client.do("POST", f"/api/2.0/pipelines/{pipeline_id}/updates", body={"full_refresh": False})
                    print(f"  Triggered refresh for pipeline {pipeline_id}")
                except Exception as e:
                    print(f"  Refresh trigger: {e}")
    except Exception as e:
        print(f"  Error checking {table_name}: {e}")

# COMMAND ----------

# Wait for sync to complete
print("Waiting for sync to complete...")
for attempt in range(18):  # Wait up to ~3 minutes
    try:
        encoded = candidates_synced.replace(".", "%2E")
        result = w.api_client.do("GET", f"/api/2.0/database/synced_tables/{encoded}")
        state = result.get("data_synchronization_status", {}).get("detailed_state", "?")
        print(f"  Attempt {attempt + 1}: {state}")
        if "ONLINE" in str(state) and "PENDING" not in str(state):
            break
    except Exception as e:
        print(f"  Attempt {attempt + 1}: {e}")
    time.sleep(10)

# Final verification
synced_count = spark.sql(f"SELECT COUNT(*) AS cnt FROM {candidates_synced}").first()["cnt"]
print(f"\nCandidates synced to Lakebase: {synced_count:,}")

pairs_synced_count = spark.sql(f"SELECT COUNT(*) AS cnt FROM {pairs_synced}").first()["cnt"]
print(f"Pairs synced to Lakebase: {pairs_synced_count:,}")

assert synced_count > 0, "No candidates in Lakebase sync table!"
print("\nLakebase sync complete!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Done!
# MAGIC
# MAGIC | Resource | Name |
# MAGIC |----------|------|
# MAGIC | Autoscaling Project | `vic-site-consolidation` |
# MAGIC | Postgres Database | `vic_consolidation_db` |
# MAGIC | UC Catalog | `vic_consolidation` |
# MAGIC | Sync Table | `consolidation_candidates_sync` (167K+ rows) |
# MAGIC | Sync Table | `consolidation_pairs_sync` (16K+ pairs) |
# MAGIC | Native Role | `vic_consolidation_app` |
# MAGIC | PostGIS View | `candidates_geo` (spatial queries) |
# MAGIC | Endpoint | `ep-icy-sound-e1pdqoh3.database.eastus2.azuredatabricks.net` |
