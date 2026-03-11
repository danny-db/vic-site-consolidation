# Databricks notebook source
# MAGIC %md
# MAGIC # Stage 5: Lakebase (PostGIS) Sync
# MAGIC
# MAGIC **Fully repeatable** — creates all Lakebase infrastructure from scratch:
# MAGIC 1. Flat sync tables in UC (without GEOMETRY column)
# MAGIC 2. Autoscaling project + production branch + endpoint
# MAGIC 3. UC catalog registration + named Postgres database
# MAGIC 4. Sync Tables (Delta → Lakebase)
# MAGIC 5. Native Postgres role for the application
# MAGIC 6. PostGIS extension + spatial view
# MAGIC 7. Verification
# MAGIC
# MAGIC Every step is idempotent — safe to re-run.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Install dependencies

# COMMAND ----------

# MAGIC %pip install psycopg2-binary "databricks-sdk>=0.90.0" --upgrade --quiet

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Create widgets for pipeline parameters
dbutils.widgets.text("catalog_name", "danny_catalog", "Catalog Name")
dbutils.widgets.text("schema_name", "dtp_hackathon", "Schema Name")

# Delta source (analytics)
DELTA_CATALOG = dbutils.widgets.get("catalog_name")
DELTA_SCHEMA = dbutils.widgets.get("schema_name")

# Lakebase Autoscaling target
LAKEBASE_PROJECT = "vic-site-consolidation"
LAKEBASE_BRANCH = "production"
LAKEBASE_DATABASE = "vic_consolidation_db"
LAKEBASE_CATALOG = "vic_consolidation"
LAKEBASE_SCHEMA = DELTA_SCHEMA  # Same schema name for simplicity

# Native Postgres role for the Databricks App
APP_ROLE = "vic_consolidation_app"
APP_PASSWORD = "VicApp2026Secure!"  # For demo; use Databricks secrets in production

print(f"Delta source: {DELTA_CATALOG}.{DELTA_SCHEMA}")
print(f"Lakebase target: {LAKEBASE_PROJECT}/{LAKEBASE_BRANCH}/{LAKEBASE_DATABASE}")
print(f"UC Catalog: {LAKEBASE_CATALOG}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Create flat sync tables in UC
# MAGIC
# MAGIC Lakebase sync requires standard column types. The `consolidation_candidates`
# MAGIC table has a native GEOMETRY column that can't be synced directly.
# MAGIC We create flat tables with `geometry_wkt` (TEXT) instead.

# COMMAND ----------

# Flat candidates table (without native GEOMETRY, adds WKT text)
spark.sql(f"""
    CREATE OR REPLACE TABLE {DELTA_CATALOG}.{DELTA_SCHEMA}.consolidation_candidates_sync AS
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
        ST_AsText(ST_Transform(geometry, 'EPSG:7899', 'EPSG:4326')) AS geometry_wkt
    FROM {DELTA_CATALOG}.{DELTA_SCHEMA}.consolidation_candidates
    WHERE centroid_lon IS NOT NULL AND centroid_lat IS NOT NULL
""")

candidates_count = spark.sql(f"SELECT COUNT(*) AS cnt FROM {DELTA_CATALOG}.{DELTA_SCHEMA}.consolidation_candidates_sync").first()["cnt"]
print(f"Created consolidation_candidates_sync: {candidates_count:,} rows")

# COMMAND ----------

# Flat pairs table (pre-computed best consolidation pairs)
spark.sql(f"""
    CREATE OR REPLACE TABLE {DELTA_CATALOG}.{DELTA_SCHEMA}.consolidation_pairs_sync AS
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
    FROM {DELTA_CATALOG}.{DELTA_SCHEMA}.parcel_adjacency a
    JOIN {DELTA_CATALOG}.{DELTA_SCHEMA}.consolidation_candidates s1 ON a.parcel_1 = s1.parcel_id
    JOIN {DELTA_CATALOG}.{DELTA_SCHEMA}.consolidation_candidates s2 ON a.parcel_2 = s2.parcel_id
    WHERE a.shared_boundary_m > 5
      AND s1.suitability_score >= 40
      AND s2.suitability_score >= 40
      AND s1.zone_code = s2.zone_code
""")

pairs_count = spark.sql(f"SELECT COUNT(*) AS cnt FROM {DELTA_CATALOG}.{DELTA_SCHEMA}.consolidation_pairs_sync").first()["cnt"]
print(f"Created consolidation_pairs_sync: {pairs_count:,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Create Lakebase Autoscaling project

# COMMAND ----------

import time
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.postgres import (
    Project, ProjectSpec, ProjectDefaultEndpointSettings,
    Branch, Endpoint, EndpointSpec, EndpointType,
)

w = WorkspaceClient()

project_path = f"projects/{LAKEBASE_PROJECT}"
branch_path = f"{project_path}/branches/{LAKEBASE_BRANCH}"
endpoint_path = f"{branch_path}/endpoints/primary"

# --- Create project ---
try:
    project = w.postgres.get_project(name=project_path)
    print(f"Project '{LAKEBASE_PROJECT}' already exists (PG{project.status.pg_version})")
except Exception as e:
    if "NOT_FOUND" in str(e) or "not found" in str(e).lower():
        print(f"Creating project '{LAKEBASE_PROJECT}'...")
        w.postgres.create_project(
            project=Project(
                spec=ProjectSpec(
                    display_name="VIC Site Consolidation",
                    pg_version=17,
                    enable_pg_native_login=True,
                    default_endpoint_settings=ProjectDefaultEndpointSettings(
                        autoscaling_limit_min_cu=0.5,
                        autoscaling_limit_max_cu=2,
                    ),
                )
            ),
            project_id=LAKEBASE_PROJECT,
        )
        for attempt in range(12):
            try:
                p = w.postgres.get_project(name=project_path)
                print(f"  Project state: created (PG{p.status.pg_version})")
                break
            except Exception:
                pass
            time.sleep(5)
        print(f"Project '{LAKEBASE_PROJECT}' created!")
    else:
        raise

# --- Verify branch exists (auto-created with project) ---
try:
    branch = w.postgres.get_branch(name=branch_path)
    print(f"Branch '{LAKEBASE_BRANCH}': {branch.status.current_state}")
except Exception as e:
    if "NOT_FOUND" in str(e) or "not found" in str(e).lower():
        print(f"Creating branch '{LAKEBASE_BRANCH}'...")
        w.postgres.create_branch(
            parent=project_path,
            branch=Branch(),
            branch_id=LAKEBASE_BRANCH,
        )
        time.sleep(5)
        print(f"Branch '{LAKEBASE_BRANCH}' created!")
    else:
        raise

# --- Verify endpoint exists (auto-created with project) ---
try:
    ep = w.postgres.get_endpoint(name=endpoint_path)
    print(f"Endpoint: {ep.status.hosts.host} ({ep.status.current_state})")
except Exception as e:
    if "NOT_FOUND" in str(e) or "not found" in str(e).lower():
        print("Creating primary endpoint...")
        w.postgres.create_endpoint(
            parent=branch_path,
            endpoint=Endpoint(
                spec=EndpointSpec(
                    endpoint_type=EndpointType.ENDPOINT_TYPE_READ_WRITE,
                    autoscaling_limit_min_cu=0.5,
                    autoscaling_limit_max_cu=2,
                )
            ),
            endpoint_id="primary",
        )
        time.sleep(10)
        ep = w.postgres.get_endpoint(name=endpoint_path)
        print(f"Endpoint created: {ep.status.hosts.host}")
    else:
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Register UC catalog (creates named database automatically)

# COMMAND ----------

db_resource_path = f"{branch_path}/databases/{LAKEBASE_DATABASE}"
try:
    cat = w.catalogs.get(name=LAKEBASE_CATALOG)
    print(f"UC Catalog '{LAKEBASE_CATALOG}' already exists (database: {cat.options.get('database', '?')})")
except Exception as e:
    if "NOT_FOUND" in str(e) or "does not exist" in str(e).lower():
        print(f"Registering UC Catalog '{LAKEBASE_CATALOG}' with database '{LAKEBASE_DATABASE}'...")
        result = w.api_client.do(
            "POST",
            "/api/2.0/postgres/catalogs",
            body={
                "name": LAKEBASE_CATALOG,
                "parent": branch_path,
                "catalog_id": LAKEBASE_CATALOG,
                "database": db_resource_path,
                "create_database_if_not_exists": True,
            },
        )
        print(f"UC Catalog registered: {result}")
    else:
        raise

# Verify the Postgres database was created
databases = list(w.postgres.list_databases(parent=branch_path))
pg_db = next((d for d in databases if d.status and d.status.postgres_database == LAKEBASE_DATABASE), None)
if pg_db:
    print(f"Postgres database: {pg_db.status.postgres_database} (resource: {pg_db.name})")
else:
    print(f"WARNING: Postgres database '{LAKEBASE_DATABASE}' not found — check catalog registration")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Create Sync Tables (Delta → Lakebase)

# COMMAND ----------

from databricks.sdk.service.database import (
    SyncedDatabaseTable,
    SyncedTableSpec,
    NewPipelineSpec,
    SyncedTableSchedulingPolicy,
)

# --- Sync consolidation_candidates_sync ---
candidates_synced_name = f"{LAKEBASE_CATALOG}.{LAKEBASE_SCHEMA}.consolidation_candidates_sync"
candidates_source_name = f"{DELTA_CATALOG}.{DELTA_SCHEMA}.consolidation_candidates_sync"

print(f"Creating Sync Table: {candidates_synced_name}")
print(f"  Source: {candidates_source_name}")

try:
    w.database.create_synced_database_table(
        SyncedDatabaseTable(
            name=candidates_synced_name,
            spec=SyncedTableSpec(
                source_table_full_name=candidates_source_name,
                primary_key_columns=["parcel_id"],
                scheduling_policy=SyncedTableSchedulingPolicy.SNAPSHOT,
                new_pipeline_spec=NewPipelineSpec(
                    storage_catalog=DELTA_CATALOG,
                    storage_schema=DELTA_SCHEMA,
                ),
            ),
        )
    )
    print("Candidates Sync Table created!")
except Exception as e:
    if "already exists" in str(e).lower() or "ALREADY_EXISTS" in str(e):
        print("Candidates Sync Table already exists — OK")
    else:
        raise

# COMMAND ----------

# --- Sync consolidation_pairs_sync ---
pairs_synced_name = f"{LAKEBASE_CATALOG}.{LAKEBASE_SCHEMA}.consolidation_pairs_sync"
pairs_source_name = f"{DELTA_CATALOG}.{DELTA_SCHEMA}.consolidation_pairs_sync"

print(f"Creating Sync Table: {pairs_synced_name}")
print(f"  Source: {pairs_source_name}")

try:
    w.database.create_synced_database_table(
        SyncedDatabaseTable(
            name=pairs_synced_name,
            spec=SyncedTableSpec(
                source_table_full_name=pairs_source_name,
                primary_key_columns=["parcel_1", "parcel_2"],
                scheduling_policy=SyncedTableSchedulingPolicy.SNAPSHOT,
                new_pipeline_spec=NewPipelineSpec(
                    storage_catalog=DELTA_CATALOG,
                    storage_schema=DELTA_SCHEMA,
                ),
            ),
        )
    )
    print("Pairs Sync Table created!")
except Exception as e:
    if "already exists" in str(e).lower() or "ALREADY_EXISTS" in str(e):
        print("Pairs Sync Table already exists — OK")
    else:
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Wait for Sync Tables to populate

# COMMAND ----------

import time

print("Waiting for candidates Sync Table...")
synced_count = 0

for attempt in range(24):  # Wait up to ~4 minutes
    try:
        result = w.database.get_synced_database_table(name=candidates_synced_name)
        state = result.data_synchronization_status.detailed_state if result.data_synchronization_status else "UNKNOWN"
        print(f"  Attempt {attempt + 1}: {state}")
        if "ONLINE" in str(state):
            synced_count = spark.sql(f"SELECT COUNT(*) AS cnt FROM {candidates_synced_name}").first()["cnt"]
            print(f"  Synced: {synced_count:,} candidates")
            break
    except Exception as e:
        print(f"  Attempt {attempt + 1}: {e}")
    time.sleep(10)

assert synced_count > 0, "Candidates Sync Table not ready after waiting. Check the DLT pipeline."
print(f"\nSync complete: {synced_count:,} candidates replicated to Lakebase!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Connect to Lakebase & get endpoint host

# COMMAND ----------

# Get the autoscaling endpoint host
endpoint = w.postgres.get_endpoint(name=endpoint_path)
pg_host = endpoint.status.hosts.host
print(f"Project: {LAKEBASE_PROJECT}")
print(f"Branch: {LAKEBASE_BRANCH}")
print(f"Host: {pg_host}")
print(f"State: {endpoint.status.current_state}")

# Generate credential for the autoscaling endpoint
cred = w.postgres.generate_database_credential(endpoint=endpoint_path)
pg_token = cred.token

# Get current user
me = w.current_user.me()
pg_user = me.user_name
print(f"Authenticated as: {pg_user}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Create native Postgres role for the application
# MAGIC
# MAGIC Create a native Postgres role with a static password.
# MAGIC The Databricks App uses this role — no OAuth token refresh needed.

# COMMAND ----------

import psycopg2

admin_conn = psycopg2.connect(
    host=pg_host, port=5432, dbname=LAKEBASE_DATABASE,
    user=pg_user, password=pg_token, sslmode="require",
)
admin_conn.autocommit = True
admin_cur = admin_conn.cursor()

# Create the native Postgres role (idempotent)
admin_cur.execute(f"""
    DO $$
    BEGIN
        IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = '{APP_ROLE}') THEN
            CREATE ROLE {APP_ROLE} WITH LOGIN PASSWORD '{APP_PASSWORD}';
            RAISE NOTICE 'Created role %', '{APP_ROLE}';
        ELSE
            ALTER ROLE {APP_ROLE} WITH PASSWORD '{APP_PASSWORD}';
            RAISE NOTICE 'Role % already exists, password updated', '{APP_ROLE}';
        END IF;
    END
    $$;
""")

# Create the schema if it doesn't exist
admin_cur.execute(f"CREATE SCHEMA IF NOT EXISTS {LAKEBASE_SCHEMA};")

# Grant permissions
admin_cur.execute(f"GRANT USAGE ON SCHEMA {LAKEBASE_SCHEMA} TO {APP_ROLE};")
admin_cur.execute(f"GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA {LAKEBASE_SCHEMA} TO {APP_ROLE};")
admin_cur.execute(f"ALTER DEFAULT PRIVILEGES IN SCHEMA {LAKEBASE_SCHEMA} GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO {APP_ROLE};")
admin_cur.execute(f"GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA {LAKEBASE_SCHEMA} TO {APP_ROLE};")
admin_cur.execute(f"ALTER DEFAULT PRIVILEGES IN SCHEMA {LAKEBASE_SCHEMA} GRANT USAGE, SELECT ON SEQUENCES TO {APP_ROLE};")

print(f"Native Postgres role '{APP_ROLE}' created with permissions on '{LAKEBASE_SCHEMA}'")

admin_cur.close()
admin_conn.close()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: PostGIS extension & spatial view
# MAGIC
# MAGIC The synced tables are owned by the Sync Table process and cannot be altered.
# MAGIC We create a view that adds the PostGIS `location` column from `centroid_lon/lat`.

# COMMAND ----------

import psycopg2

conn = psycopg2.connect(
    host=pg_host, port=5432, dbname=LAKEBASE_DATABASE,
    user=pg_user, password=pg_token, sslmode="require",
)
conn.autocommit = True
cur = conn.cursor()

# Enable PostGIS
cur.execute("CREATE EXTENSION IF NOT EXISTS postgis;")
print("PostGIS extension enabled")

# Create spatial view for candidates
cur.execute(f"""
    CREATE OR REPLACE VIEW {LAKEBASE_SCHEMA}.candidates_geo AS
    SELECT *,
           ST_SetSRID(ST_MakePoint(centroid_lon, centroid_lat), 4326)::geography AS location
    FROM {LAKEBASE_SCHEMA}.consolidation_candidates_sync;
""")
print("Spatial view 'candidates_geo' created")

# Grant access to the app role
cur.execute(f"GRANT SELECT ON {LAKEBASE_SCHEMA}.candidates_geo TO {APP_ROLE};")
cur.execute(f"GRANT SELECT ON {LAKEBASE_SCHEMA}.consolidation_candidates_sync TO {APP_ROLE};")
cur.execute(f"GRANT SELECT ON {LAKEBASE_SCHEMA}.consolidation_pairs_sync TO {APP_ROLE};")
print(f"Granted SELECT to '{APP_ROLE}'")

cur.close()
conn.close()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9: Verify with native Postgres role

# COMMAND ----------

import psycopg2

verify_conn = psycopg2.connect(
    host=pg_host, port=5432, dbname=LAKEBASE_DATABASE,
    user=APP_ROLE, password=APP_PASSWORD, sslmode="require",
)
verify_cur = verify_conn.cursor()

verify_cur.execute(f"SELECT COUNT(*) FROM {LAKEBASE_SCHEMA}.consolidation_candidates_sync;")
print(f"Candidates (Sync Table): {verify_cur.fetchone()[0]:,}")

verify_cur.execute(f"SELECT COUNT(*) FROM {LAKEBASE_SCHEMA}.consolidation_pairs_sync;")
print(f"Pairs (Sync Table): {verify_cur.fetchone()[0]:,}")

verify_cur.execute(f"""
    SELECT suitability_tier, COUNT(*) AS cnt
    FROM {LAKEBASE_SCHEMA}.consolidation_candidates_sync
    GROUP BY suitability_tier
    ORDER BY cnt DESC;
""")
print("\nTier distribution:")
for row in verify_cur.fetchall():
    print(f"  {row[0]}: {row[1]:,}")

# Test spatial query
verify_cur.execute(f"""
    SELECT COUNT(*) FROM {LAKEBASE_SCHEMA}.candidates_geo
    WHERE ST_DWithin(
        location,
        ST_SetSRID(ST_MakePoint(145.0, -37.8), 4326)::geography,
        5000
    );
""")
print(f"\nCandidates within 5km of Melbourne CBD: {verify_cur.fetchone()[0]:,}")

verify_cur.close()
verify_conn.close()

print(f"\nNative Postgres role '{APP_ROLE}' verified!")
print(f"Host: {pg_host}")
print(f"Database: {LAKEBASE_DATABASE}")
print(f"Schema: {LAKEBASE_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Done!
# MAGIC
# MAGIC | Resource | Name | Type |
# MAGIC |----------|------|------|
# MAGIC | Autoscaling Project | `vic-site-consolidation` | PG17, auto-scales 0.5–2 CU |
# MAGIC | Postgres Database | `vic_consolidation_db` | Named database (portable) |
# MAGIC | UC Catalog | `vic_consolidation` | Registered for autoscaling project |
# MAGIC | Sync Table | `consolidation_candidates_sync` | Delta → Lakebase via DLT |
# MAGIC | Sync Table | `consolidation_pairs_sync` | Delta → Lakebase via DLT |
# MAGIC | Native Role | `vic_consolidation_app` | Static credentials for the app |
# MAGIC | PostGIS | Enabled | Spatial view `candidates_geo` |
# MAGIC
# MAGIC **Configure the Databricks App** with these env vars:
# MAGIC - `LAKEBASE_HOST`: the endpoint host printed above
# MAGIC - `LAKEBASE_DATABASE`: `vic_consolidation_db`
# MAGIC - `LAKEBASE_SCHEMA`: `dtp_hackathon`
# MAGIC - `LAKEBASE_USER`: `vic_consolidation_app`
# MAGIC - `LAKEBASE_PASSWORD`: the password set above
