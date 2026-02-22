# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 1c: Activity Centre Proximity Analysis
# MAGIC
# MAGIC This notebook computes **proximity to Activity Centres** for each land parcel.
# MAGIC
# MAGIC ## Features Computed:
# MAGIC | Feature | Description | Why It Matters |
# MAGIC |---------|-------------|----------------|
# MAGIC | **Nearest Activity Centre Distance** | Distance to closest Activity Centre | Policy directs growth toward activity centres |
# MAGIC | **Activity Centre Tier** | Metropolitan, Major, or Neighbourhood | Higher-tier centres have more growth pressure |
# MAGIC | **Within Walk-up Distance** | Boolean: within 800m of centre | Walk-up accessibility premium |
# MAGIC
# MAGIC ## Activity Centre Types (Plan Melbourne):
# MAGIC - **Metropolitan Activity Centres (MAC)** - Major regional hubs (Box Hill, Sunshine, Dandenong)
# MAGIC - **Major Activity Centres** - Significant local centres
# MAGIC - **Neighbourhood Activity Centres** - Local shopping strips and small centres
# MAGIC
# MAGIC **Prerequisite:** Run `01_data_ingestion.py` to load Activity Centres data.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

# Create widgets
dbutils.widgets.text("catalog_name", "danny_catalog", "Catalog Name")
dbutils.widgets.text("schema_name", "dtp_hackathon", "Schema Name")

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

print(f"Using: {catalog_name}.{schema_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Check for Activity Centres Data
# MAGIC
# MAGIC Activity Centres data comes from Plan Melbourne spatial data.
# MAGIC If not available, we can create a simplified version using PT stops as proxies.

# COMMAND ----------

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

# Check if activity_centres table exists
try:
    ac_count = spark.sql(f"SELECT COUNT(*) FROM {catalog_name}.{schema_name}.activity_centres").collect()[0][0]
    print(f"Activity Centres table found with {ac_count:,} records")
    HAS_ACTIVITY_CENTRES = True
except Exception as e:
    print(f"Activity Centres table not found: {e}")
    print("\nWill use train stations as proxy for activity centres (common approach)")
    HAS_ACTIVITY_CENTRES = False

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Activity Centres Data Source
# MAGIC
# MAGIC **Plan Melbourne Activity Centres** data is required for accurate proximity analysis.
# MAGIC
# MAGIC ### Download Instructions:
# MAGIC 1. Go to: https://www.planning.vic.gov.au/guides-and-resources/strategies-and-initiatives/plan-melbourne/the-map-room
# MAGIC 2. Download "Activity Centres" shapefile
# MAGIC 3. Upload to UC Volume: `{volume_path}/land_parcel_raw/activity_centres/`
# MAGIC 4. Run notebook 01_data_ingestion.py to load the data
# MAGIC
# MAGIC ### Activity Centre Tiers (Plan Melbourne):
# MAGIC - **Metropolitan Activity Centres (MAC)**: Box Hill, Broadmeadows, Dandenong, Epping, Footscray, Fountain Gate-Narre Warren, Frankston, Ringwood, Sunshine
# MAGIC - **Major Activity Centres**: ~100+ significant local centres
# MAGIC - **Neighbourhood Activity Centres**: Local shopping strips

# COMMAND ----------

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

# Check if activity_centres table exists with real data
if HAS_ACTIVITY_CENTRES:
    print("Using real Activity Centres data from Plan Melbourne")
    print("\nTable schema:")
    display(spark.sql(f"DESCRIBE {catalog_name}.{schema_name}.activity_centres"))
    print("\nSample data:")
    display(spark.sql(f"""
        SELECT *
        FROM {catalog_name}.{schema_name}.activity_centres
        LIMIT 10
    """))
else:
    print("=" * 80)
    print("WARNING: Activity Centres table not found!")
    print("=" * 80)
    print("""
To get real Activity Centres data:

1. Download from Plan Melbourne Map Room:
   https://www.planning.vic.gov.au/guides-and-resources/strategies-and-initiatives/plan-melbourne/the-map-room

2. Upload the shapefile to your UC Volume:
   /Volumes/{catalog}/{schema}/source/land_parcel_raw/activity_centres/

3. Add ingestion code to 01_data_ingestion.py:

   activity_centres_path = f"{raw_data_path}/activity_centres"
   shp_file = find_shapefile(activity_centres_path)
   if shp_file:
       df_activity_centres = spark.read.format("geo").option("path", shp_file).load()
       materialize_geo_table(df_activity_centres, "activity_centres", catalog_name, schema_name)

For now, skipping Activity Centre proximity analysis.
    """)

    # Set flag to skip proximity calculation
    SKIP_AC_PROXIMITY = True

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Compute Proximity to Activity Centres
# MAGIC
# MAGIC Calculate distance from each parcel to nearest activity centre by tier.
# MAGIC
# MAGIC **Note:** This section requires the Activity Centres table. If not available, skip this notebook.

# COMMAND ----------

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

# Check if we should skip
if not HAS_ACTIVITY_CENTRES:
    print("Skipping Activity Centre proximity calculation - data not available.")
    print("Please download Activity Centres data from Plan Melbourne and re-run notebook 01.")
    dbutils.notebook.exit("Activity Centres data not available")

# Use Casey LGA - it's in Metro Melbourne where Activity Centres are located
# (Activity Centres from Plan Melbourne are concentrated in Metro Melbourne)
sample_lga = "CASEY"

# Verify the LGA exists in our data
lga_check = spark.sql(f"""
    SELECT COUNT(*) AS parcel_count
    FROM {catalog_name}.{schema_name}.parcel_edge_topology
    WHERE lga_name = '{sample_lga}'
""").collect()[0][0]

if lga_check == 0:
    print(f"WARNING: {sample_lga} not found in parcel_edge_topology. Available LGAs:")
    display(spark.sql(f"""
        SELECT DISTINCT lga_name, COUNT(*) as count
        FROM {catalog_name}.{schema_name}.parcel_edge_topology
        GROUP BY lga_name
        ORDER BY count DESC
        LIMIT 20
    """))
else:
    print(f"Computing activity centre proximity for LGA: {sample_lga} ({lga_check:,} parcels)")

# COMMAND ----------

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

# ============================================================================
# CRS DETECTION: Verify Coordinate System Before Calculations
# ============================================================================
# Coordinate Reference Systems (CRS) define how locations are represented:
# - EPSG:4326 (WGS84): Uses latitude/longitude in degrees (e.g., -37.8, 144.9)
# - EPSG:3111 (VicGrid): Uses meters from a reference point (e.g., 2500000, 5800000)
#
# WHY THIS MATTERS:
# - Distance calculations fail if coordinates are in wrong system
# - The SRID "tag" on geometry can be incorrect (data quality issue)
# - We detect actual coordinate type by checking value ranges:
#   - If X is ~144-146 and Y is ~-37 to -38 → WGS84 (degrees)
#   - If X and Y are both ~2-3 million → VicGrid (meters)
#
# If the SRID tag says 3111 but coords are actually degrees, we use
# ST_SetSRID to fix the tag instead of ST_Transform (which would corrupt data)
# ============================================================================
ac_coord_info = spark.sql(f"""
    SELECT
        MIN(ST_SRID(geometry)) AS srid,
        ROUND(MIN(ST_X(ST_Centroid(geometry))), 4) AS min_x,
        ROUND(MAX(ST_X(ST_Centroid(geometry))), 4) AS max_x,
        ROUND(MIN(ST_Y(ST_Centroid(geometry))), 4) AS min_y,
        ROUND(MAX(ST_Y(ST_Centroid(geometry))), 4) AS max_y
    FROM {catalog_name}.{schema_name}.activity_centres
    WHERE geometry IS NOT NULL
""").collect()[0]

ac_srid = ac_coord_info['srid']
min_x, max_x = ac_coord_info['min_x'], ac_coord_info['max_x']
min_y, max_y = ac_coord_info['min_y'], ac_coord_info['max_y']

print(f"Activity Centres - Tagged SRID: {ac_srid}")
print(f"  X range: [{min_x}, {max_x}]")
print(f"  Y range: [{min_y}, {max_y}]")

# Detect actual CRS based on coordinate values (not SRID tag which can be wrong)
# Victoria WGS84: lon ~140-150, lat ~-39 to -34
# Victoria VicGrid (EPSG:3111): X ~2.2M-2.9M, Y ~2.2M-2.9M
is_already_wgs84_coords = (100 < min_x < 180) and (-50 < min_y < 0)
is_vicgrid_coords = (min_x > 100000) and (min_y > 100000)

print(f"  Coordinates appear to be: {'WGS84 (degrees)' if is_already_wgs84_coords else 'VicGrid (meters)' if is_vicgrid_coords else 'Unknown'}")

# Build the transform SQL based on actual coordinate detection
if is_already_wgs84_coords:
    # Coordinates are already in degrees - just fix the SRID tag
    print("  -> Using ST_SetSRID to fix incorrect SRID tag (no coordinate transformation)")
    ac_transform_sql = "ST_SetSRID(ST_Centroid(geometry), 4326)"
elif is_vicgrid_coords:
    # Coordinates are in meters - need actual transformation
    print("  -> Using ST_Transform to convert from VicGrid to WGS84")
    ac_transform_sql = "ST_Centroid(ST_Transform(geometry, 4326))"
else:
    # Unknown - try transform as fallback
    print("  -> Unknown CRS, attempting ST_Transform")
    ac_transform_sql = "ST_Centroid(ST_Transform(geometry, 4326))"

print(f"Using: {ac_transform_sql}")

# ============================================================================
# ACTIVITY CENTRE PROXIMITY: Distance to Plan Melbourne Activity Centres
# ============================================================================
# Plan Melbourne designates "Activity Centres" as hubs for housing growth.
# State policy directs higher density development toward these centres.
#
# ACTIVITY CENTRE TIERS (from Plan Melbourne):
# - Metropolitan (MAC): Major regional hubs (Box Hill, Dandenong, Sunshine, etc.)
#   Highest priority for intensification, typically 9+ storeys
# - Major: Significant local centres with good public transport
#   Medium-high density, typically 4-8 storeys
# - Neighbourhood: Local shopping strips and small centres
#   Modest intensification, typically 2-4 storeys
#
# KEY SPATIAL FUNCTION:
# - ST_DistanceSphere(point1, point2): Calculates straight-line distance
#   on a sphere (Earth), accounting for curvature. Returns meters.
#   Requires both points to be in WGS84 (lat/lon) coordinates.
#
# WALKABILITY THRESHOLDS (Plan Melbourne guidance):
# - 400m: High walkability - most people will walk this distance
# - 800m: Walkable distance - typical 10-minute walk
# - 1600m: Extended walkable catchment
#
# Data source: Plan Melbourne Activity Centres shapefile
# Columns: Centre_Nam (name), PM2017_Cat (tier category)
# ============================================================================
spark.sql(f"""
    CREATE OR REPLACE TABLE {catalog_name}.{schema_name}.parcel_activity_centre_proximity AS
    WITH parcel_centroids AS (
        SELECT
            parcel_id,
            centroid_lon,
            centroid_lat,
            -- Create point in WGS84 for distance calculation
            ST_SetSRID(ST_Point(centroid_lon, centroid_lat), 4326) AS centroid
        FROM {catalog_name}.{schema_name}.parcel_edge_topology
        WHERE centroid_lon IS NOT NULL
          AND centroid_lat IS NOT NULL
          AND lga_name = '{sample_lga}'
    ),
    ac_transformed AS (
        -- Get activity centre centroids in WGS84 (transform only if needed)
        SELECT
            Centre_Nam AS centre_name,
            PM2017_Cat AS centre_tier,
            {ac_transform_sql} AS ac_centroid
        FROM {catalog_name}.{schema_name}.activity_centres
        WHERE geometry IS NOT NULL
    ),
    ac_distances AS (
        SELECT
            p.parcel_id,
            ac.centre_name,
            ac.centre_tier,
            -- Distance in meters using spherical calculation (point to point)
            ROUND(ST_DistanceSphere(p.centroid, ac.ac_centroid), 2) AS distance_m
        FROM parcel_centroids p
        CROSS JOIN ac_transformed ac
    ),
    nearest_by_tier AS (
        SELECT
            parcel_id,
            centre_tier,
            MIN(distance_m) AS nearest_distance_m,
            FIRST(centre_name) AS nearest_centre_name
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY parcel_id, centre_tier ORDER BY distance_m) AS rn
            FROM ac_distances
        )
        WHERE rn = 1
        GROUP BY parcel_id, centre_tier
    )
    SELECT
        parcel_id,
        -- Nearest by tier (using actual PM2017_Cat values)
        MAX(CASE WHEN centre_tier LIKE '%Metropolitan%' THEN nearest_distance_m END) AS nearest_mac_m,
        MAX(CASE WHEN centre_tier LIKE '%Metropolitan%' THEN nearest_centre_name END) AS nearest_mac_name,
        MAX(CASE WHEN centre_tier LIKE '%Major%' THEN nearest_distance_m END) AS nearest_major_m,
        MAX(CASE WHEN centre_tier LIKE '%Major%' THEN nearest_centre_name END) AS nearest_major_name,
        MAX(CASE WHEN centre_tier LIKE '%Neighbourhood%' OR centre_tier LIKE '%Local%' THEN nearest_distance_m END) AS nearest_neighbourhood_m,
        MAX(CASE WHEN centre_tier LIKE '%Neighbourhood%' OR centre_tier LIKE '%Local%' THEN nearest_centre_name END) AS nearest_neighbourhood_name,

        -- Overall nearest
        MIN(nearest_distance_m) AS nearest_any_ac_m,

        -- Walk-up distance flags (800m is typical walkable distance)
        MIN(nearest_distance_m) <= 400 AS within_400m_ac,
        MIN(nearest_distance_m) <= 800 AS within_800m_ac,
        MIN(nearest_distance_m) <= 1200 AS within_1200m_ac,

        -- Metropolitan Activity Centre flags
        MAX(CASE WHEN centre_tier LIKE '%Metropolitan%' THEN nearest_distance_m END) <= 800 AS within_800m_mac,
        MAX(CASE WHEN centre_tier LIKE '%Metropolitan%' THEN nearest_distance_m END) <= 1600 AS within_1600m_mac

    FROM nearest_by_tier
    GROUP BY parcel_id
""")

print("Created parcel_activity_centre_proximity table")
display(spark.sql(f"SELECT * FROM {catalog_name}.{schema_name}.parcel_activity_centre_proximity LIMIT 20"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Summary Statistics

# COMMAND ----------

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

# Proximity distribution
display(spark.sql(f"""
    SELECT
        CASE
            WHEN nearest_any_ac_m <= 400 THEN '0-400m'
            WHEN nearest_any_ac_m <= 800 THEN '400-800m'
            WHEN nearest_any_ac_m <= 1200 THEN '800-1200m'
            WHEN nearest_any_ac_m <= 1600 THEN '1200-1600m'
            ELSE '>1600m'
        END AS distance_band,
        COUNT(*) AS parcel_count,
        ROUND(AVG(nearest_any_ac_m), 0) AS avg_distance_m
    FROM {catalog_name}.{schema_name}.parcel_activity_centre_proximity
    GROUP BY
        CASE
            WHEN nearest_any_ac_m <= 400 THEN '0-400m'
            WHEN nearest_any_ac_m <= 800 THEN '400-800m'
            WHEN nearest_any_ac_m <= 1200 THEN '800-1200m'
            WHEN nearest_any_ac_m <= 1600 THEN '1200-1600m'
            ELSE '>1600m'
        END
    ORDER BY
        CASE
            WHEN distance_band = '0-400m' THEN 1
            WHEN distance_band = '400-800m' THEN 2
            WHEN distance_band = '800-1200m' THEN 3
            WHEN distance_band = '1200-1600m' THEN 4
            ELSE 5
        END
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### AI-Generated Summary of Activity Centre Proximity

# COMMAND ----------

# Use ai_query() to generate a human-readable summary of activity centre proximity
catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

ac_proximity_summary = spark.sql(f"""
    WITH proximity_stats AS (
        SELECT
            CASE
                WHEN nearest_any_ac_m <= 400 THEN '0-400m'
                WHEN nearest_any_ac_m <= 800 THEN '400-800m'
                WHEN nearest_any_ac_m <= 1200 THEN '800-1200m'
                WHEN nearest_any_ac_m <= 1600 THEN '1200-1600m'
                ELSE '>1600m'
            END AS distance_band,
            COUNT(*) AS parcel_count,
            ROUND(AVG(nearest_any_ac_m), 0) AS avg_distance_m,
            SUM(CASE WHEN within_800m_ac THEN 1 ELSE 0 END) AS within_walkable,
            SUM(CASE WHEN within_800m_mac THEN 1 ELSE 0 END) AS within_walkable_mac
        FROM {catalog_name}.{schema_name}.parcel_activity_centre_proximity
        GROUP BY
            CASE
                WHEN nearest_any_ac_m <= 400 THEN '0-400m'
                WHEN nearest_any_ac_m <= 800 THEN '400-800m'
                WHEN nearest_any_ac_m <= 1200 THEN '800-1200m'
                WHEN nearest_any_ac_m <= 1600 THEN '1200-1600m'
                ELSE '>1600m'
            END
    )
    SELECT ai_query(
        'databricks-claude-opus-4-6',
        CONCAT(
            'You are a Victorian urban planning analyst specializing in Plan Melbourne activity centres and transit-oriented development. Analyze these parcel proximity statistics and provide a concise 3-4 paragraph summary. ',
            'Focus on: (1) what percentage of parcels are within walkable distance (800m) of activity centres - this is key for Plan Melbourne compliance, ',
            '(2) the distribution across distance bands and what it means for housing intensification potential, ',
            '(3) proximity to Metropolitan Activity Centres (MACs) which are priority growth areas, ',
            '(4) recommendations for which distance bands should be prioritized for site consolidation to maximize transit-oriented development. ',
            'Be specific with numbers and percentages. Data: ',
            TO_JSON(COLLECT_LIST(STRUCT(*)))
        )
    ) AS ac_proximity_summary
    FROM proximity_stats
""")

# Display the ai_query result with proper markdown rendering
from IPython.display import display, Markdown, HTML

result_df = ac_proximity_summary.toPandas()
if len(result_df) > 0:
    summary_text = result_df['ac_proximity_summary'].iloc[0]
    display(Markdown(summary_text))
else:
    print("No summary generated")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Visualize Activity Centres and Parcel Proximity

# COMMAND ----------

# MAGIC %pip install folium --quiet

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

# DIAGNOSTIC: Check geometry SRID and coordinate ranges for activity centres
print("=" * 80)
print("DIAGNOSTIC: Checking Activity Centres geometry")
print("=" * 80)
display(spark.sql(f"""
    SELECT
        COUNT(*) AS total_rows,
        MIN(ST_SRID(geometry)) AS min_srid,
        MAX(ST_SRID(geometry)) AS max_srid,
        -- Native coordinates (check if already WGS84 or projected)
        ROUND(MIN(ST_X(ST_Centroid(geometry))), 2) AS min_native_x,
        ROUND(MAX(ST_X(ST_Centroid(geometry))), 2) AS max_native_x,
        ROUND(MIN(ST_Y(ST_Centroid(geometry))), 2) AS min_native_y,
        ROUND(MAX(ST_Y(ST_Centroid(geometry))), 2) AS max_native_y
    FROM {catalog_name}.{schema_name}.activity_centres
    WHERE geometry IS NOT NULL
"""))

print("\nExpected values:")
print("- SRID 3111 (VicGrid): X ~2.5M, Y ~5.8M")
print("- SRID 4326 (WGS84): X ~144-146, Y ~-37 to -38")
print("- If already WGS84 coords, don't transform again!")

# Check the actual SRID and decide transformation strategy
srid_info = spark.sql(f"""
    SELECT MIN(ST_SRID(geometry)) AS srid
    FROM {catalog_name}.{schema_name}.activity_centres
    WHERE geometry IS NOT NULL
    LIMIT 1
""").collect()[0]['srid']

native_x_sample = spark.sql(f"""
    SELECT ROUND(ST_X(ST_Centroid(geometry)), 2) AS x
    FROM {catalog_name}.{schema_name}.activity_centres
    WHERE geometry IS NOT NULL
    LIMIT 1
""").collect()[0]['x']

print(f"\nDetected SRID: {srid_info}")
print(f"Sample native X coordinate: {native_x_sample}")

# Determine if transformation is needed based on coordinate range
# If X is between 140-150, it's already lon/lat (WGS84-like)
# If X is > 100000, it's in meters (projected CRS)
needs_transform = native_x_sample > 1000

if needs_transform:
    print("-> Coordinates appear to be in meters (projected CRS), will transform to WGS84")
    transform_sql = "ST_Transform(geometry, 4326)"
else:
    print("-> Coordinates appear to be already in degrees (WGS84), using as-is")
    transform_sql = "geometry"

# Get activity centres for visualization with correct transformation
ac_df = spark.sql(f"""
    SELECT
        Centre_Nam AS centre_name,
        PM2017_Cat AS centre_tier,
        ST_X(ST_Centroid({transform_sql})) AS lon,
        ST_Y(ST_Centroid({transform_sql})) AS lat
    FROM {catalog_name}.{schema_name}.activity_centres
    WHERE geometry IS NOT NULL
""").toPandas()

print(f"\nRetrieved {len(ac_df)} activity centres")

# Validate coordinates
if len(ac_df) > 0:
    print(f"Coordinate ranges after transformation:")
    print(f"  Lon: [{ac_df['lon'].min():.4f}, {ac_df['lon'].max():.4f}]")
    print(f"  Lat: [{ac_df['lat'].min():.4f}, {ac_df['lat'].max():.4f}]")
    if ac_df['lon'].min() < 100 or ac_df['lon'].max() > 200:
        print("  -> WARNING: Lon outside Australia range (100-180)")
    if ac_df['lat'].max() > 0 or ac_df['lat'].min() < -50:
        print("  -> WARNING: Lat outside Australia range (-50 to 0)")

display(ac_df.head(20))

# COMMAND ----------

import folium
from folium.plugins import MarkerCluster

if len(ac_df) > 0:
    # Create map centered on activity centres
    center_lat = ac_df['lat'].mean()
    center_lon = ac_df['lon'].mean()

    m = folium.Map(
        location=[center_lat, center_lon],
        zoom_start=11,
        tiles='CartoDB positron'
    )

    # Color by tier
    tier_colors = {
        'Metropolitan': 'red',
        'Major': 'orange',
        'Neighbourhood': 'green',
        'Local': 'blue'
    }

    # Add activity centre markers
    for _, row in ac_df.iterrows():
        # Determine color based on tier
        color = 'gray'
        for tier_key, tier_color in tier_colors.items():
            if tier_key.lower() in str(row['centre_tier']).lower():
                color = tier_color
                break

        folium.CircleMarker(
            location=[row['lat'], row['lon']],
            radius=8 if 'Metropolitan' in str(row['centre_tier']) else 5,
            color=color,
            fill=True,
            fillColor=color,
            fillOpacity=0.7,
            tooltip=f"<b>{row['centre_name']}</b><br>Tier: {row['centre_tier']}"
        ).add_to(m)

    # Add legend
    legend_html = '''
    <div style="position: fixed;
                bottom: 50px; left: 50px; width: 180px;
                border:2px solid grey; z-index:9999; font-size:14px;
                background-color:white; padding: 10px;
                border-radius: 5px;">
        <b>Activity Centre Tier</b><br>
        <i style="background:red; width:12px; height:12px; display:inline-block; border-radius:50%;"></i> Metropolitan<br>
        <i style="background:orange; width:12px; height:12px; display:inline-block; border-radius:50%;"></i> Major<br>
        <i style="background:green; width:12px; height:12px; display:inline-block; border-radius:50%;"></i> Neighbourhood<br>
        <i style="background:blue; width:12px; height:12px; display:inline-block; border-radius:50%;"></i> Local
    </div>
    '''
    m.get_root().html.add_child(folium.Element(legend_html))

    displayHTML(m._repr_html_())
else:
    print("No activity centres found for visualization")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Export Full Activity Centres Map to UC Volume

# COMMAND ----------

# Export ALL activity centres to Folium HTML
import folium
from folium.plugins import MarkerCluster

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

if len(ac_df) > 0:
    center_lat = ac_df['lat'].mean()
    center_lon = ac_df['lon'].mean()

    m_ac_full = folium.Map(
        location=[center_lat, center_lon],
        zoom_start=10,
        tiles='CartoDB positron'
    )

    # Count by tier
    tier_counts = ac_df['centre_tier'].value_counts().to_dict()

    tier_colors = {
        'Metropolitan': 'red',
        'Major': 'orange',
        'Neighbourhood': 'green',
        'Local': 'blue'
    }

    for _, row in ac_df.iterrows():
        color = 'gray'
        for tier_key, tier_color in tier_colors.items():
            if tier_key.lower() in str(row['centre_tier']).lower():
                color = tier_color
                break

        folium.CircleMarker(
            location=[row['lat'], row['lon']],
            radius=10 if 'Metropolitan' in str(row['centre_tier']) else 6,
            color=color,
            fill=True,
            fillColor=color,
            fillOpacity=0.7,
            tooltip=f"<b>{row['centre_name']}</b><br>Tier: {row['centre_tier']}"
        ).add_to(m_ac_full)

    # Build legend with counts
    legend_items = []
    for tier, color in tier_colors.items():
        count = sum(1 for t in ac_df['centre_tier'] if tier.lower() in str(t).lower())
        if count > 0:
            legend_items.append(f'<i style="background:{color}; width:12px; height:12px; display:inline-block; border-radius:50%;"></i> {tier} ({count})<br>')

    legend_html = f'''
    <div style="position: fixed; bottom: 50px; left: 50px; width: 200px;
                border:2px solid grey; z-index:9999; font-size:14px;
                background-color:white; padding: 10px; border-radius: 5px;">
        <b>Activity Centre Tier (All)</b><br>
        {''.join(legend_items)}
        <small>Total: {len(ac_df)}</small>
    </div>
    '''
    m_ac_full.get_root().html.add_child(folium.Element(legend_html))

    # Save to UC Volume
    volume_path = f"/Volumes/{catalog_name}/{schema_name}/source"
    ac_html_path = f"{volume_path}/visualizations/folium_activity_centres_full.html"

    import os
    os.makedirs(f"{volume_path}/visualizations", exist_ok=True)

    m_ac_full.save(ac_html_path)
    print(f"Full activity centres map saved to: {ac_html_path}")
    print(f"Total activity centres exported: {len(ac_df)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Parcels Colored by Distance to Activity Centre

# COMMAND ----------

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

# Get parcels with proximity data for visualization
parcels_with_ac = spark.sql(f"""
    SELECT
        e.parcel_id,
        e.zone_code,
        e.centroid_lon,
        e.centroid_lat,
        ST_AsGeoJSON(ST_Transform(e.geometry, 4326)) AS geojson,
        ac.nearest_any_ac_m,
        ac.nearest_mac_name,
        ac.within_800m_ac,
        ac.within_800m_mac
    FROM {catalog_name}.{schema_name}.parcel_edge_topology e
    JOIN {catalog_name}.{schema_name}.parcel_activity_centre_proximity ac
        ON e.parcel_id = ac.parcel_id
    WHERE e.geometry IS NOT NULL
      AND e.centroid_lon IS NOT NULL
    ORDER BY ac.nearest_any_ac_m
    LIMIT 200
""").toPandas()

print(f"Retrieved {len(parcels_with_ac)} parcels with activity centre proximity")

# COMMAND ----------

import folium
import json

if len(parcels_with_ac) > 0:
    center_lat = parcels_with_ac['centroid_lat'].mean()
    center_lon = parcels_with_ac['centroid_lon'].mean()

    m = folium.Map(
        location=[center_lat, center_lon],
        zoom_start=14,
        tiles='CartoDB positron'
    )

    # Color by distance
    def get_distance_color(distance):
        if distance <= 400:
            return '#1a9850'  # Dark green - excellent
        elif distance <= 800:
            return '#91cf60'  # Light green - good
        elif distance <= 1200:
            return '#fee08b'  # Yellow - moderate
        elif distance <= 1600:
            return '#fc8d59'  # Orange - fair
        else:
            return '#d73027'  # Red - poor

    # Add parcels
    for _, row in parcels_with_ac.iterrows():
        try:
            geojson = json.loads(row['geojson'])
            color = get_distance_color(row['nearest_any_ac_m'])

            tooltip = f"""
                <b>Parcel:</b> {row['parcel_id']}<br>
                <b>Zone:</b> {row['zone_code']}<br>
                <b>Distance to AC:</b> {row['nearest_any_ac_m']:.0f}m<br>
                <b>Nearest MAC:</b> {row['nearest_mac_name'] or 'N/A'}<br>
                <b>Within 800m:</b> {'Yes' if row['within_800m_ac'] else 'No'}
            """

            folium.GeoJson(
                geojson,
                style_function=lambda x, c=color: {
                    'fillColor': c,
                    'color': c,
                    'weight': 1,
                    'fillOpacity': 0.6
                },
                tooltip=folium.Tooltip(tooltip)
            ).add_to(m)
        except:
            pass

    # Add legend
    legend_html = '''
    <div style="position: fixed;
                bottom: 50px; left: 50px; width: 200px;
                border:2px solid grey; z-index:9999; font-size:14px;
                background-color:white; padding: 10px;
                border-radius: 5px;">
        <b>Distance to Activity Centre</b><br>
        <i style="background:#1a9850; width:18px; height:12px; display:inline-block;"></i> 0-400m (Excellent)<br>
        <i style="background:#91cf60; width:18px; height:12px; display:inline-block;"></i> 400-800m (Good)<br>
        <i style="background:#fee08b; width:18px; height:12px; display:inline-block;"></i> 800-1200m (Moderate)<br>
        <i style="background:#fc8d59; width:18px; height:12px; display:inline-block;"></i> 1200-1600m (Fair)<br>
        <i style="background:#d73027; width:18px; height:12px; display:inline-block;"></i> >1600m (Poor)
    </div>
    '''
    m.get_root().html.add_child(folium.Element(legend_html))

    displayHTML(m._repr_html_())
else:
    print("No parcels found for visualization")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Export Full Parcels by AC Distance Map to UC Volume

# COMMAND ----------

# Export ALL parcels with activity centre proximity to Folium HTML
import folium
import json

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

# Get ALL parcels with proximity data (no limit)
all_parcels_ac = spark.sql(f"""
    SELECT
        e.parcel_id,
        e.zone_code,
        e.centroid_lon,
        e.centroid_lat,
        ST_AsGeoJSON(ST_Transform(e.geometry, 4326)) AS geojson,
        ac.nearest_any_ac_m,
        ac.nearest_mac_name,
        ac.within_800m_ac,
        ac.within_800m_mac
    FROM {catalog_name}.{schema_name}.parcel_edge_topology e
    JOIN {catalog_name}.{schema_name}.parcel_activity_centre_proximity ac
        ON e.parcel_id = ac.parcel_id
    WHERE e.geometry IS NOT NULL
      AND e.centroid_lon IS NOT NULL
""").toPandas()

print(f"Loaded {len(all_parcels_ac)} parcels for full AC proximity export")

if len(all_parcels_ac) > 0:
    center_lat = all_parcels_ac['centroid_lat'].mean()
    center_lon = all_parcels_ac['centroid_lon'].mean()

    m_ac_parcels_full = folium.Map(
        location=[center_lat, center_lon],
        zoom_start=13,
        tiles='CartoDB positron'
    )

    # Count by distance band
    excellent_count = (all_parcels_ac['nearest_any_ac_m'] <= 400).sum()
    good_count = ((all_parcels_ac['nearest_any_ac_m'] > 400) & (all_parcels_ac['nearest_any_ac_m'] <= 800)).sum()
    moderate_count = ((all_parcels_ac['nearest_any_ac_m'] > 800) & (all_parcels_ac['nearest_any_ac_m'] <= 1200)).sum()
    fair_count = ((all_parcels_ac['nearest_any_ac_m'] > 1200) & (all_parcels_ac['nearest_any_ac_m'] <= 1600)).sum()
    poor_count = (all_parcels_ac['nearest_any_ac_m'] > 1600).sum()

    def get_distance_color(distance):
        if distance <= 400:
            return '#1a9850'
        elif distance <= 800:
            return '#91cf60'
        elif distance <= 1200:
            return '#fee08b'
        elif distance <= 1600:
            return '#fc8d59'
        else:
            return '#d73027'

    for _, row in all_parcels_ac.iterrows():
        try:
            geojson = json.loads(row['geojson'])
            color = get_distance_color(row['nearest_any_ac_m'])

            folium.GeoJson(
                geojson,
                style_function=lambda x, c=color: {'fillColor': c, 'color': c, 'weight': 0.5, 'fillOpacity': 0.6},
                tooltip=f"Parcel: {row['parcel_id']}<br>Zone: {row['zone_code']}<br>Distance: {row['nearest_any_ac_m']:.0f}m"
            ).add_to(m_ac_parcels_full)
        except:
            pass

    legend_html = f'''
    <div style="position: fixed; bottom: 50px; left: 50px; width: 220px;
                border:2px solid grey; z-index:9999; font-size:14px;
                background-color:white; padding: 10px; border-radius: 5px;">
        <b>Distance to Activity Centre (Full)</b><br>
        <i style="background:#1a9850; width:18px; height:12px; display:inline-block;"></i> 0-400m ({excellent_count:,})<br>
        <i style="background:#91cf60; width:18px; height:12px; display:inline-block;"></i> 400-800m ({good_count:,})<br>
        <i style="background:#fee08b; width:18px; height:12px; display:inline-block;"></i> 800-1200m ({moderate_count:,})<br>
        <i style="background:#fc8d59; width:18px; height:12px; display:inline-block;"></i> 1200-1600m ({fair_count:,})<br>
        <i style="background:#d73027; width:18px; height:12px; display:inline-block;"></i> >1600m ({poor_count:,})<br>
        <small>Total: {len(all_parcels_ac):,}</small>
    </div>
    '''
    m_ac_parcels_full.get_root().html.add_child(folium.Element(legend_html))

    # Save to UC Volume
    volume_path = f"/Volumes/{catalog_name}/{schema_name}/source"
    ac_parcels_html_path = f"{volume_path}/visualizations/folium_parcels_ac_distance_full.html"

    m_ac_parcels_full.save(ac_parcels_html_path)
    print(f"Full parcels AC distance map saved to: {ac_parcels_html_path}")
    print(f"Breakdown: Excellent={excellent_count}, Good={good_count}, Moderate={moderate_count}, Fair={fair_count}, Poor={poor_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC This notebook computed activity centre proximity features:
# MAGIC
# MAGIC | Feature | Description |
# MAGIC |---------|-------------|
# MAGIC | `nearest_mac_m` | Distance to nearest Metropolitan Activity Centre |
# MAGIC | `nearest_major_m` | Distance to nearest Major Activity Centre |
# MAGIC | `nearest_neighbourhood_m` | Distance to nearest Neighbourhood Activity Centre |
# MAGIC | `nearest_any_ac_m` | Distance to nearest activity centre (any tier) |
# MAGIC | `within_800m_ac` | Within walkable distance of any centre |
# MAGIC | `within_800m_mac` | Within walkable distance of Metropolitan centre |
# MAGIC
# MAGIC ### Approach
# MAGIC - Used Plan Melbourne Activity Centres dataset (official source)
# MAGIC - Classified by tier (Metropolitan, Major, Neighbourhood) from PM2017_Cat field
# MAGIC - Calculated straight-line (spherical) distance using ST_DistanceSphere
# MAGIC
# MAGIC ### Output Table
# MAGIC - `activity_centres` - Reference table of activity centre locations
# MAGIC - `parcel_activity_centre_proximity` - Proximity features per parcel
# MAGIC
# MAGIC ### Next Steps
# MAGIC - **04_suitability_scoring.py** - Include activity centre proximity in scoring
