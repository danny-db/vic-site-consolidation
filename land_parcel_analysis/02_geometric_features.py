# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 1: Parcel Geometric Features
# MAGIC
# MAGIC This notebook computes **geometric features** for each land parcel using Databricks Spatial SQL.
# MAGIC
# MAGIC ## Features Computed:
# MAGIC | Feature | Description | Why It Matters |
# MAGIC |---------|-------------|----------------|
# MAGIC | **Lot Area** | Total area of the parcel | Parcels below minimum need consolidation |
# MAGIC | **Perimeter** | Boundary length | Relates to setback exposure |
# MAGIC | **Compactness Index** | How close to a circle (4π × area ÷ perimeter²) | Irregular shapes waste buildable area |
# MAGIC | **Aspect Ratio** | Width ÷ depth | Narrow or deep lots are less efficient |
# MAGIC | **Elongation Index** | Ratio of length to width | Highly elongated lots are harder to develop |
# MAGIC | **Bounding Box** | Min/max coordinates | Used for width/depth calculations |
# MAGIC | **Centroid** | Center point | Used for proximity calculations |
# MAGIC
# MAGIC ## CRS Strategy:
# MAGIC - **Storage**: VicGrid GDA94 (EPSG:3111) - Victorian government standard, meters
# MAGIC - **Area/Distance Calculations**: Direct from EPSG:3111 (no transformation needed)
# MAGIC - **Visualization**: Transform to EPSG:4326 (WGS84) at query time only

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

# MAGIC %pip install folium keplergl pydeck mapclassify --quiet

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# Create widgets
dbutils.widgets.text("catalog_name", "danny_catalog", "Catalog Name")
dbutils.widgets.text("schema_name", "dtp_hackathon", "Schema Name")
dbutils.widgets.text("ai_model", "databricks-claude-opus-4-6", "AI Model Endpoint")

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

print(f"Using: {catalog_name}.{schema_name}")

# COMMAND ----------

# Utility function for handling NaN/None in visualization data
import math

def safe_float(val, default=0):
    """Convert to float, handling NaN and None"""
    if val is None:
        return default
    try:
        f = float(val)
        return default if math.isnan(f) else f
    except:
        return default

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Explore Real Data Schema & CRS

# COMMAND ----------

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

# Show parcels schema
print("Parcels table schema:")
spark.sql(f"DESCRIBE {catalog_name}.{schema_name}.parcels").display()

# COMMAND ----------

# Check sample geometry to understand the CRS
catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

display(spark.sql(f"""
    SELECT
        ST_AsWKT(geometry) AS geometry_wkt,
        ST_X(ST_Centroid(geometry)) AS centroid_x,
        ST_Y(ST_Centroid(geometry)) AS centroid_y
    FROM {catalog_name}.{schema_name}.parcels
    LIMIT 5
"""))

# COMMAND ----------

# Show zones schema
catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

print("Zones table schema:")
spark.sql(f"DESCRIBE {catalog_name}.{schema_name}.zones").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Join Parcels with Zones (Spatial Join)
# MAGIC
# MAGIC Since parcels don't have zone info directly, we perform a spatial join with the zones table.
# MAGIC We also transform the geometry to EPSG:4326 for visualization compatibility.

# COMMAND ----------

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

# ============================================================================
# SPATIAL JOIN: Link Parcels to Planning Zones
# ============================================================================
# A "spatial join" connects two datasets based on their geographic relationship.
# Here we're finding which planning zone (GRZ, NRZ, etc.) each parcel sits in.
#
# How it works:
# - ST_Centroid(p.geometry): Gets the center point of each parcel
# - ST_Contains(z.geometry, ...): Checks if that center point falls inside a zone
# - This assigns each parcel to the zone that contains its center
#
# IMPORTANT: Geometry is kept in VicGrid (EPSG:3111) - the Victorian standard
# coordinate system that uses meters. This ensures accurate area calculations.
# ============================================================================
spark.sql(f"""
    CREATE OR REPLACE TABLE {catalog_name}.{schema_name}.parcels_with_zones AS
    WITH ranked AS (
        SELECT
            p.PARCEL_PFI AS parcel_id,
            p.PC_PLANNO AS plan_number,
            p.PC_LOTNO AS lot_number,
            p.PC_LGAC AS lga_code,
            p.PARCEL_SPI AS spi,
            -- Keep geometry in native CRS (EPSG:3111 VicGrid)
            p.geometry AS geometry,
            z.ZONE_CODE AS zone_code,
            z.ZONE_DESCRIPTION AS zone_description,
            z.LGA AS lga_name,
            -- Deduplicate: when zone polygons overlap, keep the most specific
            -- (smallest area) to avoid row multiplication
            ROW_NUMBER() OVER (
                PARTITION BY p.PARCEL_PFI
                ORDER BY ST_Area(z.geometry) ASC NULLS LAST
            ) AS rn
        FROM {catalog_name}.{schema_name}.parcels p
        LEFT JOIN {catalog_name}.{schema_name}.zones z
            ON ST_Contains(z.geometry, ST_Centroid(p.geometry))
    )
    SELECT
        parcel_id, plan_number, lot_number, lga_code, spi,
        geometry, zone_code, zone_description, lga_name
    FROM ranked
    WHERE rn = 1
""")

print("Created parcels_with_zones table (geometry in EPSG:3111 VicGrid)")
display(spark.sql(f"SELECT COUNT(*) AS total_parcels FROM {catalog_name}.{schema_name}.parcels_with_zones"))

# COMMAND ----------

# Verify the geometry - show both native CRS and transformed coordinates
catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

display(spark.sql(f"""
    SELECT
        parcel_id,
        zone_code,
        -- Native CRS coordinates (EPSG:3111 VicGrid - should be large numbers like 2.5M, 5.8M)
        ROUND(ST_X(ST_Centroid(geometry)), 2) AS centroid_easting_3111,
        ROUND(ST_Y(ST_Centroid(geometry)), 2) AS centroid_northing_3111,
        -- Transformed to WGS84 (should be ~144-146, ~-37 to -38)
        ROUND(ST_X(ST_Centroid(ST_Transform(geometry, 4326))), 6) AS centroid_lon_4326,
        ROUND(ST_Y(ST_Centroid(ST_Transform(geometry, 4326))), 6) AS centroid_lat_4326,
        -- SRID verification
        ST_SRID(geometry) AS geometry_srid
    FROM {catalog_name}.{schema_name}.parcels_with_zones
    WHERE geometry IS NOT NULL
    LIMIT 10
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Basic Geometric Measurements
# MAGIC
# MAGIC Calculate area, perimeter, and bounding box for each parcel.
# MAGIC - **Geometry Storage**: EPSG:3111 (VicGrid GDA94) - native Victorian government CRS
# MAGIC - **Area/Perimeter**: Direct from EPSG:3111 (already in meters)
# MAGIC - **Visualization**: Transform to EPSG:4326 (WGS84) only at query time

# COMMAND ----------

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

# Basic geometric measurements using Spatial SQL
# Geometry is in EPSG:3111 (VicGrid) - native meters for area/perimeter
display(spark.sql(f"""
    SELECT
        parcel_id,
        plan_number,
        zone_code,
        -- Area in square meters (geometry is already in projected CRS 3111)
        ROUND(ST_Area(geometry), 2) AS area_sqm,
        -- Perimeter in meters
        ROUND(ST_Perimeter(geometry), 2) AS perimeter_m,
        -- Bounding box in VicGrid (meters)
        ROUND(ST_XMin(geometry), 2) AS bbox_min_easting,
        ROUND(ST_YMin(geometry), 2) AS bbox_min_northing,
        ROUND(ST_XMax(geometry), 2) AS bbox_max_easting,
        ROUND(ST_YMax(geometry), 2) AS bbox_max_northing,
        -- Centroid in WGS84 for visualization
        ROUND(ST_X(ST_Centroid(ST_Transform(geometry, 4326))), 6) AS centroid_lon,
        ROUND(ST_Y(ST_Centroid(ST_Transform(geometry, 4326))), 6) AS centroid_lat
    FROM {catalog_name}.{schema_name}.parcels_with_zones
    WHERE geometry IS NOT NULL
    LIMIT 100
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Compute Shape Indices
# MAGIC
# MAGIC Calculate compactness, aspect ratio, and elongation indices for all parcels.

# COMMAND ----------

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

# ============================================================================
# GEOMETRIC FEATURES: Calculate Shape Metrics for Each Parcel
# ============================================================================
# This query calculates several geometric properties that help identify lots
# that may benefit from consolidation (merging with neighbors).
#
# KEY CONCEPTS FOR PLANNERS:
#
# 1. AREA & PERIMETER: Basic measurements in square meters and meters
#    - Victorian planning schemes often have minimum lot sizes (300-500sqm)
#    - Lots below these thresholds may need consolidation to develop
#
# 2. BOUNDING BOX: The smallest rectangle that fits around the parcel
#    - Width and depth help identify narrow or deep lots
#    - Narrow lots (<15m) often can't meet setback requirements
#
# 3. COMPACTNESS INDEX (Polsby-Popper formula):
#    - Measures how close to a circle a shape is (1.0 = perfect circle)
#    - Lower values = more irregular shapes that waste buildable area
#    - Formula: 4π × area ÷ perimeter²
#
# 4. ASPECT RATIO: Width divided by depth
#    - 1.0 = roughly square lot
#    - <1.0 = deeper than wide (battleaxe-style)
#    - >1.0 = wider than deep
#
# 5. ELONGATION INDEX: Longest side ÷ shortest side
#    - 1.0 = square, higher = more stretched out
#    - Highly elongated lots (>3) are harder to develop
#
# 6. COORDINATE TRANSFORMATION:
#    - ST_Transform converts between coordinate systems
#    - VicGrid (EPSG:3111) uses meters - good for area calculations
#    - WGS84 (EPSG:4326) uses lat/lon - needed for web maps
# ============================================================================
spark.sql(f"""
    CREATE OR REPLACE TABLE {catalog_name}.{schema_name}.parcel_geometric_features AS
    -- Step 1: Compute raw metrics including convex hull properties
    WITH hull_metrics AS (
        SELECT
            parcel_id,
            plan_number,
            lot_number,
            zone_code,
            zone_description,
            lga_code,
            lga_name,
            geometry,
            -- Area and perimeter in meters (geometry is already in EPSG:3111)
            ST_Area(geometry) AS area_sqm,
            ST_Perimeter(geometry) AS perimeter_m,
            -- Convex hull properties for rotation-invariant dimension estimates
            ST_Perimeter(ST_ConvexHull(geometry)) / 4.0 AS hull_quarter_perim,
            ST_Area(ST_ConvexHull(geometry)) AS hull_area,
            -- Centroid in WGS84 for visualization (transform from 3111)
            ST_X(ST_Centroid(ST_Transform(geometry, 4326))) AS centroid_lon,
            ST_Y(ST_Centroid(ST_Transform(geometry, 4326))) AS centroid_lat
        FROM {catalog_name}.{schema_name}.parcels_with_zones
        WHERE geometry IS NOT NULL
    ),
    -- Step 2: Derive rotation-invariant width/depth from convex hull
    -- Models the hull as a rectangle: Area = w*h, Perimeter = 2(w+h)
    -- Solving the quadratic: dimensions = P/4 +/- sqrt((P/4)^2 - A)
    -- Unlike axis-aligned bounding box, this handles rotated parcels correctly
    --
    -- CAVEAT: This estimate is only reliable for roughly convex parcels.
    -- For concave lots (L-shaped, T-shaped, etc.) the convex hull is much larger
    -- than the actual parcel, making width/depth estimates meaningless.
    -- We use hull_efficiency (actual area / hull area) to flag unreliable estimates.
    measurements AS (
        SELECT
            *,
            -- Hull efficiency: 1.0 = convex, lower = more concave
            CASE
                WHEN hull_area > 0 THEN area_sqm / hull_area
                ELSE NULL
            END AS hull_efficiency,
            -- Only estimate width/depth when hull efficiency >= 0.6
            -- Below that threshold, the rectangular model breaks down
            CASE
                WHEN hull_quarter_perim * hull_quarter_perim >= hull_area
                     AND hull_area > 0
                     AND (area_sqm / hull_area) >= 0.6
                THEN hull_quarter_perim - SQRT(hull_quarter_perim * hull_quarter_perim - hull_area)
                ELSE NULL
            END AS est_width_m,
            CASE
                WHEN hull_quarter_perim * hull_quarter_perim >= hull_area
                     AND hull_area > 0
                     AND (area_sqm / hull_area) >= 0.6
                THEN hull_quarter_perim + SQRT(hull_quarter_perim * hull_quarter_perim - hull_area)
                ELSE NULL
            END AS est_depth_m
        FROM hull_metrics
    )
    SELECT
        parcel_id,
        plan_number,
        lot_number,
        zone_code,
        zone_description,
        lga_code,
        lga_name,
        geometry,

        -- Basic measurements
        ROUND(area_sqm, 2) AS area_sqm,
        ROUND(perimeter_m, 2) AS perimeter_m,
        ROUND(est_width_m, 2) AS est_width_m,
        ROUND(est_depth_m, 2) AS est_depth_m,

        -- Hull efficiency: actual area / convex hull area
        -- 1.0 = convex shape, lower = concave (L-shaped, T-shaped, etc.)
        -- When < 0.6, width/depth estimates above are NULLed out as unreliable
        ROUND(hull_efficiency, 4) AS hull_efficiency,

        -- Centroid (WGS84)
        ROUND(centroid_lon, 6) AS centroid_lon,
        ROUND(centroid_lat, 6) AS centroid_lat,

        -- Compactness Index (Polsby-Popper): 4pi x area / perimeter^2
        -- Value of 1 = perfect circle, lower = more irregular
        ROUND(
            CASE
                WHEN perimeter_m > 0 THEN (4 * 3.14159265359 * area_sqm) / (perimeter_m * perimeter_m)
                ELSE NULL
            END,
            4
        ) AS compactness_index,

        -- Aspect Ratio: width / depth
        -- Value of 1 = square, <1 = deep lot, >1 = wide lot
        ROUND(
            CASE
                WHEN est_depth_m > 0 THEN est_width_m / est_depth_m
                ELSE NULL
            END,
            4
        ) AS aspect_ratio,

        -- Elongation Index: max(width,depth) / min(width,depth)
        -- Value of 1 = square, higher = more elongated
        ROUND(
            CASE
                WHEN LEAST(est_width_m, est_depth_m) > 0
                THEN GREATEST(est_width_m, est_depth_m) / LEAST(est_width_m, est_depth_m)
                ELSE NULL
            END,
            4
        ) AS elongation_index,

        -- Sliver detection: parcels below 5 sqm are data artifacts, not real lots
        CASE WHEN area_sqm < 5 THEN TRUE ELSE FALSE END AS is_sliver,

        -- Minimum site area flags (typical Victorian thresholds)
        -- Excludes slivers (< 5 sqm) which are data quality issues, not consolidation candidates
        CASE WHEN area_sqm >= 5 AND area_sqm < 300 THEN TRUE ELSE FALSE END AS below_min_area_300,
        CASE WHEN area_sqm >= 5 AND area_sqm < 500 THEN TRUE ELSE FALSE END AS below_min_area_500,

        -- Lot width flags (typical frontage requirements)
        CASE WHEN LEAST(est_width_m, est_depth_m) < 10 THEN TRUE ELSE FALSE END AS narrow_lot,
        CASE WHEN LEAST(est_width_m, est_depth_m) < 15 THEN TRUE ELSE FALSE END AS below_frontage_15m

    FROM measurements
""")

print(f"Created parcel_geometric_features table")
display(spark.sql(f"SELECT COUNT(*) AS total_parcels FROM {catalog_name}.{schema_name}.parcel_geometric_features"))

# COMMAND ----------

# Display the geometric features
catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

display(spark.sql(f"""
    SELECT
        parcel_id,
        zone_code,
        lga_name,
        area_sqm,
        perimeter_m,
        est_width_m,
        est_depth_m,
        compactness_index,
        aspect_ratio,
        elongation_index,
        below_min_area_500,
        narrow_lot,
        centroid_lon,
        centroid_lat
    FROM {catalog_name}.{schema_name}.parcel_geometric_features
    ORDER BY area_sqm
    LIMIT 100
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Visualize Parcels with Folium
# MAGIC
# MAGIC Create an interactive map showing parcels colored by area size.

# COMMAND ----------

import folium
from folium.plugins import MarkerCluster
import json

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

# Get sample parcels for visualization (limit for performance)
# IMPORTANT: Transform geometry to WGS84 (EPSG:4326) for web mapping

# DIAGNOSTIC: Check geometry SRID and coordinate ranges
print("=" * 80)
print("DIAGNOSTIC STEP 1: Check table exists and has data")
print("=" * 80)

# Step 1: Check if table exists and has data
table_check = spark.sql(f"""
    SELECT
        COUNT(*) AS total_rows,
        COUNT(geometry) AS non_null_geom,
        COUNT(centroid_lon) AS non_null_centroid_lon,
        COUNT(centroid_lat) AS non_null_centroid_lat
    FROM {catalog_name}.{schema_name}.parcel_geometric_features
""")
display(table_check)

print("\n" + "=" * 80)
print("DIAGNOSTIC STEP 2: Check geometry SRID")
print("=" * 80)
srid_check = spark.sql(f"""
    SELECT
        MIN(ST_SRID(geometry)) AS min_srid,
        MAX(ST_SRID(geometry)) AS max_srid,
        COUNT(CASE WHEN ST_SRID(geometry) = 3111 THEN 1 END) AS srid_3111_count,
        COUNT(CASE WHEN ST_SRID(geometry) = 4326 THEN 1 END) AS srid_4326_count,
        COUNT(CASE WHEN ST_SRID(geometry) = 0 OR ST_SRID(geometry) IS NULL THEN 1 END) AS srid_unknown_count
    FROM {catalog_name}.{schema_name}.parcel_geometric_features
    WHERE geometry IS NOT NULL
""")
display(srid_check)

print("\n" + "=" * 80)
print("DIAGNOSTIC STEP 3: Check coordinate ranges (native vs pre-computed)")
print("=" * 80)
coord_check = spark.sql(f"""
    SELECT
        -- Native coordinates from geometry
        ROUND(MIN(ST_X(ST_Centroid(geometry))), 2) AS min_native_x,
        ROUND(MAX(ST_X(ST_Centroid(geometry))), 2) AS max_native_x,
        ROUND(MIN(ST_Y(ST_Centroid(geometry))), 2) AS min_native_y,
        ROUND(MAX(ST_Y(ST_Centroid(geometry))), 2) AS max_native_y,
        -- Pre-computed centroids (should be WGS84)
        ROUND(MIN(centroid_lon), 4) AS min_precomputed_lon,
        ROUND(MAX(centroid_lon), 4) AS max_precomputed_lon,
        ROUND(MIN(centroid_lat), 4) AS min_precomputed_lat,
        ROUND(MAX(centroid_lat), 4) AS max_precomputed_lat
    FROM {catalog_name}.{schema_name}.parcel_geometric_features
    WHERE geometry IS NOT NULL
""")
display(coord_check)

print("\nExpected values:")
print("- If SRID 3111 (VicGrid): native X ~2.5M, Y ~5.8M")
print("- If SRID 4326 (WGS84): native X ~144-146, Y ~-37 to -38")
print("- Pre-computed centroids should be WGS84: Lon ~144-146, Lat ~-37 to -38")

print("\n" + "=" * 80)
print("DIAGNOSTIC STEP 4: Sample raw data")
print("=" * 80)
sample_raw = spark.sql(f"""
    SELECT
        parcel_id,
        zone_code,
        area_sqm,
        centroid_lon,
        centroid_lat,
        ST_SRID(geometry) AS geom_srid,
        SUBSTRING(ST_AsGeoJSON(geometry), 1, 100) AS native_geojson_preview
    FROM {catalog_name}.{schema_name}.parcel_geometric_features
    WHERE geometry IS NOT NULL
    LIMIT 5
""")
display(sample_raw)

print("\n" + "=" * 80)
print("DIAGNOSTIC STEP 5: Test ST_Transform to 4326")
print("=" * 80)
transform_test = spark.sql(f"""
    SELECT
        parcel_id,
        centroid_lon AS precomputed_lon,
        centroid_lat AS precomputed_lat,
        ROUND(ST_X(ST_Centroid(ST_Transform(geometry, 4326))), 6) AS transformed_lon,
        ROUND(ST_Y(ST_Centroid(ST_Transform(geometry, 4326))), 6) AS transformed_lat,
        SUBSTRING(ST_AsGeoJSON(ST_Transform(geometry, 4326)), 1, 150) AS transformed_geojson_preview
    FROM {catalog_name}.{schema_name}.parcel_geometric_features
    WHERE geometry IS NOT NULL
      AND centroid_lon IS NOT NULL
    LIMIT 5
""")
display(transform_test)

print("\n" + "=" * 80)
print("LOADING DATA FOR VISUALIZATION")
print("=" * 80)

# Use pre-computed centroid_lon, centroid_lat columns (already in WGS84)
# This matches the pattern used in notebooks 03 and 04
# IMPORTANT: Filter to a specific LGA to keep parcels clustered for visualization
# First, find an LGA with data
lga_with_data = spark.sql(f"""
    SELECT lga_name, COUNT(*) as cnt
    FROM {catalog_name}.{schema_name}.parcel_geometric_features
    WHERE geometry IS NOT NULL AND lga_name IS NOT NULL
    GROUP BY lga_name
    ORDER BY cnt DESC
    LIMIT 1
""").collect()

# Use Casey LGA - Metro Melbourne area with good Activity Centre coverage
target_lga = "CASEY"

# Verify the LGA exists
if lga_with_data:
    available_lgas = [row['lga_name'] for row in lga_with_data]
    if target_lga not in available_lgas:
        print(f"WARNING: {target_lga} not in top LGAs, using anyway")
    print(f"Filtering to LGA: {target_lga}")
else:
    print(f"Using target LGA: {target_lga}")

sample_parcels = spark.sql(f"""
    SELECT
        parcel_id,
        zone_code,
        area_sqm,
        compactness_index,
        centroid_lon,
        centroid_lat,
        ST_AsGeoJSON(ST_Transform(geometry, 4326)) AS geojson
    FROM {catalog_name}.{schema_name}.parcel_geometric_features
    WHERE geometry IS NOT NULL
      AND centroid_lon IS NOT NULL
      {f"AND lga_name = '{target_lga}'" if target_lga else ""}
    ORDER BY area_sqm
    LIMIT 100
""").toPandas()

print(f"Loaded {len(sample_parcels)} parcels for visualization")

# Debug: Show coordinate ranges to verify transformation worked
if len(sample_parcels) > 0:
    print(f"\nCentroid ranges:")
    print(f"  Lon: [{sample_parcels['centroid_lon'].min():.4f}, {sample_parcels['centroid_lon'].max():.4f}]")
    print(f"  Lat: [{sample_parcels['centroid_lat'].min():.4f}, {sample_parcels['centroid_lat'].max():.4f}]")
    print(f"\nExpected Melbourne coords: Lon ~144-146, Lat ~-37 to -38")

    # Check GeoJSON validity
    print(f"\nGeoJSON validation:")
    import json
    valid_geojson_count = 0
    invalid_geojson_count = 0
    for idx, geojson_str in enumerate(sample_parcels['geojson']):
        try:
            geojson = json.loads(geojson_str)
            if 'coordinates' in geojson:
                valid_geojson_count += 1
        except:
            invalid_geojson_count += 1
    print(f"  Valid GeoJSON: {valid_geojson_count}")
    print(f"  Invalid GeoJSON: {invalid_geojson_count}")

    # Show first geojson sample
    print(f"\nFirst GeoJSON (truncated):")
    print(f"  {sample_parcels['geojson'].iloc[0][:300]}...")

    # Parse and show first coordinate
    try:
        first_geom = json.loads(sample_parcels['geojson'].iloc[0])
        if first_geom['type'] == 'Polygon':
            first_coord = first_geom['coordinates'][0][0]
            print(f"\nFirst coordinate of first polygon: {first_coord}")
            print(f"  If these are ~144, -37 -> WGS84 (correct)")
            print(f"  If these are ~2.5M, 5.8M -> still in EPSG:3111 (transform failed)")
        elif first_geom['type'] == 'MultiPolygon':
            first_coord = first_geom['coordinates'][0][0][0]
            print(f"\nFirst coordinate of first multipolygon: {first_coord}")
    except Exception as e:
        print(f"Error parsing first geojson: {e}")
else:
    print("\n*** WARNING: No parcels returned! ***")
    print("Check if parcel_geometric_features table exists and has valid data.")
    print("Re-run notebook cell that creates parcel_geometric_features table.")

# COMMAND ----------

# Create folium map centered on the data
if len(sample_parcels) > 0:
    center_lat = sample_parcels['centroid_lat'].mean()
    center_lon = sample_parcels['centroid_lon'].mean()

    # Create map
    m = folium.Map(
        location=[center_lat, center_lon],
        zoom_start=15,
        tiles='CartoDB positron'
    )

    # Color function based on area
    def get_color(area):
        if area < 300:
            return '#d73027'  # Red - very small
        elif area < 500:
            return '#fc8d59'  # Orange - small
        elif area < 800:
            return '#fee090'  # Yellow - medium
        else:
            return '#91cf60'  # Green - adequate

    # Add parcels as GeoJSON
    for _, row in sample_parcels.iterrows():
        try:
            geojson = json.loads(row['geojson'])
            color = get_color(row['area_sqm'])

            folium.GeoJson(
                geojson,
                style_function=lambda x, c=color: {
                    'fillColor': c,
                    'color': '#333',
                    'weight': 1,
                    'fillOpacity': 0.6
                },
                tooltip=f"ID: {row['parcel_id']}<br>Zone: {row['zone_code']}<br>Area: {row['area_sqm']:.0f} sqm"
            ).add_to(m)
        except:
            pass

    # Add legend
    legend_html = '''
    <div style="position: fixed; bottom: 50px; left: 50px; z-index: 1000;
                background-color: white; padding: 10px; border-radius: 5px;
                border: 2px solid gray;">
        <p><strong>Parcel Area (sqm)</strong></p>
        <p><span style="background-color: #d73027; padding: 2px 10px;">&nbsp;</span> &lt; 300 (Critical)</p>
        <p><span style="background-color: #fc8d59; padding: 2px 10px;">&nbsp;</span> 300-500 (Small)</p>
        <p><span style="background-color: #fee090; padding: 2px 10px;">&nbsp;</span> 500-800 (Medium)</p>
        <p><span style="background-color: #91cf60; padding: 2px 10px;">&nbsp;</span> &gt; 800 (Adequate)</p>
    </div>
    '''
    m.get_root().html.add_child(folium.Element(legend_html))

    # Use displayHTML for proper rendering in Databricks
    displayHTML(m._repr_html_())
else:
    print("No parcels found for visualization")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Export Full LGA Parcel Map to UC Volume

# COMMAND ----------

# Export ALL parcels in the target LGA to a Folium HTML file (for large datasets)
import folium
import json

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

# Get ALL parcels for the target LGA (no limit)
all_parcels_lga = spark.sql(f"""
    SELECT
        parcel_id,
        zone_code,
        area_sqm,
        compactness_index,
        centroid_lon,
        centroid_lat,
        ST_AsGeoJSON(ST_Transform(geometry, 4326)) AS geojson
    FROM {catalog_name}.{schema_name}.parcel_geometric_features
    WHERE geometry IS NOT NULL
      AND centroid_lon IS NOT NULL
      AND lga_name = '{target_lga}'
""").toPandas()

print(f"Loaded {len(all_parcels_lga)} parcels for full LGA export")

if len(all_parcels_lga) > 0:
    center_lat = all_parcels_lga['centroid_lat'].mean()
    center_lon = all_parcels_lga['centroid_lon'].mean()

    m_full = folium.Map(
        location=[center_lat, center_lon],
        zoom_start=13,
        tiles='CartoDB positron'
    )

    # Color function based on area
    def get_color(area):
        if area < 300:
            return '#d73027'
        elif area < 500:
            return '#fc8d59'
        elif area < 800:
            return '#fee090'
        else:
            return '#91cf60'

    # Add all parcels
    for _, row in all_parcels_lga.iterrows():
        try:
            geojson = json.loads(row['geojson'])
            color = get_color(row['area_sqm'])
            folium.GeoJson(
                geojson,
                style_function=lambda x, c=color: {
                    'fillColor': c,
                    'color': '#333',
                    'weight': 0.5,
                    'fillOpacity': 0.6
                },
                tooltip=f"ID: {row['parcel_id']}<br>Zone: {row['zone_code']}<br>Area: {row['area_sqm']:.0f} sqm"
            ).add_to(m_full)
        except:
            pass

    # Add legend
    legend_html = f'''
    <div style="position: fixed; bottom: 50px; left: 50px; z-index: 1000;
                background-color: white; padding: 10px; border-radius: 5px;
                border: 2px solid gray;">
        <p><strong>{target_lga} - Parcel Area</strong></p>
        <p><span style="background-color: #d73027; padding: 2px 10px;">&nbsp;</span> &lt; 300 sqm</p>
        <p><span style="background-color: #fc8d59; padding: 2px 10px;">&nbsp;</span> 300-500 sqm</p>
        <p><span style="background-color: #fee090; padding: 2px 10px;">&nbsp;</span> 500-800 sqm</p>
        <p><span style="background-color: #91cf60; padding: 2px 10px;">&nbsp;</span> &gt; 800 sqm</p>
        <p><small>Total: {len(all_parcels_lga):,} parcels</small></p>
    </div>
    '''
    m_full.get_root().html.add_child(folium.Element(legend_html))

    # Save to UC Volume
    volume_path = f"/Volumes/{catalog_name}/{schema_name}/source"
    folium_html_path = f"{volume_path}/visualizations/folium_parcels_by_area_{target_lga.replace(' ', '_')}.html"

    import os
    os.makedirs(f"{volume_path}/visualizations", exist_ok=True)

    m_full.save(folium_html_path)
    print(f"Full LGA Folium map saved to: {folium_html_path}")
    print(f"Total parcels exported: {len(all_parcels_lga):,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Visualize with Kepler.gl
# MAGIC
# MAGIC Kepler.gl provides powerful visualization for large geospatial datasets.

# COMMAND ----------

from keplergl import KeplerGl

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

# Get parcels for Kepler - transform to WGS84 for visualization
# Since Kepler exports to HTML file, we can visualize more data (no 10MB limit)
# Filter to a specific LGA to keep parcels clustered
# Use Casey LGA - Metro Melbourne area with good Activity Centre coverage
target_lga = "CASEY"
print(f"Kepler: Filtering to LGA: {target_lga}")

kepler_data = spark.sql(f"""
    SELECT
        parcel_id,
        zone_code,
        lga_name,
        area_sqm,
        perimeter_m,
        compactness_index,
        aspect_ratio,
        elongation_index,
        below_min_area_500,
        narrow_lot,
        centroid_lon,
        centroid_lat,
        ST_AsGeoJSON(ST_Transform(geometry, 4326)) AS geojson
    FROM {catalog_name}.{schema_name}.parcel_geometric_features
    WHERE geometry IS NOT NULL
      AND centroid_lon IS NOT NULL
      AND lga_name = '{target_lga}'
""").toPandas()

print(f"Loaded {len(kepler_data)} parcels for Kepler visualization")

# COMMAND ----------

# Create GeoJSON FeatureCollection for Kepler
import json
import math

def safe_float(val, default=0):
    """Convert to float, handling NaN and None"""
    if val is None:
        return default
    try:
        f = float(val)
        return default if math.isnan(f) else f
    except:
        return default

features = []
for _, row in kepler_data.iterrows():
    try:
        geom = json.loads(row['geojson'])
        feature = {
            "type": "Feature",
            "geometry": geom,
            "properties": {
                "parcel_id": str(row['parcel_id']) if row['parcel_id'] else "",
                "zone_code": str(row['zone_code']) if row['zone_code'] else "",
                "lga_name": str(row['lga_name']) if row['lga_name'] else "",
                "area_sqm": safe_float(row['area_sqm']),
                "compactness_index": safe_float(row['compactness_index']),
                "below_min_area_500": bool(row['below_min_area_500']) if row['below_min_area_500'] is not None else False,
                "narrow_lot": bool(row['narrow_lot']) if row['narrow_lot'] is not None else False
            }
        }
        features.append(feature)
    except Exception as e:
        pass  # Skip malformed geometries

geojson_fc = {
    "type": "FeatureCollection",
    "features": features
}

print(f"Created GeoJSON with {len(features)} features")

# COMMAND ----------

# Kepler.gl configuration for parcel visualization
kepler_config = {
    "version": "v1",
    "config": {
        "mapState": {
            "latitude": kepler_data['centroid_lat'].mean() if len(kepler_data) > 0 else -37.8,
            "longitude": kepler_data['centroid_lon'].mean() if len(kepler_data) > 0 else 145.0,
            "zoom": 14
        },
        "visState": {
            "layers": [{
                "type": "geojson",
                "config": {
                    "dataId": "parcels",
                    "label": "Land Parcels",
                    "color": [255, 153, 31],
                    "columns": {"geojson": "geometry"},
                    "isVisible": True,
                    "visConfig": {
                        "opacity": 0.7,
                        "strokeOpacity": 0.8,
                        "thickness": 1,
                        "strokeColor": [50, 50, 50],
                        "colorRange": {
                            "name": "ColorBrewer RdYlGn-6",
                            "type": "diverging",
                            "category": "ColorBrewer",
                            "colors": ["#d73027", "#fc8d59", "#fee08b", "#d9ef8b", "#91cf60", "#1a9850"]
                        },
                        "filled": True
                    },
                    "colorField": {"name": "area_sqm", "type": "real"}
                }
            }]
        }
    }
}

# Create Kepler map
kepler_map = KeplerGl(height=600, config=kepler_config)
kepler_map.add_data(data=geojson_fc, name="parcels")

# Save Kepler HTML to UC Volume to avoid 10MB output limit
volume_path = f"/Volumes/{catalog_name}/{schema_name}/source"
kepler_html_path = f"{volume_path}/visualizations/kepler_parcels.html"

import os
os.makedirs(f"{volume_path}/visualizations", exist_ok=True)

# Save the HTML file
kepler_map.save_to_html(file_name=kepler_html_path)
print(f"Kepler map saved to: {kepler_html_path}")
print("Open this file in a browser to view the interactive map.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Visualize with PyDeck (3D Visualization)
# MAGIC
# MAGIC PyDeck allows 3D visualization - we can extrude parcels by area.

# COMMAND ----------

import pydeck as pdk

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

# Use Casey LGA - Metro Melbourne area with good Activity Centre coverage
target_lga_pydeck = "CASEY"
print(f"PyDeck: Filtering to LGA: {target_lga_pydeck}")

pydeck_data = spark.sql(f"""
    SELECT
        parcel_id,
        zone_code,
        area_sqm,
        compactness_index,
        centroid_lon,
        centroid_lat,
        ST_AsGeoJSON(ST_Transform(geometry, 4326)) AS geojson
    FROM {catalog_name}.{schema_name}.parcel_geometric_features
    WHERE centroid_lon IS NOT NULL
      AND geometry IS NOT NULL
      AND (zone_code LIKE 'GRZ%' OR zone_code LIKE 'NRZ%')
      {f"AND lga_name = '{target_lga_pydeck}'" if target_lga_pydeck else ""}
    LIMIT 50
""").toPandas()

# Parse GeoJSON for pydeck - handle both Polygon and MultiPolygon
import json

pydeck_features = []
for _, row in pydeck_data.iterrows():
    try:
        geom = json.loads(row['geojson'])
        compactness = safe_float(row['compactness_index'], 0.5)
        area = safe_float(row['area_sqm'], 500)

        if geom['type'] == 'Polygon':
            coords = geom['coordinates'][0]
            pydeck_features.append({
                "polygon": coords,
                "area_sqm": area,
                "zone_code": str(row['zone_code']) if row['zone_code'] else "",
                "compactness": compactness
            })
        elif geom['type'] == 'MultiPolygon':
            # Take the first polygon from MultiPolygon
            coords = geom['coordinates'][0][0]
            pydeck_features.append({
                "polygon": coords,
                "area_sqm": area,
                "zone_code": str(row['zone_code']) if row['zone_code'] else "",
                "compactness": compactness
            })
    except:
        pass

print(f"Prepared {len(pydeck_features)} features for PyDeck")

# COMMAND ----------

# Create PyDeck 3D visualization
if len(pydeck_features) > 0:
    # Calculate center
    center_lon = pydeck_data['centroid_lon'].mean()
    center_lat = pydeck_data['centroid_lat'].mean()

    # Create polygon layer with extrusion based on area
    layer = pdk.Layer(
        "PolygonLayer",
        pydeck_features,
        get_polygon="polygon",
        get_elevation="area_sqm * 0.1",  # Scale for visibility
        elevation_scale=1,
        extruded=True,
        get_fill_color="[255 - compactness * 200, 100 + compactness * 100, 50, 180]",
        get_line_color=[50, 50, 50],
        line_width_min_pixels=1,
        pickable=True,
        auto_highlight=True,
    )

    # Set the viewport
    view_state = pdk.ViewState(
        latitude=center_lat,
        longitude=center_lon,
        zoom=15,
        pitch=45,
        bearing=0
    )

    # Create the deck
    deck = pdk.Deck(
        layers=[layer],
        initial_view_state=view_state,
        tooltip={
            "text": "Zone: {zone_code}\nArea: {area_sqm} sqm\nCompactness: {compactness}"
        }
    )

    # Display in Databricks - use display() for pydeck objects
    display(deck)
else:
    print("No data available for PyDeck visualization")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Export Full LGA 3D PyDeck Map to UC Volume

# COMMAND ----------

# Export ALL residential parcels in the LGA as a PyDeck HTML file
import pydeck as pdk
import json

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

# Get ALL GRZ/NRZ parcels for the target LGA (no limit)
pydeck_all_data = spark.sql(f"""
    SELECT
        parcel_id,
        zone_code,
        area_sqm,
        compactness_index,
        centroid_lon,
        centroid_lat,
        ST_AsGeoJSON(ST_Transform(geometry, 4326)) AS geojson
    FROM {catalog_name}.{schema_name}.parcel_geometric_features
    WHERE centroid_lon IS NOT NULL
      AND geometry IS NOT NULL
      AND (zone_code LIKE 'GRZ%' OR zone_code LIKE 'NRZ%')
      AND lga_name = '{target_lga_pydeck}'
""").toPandas()

print(f"Loaded {len(pydeck_all_data)} residential parcels for full 3D export")

# Parse all GeoJSON for pydeck
pydeck_all_features = []
for _, row in pydeck_all_data.iterrows():
    try:
        geom = json.loads(row['geojson'])
        compactness = safe_float(row['compactness_index'], 0.5)
        area = safe_float(row['area_sqm'], 500)

        if geom['type'] == 'Polygon':
            coords = geom['coordinates'][0]
            pydeck_all_features.append({
                "polygon": coords,
                "area_sqm": area,
                "zone_code": str(row['zone_code']) if row['zone_code'] else "",
                "compactness": compactness
            })
        elif geom['type'] == 'MultiPolygon':
            coords = geom['coordinates'][0][0]
            pydeck_all_features.append({
                "polygon": coords,
                "area_sqm": area,
                "zone_code": str(row['zone_code']) if row['zone_code'] else "",
                "compactness": compactness
            })
    except:
        pass

print(f"Prepared {len(pydeck_all_features)} features for full PyDeck export")

if len(pydeck_all_features) > 0:
    center_lon = pydeck_all_data['centroid_lon'].mean()
    center_lat = pydeck_all_data['centroid_lat'].mean()

    layer = pdk.Layer(
        "PolygonLayer",
        pydeck_all_features,
        get_polygon="polygon",
        get_elevation="area_sqm * 0.1",
        elevation_scale=1,
        extruded=True,
        get_fill_color="[255 - compactness * 200, 100 + compactness * 100, 50, 180]",
        get_line_color=[50, 50, 50],
        line_width_min_pixels=1,
        pickable=True,
        auto_highlight=True,
    )

    view_state = pdk.ViewState(
        latitude=center_lat,
        longitude=center_lon,
        zoom=13,
        pitch=45,
        bearing=0
    )

    deck_full = pdk.Deck(
        layers=[layer],
        initial_view_state=view_state,
        tooltip={"text": "Zone: {zone_code}\nArea: {area_sqm} sqm\nCompactness: {compactness}"}
    )

    # Save to UC Volume
    volume_path = f"/Volumes/{catalog_name}/{schema_name}/source"
    pydeck_html_path = f"{volume_path}/visualizations/pydeck_3d_residential_{target_lga_pydeck.replace(' ', '_')}.html"

    deck_full.to_html(pydeck_html_path)
    print(f"Full LGA PyDeck 3D map saved to: {pydeck_html_path}")
    print(f"Total residential parcels exported: {len(pydeck_all_features):,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Analyze Shape Quality
# MAGIC
# MAGIC Identify lots with poor shape characteristics that would benefit from consolidation.

# COMMAND ----------

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

# Analyze shape quality
display(spark.sql(f"""
    SELECT
        parcel_id,
        zone_code,
        lga_name,
        area_sqm,
        compactness_index,
        aspect_ratio,
        elongation_index,
        -- Shape quality assessment
        CASE
            WHEN compactness_index >= 0.7 THEN 'Excellent'
            WHEN compactness_index >= 0.5 THEN 'Good'
            WHEN compactness_index >= 0.3 THEN 'Fair'
            ELSE 'Poor'
        END AS shape_quality,
        -- Lot type classification
        CASE
            WHEN aspect_ratio BETWEEN 0.8 AND 1.2 THEN 'Square'
            WHEN aspect_ratio < 0.8 THEN 'Deep'
            ELSE 'Wide'
        END AS lot_type,
        -- Consolidation recommendation (slivers excluded — they are data quality issues)
        CASE
            WHEN is_sliver THEN 'Data Quality'
            WHEN below_min_area_500 OR narrow_lot OR compactness_index < 0.3 THEN 'High Priority'
            WHEN elongation_index > 3 THEN 'Medium Priority'
            ELSE 'Low Priority'
        END AS consolidation_priority
    FROM {catalog_name}.{schema_name}.parcel_geometric_features
    WHERE zone_code IS NOT NULL
    ORDER BY
        CASE
            WHEN below_min_area_500 OR narrow_lot OR compactness_index < 0.3 THEN 1
            WHEN elongation_index > 3 THEN 2
            ELSE 3
        END,
        area_sqm
    LIMIT 100
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Visualize Shape Quality with Kepler.gl

# COMMAND ----------

# Export shape quality visualization to UC Volume using Kepler.gl
from keplergl import KeplerGl
import json

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

# Use Casey LGA - Metro Melbourne area with good Activity Centre coverage
target_lga_shape = "CASEY"

shape_data = spark.sql(f"""
    SELECT
        parcel_id,
        zone_code,
        lga_name,
        area_sqm,
        compactness_index,
        aspect_ratio,
        elongation_index,
        centroid_lon,
        centroid_lat,
        ST_AsGeoJSON(ST_Transform(geometry, 4326)) AS geojson,
        CASE
            WHEN compactness_index >= 0.7 THEN 'Excellent'
            WHEN compactness_index >= 0.5 THEN 'Good'
            WHEN compactness_index >= 0.3 THEN 'Fair'
            ELSE 'Poor'
        END AS shape_quality,
        CASE
            WHEN is_sliver THEN 'Data Quality'
            WHEN below_min_area_500 OR narrow_lot OR compactness_index < 0.3 THEN 'High'
            WHEN elongation_index > 3 THEN 'Medium'
            ELSE 'Low'
        END AS consolidation_priority
    FROM {catalog_name}.{schema_name}.parcel_geometric_features
    WHERE geometry IS NOT NULL
      AND centroid_lon IS NOT NULL
      AND zone_code IS NOT NULL
      AND is_sliver = FALSE
      AND lga_name = '{target_lga_shape}'
""").toPandas()

print(f"Loaded {len(shape_data)} parcels for shape quality visualization")

# Build GeoJSON FeatureCollection
shape_features = []
for _, row in shape_data.iterrows():
    try:
        geom = json.loads(row['geojson'])
        feature = {
            "type": "Feature",
            "geometry": geom,
            "properties": {
                "parcel_id": str(row['parcel_id']),
                "zone_code": str(row['zone_code']) if row['zone_code'] else "",
                "area_sqm": safe_float(row['area_sqm']),
                "compactness_index": safe_float(row['compactness_index']),
                "shape_quality": str(row['shape_quality']),
                "consolidation_priority": str(row['consolidation_priority'])
            }
        }
        shape_features.append(feature)
    except:
        pass

shape_geojson = {"type": "FeatureCollection", "features": shape_features}
print(f"Created GeoJSON with {len(shape_features)} features")

# Create Kepler map
kepler_shape_config = {
    "version": "v1",
    "config": {
        "mapState": {
            "latitude": shape_data['centroid_lat'].mean() if len(shape_data) > 0 else -37.8,
            "longitude": shape_data['centroid_lon'].mean() if len(shape_data) > 0 else 145.0,
            "zoom": 13
        }
    }
}

kepler_shape_map = KeplerGl(height=600, config=kepler_shape_config)
kepler_shape_map.add_data(data=shape_geojson, name="shape_quality")

# Save to UC Volume
volume_path = f"/Volumes/{catalog_name}/{schema_name}/source"
kepler_shape_path = f"{volume_path}/visualizations/kepler_shape_quality_{target_lga_shape.replace(' ', '_')}.html"

kepler_shape_map.save_to_html(file_name=kepler_shape_path)
print(f"Shape quality Kepler map saved to: {kepler_shape_path}")
print(f"Total parcels: {len(shape_features):,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Visualize Consolidation Priority with Folium

# COMMAND ----------

import folium

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

# Use Casey LGA - Metro Melbourne area with good Activity Centre coverage
target_lga_priority = "CASEY"
print(f"Priority Map: Filtering to LGA: {target_lga_priority}")

priority_parcels = spark.sql(f"""
    SELECT
        parcel_id,
        zone_code,
        area_sqm,
        compactness_index,
        centroid_lon,
        centroid_lat,
        ST_AsGeoJSON(ST_Transform(geometry, 4326)) AS geojson,
        CASE
            WHEN is_sliver THEN 'Data Quality'
            WHEN below_min_area_500 OR narrow_lot OR compactness_index < 0.3 THEN 'High'
            WHEN elongation_index > 3 THEN 'Medium'
            ELSE 'Low'
        END AS priority
    FROM {catalog_name}.{schema_name}.parcel_geometric_features
    WHERE geometry IS NOT NULL
      AND centroid_lon IS NOT NULL
      AND zone_code LIKE 'GRZ%'
      AND is_sliver = FALSE
      {f"AND lga_name = '{target_lga_priority}'" if target_lga_priority else ""}
    LIMIT 50
""").toPandas()

print(f"Loaded {len(priority_parcels)} parcels")

# COMMAND ----------

# Create map colored by consolidation priority
if len(priority_parcels) > 0:
    center_lat = priority_parcels['centroid_lat'].mean()
    center_lon = priority_parcels['centroid_lon'].mean()

    m = folium.Map(
        location=[center_lat, center_lon],
        zoom_start=15,
        tiles='CartoDB positron'
    )

    # Color by priority
    priority_colors = {
        'High': '#d73027',    # Red
        'Medium': '#fee08b',  # Yellow
        'Low': '#1a9850'      # Green
    }

    for _, row in priority_parcels.iterrows():
        try:
            geojson = json.loads(row['geojson'])
            color = priority_colors.get(row['priority'], '#999')

            folium.GeoJson(
                geojson,
                style_function=lambda x, c=color: {
                    'fillColor': c,
                    'color': '#333',
                    'weight': 1,
                    'fillOpacity': 0.6
                },
                tooltip=f"Priority: {row['priority']}<br>Area: {row['area_sqm']:.0f} sqm<br>Compactness: {row['compactness_index']:.2f}"
            ).add_to(m)
        except:
            pass

    # Add legend
    legend_html = '''
    <div style="position: fixed; bottom: 50px; left: 50px; z-index: 1000;
                background-color: white; padding: 10px; border-radius: 5px;
                border: 2px solid gray;">
        <p><strong>Consolidation Priority</strong></p>
        <p><span style="background-color: #d73027; padding: 2px 10px;">&nbsp;</span> High Priority</p>
        <p><span style="background-color: #fee08b; padding: 2px 10px;">&nbsp;</span> Medium Priority</p>
        <p><span style="background-color: #1a9850; padding: 2px 10px;">&nbsp;</span> Low Priority</p>
    </div>
    '''
    m.get_root().html.add_child(folium.Element(legend_html))

    # Use displayHTML for proper rendering in Databricks
    displayHTML(m._repr_html_())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Export Full Consolidation Priority Map to UC Volume

# COMMAND ----------

# Export ALL GRZ parcels with consolidation priority to Folium HTML
import folium
import json

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

# Get ALL GRZ parcels for the target LGA (no limit)
all_priority_parcels = spark.sql(f"""
    SELECT
        parcel_id,
        zone_code,
        area_sqm,
        compactness_index,
        elongation_index,
        centroid_lon,
        centroid_lat,
        ST_AsGeoJSON(ST_Transform(geometry, 4326)) AS geojson,
        CASE
            WHEN is_sliver THEN 'Data Quality'
            WHEN below_min_area_500 OR narrow_lot OR compactness_index < 0.3 THEN 'High'
            WHEN elongation_index > 3 THEN 'Medium'
            ELSE 'Low'
        END AS priority
    FROM {catalog_name}.{schema_name}.parcel_geometric_features
    WHERE geometry IS NOT NULL
      AND centroid_lon IS NOT NULL
      AND zone_code LIKE 'GRZ%'
      AND is_sliver = FALSE
      AND lga_name = '{target_lga_priority}'
""").toPandas()

print(f"Loaded {len(all_priority_parcels)} GRZ parcels for full priority map")

if len(all_priority_parcels) > 0:
    center_lat = all_priority_parcels['centroid_lat'].mean()
    center_lon = all_priority_parcels['centroid_lon'].mean()

    m_priority = folium.Map(
        location=[center_lat, center_lon],
        zoom_start=13,
        tiles='CartoDB positron'
    )

    priority_colors = {
        'High': '#d73027',
        'Medium': '#fee08b',
        'Low': '#1a9850'
    }

    # Count by priority
    priority_counts = all_priority_parcels['priority'].value_counts().to_dict()

    for _, row in all_priority_parcels.iterrows():
        try:
            geojson = json.loads(row['geojson'])
            color = priority_colors.get(row['priority'], '#999')
            folium.GeoJson(
                geojson,
                style_function=lambda x, c=color: {
                    'fillColor': c,
                    'color': '#333',
                    'weight': 0.5,
                    'fillOpacity': 0.6
                },
                tooltip=f"Priority: {row['priority']}<br>Area: {row['area_sqm']:.0f} sqm<br>Compactness: {row['compactness_index']:.2f}"
            ).add_to(m_priority)
        except:
            pass

    # Legend with counts
    legend_html = f'''
    <div style="position: fixed; bottom: 50px; left: 50px; z-index: 1000;
                background-color: white; padding: 10px; border-radius: 5px;
                border: 2px solid gray;">
        <p><strong>{target_lga_priority} - Consolidation Priority</strong></p>
        <p><span style="background-color: #d73027; padding: 2px 10px;">&nbsp;</span> High ({priority_counts.get('High', 0):,})</p>
        <p><span style="background-color: #fee08b; padding: 2px 10px;">&nbsp;</span> Medium ({priority_counts.get('Medium', 0):,})</p>
        <p><span style="background-color: #1a9850; padding: 2px 10px;">&nbsp;</span> Low ({priority_counts.get('Low', 0):,})</p>
        <p><small>Total GRZ: {len(all_priority_parcels):,}</small></p>
    </div>
    '''
    m_priority.get_root().html.add_child(folium.Element(legend_html))

    # Save to UC Volume
    volume_path = f"/Volumes/{catalog_name}/{schema_name}/source"
    priority_html_path = f"{volume_path}/visualizations/folium_consolidation_priority_{target_lga_priority.replace(' ', '_')}.html"

    m_priority.save(priority_html_path)
    print(f"Full consolidation priority map saved to: {priority_html_path}")
    print(f"Priority breakdown: High={priority_counts.get('High', 0)}, Medium={priority_counts.get('Medium', 0)}, Low={priority_counts.get('Low', 0)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Convex Hull Analysis

# COMMAND ----------

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

# Convex hull efficiency analysis
display(spark.sql(f"""
    SELECT
        parcel_id,
        zone_code,
        ROUND(ST_Area(geometry), 2) AS actual_area_sqm,
        ROUND(ST_Area(ST_ConvexHull(geometry)), 2) AS convex_hull_area_sqm,
        -- Convex hull efficiency: actual area / convex hull area
        -- Value of 1 = convex shape (no indentations), lower = more irregular
        ROUND(
            ST_Area(geometry) /
            NULLIF(ST_Area(ST_ConvexHull(geometry)), 0),
            4
        ) AS convex_hull_efficiency
    FROM {catalog_name}.{schema_name}.parcels_with_zones
    WHERE geometry IS NOT NULL
    LIMIT 100
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Summary Statistics by Zone

# COMMAND ----------

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

# Summary statistics by zone
display(spark.sql(f"""
    SELECT
        zone_code,
        COUNT(*) AS parcel_count,
        ROUND(AVG(area_sqm), 2) AS avg_area_sqm,
        ROUND(MIN(area_sqm), 2) AS min_area_sqm,
        ROUND(MAX(area_sqm), 2) AS max_area_sqm,
        ROUND(AVG(compactness_index), 4) AS avg_compactness,
        ROUND(AVG(aspect_ratio), 4) AS avg_aspect_ratio,
        SUM(CASE WHEN below_min_area_500 THEN 1 ELSE 0 END) AS below_500sqm_count,
        SUM(CASE WHEN narrow_lot THEN 1 ELSE 0 END) AS narrow_lot_count
    FROM {catalog_name}.{schema_name}.parcel_geometric_features
    WHERE zone_code IS NOT NULL
    GROUP BY zone_code
    ORDER BY parcel_count DESC
    LIMIT 50
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### AI-Generated Summary by Zone

# COMMAND ----------

# Use ai_query() to generate a human-readable summary of zone statistics
catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")
ai_model = dbutils.widgets.get("ai_model")

zone_summary = spark.sql(f"""
    WITH zone_stats AS (
        SELECT
            zone_code,
            COUNT(*) AS parcel_count,
            ROUND(AVG(area_sqm), 0) AS avg_area_sqm,
            ROUND(MIN(area_sqm), 0) AS min_area_sqm,
            ROUND(MAX(area_sqm), 0) AS max_area_sqm,
            ROUND(AVG(compactness_index), 2) AS avg_compactness,
            SUM(CASE WHEN below_min_area_500 THEN 1 ELSE 0 END) AS below_500sqm_count,
            SUM(CASE WHEN narrow_lot THEN 1 ELSE 0 END) AS narrow_lot_count
        FROM {catalog_name}.{schema_name}.parcel_geometric_features
        WHERE zone_code IS NOT NULL
        GROUP BY zone_code
        ORDER BY parcel_count DESC
        LIMIT 10
    )
    SELECT ai_query(
        '{ai_model}',
        CONCAT(
            'You are a Victorian land use planning analyst. Analyze these zone statistics and provide a concise 3-4 paragraph summary. ',
            'Focus on: (1) which zones have the most parcels, (2) which zones have small lots that may need consolidation, ',
            '(3) overall parcel shape quality, (4) recommendations for site consolidation focus areas. ',
            'Be specific with numbers. Data: ',
            TO_JSON(COLLECT_LIST(STRUCT(*)))
        )
    ) AS zone_analysis_summary
    FROM zone_stats
""")

# Display the ai_query result with proper markdown rendering
from IPython.display import display, Markdown, HTML

result_df = zone_summary.toPandas()
if len(result_df) > 0:
    summary_text = result_df['zone_analysis_summary'].iloc[0]
    display(Markdown(summary_text))
else:
    print("No summary generated")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Summary Statistics by LGA

# COMMAND ----------

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

# Summary statistics by LGA
display(spark.sql(f"""
    SELECT
        lga_name,
        COUNT(*) AS parcel_count,
        ROUND(AVG(area_sqm), 2) AS avg_area_sqm,
        ROUND(MIN(area_sqm), 2) AS min_area_sqm,
        ROUND(MAX(area_sqm), 2) AS max_area_sqm,
        SUM(CASE WHEN below_min_area_500 THEN 1 ELSE 0 END) AS below_500sqm_count,
        ROUND(100.0 * SUM(CASE WHEN below_min_area_500 THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_below_500sqm
    FROM {catalog_name}.{schema_name}.parcel_geometric_features
    WHERE lga_name IS NOT NULL
    GROUP BY lga_name
    ORDER BY parcel_count DESC
    LIMIT 50
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### AI-Generated Summary by LGA

# COMMAND ----------

# Use ai_query() to generate a human-readable summary of LGA statistics
catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")
ai_model = dbutils.widgets.get("ai_model")

lga_summary = spark.sql(f"""
    WITH lga_stats AS (
        SELECT
            lga_name,
            COUNT(*) AS parcel_count,
            ROUND(AVG(area_sqm), 0) AS avg_area_sqm,
            ROUND(MIN(area_sqm), 0) AS min_area_sqm,
            ROUND(MAX(area_sqm), 0) AS max_area_sqm,
            SUM(CASE WHEN below_min_area_500 THEN 1 ELSE 0 END) AS below_500sqm_count,
            ROUND(100.0 * SUM(CASE WHEN below_min_area_500 THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_below_500sqm,
            SUM(CASE WHEN narrow_lot THEN 1 ELSE 0 END) AS narrow_lot_count
        FROM {catalog_name}.{schema_name}.parcel_geometric_features
        WHERE lga_name IS NOT NULL
        GROUP BY lga_name
        ORDER BY parcel_count DESC
        LIMIT 10
    )
    SELECT ai_query(
        '{ai_model}',
        CONCAT(
            'You are a Victorian housing policy analyst. Analyze these Local Government Area (LGA) statistics and provide a concise 3-4 paragraph summary. ',
            'Focus on: (1) which LGAs have the most land parcels, (2) which LGAs have the highest percentage of small lots (<500sqm) indicating potential consolidation opportunities, ',
            '(3) which LGAs have narrow lots that may constrain development, (4) prioritized recommendations for which LGAs should be targeted for housing consolidation studies. ',
            'Be specific with numbers and percentages. Data: ',
            TO_JSON(COLLECT_LIST(STRUCT(*)))
        )
    ) AS lga_analysis_summary
    FROM lga_stats
""")

# Display the ai_query result with proper markdown rendering
from IPython.display import display, Markdown, HTML

result_df = lga_summary.toPandas()
if len(result_df) > 0:
    summary_text = result_df['lga_analysis_summary'].iloc[0]
    display(Markdown(summary_text))
else:
    print("No summary generated")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC This notebook computed the following geometric features:
# MAGIC
# MAGIC | Feature | Spatial SQL Function |
# MAGIC |---------|---------------------|
# MAGIC | Area | `ST_Area(geometry)` — geometry already in EPSG:3111, no transform needed |
# MAGIC | Perimeter | `ST_Perimeter(geometry)` — geometry already in EPSG:3111, no transform needed |
# MAGIC | Centroid | `ST_Centroid(ST_Transform(geometry, 4326))` — transformed for web maps |
# MAGIC | Convex Hull | `ST_ConvexHull(geometry)` |
# MAGIC | Hull Efficiency | `ST_Area(geometry) / ST_Area(ST_ConvexHull(geometry))` — flags concave lots |
# MAGIC | Compactness Index | `4π × area / perimeter²` |
# MAGIC | Aspect Ratio | `width / depth` (from convex hull, NULL when hull efficiency < 0.6) |
# MAGIC | Elongation Index | `max(width,depth) / min(width,depth)` (NULL when hull efficiency < 0.6) |
# MAGIC
# MAGIC ### CRS Strategy
# MAGIC - **Storage & calculations**: EPSG:3111 (VicGrid GDA94) — native metres for accurate area/distance
# MAGIC - **Visualization only**: Transform to EPSG:4326 (WGS84) at query time for web maps
# MAGIC
# MAGIC ### Visualization Libraries Demonstrated
# MAGIC - **Folium**: Interactive Leaflet-based maps with tooltips
# MAGIC - **Kepler.gl**: Powerful visualization for large datasets with filtering
# MAGIC - **PyDeck**: 3D visualization with extrusion capabilities
# MAGIC
# MAGIC ### Output Tables
# MAGIC - `parcels_with_zones` - Parcels joined with zone information (geometry in EPSG:3111)
# MAGIC - `parcel_geometric_features` - All geometric features computed (geometry in EPSG:3111, centroids in WGS84)
# MAGIC
# MAGIC ### Next Steps
# MAGIC - **03_edge_topology.py** - Compute frontages, shared boundaries, corner lots
