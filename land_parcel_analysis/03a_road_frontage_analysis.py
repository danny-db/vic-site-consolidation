# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 1b: Road Frontage Analysis
# MAGIC
# MAGIC This notebook computes **road frontage features** for each land parcel using Databricks Spatial SQL.
# MAGIC
# MAGIC ## Features Computed:
# MAGIC | Feature | Description | Why It Matters |
# MAGIC |---------|-------------|----------------|
# MAGIC | **Number of Road Frontages** | Count of distinct road edges touching parcel | Corner lots have 2+; internal lots may have 1 |
# MAGIC | **Total Road Frontage Length** | Sum of all road-facing edges | Must meet minimum frontage requirements |
# MAGIC | **Frontage by Road Class** | Length along arterial vs local vs laneway | Affects access and planning requirements |
# MAGIC | **Longest Continuous Frontage** | Single longest road-facing edge | Must meet minimum frontage threshold |
# MAGIC | **Corner Lot Indicator** | Boolean: two or more road frontages | Corner lots have different setback rules |
# MAGIC | **Laneway Access** | Boolean: has edge touching a laneway | Secondary access improves development potential |
# MAGIC | **Frontage-to-Perimeter Ratio** | Proportion of boundary that is road | Low ratio = mostly enclosed by other parcels |
# MAGIC
# MAGIC **Prerequisite:** Run `01_data_ingestion.py` to load parcels and roads data.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

# MAGIC %pip install folium --quiet

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# Create widgets
dbutils.widgets.text("catalog_name", "danny_catalog", "Catalog Name")
dbutils.widgets.text("schema_name", "dtp_hackathon", "Schema Name")

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

print(f"Using: {catalog_name}.{schema_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Check Available Tables

# COMMAND ----------

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

print("Available tables:")
display(spark.sql(f"SHOW TABLES IN {catalog_name}.{schema_name}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Explore Road Network Data
# MAGIC
# MAGIC Understand the road classification schema to properly categorize frontages.

# COMMAND ----------

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

# Check roads table schema
print("Roads table schema:")
display(spark.sql(f"DESCRIBE {catalog_name}.{schema_name}.roads"))

# COMMAND ----------

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

# Explore road classification columns
# Common columns in Vicmap Transport: CLASS_CODE, ROAD_TYPE, HIERARCHY, etc.
display(spark.sql(f"""
    SELECT *
    FROM {catalog_name}.{schema_name}.roads
    LIMIT 5
"""))

# COMMAND ----------

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

# Get road classification distribution
# Adjust column name based on actual schema (CLASS_CODE, ROAD_TYPE, or similar)
display(spark.sql(f"""
    SELECT
        CLASS_CODE,
        COUNT(*) AS count
    FROM {catalog_name}.{schema_name}.roads
    GROUP BY CLASS_CODE
    ORDER BY count DESC
    LIMIT 20
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Define Road Classification Mapping
# MAGIC
# MAGIC Map Vicmap road classes to simplified categories for frontage analysis.

# COMMAND ----------

# Vicmap Transport road classification mapping
# Reference: Vicmap Transport Data Dictionary
# CLASS_CODE values:
#   1 = Freeway
#   2 = Highway
#   3 = Arterial
#   4 = Sub-Arterial
#   5 = Collector
#   6 = Local
#   7 = Track/Trail
#   8 = Laneway/Service Road
#   9 = Other

road_class_sql = """
    CASE
        WHEN CLASS_CODE IN (1, 2) THEN 'highway'
        WHEN CLASS_CODE IN (3, 4) THEN 'arterial'
        WHEN CLASS_CODE IN (5) THEN 'collector'
        WHEN CLASS_CODE IN (6) THEN 'local'
        WHEN CLASS_CODE IN (8) THEN 'laneway'
        ELSE 'other'
    END
"""

print("Road classification mapping defined")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Compute Road Frontage for Sample Area
# MAGIC
# MAGIC Due to computational cost of spatial joins, we compute for a sample LGA first.

# COMMAND ----------

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

# Use Casey LGA - Metro Melbourne area with good Activity Centre coverage
sample_lga = "CASEY"

# Verify the LGA exists in edge topology
lga_check = spark.sql(f"""
    SELECT COUNT(*) AS cnt
    FROM {catalog_name}.{schema_name}.parcel_edge_topology
    WHERE lga_name = '{sample_lga}'
""").collect()[0]['cnt']

if lga_check == 0:
    print(f"WARNING: {sample_lga} not found in edge topology. Falling back to first available LGA.")
    sample_lga = spark.sql(f"""
        SELECT DISTINCT lga_name FROM {catalog_name}.{schema_name}.parcel_edge_topology LIMIT 1
    """).collect()[0][0]

print(f"Computing road frontage for LGA: {sample_lga} ({lga_check:,} parcels)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.1 Find Parcel-Road Intersections
# MAGIC
# MAGIC Identify which road segments touch each parcel boundary.

# COMMAND ----------

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

# ============================================================================
# ROAD FRONTAGE ANALYSIS: Measure How Much Each Parcel Faces Roads
# ============================================================================
# Victorian planning schemes typically require minimum road frontage (often 15m)
# for new developments. This analysis identifies:
# - How much road frontage each parcel has
# - What type of roads (arterial, local, laneway)
# - Whether it's a corner lot (multiple frontages)
#
# KEY SPATIAL FUNCTIONS:
#
# - ST_Boundary(geometry): Gets the parcel's perimeter as a line
# - ST_Buffer(road_geom, 1): Creates a 1-meter buffer around roads
#   (This helps catch parcels that nearly touch roads but don't exactly touch)
# - ST_Intersects(...): Tests if parcel boundary touches the road buffer
# - ST_Intersection(...): Gets the portion of boundary that touches road
# - ST_Length(...): Measures that frontage length in meters
# - ST_SetSRID(...): Ensures coordinate system tags are correct
#   (SRID = Spatial Reference ID, like EPSG:3111 for VicGrid)
#
# WHY FRONTAGE MATTERS:
# - Minimum frontage requirements (typically 15m in Victoria)
# - Corner lots have special setback rules
# - Laneway access can enable rear development
# - Battle-axe lots with no frontage have limited development potential
# ============================================================================
spark.sql(f"""
    CREATE OR REPLACE TABLE {catalog_name}.{schema_name}.parcel_road_intersections AS
    WITH parcel_boundaries AS (
        SELECT
            parcel_id,
            zone_code,
            perimeter_m,
            -- Ensure boundary has valid SRID
            ST_SetSRID(ST_Boundary(geometry), ST_SRID(geometry)) AS boundary,
            geometry
        FROM {catalog_name}.{schema_name}.parcel_edge_topology
        -- Limit to sample LGA for performance
        WHERE lga_name = '{sample_lga}'
          AND geometry IS NOT NULL
          AND ST_SRID(geometry) > 0  -- Filter invalid SRIDs
    ),
    road_segments AS (
        SELECT
            ROAD_NAME,
            CLASS_CODE,
            {road_class_sql} AS road_class,
            -- Ensure road geometry has valid SRID for buffer operation
            ST_SetSRID(geometry, ST_SRID(geometry)) AS road_geom,
            ST_SRID(geometry) AS road_srid
        FROM {catalog_name}.{schema_name}.roads
        WHERE geometry IS NOT NULL
          AND ST_SRID(geometry) > 0  -- Filter invalid SRIDs
    ),
    intersections AS (
        SELECT
            p.parcel_id,
            p.zone_code,
            p.perimeter_m,
            r.ROAD_NAME,
            r.CLASS_CODE,
            r.road_class,
            p.boundary,
            r.road_geom,
            r.road_srid
        FROM parcel_boundaries p
        JOIN road_segments r
            ON ST_Intersects(p.boundary, ST_Buffer(r.road_geom, 1))
    )
    SELECT
        parcel_id,
        zone_code,
        perimeter_m,
        ROAD_NAME,
        CLASS_CODE,
        road_class,
        -- Calculate frontage length
        ROUND(
            ST_Length(
                ST_SetSRID(
                    ST_Intersection(boundary, ST_Buffer(road_geom, 1)),
                    road_srid
                )
            ),
            2
        ) AS frontage_length_m
    FROM intersections
    WHERE ST_Length(ST_Intersection(boundary, ST_Buffer(road_geom, 1))) > 0.5  -- Filter noise
""")

print("Created parcel_road_intersections table")
display(spark.sql(f"""
    SELECT parcel_id, zone_code, ROAD_NAME, road_class, frontage_length_m
    FROM {catalog_name}.{schema_name}.parcel_road_intersections
    ORDER BY frontage_length_m DESC
    LIMIT 100
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2 Aggregate Frontage Statistics per Parcel

# COMMAND ----------

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

# ============================================================================
# AGGREGATE FRONTAGE: Summarize Road Access for Each Parcel
# ============================================================================
# This combines all the road intersection data into useful metrics per parcel:
#
# - num_road_frontages: Count of distinct roads touching the parcel
#   2+ roads = corner lot with special planning considerations
#
# - total_frontage_m: Sum of all road-facing edges
# - longest_frontage_m: The single longest road edge (checked against minimums)
#
# - Frontage by road class: Useful for understanding access quality
#   - Arterial frontage: Higher traffic, commercial potential but noise issues
#   - Local frontage: Standard residential access
#   - Laneway frontage: Secondary access, enables rear development
#
# - frontage_to_perimeter_ratio: What portion of boundary faces roads
#   Low ratio = mostly enclosed by neighbors (internal lot)
#
# - meets_min_frontage_15m / 20m: Quick check against common requirements
# ============================================================================
spark.sql(f"""
    CREATE OR REPLACE TABLE {catalog_name}.{schema_name}.parcel_road_frontage AS
    WITH frontage_stats AS (
        SELECT
            parcel_id,
            -- Total road frontage
            COUNT(DISTINCT ROAD_NAME) AS num_road_frontages,
            SUM(frontage_length_m) AS total_frontage_m,
            MAX(frontage_length_m) AS longest_frontage_m,

            -- Frontage by road class
            SUM(CASE WHEN road_class = 'arterial' THEN frontage_length_m ELSE 0 END) AS arterial_frontage_m,
            SUM(CASE WHEN road_class = 'collector' THEN frontage_length_m ELSE 0 END) AS collector_frontage_m,
            SUM(CASE WHEN road_class = 'local' THEN frontage_length_m ELSE 0 END) AS local_frontage_m,
            SUM(CASE WHEN road_class = 'laneway' THEN frontage_length_m ELSE 0 END) AS laneway_frontage_m,

            -- Road class flags
            MAX(CASE WHEN road_class = 'arterial' THEN 1 ELSE 0 END) AS has_arterial_frontage,
            MAX(CASE WHEN road_class = 'laneway' THEN 1 ELSE 0 END) AS has_laneway_access,

            -- List of road names for reference
            COLLECT_SET(ROAD_NAME) AS road_names
        FROM {catalog_name}.{schema_name}.parcel_road_intersections
        GROUP BY parcel_id
    )
    SELECT
        e.parcel_id,
        e.plan_number,
        e.lot_number,
        e.zone_code,
        e.zone_description,
        e.lga_code,
        e.lga_name,
        e.geometry,
        e.area_sqm,
        e.perimeter_m,
        e.compactness_index,
        e.centroid_lon,
        e.centroid_lat,

        -- Existing edge topology features
        e.num_adjacent_parcels,
        e.total_shared_boundary_m,
        e.longest_shared_boundary_m,
        e.adjacent_same_zone_count,

        -- New road frontage features
        COALESCE(f.num_road_frontages, 0) AS num_road_frontages,
        ROUND(COALESCE(f.total_frontage_m, 0), 2) AS total_frontage_m,
        ROUND(COALESCE(f.longest_frontage_m, 0), 2) AS longest_frontage_m,
        ROUND(COALESCE(f.arterial_frontage_m, 0), 2) AS arterial_frontage_m,
        ROUND(COALESCE(f.collector_frontage_m, 0), 2) AS collector_frontage_m,
        ROUND(COALESCE(f.local_frontage_m, 0), 2) AS local_frontage_m,
        ROUND(COALESCE(f.laneway_frontage_m, 0), 2) AS laneway_frontage_m,

        -- Derived flags
        COALESCE(f.num_road_frontages, 0) >= 2 AS is_corner_lot,
        COALESCE(f.has_arterial_frontage, 0) = 1 AS has_arterial_frontage,
        COALESCE(f.has_laneway_access, 0) = 1 AS has_laneway_access,

        -- Frontage-to-perimeter ratio
        ROUND(
            CASE
                WHEN e.perimeter_m > 0 THEN COALESCE(f.total_frontage_m, 0) / e.perimeter_m
                ELSE 0
            END,
            4
        ) AS frontage_to_perimeter_ratio,

        -- Minimum frontage checks (15m is common minimum)
        COALESCE(f.longest_frontage_m, 0) >= 15 AS meets_min_frontage_15m,
        COALESCE(f.longest_frontage_m, 0) >= 20 AS meets_min_frontage_20m,

        -- Road names for reference
        f.road_names

    FROM {catalog_name}.{schema_name}.parcel_edge_topology e
    LEFT JOIN frontage_stats f ON e.parcel_id = f.parcel_id
    WHERE e.lga_name = '{sample_lga}'
""")

print("Created parcel_road_frontage table")

# COMMAND ----------

# Display road frontage features
catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

display(spark.sql(f"""
    SELECT
        parcel_id,
        zone_code,
        area_sqm,
        perimeter_m,
        num_road_frontages,
        total_frontage_m,
        longest_frontage_m,
        is_corner_lot,
        has_laneway_access,
        frontage_to_perimeter_ratio,
        meets_min_frontage_15m,
        road_names
    FROM {catalog_name}.{schema_name}.parcel_road_frontage
    ORDER BY num_road_frontages DESC, longest_frontage_m DESC
    LIMIT 100
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Summary Statistics

# COMMAND ----------

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

# Summary by zone
display(spark.sql(f"""
    SELECT
        zone_code,
        COUNT(*) AS parcel_count,
        ROUND(AVG(num_road_frontages), 2) AS avg_road_frontages,
        ROUND(AVG(total_frontage_m), 2) AS avg_total_frontage_m,
        ROUND(AVG(longest_frontage_m), 2) AS avg_longest_frontage_m,
        SUM(CASE WHEN is_corner_lot THEN 1 ELSE 0 END) AS corner_lot_count,
        SUM(CASE WHEN has_laneway_access THEN 1 ELSE 0 END) AS laneway_access_count,
        SUM(CASE WHEN meets_min_frontage_15m THEN 1 ELSE 0 END) AS meets_min_frontage_count,
        ROUND(AVG(frontage_to_perimeter_ratio), 4) AS avg_frontage_ratio
    FROM {catalog_name}.{schema_name}.parcel_road_frontage
    WHERE zone_code IS NOT NULL
    GROUP BY zone_code
    ORDER BY parcel_count DESC
    LIMIT 20
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### AI-Generated Summary of Road Frontage

# COMMAND ----------

# Use ai_query() to generate a human-readable summary of road frontage statistics
catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

frontage_summary = spark.sql(f"""
    WITH frontage_stats AS (
        SELECT
            zone_code,
            COUNT(*) AS parcel_count,
            ROUND(AVG(num_road_frontages), 2) AS avg_road_frontages,
            ROUND(AVG(total_frontage_m), 2) AS avg_total_frontage_m,
            ROUND(AVG(longest_frontage_m), 2) AS avg_longest_frontage_m,
            SUM(CASE WHEN is_corner_lot THEN 1 ELSE 0 END) AS corner_lot_count,
            SUM(CASE WHEN has_laneway_access THEN 1 ELSE 0 END) AS laneway_access_count,
            SUM(CASE WHEN meets_min_frontage_15m THEN 1 ELSE 0 END) AS meets_min_frontage_count,
            SUM(CASE WHEN NOT meets_min_frontage_15m THEN 1 ELSE 0 END) AS below_min_frontage_count,
            ROUND(AVG(frontage_to_perimeter_ratio), 4) AS avg_frontage_ratio
        FROM {catalog_name}.{schema_name}.parcel_road_frontage
        WHERE zone_code IS NOT NULL
        GROUP BY zone_code
        ORDER BY parcel_count DESC
        LIMIT 10
    )
    SELECT ai_query(
        'databricks-claude-opus-4-6',
        CONCAT(
            'You are a Victorian land use planning analyst specializing in site consolidation and road frontage requirements. Analyze these road frontage statistics and provide a concise 3-4 paragraph summary. ',
            'Focus on: (1) which zones have the most corner lots and what that means for development, (2) the prevalence of frontage-deficient parcels (<15m) and their consolidation potential, ',
            '(3) laneway access availability and its value for development, (4) specific recommendations for which zones should be prioritized for frontage-based consolidation studies. ',
            'Note: Victorian residential development typically requires minimum 15m frontage. Be specific with numbers and percentages. Data: ',
            TO_JSON(COLLECT_LIST(STRUCT(*)))
        )
    ) AS frontage_analysis_summary
    FROM frontage_stats
""")

# Display the ai_query result with proper markdown rendering
from IPython.display import display, Markdown, HTML

result_df = frontage_summary.toPandas()
if len(result_df) > 0:
    summary_text = result_df['frontage_analysis_summary'].iloc[0]
    display(Markdown(summary_text))
else:
    print("No summary generated")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Identify Frontage-Deficient Parcels
# MAGIC
# MAGIC Find parcels that don't meet minimum frontage requirements - prime consolidation candidates.

# COMMAND ----------

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

display(spark.sql(f"""
    SELECT
        parcel_id,
        zone_code,
        area_sqm,
        num_road_frontages,
        longest_frontage_m,
        is_corner_lot,
        has_laneway_access,
        frontage_to_perimeter_ratio,
        num_adjacent_parcels,
        longest_shared_boundary_m,
        -- Consolidation recommendation based on frontage
        CASE
            WHEN longest_frontage_m < 10 AND num_adjacent_parcels > 0
                THEN 'High Priority - Very narrow frontage with neighbors'
            WHEN longest_frontage_m < 15 AND num_adjacent_parcels > 0
                THEN 'High Priority - Below min frontage with neighbors'
            WHEN num_road_frontages = 0
                THEN 'High Priority - No road frontage (battle-axe lot)'
            WHEN frontage_to_perimeter_ratio < 0.15 AND num_adjacent_parcels > 0
                THEN 'Medium Priority - Low frontage ratio with neighbors'
            ELSE 'Low Priority'
        END AS frontage_recommendation
    FROM {catalog_name}.{schema_name}.parcel_road_frontage
    WHERE (longest_frontage_m < 15 OR num_road_frontages = 0)
      AND (zone_code LIKE 'GRZ%' OR zone_code LIKE 'NRZ%' OR zone_code LIKE 'RGZ%')
    ORDER BY
        CASE
            WHEN longest_frontage_m < 10 THEN 1
            WHEN longest_frontage_m < 15 THEN 2
            WHEN num_road_frontages = 0 THEN 3
            ELSE 4
        END,
        longest_frontage_m ASC
    LIMIT 100
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Visualize Corner Lots and Frontage

# COMMAND ----------

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

# Get corner lots for visualization
corner_lots_df = spark.sql(f"""
    SELECT
        parcel_id,
        zone_code,
        area_sqm,
        num_road_frontages,
        total_frontage_m,
        longest_frontage_m,
        is_corner_lot,
        has_laneway_access,
        frontage_to_perimeter_ratio,
        centroid_lon,
        centroid_lat,
        ST_AsGeoJSON(ST_Transform(geometry, 4326)) AS geojson
    FROM {catalog_name}.{schema_name}.parcel_road_frontage
    WHERE geometry IS NOT NULL
      AND centroid_lon IS NOT NULL
      AND (is_corner_lot = TRUE OR has_laneway_access = TRUE OR num_road_frontages = 0)
    ORDER BY num_road_frontages DESC
    LIMIT 50
""")

corner_lots = corner_lots_df.toPandas()
print(f"Retrieved {len(corner_lots)} parcels with notable frontage characteristics")

# COMMAND ----------

import folium
import json

if len(corner_lots) > 0:
    # Create map
    center_lat = corner_lots['centroid_lat'].mean()
    center_lon = corner_lots['centroid_lon'].mean()

    m = folium.Map(
        location=[center_lat, center_lon],
        zoom_start=16,
        tiles='CartoDB positron'
    )

    # Add parcels colored by frontage type
    for _, row in corner_lots.iterrows():
        try:
            geojson = json.loads(row['geojson'])

            # Color based on frontage characteristics
            if row['is_corner_lot']:
                color = '#e31a1c'  # Red for corner lots
                category = "Corner Lot"
            elif row['has_laneway_access']:
                color = '#33a02c'  # Green for laneway access
                category = "Laneway Access"
            elif row['num_road_frontages'] == 0:
                color = '#ff7f00'  # Orange for no frontage
                category = "No Road Frontage"
            else:
                color = '#1f78b4'  # Blue for normal
                category = "Normal"

            tooltip = f"""
                <b>Parcel:</b> {row['parcel_id']}<br>
                <b>Zone:</b> {row['zone_code']}<br>
                <b>Area:</b> {row['area_sqm']:.0f} sqm<br>
                <b>Road Frontages:</b> {row['num_road_frontages']}<br>
                <b>Total Frontage:</b> {row['total_frontage_m']:.1f}m<br>
                <b>Longest Frontage:</b> {row['longest_frontage_m']:.1f}m<br>
                <b>Type:</b> {category}
            """

            folium.GeoJson(
                geojson,
                style_function=lambda x, c=color: {
                    'fillColor': c,
                    'color': c,
                    'weight': 2,
                    'fillOpacity': 0.5
                },
                tooltip=folium.Tooltip(tooltip)
            ).add_to(m)
        except:
            pass

    # Add legend
    legend_html = '''
    <div style="position: fixed;
                bottom: 50px; left: 50px; width: 200px; height: 130px;
                border:2px solid grey; z-index:9999; font-size:14px;
                background-color:white; padding: 10px;
                border-radius: 5px;">
        <b>Frontage Classification</b><br>
        <i style="background:#e31a1c; width:18px; height:18px; display:inline-block;"></i> Corner Lot (2+ frontages)<br>
        <i style="background:#33a02c; width:18px; height:18px; display:inline-block;"></i> Laneway Access<br>
        <i style="background:#ff7f00; width:18px; height:18px; display:inline-block;"></i> No Road Frontage<br>
        <i style="background:#1f78b4; width:18px; height:18px; display:inline-block;"></i> Normal
    </div>
    '''
    m.get_root().html.add_child(folium.Element(legend_html))

    displayHTML(m._repr_html_())
else:
    print("No parcels found for visualization")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Export Full Corner Lots Map to UC Volume

# COMMAND ----------

# Export ALL corner lots and special frontage parcels to Folium HTML
import folium
import json

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

# Get ALL corner lots and special frontage parcels (no limit)
all_corner_lots = spark.sql(f"""
    SELECT
        parcel_id,
        zone_code,
        area_sqm,
        num_road_frontages,
        total_frontage_m,
        longest_frontage_m,
        is_corner_lot,
        has_laneway_access,
        frontage_to_perimeter_ratio,
        centroid_lon,
        centroid_lat,
        ST_AsGeoJSON(ST_Transform(geometry, 4326)) AS geojson
    FROM {catalog_name}.{schema_name}.parcel_road_frontage
    WHERE geometry IS NOT NULL
      AND centroid_lon IS NOT NULL
""").toPandas()

print(f"Loaded {len(all_corner_lots)} parcels for full frontage export")

if len(all_corner_lots) > 0:
    center_lat = all_corner_lots['centroid_lat'].mean()
    center_lon = all_corner_lots['centroid_lon'].mean()

    m_corner_full = folium.Map(
        location=[center_lat, center_lon],
        zoom_start=14,
        tiles='CartoDB positron'
    )

    # Count by category
    corner_count = all_corner_lots['is_corner_lot'].sum()
    laneway_count = all_corner_lots['has_laneway_access'].sum()
    no_frontage_count = (all_corner_lots['num_road_frontages'] == 0).sum()

    for _, row in all_corner_lots.iterrows():
        try:
            geojson = json.loads(row['geojson'])

            if row['is_corner_lot']:
                color = '#e31a1c'
                category = "Corner Lot"
            elif row['has_laneway_access']:
                color = '#33a02c'
                category = "Laneway Access"
            elif row['num_road_frontages'] == 0:
                color = '#ff7f00'
                category = "No Road Frontage"
            else:
                color = '#1f78b4'
                category = "Normal"

            folium.GeoJson(
                geojson,
                style_function=lambda x, c=color: {'fillColor': c, 'color': c, 'weight': 0.5, 'fillOpacity': 0.5},
                tooltip=f"Parcel: {row['parcel_id']}<br>Zone: {row['zone_code']}<br>Type: {category}<br>Frontages: {row['num_road_frontages']}"
            ).add_to(m_corner_full)
        except:
            pass

    legend_html = f'''
    <div style="position: fixed; bottom: 50px; left: 50px; width: 220px;
                border:2px solid grey; z-index:9999; font-size:14px;
                background-color:white; padding: 10px; border-radius: 5px;">
        <b>Frontage Classification (Full)</b><br>
        <i style="background:#e31a1c; width:18px; height:18px; display:inline-block;"></i> Corner Lot ({corner_count:,})<br>
        <i style="background:#33a02c; width:18px; height:18px; display:inline-block;"></i> Laneway Access ({laneway_count:,})<br>
        <i style="background:#ff7f00; width:18px; height:18px; display:inline-block;"></i> No Frontage ({no_frontage_count:,})<br>
        <i style="background:#1f78b4; width:18px; height:18px; display:inline-block;"></i> Normal<br>
        <small>Total: {len(all_corner_lots):,}</small>
    </div>
    '''
    m_corner_full.get_root().html.add_child(folium.Element(legend_html))

    # Save to UC Volume
    volume_path = f"/Volumes/{catalog_name}/{schema_name}/source"
    corner_html_path = f"{volume_path}/visualizations/folium_corner_lots_full.html"

    import os
    os.makedirs(f"{volume_path}/visualizations", exist_ok=True)

    m_corner_full.save(corner_html_path)
    print(f"Full corner lots map saved to: {corner_html_path}")
    print(f"Breakdown: Corner={corner_count}, Laneway={laneway_count}, No Frontage={no_frontage_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Parcels Colored by Frontage Length
# MAGIC
# MAGIC Visualize parcels by their longest road frontage - highlighting those below minimum requirements.

# COMMAND ----------

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

# Get parcels colored by frontage length
frontage_parcels_df = spark.sql(f"""
    SELECT
        parcel_id,
        zone_code,
        area_sqm,
        longest_frontage_m,
        meets_min_frontage_15m,
        meets_min_frontage_20m,
        frontage_to_perimeter_ratio,
        centroid_lon,
        centroid_lat,
        ST_AsGeoJSON(ST_Transform(geometry, 4326)) AS geojson
    FROM {catalog_name}.{schema_name}.parcel_road_frontage
    WHERE geometry IS NOT NULL
      AND centroid_lon IS NOT NULL
      AND (zone_code LIKE 'GRZ%' OR zone_code LIKE 'NRZ%' OR zone_code LIKE 'RGZ%')
    ORDER BY longest_frontage_m
    LIMIT 100
""")

frontage_parcels = frontage_parcels_df.toPandas()
print(f"Retrieved {len(frontage_parcels)} residential parcels for frontage visualization")

# COMMAND ----------

import folium
import json

if len(frontage_parcels) > 0:
    center_lat = frontage_parcels['centroid_lat'].mean()
    center_lon = frontage_parcels['centroid_lon'].mean()

    m = folium.Map(
        location=[center_lat, center_lon],
        zoom_start=16,
        tiles='CartoDB positron'
    )

    # Color by frontage length
    def get_frontage_color(frontage):
        if frontage < 10:
            return '#d73027'  # Red - critically narrow
        elif frontage < 15:
            return '#fc8d59'  # Orange - below minimum
        elif frontage < 20:
            return '#fee08b'  # Yellow - marginal
        else:
            return '#91cf60'  # Green - adequate

    for _, row in frontage_parcels.iterrows():
        try:
            geojson = json.loads(row['geojson'])
            color = get_frontage_color(row['longest_frontage_m'])

            tooltip = f"""
                <b>Parcel:</b> {row['parcel_id']}<br>
                <b>Zone:</b> {row['zone_code']}<br>
                <b>Area:</b> {row['area_sqm']:.0f} sqm<br>
                <b>Longest Frontage:</b> {row['longest_frontage_m']:.1f}m<br>
                <b>Meets 15m min:</b> {'Yes' if row['meets_min_frontage_15m'] else 'No'}<br>
                <b>Frontage Ratio:</b> {row['frontage_to_perimeter_ratio']:.2%}
            """

            folium.GeoJson(
                geojson,
                style_function=lambda x, c=color: {
                    'fillColor': c,
                    'color': '#333',
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
        <b>Longest Road Frontage</b><br>
        <i style="background:#d73027; width:18px; height:12px; display:inline-block;"></i> &lt;10m (Critical)<br>
        <i style="background:#fc8d59; width:18px; height:12px; display:inline-block;"></i> 10-15m (Below Min)<br>
        <i style="background:#fee08b; width:18px; height:12px; display:inline-block;"></i> 15-20m (Marginal)<br>
        <i style="background:#91cf60; width:18px; height:12px; display:inline-block;"></i> &gt;20m (Adequate)
    </div>
    '''
    m.get_root().html.add_child(folium.Element(legend_html))

    displayHTML(m._repr_html_())
else:
    print("No parcels found for visualization")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Export Full Frontage Length Map to UC Volume

# COMMAND ----------

# Export ALL residential parcels with frontage coloring to Folium HTML
import folium
import json

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

# Get ALL residential parcels (no limit)
all_frontage_parcels = spark.sql(f"""
    SELECT
        parcel_id,
        zone_code,
        area_sqm,
        longest_frontage_m,
        meets_min_frontage_15m,
        meets_min_frontage_20m,
        frontage_to_perimeter_ratio,
        centroid_lon,
        centroid_lat,
        ST_AsGeoJSON(ST_Transform(geometry, 4326)) AS geojson
    FROM {catalog_name}.{schema_name}.parcel_road_frontage
    WHERE geometry IS NOT NULL
      AND centroid_lon IS NOT NULL
      AND (zone_code LIKE 'GRZ%' OR zone_code LIKE 'NRZ%' OR zone_code LIKE 'RGZ%')
""").toPandas()

print(f"Loaded {len(all_frontage_parcels)} residential parcels for full frontage export")

if len(all_frontage_parcels) > 0:
    center_lat = all_frontage_parcels['centroid_lat'].mean()
    center_lon = all_frontage_parcels['centroid_lon'].mean()

    m_frontage_full = folium.Map(
        location=[center_lat, center_lon],
        zoom_start=14,
        tiles='CartoDB positron'
    )

    # Count by frontage category
    critical_count = (all_frontage_parcels['longest_frontage_m'] < 10).sum()
    below_min_count = ((all_frontage_parcels['longest_frontage_m'] >= 10) & (all_frontage_parcels['longest_frontage_m'] < 15)).sum()
    marginal_count = ((all_frontage_parcels['longest_frontage_m'] >= 15) & (all_frontage_parcels['longest_frontage_m'] < 20)).sum()
    adequate_count = (all_frontage_parcels['longest_frontage_m'] >= 20).sum()

    def get_frontage_color(frontage):
        if frontage < 10:
            return '#d73027'
        elif frontage < 15:
            return '#fc8d59'
        elif frontage < 20:
            return '#fee08b'
        else:
            return '#91cf60'

    for _, row in all_frontage_parcels.iterrows():
        try:
            geojson = json.loads(row['geojson'])
            color = get_frontage_color(row['longest_frontage_m'])

            folium.GeoJson(
                geojson,
                style_function=lambda x, c=color: {'fillColor': c, 'color': '#333', 'weight': 0.5, 'fillOpacity': 0.6},
                tooltip=f"Parcel: {row['parcel_id']}<br>Zone: {row['zone_code']}<br>Frontage: {row['longest_frontage_m']:.1f}m"
            ).add_to(m_frontage_full)
        except:
            pass

    legend_html = f'''
    <div style="position: fixed; bottom: 50px; left: 50px; width: 220px;
                border:2px solid grey; z-index:9999; font-size:14px;
                background-color:white; padding: 10px; border-radius: 5px;">
        <b>Longest Road Frontage (Full)</b><br>
        <i style="background:#d73027; width:18px; height:12px; display:inline-block;"></i> &lt;10m Critical ({critical_count:,})<br>
        <i style="background:#fc8d59; width:18px; height:12px; display:inline-block;"></i> 10-15m Below Min ({below_min_count:,})<br>
        <i style="background:#fee08b; width:18px; height:12px; display:inline-block;"></i> 15-20m Marginal ({marginal_count:,})<br>
        <i style="background:#91cf60; width:18px; height:12px; display:inline-block;"></i> &gt;20m Adequate ({adequate_count:,})<br>
        <small>Total: {len(all_frontage_parcels):,}</small>
    </div>
    '''
    m_frontage_full.get_root().html.add_child(folium.Element(legend_html))

    # Save to UC Volume
    volume_path = f"/Volumes/{catalog_name}/{schema_name}/source"
    frontage_html_path = f"{volume_path}/visualizations/folium_frontage_length_full.html"

    m_frontage_full.save(frontage_html_path)
    print(f"Full frontage length map saved to: {frontage_html_path}")
    print(f"Breakdown: Critical={critical_count}, Below Min={below_min_count}, Marginal={marginal_count}, Adequate={adequate_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC This notebook computed the following road frontage features:
# MAGIC
# MAGIC | Feature | Description |
# MAGIC |---------|-------------|
# MAGIC | `num_road_frontages` | Number of distinct road edges touching parcel |
# MAGIC | `total_frontage_m` | Total length of road-facing boundary |
# MAGIC | `longest_frontage_m` | Longest single road frontage |
# MAGIC | `arterial_frontage_m` | Frontage on arterial roads |
# MAGIC | `local_frontage_m` | Frontage on local roads |
# MAGIC | `laneway_frontage_m` | Frontage on laneways |
# MAGIC | `is_corner_lot` | Has 2+ road frontages |
# MAGIC | `has_laneway_access` | Has laneway frontage |
# MAGIC | `frontage_to_perimeter_ratio` | Proportion of boundary that is road |
# MAGIC | `meets_min_frontage_15m` | Longest frontage >= 15m |
# MAGIC
# MAGIC ### Key Spatial SQL Functions Used:
# MAGIC - `ST_Boundary(geometry)` - Get parcel boundary as linestring
# MAGIC - `ST_Buffer(geometry, distance)` - Create buffer around road
# MAGIC - `ST_Intersects(geom1, geom2)` - Test for intersection
# MAGIC - `ST_Intersection(geom1, geom2)` - Get shared geometry
# MAGIC - `ST_Length(geometry)` - Calculate line length
# MAGIC
# MAGIC ### Output Table
# MAGIC - `parcel_road_intersections` - Raw parcel-road intersection data
# MAGIC - `parcel_road_frontage` - Aggregated frontage features per parcel
# MAGIC
# MAGIC ### Next Steps
# MAGIC - **04_suitability_scoring.py** - Include frontage features in scoring model
