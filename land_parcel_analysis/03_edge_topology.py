# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 1: Parcel Edge Topology
# MAGIC
# MAGIC This notebook computes **edge topology features** for each land parcel using Databricks Spatial SQL.
# MAGIC
# MAGIC ## Features Computed:
# MAGIC | Feature | Description | Why It Matters |
# MAGIC |---------|-------------|----------------|
# MAGIC | **Road Frontages** | Number of distinct road edges | Corner lots have two; internal lots may have one |
# MAGIC | **Frontage Length** | Length along each road type | Affects access and planning requirements |
# MAGIC | **Shared Boundaries** | Length of edges shared with neighbours | More shared boundary = easier to consolidate |
# MAGIC | **Corner Lot** | Boolean: two or more road frontages | Corner lots have different setback rules |
# MAGIC | **Frontage-to-Perimeter Ratio** | Proportion of boundary that is road | Low ratio = mostly enclosed by other parcels |
# MAGIC
# MAGIC **Note:** This notebook requires road network data. If not available, it will compute adjacency features only.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

# MAGIC %pip install folium pydeck --quiet

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

# MAGIC %md
# MAGIC ## 1. Check Available Tables

# COMMAND ----------

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

# Check what tables are available
print("Available tables:")
display(spark.sql(f"SHOW TABLES IN {catalog_name}.{schema_name}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Identify Shared Boundaries Between Parcels
# MAGIC
# MAGIC Find which parcels share boundaries (adjacent parcels). This is the most important feature for consolidation analysis.

# COMMAND ----------

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

# For large datasets, we need to limit the scope to avoid cross-join explosion
# We'll use a sample or filter to a specific LGA for demonstration

# First, let's see the distribution by LGA
display(spark.sql(f"""
    SELECT
        lga_name,
        COUNT(*) AS parcel_count
    FROM {catalog_name}.{schema_name}.parcel_geometric_features
    GROUP BY lga_name
    ORDER BY parcel_count DESC
    LIMIT 20
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Adjacency Table for a Sample LGA
# MAGIC
# MAGIC Due to the large dataset size, we'll compute adjacency for a sample area first.

# COMMAND ----------

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

# Create adjacency table for parcels
# Find adjacent parcels using ST_Intersects on boundaries (share boundary but don't overlap)
# Limiting to a sample LGA for performance

# Use Casey LGA - Metro Melbourne area with good Activity Centre coverage
sample_lga = "CASEY"

# Verify the LGA exists
lga_check = spark.sql(f"""
    SELECT COUNT(*) AS cnt
    FROM {catalog_name}.{schema_name}.parcel_geometric_features
    WHERE lga_name = '{sample_lga}'
""").collect()[0]['cnt']

if lga_check == 0:
    print(f"WARNING: {sample_lga} not found. Falling back to most common LGA.")
    sample_lga = spark.sql(f"""
        SELECT lga_name FROM {catalog_name}.{schema_name}.parcel_geometric_features
        WHERE zone_code LIKE 'GRZ%' OR zone_code LIKE 'NRZ%'
        GROUP BY lga_name ORDER BY COUNT(*) DESC LIMIT 1
    """).collect()[0][0]

print(f"Computing adjacency for LGA: {sample_lga} ({lga_check:,} parcels)")

# COMMAND ----------

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

# ============================================================================
# ADJACENCY ANALYSIS: Find Neighboring Parcels with Shared Boundaries
# ============================================================================
# This identifies which parcels share a common boundary - critical for
# consolidation analysis since adjacent lots can potentially be merged.
#
# KEY SPATIAL FUNCTIONS:
#
# - ST_Boundary(geometry): Extracts the outline/perimeter of a parcel as a line
# - ST_Intersects(geom1, geom2): TRUE if geometries overlap or touch
# - ST_Intersection(geom1, geom2): Returns the geometry where two shapes meet
# - ST_Length(geometry): Calculates the length of a line in meters
#
# WHY THIS MATTERS FOR CONSOLIDATION:
# - Longer shared boundaries = easier to merge (fewer complications)
# - Same-zone neighbors = simpler planning approval process
# - This finds all parcel pairs that could potentially be consolidated
#
# NOTE: CROSS JOIN compares every parcel to every other parcel - we use
# filters to avoid duplicates and only keep pairs that actually touch.
# ============================================================================
spark.sql(f"""
    CREATE OR REPLACE TABLE {catalog_name}.{schema_name}.parcel_adjacency AS
    WITH sample_parcels AS (
        SELECT parcel_id, zone_code, geometry
        FROM {catalog_name}.{schema_name}.parcel_geometric_features
        WHERE lga_name = '{sample_lga}'
          AND geometry IS NOT NULL
    )
    SELECT
        p1.parcel_id AS parcel_1,
        p2.parcel_id AS parcel_2,
        p1.zone_code AS zone_1,
        p2.zone_code AS zone_2,
        -- Calculate shared boundary length
        -- Geometry is already in EPSG:3111 (VicGrid, metres) — no transform needed
        ROUND(
            ST_Length(
                ST_Intersection(ST_Boundary(p1.geometry), ST_Boundary(p2.geometry))
            ),
            2
        ) AS shared_boundary_m
    FROM sample_parcels p1
    CROSS JOIN sample_parcels p2
    WHERE p1.parcel_id < p2.parcel_id  -- Avoid duplicates and self-comparison
      AND ST_Intersects(p1.geometry, p2.geometry)
""")

print("Created parcel_adjacency table")
display(spark.sql(f"""
    SELECT * FROM {catalog_name}.{schema_name}.parcel_adjacency
    WHERE shared_boundary_m > 0
    ORDER BY shared_boundary_m DESC
    LIMIT 100
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Aggregate Adjacency Statistics per Parcel

# COMMAND ----------

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

# Aggregate adjacency statistics per parcel
display(spark.sql(f"""
    WITH adjacency_stats AS (
        SELECT
            parcel_id,
            COUNT(*) AS num_adjacent_parcels,
            SUM(shared_boundary_m) AS total_shared_boundary_m,
            MAX(shared_boundary_m) AS longest_shared_boundary_m,
            -- Count adjacent parcels in same zone
            SUM(CASE WHEN same_zone THEN 1 ELSE 0 END) AS adjacent_same_zone_count
        FROM (
            -- Union both directions of adjacency
            SELECT parcel_1 AS parcel_id, parcel_2 AS adjacent_id, shared_boundary_m,
                   zone_1 = zone_2 AS same_zone
            FROM {catalog_name}.{schema_name}.parcel_adjacency
            WHERE shared_boundary_m > 0
            UNION ALL
            SELECT parcel_2 AS parcel_id, parcel_1 AS adjacent_id, shared_boundary_m,
                   zone_1 = zone_2 AS same_zone
            FROM {catalog_name}.{schema_name}.parcel_adjacency
            WHERE shared_boundary_m > 0
        )
        GROUP BY parcel_id
    )
    SELECT
        p.parcel_id,
        p.zone_code,
        p.area_sqm,
        COALESCE(a.num_adjacent_parcels, 0) AS num_adjacent_parcels,
        COALESCE(a.total_shared_boundary_m, 0) AS total_shared_boundary_m,
        COALESCE(a.longest_shared_boundary_m, 0) AS longest_shared_boundary_m,
        COALESCE(a.adjacent_same_zone_count, 0) AS adjacent_same_zone_count
    FROM {catalog_name}.{schema_name}.parcel_geometric_features p
    LEFT JOIN adjacency_stats a ON p.parcel_id = a.parcel_id
    WHERE p.lga_name = '{sample_lga}'
    ORDER BY total_shared_boundary_m DESC
    LIMIT 100
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Create Comprehensive Edge Topology Table

# COMMAND ----------

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

# ============================================================================
# EDGE TOPOLOGY TABLE: Comprehensive Boundary Analysis for Each Parcel
# ============================================================================
# This creates a full picture of each parcel's boundary relationships:
#
# KEY METRICS EXPLAINED:
#
# - num_adjacent_parcels: How many neighbors does this lot have?
#   More neighbors = more consolidation options
#
# - total_shared_boundary_m: Sum of all boundaries shared with neighbors
#   Higher = more "embedded" in the neighborhood
#
# - longest_shared_boundary_m: The longest single shared edge
#   Important because this becomes the internal boundary after consolidation
#
# - adjacent_same_zone_count: Neighbors in the same planning zone
#   Same-zone consolidation is simpler from a planning perspective
#
# - estimated_frontage_ratio: (Perimeter - Shared) / Perimeter
#   Approximates how much boundary faces roads vs neighbors
#   Low ratio = "internal lot" mostly surrounded by other parcels
#   Clamped to [0, 1] to handle topology quirks where shared > perimeter
#
# - is_internal_lot: TRUE if >95% of boundary is shared with neighbors
#   A standard mid-block suburban lot shares ~83% of its perimeter
#   (two sides + rear), so 0.8 would misclassify most normal lots.
#   0.95 captures genuinely land-locked lots with no/minimal road frontage.
#
# - has_long_shared_boundary: TRUE if any shared boundary >15m
#   Indicates good consolidation potential
# ============================================================================
spark.sql(f"""
    CREATE OR REPLACE TABLE {catalog_name}.{schema_name}.parcel_edge_topology AS
    WITH adjacency_stats AS (
        SELECT
            parcel_id,
            COUNT(*) AS num_adjacent_parcels,
            SUM(shared_boundary_m) AS total_shared_boundary_m,
            MAX(shared_boundary_m) AS longest_shared_boundary_m,
            SUM(CASE WHEN same_zone THEN 1 ELSE 0 END) AS adjacent_same_zone_count
        FROM (
            SELECT parcel_1 AS parcel_id, parcel_2 AS adjacent_id, shared_boundary_m,
                   zone_1 = zone_2 AS same_zone
            FROM {catalog_name}.{schema_name}.parcel_adjacency
            WHERE shared_boundary_m > 0
            UNION ALL
            SELECT parcel_2 AS parcel_id, parcel_1 AS adjacent_id, shared_boundary_m,
                   zone_1 = zone_2 AS same_zone
            FROM {catalog_name}.{schema_name}.parcel_adjacency
            WHERE shared_boundary_m > 0
        )
        GROUP BY parcel_id
    )
    SELECT
        p.parcel_id,
        p.plan_number,
        p.lot_number,
        p.zone_code,
        p.zone_description,
        p.lga_code,
        p.lga_name,
        p.geometry,
        p.area_sqm,
        p.perimeter_m,
        p.compactness_index,
        p.aspect_ratio,
        p.elongation_index,
        p.below_min_area_300,
        p.below_min_area_500,
        p.narrow_lot,
        p.below_frontage_15m,
        p.is_sliver,
        p.hull_efficiency,
        p.centroid_lon,
        p.centroid_lat,

        -- Adjacency features
        COALESCE(a.num_adjacent_parcels, 0) AS num_adjacent_parcels,
        ROUND(COALESCE(a.total_shared_boundary_m, 0), 2) AS total_shared_boundary_m,
        ROUND(COALESCE(a.longest_shared_boundary_m, 0), 2) AS longest_shared_boundary_m,
        COALESCE(a.adjacent_same_zone_count, 0) AS adjacent_same_zone_count,

        -- Estimated frontage ratio: (perimeter - shared) / perimeter
        -- Clamped to [0, 1] — shared boundary can exceed perimeter due to
        -- topology quirks (overlapping neighbour boundaries at shared vertices)
        ROUND(
            GREATEST(
                CASE
                    WHEN p.perimeter_m > 0
                    THEN (p.perimeter_m - COALESCE(a.total_shared_boundary_m, 0)) / p.perimeter_m
                    ELSE 0
                END,
                0
            ),
            4
        ) AS estimated_frontage_ratio,

        -- Flags for edge topology characteristics
        COALESCE(a.num_adjacent_parcels, 0) = 0 AS is_isolated_lot,
        COALESCE(a.longest_shared_boundary_m, 0) > 15 AS has_long_shared_boundary,

        -- Internal lot indicator: >95% of boundary shared with neighbours
        -- A standard mid-block lot shares ~83% (two sides + rear = ~75m of ~90m),
        -- so 0.8 would flag most normal lots. 0.95 captures genuinely
        -- land-locked / battleaxe lots with no or minimal road frontage.
        CASE
            WHEN p.perimeter_m > 0 AND COALESCE(a.total_shared_boundary_m, 0) / p.perimeter_m > 0.95
            THEN TRUE
            ELSE FALSE
        END AS is_internal_lot

    FROM {catalog_name}.{schema_name}.parcel_geometric_features p
    LEFT JOIN adjacency_stats a ON p.parcel_id = a.parcel_id
    WHERE p.lga_name = '{sample_lga}'
""")

print("Created parcel_edge_topology table")

# COMMAND ----------

# Display edge topology features
catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

display(spark.sql(f"""
    SELECT
        parcel_id,
        zone_code,
        area_sqm,
        perimeter_m,
        num_adjacent_parcels,
        total_shared_boundary_m,
        longest_shared_boundary_m,
        adjacent_same_zone_count,
        estimated_frontage_ratio,
        is_internal_lot,
        has_long_shared_boundary
    FROM {catalog_name}.{schema_name}.parcel_edge_topology
    ORDER BY longest_shared_boundary_m DESC
    LIMIT 100
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Identify Consolidation Candidates Based on Edge Topology

# COMMAND ----------

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

# Identify consolidation candidates based on edge topology
display(spark.sql(f"""
    SELECT
        parcel_id,
        zone_code,
        area_sqm,
        num_adjacent_parcels,
        longest_shared_boundary_m,
        adjacent_same_zone_count,
        estimated_frontage_ratio,
        is_internal_lot,
        -- Edge topology based consolidation recommendation
        CASE
            WHEN below_min_area_500 AND num_adjacent_parcels > 0 THEN 'High Priority - Below min area with neighbors'
            WHEN is_internal_lot AND num_adjacent_parcels > 0 THEN 'High Priority - Internal lot'
            WHEN num_adjacent_parcels >= 2 AND longest_shared_boundary_m > 10 THEN 'Medium Priority - Good consolidation potential'
            WHEN narrow_lot AND num_adjacent_parcels > 0 THEN 'Medium Priority - Narrow lot with neighbors'
            ELSE 'Low Priority'
        END AS consolidation_recommendation,
        -- Consolidation potential description
        CASE
            WHEN num_adjacent_parcels > 0 AND adjacent_same_zone_count > 0
            THEN CONCAT('Has ', adjacent_same_zone_count, ' same-zone neighbors')
            WHEN num_adjacent_parcels > 0
            THEN CONCAT('Has ', num_adjacent_parcels, ' neighbors (different zones)')
            ELSE 'No adjacent parcels'
        END AS consolidation_potential
    FROM {catalog_name}.{schema_name}.parcel_edge_topology
    ORDER BY
        CASE
            WHEN below_min_area_500 AND num_adjacent_parcels > 0 THEN 1
            WHEN is_internal_lot AND num_adjacent_parcels > 0 THEN 2
            WHEN num_adjacent_parcels >= 2 AND longest_shared_boundary_m > 10 THEN 3
            ELSE 4
        END,
        longest_shared_boundary_m DESC
    LIMIT 100
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Find Best Consolidation Pairs

# COMMAND ----------

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

# Find the best consolidation pairs (adjacent parcels with longest shared boundary)
display(spark.sql(f"""
    SELECT
        a.parcel_1,
        a.parcel_2,
        a.zone_1,
        a.zone_2,
        ROUND(a.shared_boundary_m, 2) AS shared_boundary_m,
        CASE WHEN a.zone_1 = a.zone_2 THEN 'Same Zone' ELSE 'Different Zone' END AS zone_match,
        -- Get area info for both parcels
        ROUND(p1.area_sqm, 2) AS area_1_sqm,
        ROUND(p2.area_sqm, 2) AS area_2_sqm,
        ROUND(p1.area_sqm + p2.area_sqm, 2) AS combined_area_sqm,
        -- Consolidation score based on shared boundary length
        CASE
            WHEN a.shared_boundary_m >= 20 AND a.zone_1 = a.zone_2 THEN 'Excellent'
            WHEN a.shared_boundary_m >= 15 AND a.zone_1 = a.zone_2 THEN 'Very Good'
            WHEN a.shared_boundary_m >= 10 THEN 'Good'
            WHEN a.shared_boundary_m >= 5 THEN 'Fair'
            ELSE 'Poor'
        END AS consolidation_quality
    FROM {catalog_name}.{schema_name}.parcel_adjacency a
    JOIN {catalog_name}.{schema_name}.parcel_geometric_features p1 ON a.parcel_1 = p1.parcel_id
    JOIN {catalog_name}.{schema_name}.parcel_geometric_features p2 ON a.parcel_2 = p2.parcel_id
    WHERE a.shared_boundary_m > 0
    ORDER BY a.shared_boundary_m DESC
    LIMIT 100
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Summary Statistics

# COMMAND ----------

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

# Summary statistics for edge topology
display(spark.sql(f"""
    SELECT
        zone_code,
        COUNT(*) AS parcel_count,
        ROUND(AVG(num_adjacent_parcels), 2) AS avg_neighbors,
        ROUND(AVG(total_shared_boundary_m), 2) AS avg_shared_boundary_m,
        ROUND(AVG(longest_shared_boundary_m), 2) AS avg_longest_shared_m,
        SUM(CASE WHEN is_internal_lot THEN 1 ELSE 0 END) AS internal_lot_count,
        SUM(CASE WHEN has_long_shared_boundary THEN 1 ELSE 0 END) AS good_consolidation_count
    FROM {catalog_name}.{schema_name}.parcel_edge_topology
    WHERE zone_code IS NOT NULL
    GROUP BY zone_code
    ORDER BY parcel_count DESC
    LIMIT 20
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### AI-Generated Summary of Edge Topology

# COMMAND ----------

# Use ai_query() to generate a human-readable summary of edge topology statistics
catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")
ai_model = dbutils.widgets.get("ai_model")

topology_summary = spark.sql(f"""
    WITH topology_stats AS (
        SELECT
            zone_code,
            COUNT(*) AS parcel_count,
            ROUND(AVG(num_adjacent_parcels), 2) AS avg_neighbors,
            ROUND(AVG(total_shared_boundary_m), 2) AS avg_shared_boundary_m,
            ROUND(AVG(longest_shared_boundary_m), 2) AS avg_longest_shared_m,
            SUM(CASE WHEN is_internal_lot THEN 1 ELSE 0 END) AS internal_lot_count,
            SUM(CASE WHEN is_isolated_lot THEN 1 ELSE 0 END) AS isolated_lot_count,
            SUM(CASE WHEN has_long_shared_boundary THEN 1 ELSE 0 END) AS good_consolidation_count,
            ROUND(AVG(estimated_frontage_ratio), 2) AS avg_frontage_ratio
        FROM {catalog_name}.{schema_name}.parcel_edge_topology
        WHERE zone_code IS NOT NULL
        GROUP BY zone_code
        ORDER BY parcel_count DESC
        LIMIT 10
    )
    SELECT ai_query(
        '{ai_model}',
        CONCAT(
            'You are a Victorian land use planning analyst specializing in site consolidation. Analyze these edge topology statistics and provide a concise 3-4 paragraph summary. ',
            'Focus on: (1) which zones have the best consolidation potential based on shared boundaries, (2) the prevalence of internal lots vs isolated lots, ',
            '(3) average neighbor counts and what that means for consolidation, (4) specific recommendations for which zones should be prioritized for site amalgamation. ',
            'Be specific with numbers and percentages. Data: ',
            TO_JSON(COLLECT_LIST(STRUCT(*)))
        )
    ) AS topology_analysis_summary
    FROM topology_stats
""")

# Display the ai_query result with proper markdown rendering
from IPython.display import display, Markdown, HTML

result_df = topology_summary.toPandas()
if len(result_df) > 0:
    summary_text = result_df['topology_analysis_summary'].iloc[0]
    display(Markdown(summary_text))
else:
    print("No summary generated")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Visualize Adjacent Parcels with Folium
# MAGIC
# MAGIC Visualize parcels and their shared boundaries on an interactive map.

# COMMAND ----------

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

# Get sample adjacent parcel pairs for visualization - transform to WGS84
adjacency_pairs_df = spark.sql(f"""
    SELECT
        a.parcel_1,
        a.parcel_2,
        a.zone_1,
        a.zone_2,
        ROUND(a.shared_boundary_m, 2) AS shared_boundary_m,
        ST_AsGeoJSON(ST_Transform(p1.geometry, 4326)) AS geojson_1,
        ST_AsGeoJSON(ST_Transform(p2.geometry, 4326)) AS geojson_2,
        ST_X(ST_Centroid(ST_Transform(p1.geometry, 4326))) AS centroid_lon_1,
        ST_Y(ST_Centroid(ST_Transform(p1.geometry, 4326))) AS centroid_lat_1,
        ST_X(ST_Centroid(ST_Transform(p2.geometry, 4326))) AS centroid_lon_2,
        ST_Y(ST_Centroid(ST_Transform(p2.geometry, 4326))) AS centroid_lat_2,
        -- Get shared boundary as GeoJSON (transform to WGS84)
        ST_AsGeoJSON(ST_Transform(ST_Intersection(ST_Boundary(p1.geometry), ST_Boundary(p2.geometry)), 4326)) AS shared_boundary_geojson
    FROM {catalog_name}.{schema_name}.parcel_adjacency a
    JOIN {catalog_name}.{schema_name}.parcel_edge_topology p1 ON a.parcel_1 = p1.parcel_id
    JOIN {catalog_name}.{schema_name}.parcel_edge_topology p2 ON a.parcel_2 = p2.parcel_id
    WHERE a.shared_boundary_m > 10  -- Focus on pairs with long shared boundaries
    ORDER BY a.shared_boundary_m DESC
    LIMIT 50
""")

adjacency_pairs = adjacency_pairs_df.toPandas()
print(f"Retrieved {len(adjacency_pairs)} adjacency pairs for visualization")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Folium Map: Adjacent Parcels with Shared Boundaries
# MAGIC
# MAGIC This map shows pairs of adjacent parcels with their shared boundaries highlighted in red.

# COMMAND ----------

import folium
import json

# Create map centered on the first pair
if len(adjacency_pairs) > 0:
    center_lat = adjacency_pairs['centroid_lat_1'].mean()
    center_lon = adjacency_pairs['centroid_lon_1'].mean()

    # Create the map
    m = folium.Map(
        location=[center_lat, center_lon],
        zoom_start=16,
        tiles='CartoDB positron'
    )

    # Color palette for parcel pairs
    pair_colors = ['#3388ff', '#33ff88', '#ff8833', '#8833ff', '#ff3388', '#88ff33']

    for idx, row in adjacency_pairs.iterrows():
        color = pair_colors[idx % len(pair_colors)]

        # Add first parcel
        try:
            geojson_1 = json.loads(row['geojson_1'])
            folium.GeoJson(
                geojson_1,
                style_function=lambda x, c=color: {
                    'fillColor': c,
                    'color': c,
                    'weight': 2,
                    'fillOpacity': 0.3
                },
                tooltip=f"Parcel 1: {row['parcel_1']}<br>Zone: {row['zone_1']}"
            ).add_to(m)
        except:
            pass

        # Add second parcel
        try:
            geojson_2 = json.loads(row['geojson_2'])
            folium.GeoJson(
                geojson_2,
                style_function=lambda x, c=color: {
                    'fillColor': c,
                    'color': c,
                    'weight': 2,
                    'fillOpacity': 0.3
                },
                tooltip=f"Parcel 2: {row['parcel_2']}<br>Zone: {row['zone_2']}"
            ).add_to(m)
        except:
            pass

        # Add shared boundary line (highlighted in red)
        try:
            if row['shared_boundary_geojson']:
                shared_geom = json.loads(row['shared_boundary_geojson'])
                folium.GeoJson(
                    shared_geom,
                    style_function=lambda x: {
                        'color': 'red',
                        'weight': 4,
                        'opacity': 0.8
                    },
                    tooltip=f"Shared Boundary: {row['shared_boundary_m']}m"
                ).add_to(m)
        except:
            pass

    # Add legend
    legend_html = '''
    <div style="position: fixed;
                bottom: 50px; left: 50px; width: 200px; height: 90px;
                border:2px solid grey; z-index:9999; font-size:14px;
                background-color:white; padding: 10px;
                border-radius: 5px;">
        <b>Edge Topology Legend</b><br>
        <i style="background:rgba(51,136,255,0.3); width:18px; height:18px; display:inline-block;"></i> Parcel Pairs<br>
        <i style="background:red; width:18px; height:3px; display:inline-block;"></i> Shared Boundary
    </div>
    '''
    m.get_root().html.add_child(folium.Element(legend_html))

    # Use displayHTML for proper rendering in Databricks
    displayHTML(m._repr_html_())
else:
    print("No adjacency pairs found for visualization")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Export Full Adjacency Pairs Map to UC Volume

# COMMAND ----------

# Export adjacency pairs for target LGA to Folium HTML (filtered to avoid OOM)
import folium
import json

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

# Use same sample_lga as earlier in the notebook
# Get adjacent parcel pairs filtered by LGA
all_adjacency_pairs = spark.sql(f"""
    SELECT
        a.parcel_1,
        a.parcel_2,
        a.zone_1,
        a.zone_2,
        ROUND(a.shared_boundary_m, 2) AS shared_boundary_m,
        ST_AsGeoJSON(ST_Transform(p1.geometry, 4326)) AS geojson_1,
        ST_AsGeoJSON(ST_Transform(p2.geometry, 4326)) AS geojson_2,
        ST_X(ST_Centroid(ST_Transform(p1.geometry, 4326))) AS centroid_lon_1,
        ST_Y(ST_Centroid(ST_Transform(p1.geometry, 4326))) AS centroid_lat_1,
        ST_AsGeoJSON(ST_Transform(ST_Intersection(ST_Boundary(p1.geometry), ST_Boundary(p2.geometry)), 4326)) AS shared_boundary_geojson
    FROM {catalog_name}.{schema_name}.parcel_adjacency a
    JOIN {catalog_name}.{schema_name}.parcel_edge_topology p1 ON a.parcel_1 = p1.parcel_id
    JOIN {catalog_name}.{schema_name}.parcel_edge_topology p2 ON a.parcel_2 = p2.parcel_id
    WHERE a.shared_boundary_m > 5
      AND p1.lga_name = '{sample_lga}'
    ORDER BY a.shared_boundary_m DESC
""").toPandas()

print(f"Loaded {len(all_adjacency_pairs)} adjacency pairs for full export")

if len(all_adjacency_pairs) > 0:
    center_lat = all_adjacency_pairs['centroid_lat_1'].mean()
    center_lon = all_adjacency_pairs['centroid_lon_1'].mean()

    m_adj_full = folium.Map(
        location=[center_lat, center_lon],
        zoom_start=14,
        tiles='CartoDB positron'
    )

    pair_colors = ['#3388ff', '#33ff88', '#ff8833', '#8833ff', '#ff3388', '#88ff33']

    for idx, row in all_adjacency_pairs.iterrows():
        color = pair_colors[idx % len(pair_colors)]
        try:
            geojson_1 = json.loads(row['geojson_1'])
            folium.GeoJson(
                geojson_1,
                style_function=lambda x, c=color: {'fillColor': c, 'color': c, 'weight': 1, 'fillOpacity': 0.3},
                tooltip=f"Parcel: {row['parcel_1']}<br>Zone: {row['zone_1']}<br>Shared: {row['shared_boundary_m']}m"
            ).add_to(m_adj_full)
        except:
            pass

        try:
            geojson_2 = json.loads(row['geojson_2'])
            folium.GeoJson(
                geojson_2,
                style_function=lambda x, c=color: {'fillColor': c, 'color': c, 'weight': 1, 'fillOpacity': 0.3},
                tooltip=f"Parcel: {row['parcel_2']}<br>Zone: {row['zone_2']}<br>Shared: {row['shared_boundary_m']}m"
            ).add_to(m_adj_full)
        except:
            pass

        try:
            if row['shared_boundary_geojson']:
                shared_geom = json.loads(row['shared_boundary_geojson'])
                folium.GeoJson(
                    shared_geom,
                    style_function=lambda x: {'color': 'red', 'weight': 3, 'opacity': 0.8},
                    tooltip=f"Shared Boundary: {row['shared_boundary_m']}m"
                ).add_to(m_adj_full)
        except:
            pass

    legend_html = f'''
    <div style="position: fixed; bottom: 50px; left: 50px; width: 200px;
                border:2px solid grey; z-index:9999; font-size:14px;
                background-color:white; padding: 10px; border-radius: 5px;">
        <b>All Adjacency Pairs</b><br>
        <i style="background:rgba(51,136,255,0.3); width:18px; height:18px; display:inline-block;"></i> Parcel Pairs<br>
        <i style="background:red; width:18px; height:3px; display:inline-block;"></i> Shared Boundary<br>
        <small>Total pairs: {len(all_adjacency_pairs):,}</small>
    </div>
    '''
    m_adj_full.get_root().html.add_child(folium.Element(legend_html))

    # Save to UC Volume
    volume_path = f"/Volumes/{catalog_name}/{schema_name}/source"
    adj_html_path = f"{volume_path}/visualizations/folium_adjacency_pairs_{sample_lga.replace(' ', '_')}.html"

    import os
    os.makedirs(f"{volume_path}/visualizations", exist_ok=True)

    m_adj_full.save(adj_html_path)
    print(f"Adjacency pairs map for {sample_lga} saved to: {adj_html_path}")
    print(f"Total pairs exported: {len(all_adjacency_pairs):,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Folium Map: Internal Lots vs Normal Lots
# MAGIC
# MAGIC Visualize parcels by their topology classification - internal lots (high shared boundary ratio) vs normal lots.

# COMMAND ----------

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

# Get parcels for topology classification visualization
topology_df = spark.sql(f"""
    SELECT
        parcel_id,
        zone_code,
        area_sqm,
        num_adjacent_parcels,
        total_shared_boundary_m,
        longest_shared_boundary_m,
        estimated_frontage_ratio,
        is_internal_lot,
        is_isolated_lot,
        has_long_shared_boundary,
        centroid_lon,
        centroid_lat,
        ST_AsGeoJSON(ST_Transform(geometry, 4326)) AS geojson
    FROM {catalog_name}.{schema_name}.parcel_edge_topology
    WHERE geometry IS NOT NULL
      AND centroid_lon IS NOT NULL
      AND (is_internal_lot = TRUE OR is_isolated_lot = TRUE OR has_long_shared_boundary = TRUE)
    ORDER BY total_shared_boundary_m DESC
    LIMIT 200
""")

topology_parcels = topology_df.toPandas()
print(f"Retrieved {len(topology_parcels)} parcels with notable topology characteristics")

# COMMAND ----------

import folium
import json

if len(topology_parcels) > 0:
    # Create map
    center_lat = topology_parcels['centroid_lat'].mean()
    center_lon = topology_parcels['centroid_lon'].mean()

    m = folium.Map(
        location=[center_lat, center_lon],
        zoom_start=15,
        tiles='CartoDB positron'
    )

    # Add parcels colored by topology type
    for _, row in topology_parcels.iterrows():
        try:
            geojson = json.loads(row['geojson'])

            # Color based on topology classification
            if row['is_internal_lot']:
                color = '#e31a1c'  # Red for internal lots
                category = "Internal Lot"
            elif row['is_isolated_lot']:
                color = '#ff7f00'  # Orange for isolated lots
                category = "Isolated Lot"
            elif row['has_long_shared_boundary']:
                color = '#33a02c'  # Green for good consolidation potential
                category = "Long Shared Boundary"
            else:
                color = '#1f78b4'  # Blue for others
                category = "Normal"

            tooltip = f"""
                <b>Parcel:</b> {row['parcel_id']}<br>
                <b>Zone:</b> {row['zone_code']}<br>
                <b>Area:</b> {row['area_sqm']:.0f} sqm<br>
                <b>Neighbors:</b> {row['num_adjacent_parcels']}<br>
                <b>Shared Boundary:</b> {row['total_shared_boundary_m']:.1f}m<br>
                <b>Type:</b> {category}
            """

            folium.GeoJson(
                geojson,
                style_function=lambda x, c=color: {
                    'fillColor': c,
                    'color': c,
                    'weight': 1,
                    'fillOpacity': 0.5
                },
                tooltip=folium.Tooltip(tooltip)
            ).add_to(m)
        except:
            pass

    # Add legend
    legend_html = '''
    <div style="position: fixed;
                bottom: 50px; left: 50px; width: 220px; height: 130px;
                border:2px solid grey; z-index:9999; font-size:14px;
                background-color:white; padding: 10px;
                border-radius: 5px;">
        <b>Topology Classification</b><br>
        <i style="background:#e31a1c; width:18px; height:18px; display:inline-block;"></i> Internal Lot (>95% shared)<br>
        <i style="background:#ff7f00; width:18px; height:18px; display:inline-block;"></i> Isolated Lot (no neighbors)<br>
        <i style="background:#33a02c; width:18px; height:18px; display:inline-block;"></i> Long Shared Boundary (>15m)<br>
        <i style="background:#1f78b4; width:18px; height:18px; display:inline-block;"></i> Normal
    </div>
    '''
    m.get_root().html.add_child(folium.Element(legend_html))

    # Use displayHTML for proper rendering in Databricks
    displayHTML(m._repr_html_())
else:
    print("No parcels found for topology visualization")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Export Full Topology Classification Map to UC Volume

# COMMAND ----------

# Export topology classified parcels for target LGA to Folium HTML (filtered to avoid OOM)
import folium
import json

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

# Get parcels with topology features filtered by LGA
all_topology_parcels = spark.sql(f"""
    SELECT
        parcel_id,
        zone_code,
        area_sqm,
        num_adjacent_parcels,
        total_shared_boundary_m,
        longest_shared_boundary_m,
        estimated_frontage_ratio,
        is_internal_lot,
        is_isolated_lot,
        has_long_shared_boundary,
        centroid_lon,
        centroid_lat,
        ST_AsGeoJSON(ST_Transform(geometry, 4326)) AS geojson
    FROM {catalog_name}.{schema_name}.parcel_edge_topology
    WHERE geometry IS NOT NULL
      AND centroid_lon IS NOT NULL
      AND lga_name = '{sample_lga}'
    ORDER BY total_shared_boundary_m DESC
""").toPandas()

print(f"Loaded {len(all_topology_parcels)} parcels for full topology export")

if len(all_topology_parcels) > 0:
    center_lat = all_topology_parcels['centroid_lat'].mean()
    center_lon = all_topology_parcels['centroid_lon'].mean()

    m_topo_full = folium.Map(
        location=[center_lat, center_lon],
        zoom_start=13,
        tiles='CartoDB positron'
    )

    # Count by category
    internal_count = all_topology_parcels['is_internal_lot'].sum()
    isolated_count = all_topology_parcels['is_isolated_lot'].sum()
    long_boundary_count = all_topology_parcels['has_long_shared_boundary'].sum()

    for _, row in all_topology_parcels.iterrows():
        try:
            geojson = json.loads(row['geojson'])

            if row['is_internal_lot']:
                color = '#e31a1c'
                category = "Internal Lot"
            elif row['is_isolated_lot']:
                color = '#ff7f00'
                category = "Isolated Lot"
            elif row['has_long_shared_boundary']:
                color = '#33a02c'
                category = "Long Shared Boundary"
            else:
                color = '#1f78b4'
                category = "Normal"

            folium.GeoJson(
                geojson,
                style_function=lambda x, c=color: {'fillColor': c, 'color': c, 'weight': 0.5, 'fillOpacity': 0.5},
                tooltip=f"Parcel: {row['parcel_id']}<br>Zone: {row['zone_code']}<br>Type: {category}<br>Neighbors: {row['num_adjacent_parcels']}"
            ).add_to(m_topo_full)
        except:
            pass

    legend_html = f'''
    <div style="position: fixed; bottom: 50px; left: 50px; width: 250px;
                border:2px solid grey; z-index:9999; font-size:14px;
                background-color:white; padding: 10px; border-radius: 5px;">
        <b>Topology Classification (Full)</b><br>
        <i style="background:#e31a1c; width:18px; height:18px; display:inline-block;"></i> Internal Lot ({internal_count:,})<br>
        <i style="background:#ff7f00; width:18px; height:18px; display:inline-block;"></i> Isolated Lot ({isolated_count:,})<br>
        <i style="background:#33a02c; width:18px; height:18px; display:inline-block;"></i> Long Shared Boundary ({long_boundary_count:,})<br>
        <i style="background:#1f78b4; width:18px; height:18px; display:inline-block;"></i> Normal<br>
        <small>Total: {len(all_topology_parcels):,}</small>
    </div>
    '''
    m_topo_full.get_root().html.add_child(folium.Element(legend_html))

    # Save to UC Volume
    volume_path = f"/Volumes/{catalog_name}/{schema_name}/source"
    topo_html_path = f"{volume_path}/visualizations/folium_topology_classification_{sample_lga.replace(' ', '_')}.html"

    m_topo_full.save(topo_html_path)
    print(f"Topology classification map for {sample_lga} saved to: {topo_html_path}")
    print(f"Breakdown: Internal={internal_count}, Isolated={isolated_count}, Long Boundary={long_boundary_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### PyDeck 3D Visualization: Adjacency Density
# MAGIC
# MAGIC 3D column chart showing parcels with height proportional to number of adjacent parcels.

# COMMAND ----------

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

# Get data for 3D visualization
adjacency_3d_df = spark.sql(f"""
    SELECT
        parcel_id,
        centroid_lon,
        centroid_lat,
        num_adjacent_parcels,
        total_shared_boundary_m,
        area_sqm
    FROM {catalog_name}.{schema_name}.parcel_edge_topology
    WHERE centroid_lon IS NOT NULL
      AND num_adjacent_parcels > 0
    LIMIT 300
""")

adjacency_3d = adjacency_3d_df.toPandas()
print(f"Retrieved {len(adjacency_3d)} parcels for 3D visualization")

# COMMAND ----------

import pydeck as pdk

if len(adjacency_3d) > 0:
    # Prepare data for pydeck
    adjacency_3d['elevation'] = adjacency_3d['num_adjacent_parcels'] * 50  # Scale for visibility

    # Color based on total shared boundary
    def get_color(shared_boundary):
        if shared_boundary > 50:
            return [227, 26, 28, 180]  # Red
        elif shared_boundary > 30:
            return [255, 127, 0, 180]  # Orange
        elif shared_boundary > 15:
            return [255, 255, 51, 180]  # Yellow
        else:
            return [51, 160, 44, 180]  # Green

    adjacency_3d['color'] = adjacency_3d['total_shared_boundary_m'].apply(get_color)

    # Create the deck
    layer = pdk.Layer(
        "ColumnLayer",
        data=adjacency_3d,
        get_position=["centroid_lon", "centroid_lat"],
        get_elevation="elevation",
        elevation_scale=1,
        radius=15,
        get_fill_color="color",
        pickable=True,
        auto_highlight=True
    )

    view_state = pdk.ViewState(
        latitude=adjacency_3d['centroid_lat'].mean(),
        longitude=adjacency_3d['centroid_lon'].mean(),
        zoom=14,
        pitch=45,
        bearing=0
    )

    deck = pdk.Deck(
        layers=[layer],
        initial_view_state=view_state,
        tooltip={
            "html": "<b>Parcel:</b> {parcel_id}<br/>"
                    "<b>Neighbors:</b> {num_adjacent_parcels}<br/>"
                    "<b>Total Shared Boundary:</b> {total_shared_boundary_m:.1f}m<br/>"
                    "<b>Area:</b> {area_sqm:.0f} sqm",
            "style": {"backgroundColor": "steelblue", "color": "white"}
        }
    )

    display(deck)
else:
    print("No data available for 3D visualization")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Export Full 3D Adjacency Map to UC Volume

# COMMAND ----------

# Export parcels with adjacency data for target LGA as PyDeck 3D HTML (filtered to avoid OOM)
import pydeck as pdk

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

# Get parcels with adjacency filtered by LGA
all_adjacency_3d = spark.sql(f"""
    SELECT
        parcel_id,
        centroid_lon,
        centroid_lat,
        num_adjacent_parcels,
        total_shared_boundary_m,
        area_sqm
    FROM {catalog_name}.{schema_name}.parcel_edge_topology
    WHERE centroid_lon IS NOT NULL
      AND num_adjacent_parcels > 0
      AND lga_name = '{sample_lga}'
    ORDER BY num_adjacent_parcels DESC
""").toPandas()

print(f"Loaded {len(all_adjacency_3d)} parcels for full 3D export")

if len(all_adjacency_3d) > 0:
    # Prepare data
    all_adjacency_3d['elevation'] = all_adjacency_3d['num_adjacent_parcels'] * 50

    def get_color(shared_boundary):
        if shared_boundary > 50:
            return [227, 26, 28, 180]
        elif shared_boundary > 30:
            return [255, 127, 0, 180]
        elif shared_boundary > 15:
            return [255, 255, 51, 180]
        else:
            return [51, 160, 44, 180]

    all_adjacency_3d['color'] = all_adjacency_3d['total_shared_boundary_m'].apply(get_color)

    layer = pdk.Layer(
        "ColumnLayer",
        data=all_adjacency_3d,
        get_position=["centroid_lon", "centroid_lat"],
        get_elevation="elevation",
        elevation_scale=1,
        radius=10,
        get_fill_color="color",
        pickable=True,
        auto_highlight=True
    )

    view_state = pdk.ViewState(
        latitude=all_adjacency_3d['centroid_lat'].mean(),
        longitude=all_adjacency_3d['centroid_lon'].mean(),
        zoom=13,
        pitch=45,
        bearing=0
    )

    deck_full = pdk.Deck(
        layers=[layer],
        initial_view_state=view_state,
        tooltip={
            "html": "<b>Parcel:</b> {parcel_id}<br/>"
                    "<b>Neighbors:</b> {num_adjacent_parcels}<br/>"
                    "<b>Shared Boundary:</b> {total_shared_boundary_m:.1f}m<br/>"
                    "<b>Area:</b> {area_sqm:.0f} sqm",
            "style": {"backgroundColor": "steelblue", "color": "white"}
        }
    )

    # Save to UC Volume
    volume_path = f"/Volumes/{catalog_name}/{schema_name}/source"
    pydeck_3d_path = f"{volume_path}/visualizations/pydeck_adjacency_3d_{sample_lga.replace(' ', '_')}.html"

    deck_full.to_html(pydeck_3d_path)
    print(f"3D adjacency map for {sample_lga} saved to: {pydeck_3d_path}")
    print(f"Total parcels exported: {len(all_adjacency_3d):,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC This notebook computed the following edge topology features:
# MAGIC
# MAGIC | Feature | Description |
# MAGIC |---------|-------------|
# MAGIC | `num_adjacent_parcels` | Number of neighbouring parcels |
# MAGIC | `total_shared_boundary_m` | Total length shared with neighbours |
# MAGIC | `longest_shared_boundary_m` | Longest shared boundary |
# MAGIC | `adjacent_same_zone_count` | Neighbors in the same zone |
# MAGIC | `estimated_frontage_ratio` | Estimated proportion of boundary that is road (clamped to 0-1) |
# MAGIC | `is_internal_lot` | High shared boundary ratio (>95%) — genuinely land-locked lots |
# MAGIC | `has_long_shared_boundary` | Has shared boundary > 15m |
# MAGIC | `is_sliver` | Propagated from geometric features — data quality flag |
# MAGIC | `hull_efficiency` | Propagated from geometric features — concavity measure |
# MAGIC
# MAGIC ### Key Spatial SQL Functions Used:
# MAGIC - `ST_Boundary(geometry)` - Get parcel boundary as linestring
# MAGIC - `ST_Intersection(geom1, geom2)` - Get shared geometry
# MAGIC - `ST_Intersects(geom1, geom2)` - Check if geometries intersect
# MAGIC - `ST_Length(geometry)` - Calculate line length
# MAGIC
# MAGIC ### Output Tables
# MAGIC - `parcel_adjacency` - Pairwise adjacency relationships
# MAGIC - `parcel_edge_topology` - All edge topology features per parcel
# MAGIC
# MAGIC ### Next Steps
# MAGIC - **04_suitability_scoring.py** - Combine all features for multi-criteria scoring
