# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 2: Multi-Criteria Suitability Scoring
# MAGIC
# MAGIC This notebook combines all features to **score each parcel** on its site consolidation suitability.
# MAGIC
# MAGIC ## Scoring Framework:
# MAGIC
# MAGIC ### Opportunities (Increase Score)
# MAGIC - Within HCTZ zone or close to PT nodes
# MAGIC - Below minimum site area threshold
# MAGIC - Below minimum frontage requirement
# MAGIC - Irregular or inefficient shape
# MAGIC - Strong adjacency (long shared boundaries)
# MAGIC
# MAGIC ### Constraints (Decrease Score)
# MAGIC - Heritage overlay
# MAGIC - Flood overlay (LSIO/SBO)
# MAGIC - Environmental significance overlay
# MAGIC - Corner lot (strategic value, less need to consolidate)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

# MAGIC %pip install folium keplergl pydeck --quiet

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

# Check what tables are available
print("Available tables:")
display(spark.sql(f"SHOW TABLES IN {catalog_name}.{schema_name}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Calculate Proximity to PT Stops

# COMMAND ----------

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

# Check if PT stops table exists and has the expected schema
print("PT Stops schema:")
display(spark.sql(f"DESCRIBE {catalog_name}.{schema_name}.pt_stops"))

# COMMAND ----------

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

# ============================================================================
# PUBLIC TRANSPORT PROXIMITY: Distance to Train, Tram, and Bus Stops
# ============================================================================
# Victorian planning (especially HCTZ - Housing in Centres & Transit Zones)
# encourages higher density near public transport.
#
# KEY SPATIAL FUNCTION:
# - ST_DistanceSphere(point1, point2): Straight-line distance in meters
#   Uses spherical Earth model for accuracy over longer distances
#   Both points must be in WGS84 (EPSG:4326) coordinates
#
# HCTZ ELIGIBILITY THRESHOLDS:
# - 800m from train station: "Walk-up" distance, premium for development
# - 800m from tram stop: Similar premium
# - 400m from bus stop: Shorter distance reflects lower service level
#
# This creates proximity flags for each parcel that feed into the
# suitability scoring model.
# ============================================================================
spark.sql(f"""
    CREATE OR REPLACE TABLE {catalog_name}.{schema_name}.parcel_pt_proximity AS
    WITH parcel_centroids AS (
        SELECT
            parcel_id,
            centroid_lon,
            centroid_lat,
            -- Create point with SRID 4326 to match PT stops geometry
            ST_SetSRID(ST_Point(centroid_lon, centroid_lat), 4326) AS centroid
        FROM {catalog_name}.{schema_name}.parcel_edge_topology
        WHERE centroid_lon IS NOT NULL AND centroid_lat IS NOT NULL
    ),
    pt_distances AS (
        SELECT
            p.parcel_id,
            pt.MODE AS mode,
            -- Distance in meters using spherical calculation
            ROUND(
                ST_DistanceSphere(p.centroid, pt.geometry),
                2
            ) AS distance_m
        FROM parcel_centroids p
        CROSS JOIN {catalog_name}.{schema_name}.pt_stops pt
    ),
    nearest_by_mode AS (
        SELECT
            parcel_id,
            mode,
            MIN(distance_m) AS nearest_distance_m
        FROM pt_distances
        GROUP BY parcel_id, mode
    )
    SELECT
        parcel_id,
        MAX(CASE WHEN mode LIKE '%Train%' THEN nearest_distance_m END) AS nearest_train_m,
        MAX(CASE WHEN mode LIKE '%Tram%' THEN nearest_distance_m END) AS nearest_tram_m,
        MAX(CASE WHEN mode LIKE '%Bus%' THEN nearest_distance_m END) AS nearest_bus_m,
        MIN(nearest_distance_m) AS nearest_any_pt_m,
        -- HCTZ eligibility flags
        MAX(CASE WHEN mode LIKE '%Train%' THEN nearest_distance_m END) <= 800 AS within_800m_train,
        MAX(CASE WHEN mode LIKE '%Tram%' THEN nearest_distance_m END) <= 800 AS within_800m_tram,
        MAX(CASE WHEN mode LIKE '%Bus%' THEN nearest_distance_m END) <= 400 AS within_400m_bus
    FROM nearest_by_mode
    GROUP BY parcel_id
""")

print("Created parcel_pt_proximity table")
display(spark.sql(f"SELECT * FROM {catalog_name}.{schema_name}.parcel_pt_proximity LIMIT 20"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Check for Overlay Data (Heritage, Flood, ESO)

# COMMAND ----------

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

# Check if overlays table exists
try:
    overlay_count = spark.sql(f"SELECT COUNT(*) FROM {catalog_name}.{schema_name}.overlays").collect()[0][0]
    print(f"Overlays table has {overlay_count:,} records")

    # Show overlay types
    print("\nOverlay types (ZONE_CODE):")
    display(spark.sql(f"""
        SELECT ZONE_CODE, COUNT(*) AS count
        FROM {catalog_name}.{schema_name}.overlays
        GROUP BY ZONE_CODE
        ORDER BY count DESC
        LIMIT 30
    """))
except Exception as e:
    print(f"Overlays table not available: {e}")
    print("Overlay constraints will not be included in scoring.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Define Scoring Weights

# COMMAND ----------

# ============================================================================
# MULTI-CRITERIA SCORING WEIGHTS
# ============================================================================
# This is a weighted scoring model that ranks parcels by consolidation potential.
# Higher total score = better candidate for site consolidation.
#
# HOW IT WORKS:
# - Each parcel gets points added/subtracted based on its characteristics
# - Opportunity factors ADD points (these parcels need/benefit from consolidation)
# - Constraint factors SUBTRACT points (these parcels have complications)
#
# OPPORTUNITY FACTORS (Why this parcel should be consolidated):
# - Location: Near transit, in growth zones
# - Size issues: Too small for compliant development
# - Shape issues: Poor shape that wastes buildable area
# - Adjacency: Has neighbors to consolidate with
#
# CONSTRAINT FACTORS (Why consolidation may be difficult):
# - Overlays: Heritage, flood, environmental restrictions
# - Already adequate: Large lots don't need consolidation
# - Isolated: No neighbors to consolidate with
#
# WEIGHTS CAN BE ADJUSTED to reflect policy priorities (e.g., increase
# weight for HCTZ proximity if transit-oriented development is the focus)
# ============================================================================

scoring_weights = {
    # Opportunity factors (positive scores)
    "hctz_zone": 30,                    # In HCTZ zone
    "grz_zone": 15,                     # In GRZ zone (good for consolidation)
    "within_800m_train_tram": 20,       # Within 800m of train/tram
    "within_400m_bus": 10,              # Within 400m of bus
    "below_min_area_500": 25,           # Below 500sqm minimum
    "below_min_area_300": 35,           # Below 300sqm (critical)
    "below_frontage_15m": 20,           # Below 15m frontage
    "narrow_lot": 30,                   # Narrow lot (<10m)
    "poor_compactness": 15,             # Compactness < 0.3
    "high_elongation": 10,              # Elongation > 3
    "internal_lot": 20,                 # Internal lot (low frontage ratio)
    "good_adjacency": 15,               # Has adjacent parcels in same zone
    "long_shared_boundary": 10,         # Long shared boundary (>15m)

    # Constraint factors (negative scores)
    "isolated_lot": -20,                # No adjacent parcels
    "heritage_overlay": -50,            # Heritage constraint
    "flood_overlay": -40,               # Flood constraint
    "eso_overlay": -30,                 # Environmental significance
    "large_lot": -15,                   # Already large (>1000sqm)
}

print("Scoring weights defined:")
for factor, weight in sorted(scoring_weights.items(), key=lambda x: -x[1]):
    print(f"  {factor}: {weight:+d}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Calculate Suitability Scores

# COMMAND ----------

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

# ============================================================================
# SUITABILITY SCORING: Combine All Factors into Final Ranking
# ============================================================================
# This query brings together all the analysis from previous notebooks:
# - Geometric features (area, shape, frontage)
# - Edge topology (neighbors, shared boundaries)
# - Public transport proximity
# - Activity centre proximity
# - Road frontage characteristics
# - Planning overlays (heritage, flood, environmental)
#
# OVERLAY DETECTION:
# - ST_Intersects checks if parcel touches any overlay polygon
# - Heritage Overlay (HO): Restricts changes to historic buildings
# - Flood Overlays (LSIO/SBO): Land Subject to Inundation/Special Building
# - ESO: Environmentally Significant Overlay
# - VPO: Vegetation Protection Overlay
#
# SCORING TIERS (based on total score):
# - Tier 1 (100+): Excellent candidates - high need and good conditions
# - Tier 2 (80-99): Very Good - strong consolidation potential
# - Tier 3 (60-79): Good - worth considering
# - Tier 4 (40-59): Moderate - may have some constraints
# - Tier 5 (<40): Low priority - significant constraints or no need
# ============================================================================
spark.sql(f"""
    CREATE OR REPLACE TABLE {catalog_name}.{schema_name}.parcel_suitability_scores AS
    WITH overlay_flags AS (
        -- Identify parcels affected by constraint overlays
        SELECT
            e.parcel_id,
            MAX(CASE WHEN o.ZONE_CODE LIKE 'HO%' THEN 1 ELSE 0 END) AS has_heritage_overlay,
            MAX(CASE WHEN o.ZONE_CODE IN ('LSIO', 'SBO') OR o.ZONE_CODE LIKE 'LSIO%' OR o.ZONE_CODE LIKE 'SBO%' THEN 1 ELSE 0 END) AS has_flood_overlay,
            MAX(CASE WHEN o.ZONE_CODE LIKE 'ESO%' THEN 1 ELSE 0 END) AS has_eso_overlay,
            MAX(CASE WHEN o.ZONE_CODE LIKE 'VPO%' THEN 1 ELSE 0 END) AS has_vpo_overlay
        FROM {catalog_name}.{schema_name}.parcel_edge_topology e
        LEFT JOIN {catalog_name}.{schema_name}.overlays o
            ON ST_Intersects(e.geometry, o.geometry)
        GROUP BY e.parcel_id
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

        -- Key metrics for reference
        e.area_sqm,
        e.perimeter_m,
        e.compactness_index,
        e.aspect_ratio,
        e.elongation_index,
        e.num_adjacent_parcels,
        e.longest_shared_boundary_m,
        e.adjacent_same_zone_count,
        pt.nearest_any_pt_m,
        ac.nearest_any_ac_m,
        COALESCE(rf.num_road_frontages, 0) AS num_road_frontages,
        COALESCE(rf.longest_frontage_m, 0) AS longest_frontage_m,
        COALESCE(rf.is_corner_lot, FALSE) AS is_corner_lot,
        COALESCE(rf.has_laneway_access, FALSE) AS has_laneway_access,
        e.centroid_lon,
        e.centroid_lat,

        -- Individual opportunity scores
        CASE WHEN e.zone_code LIKE '%HCTZ%' OR e.zone_code LIKE '%MUZ%' THEN 30 ELSE 0 END AS score_hctz_zone,
        CASE WHEN e.zone_code LIKE 'GRZ%' OR e.zone_code LIKE 'NRZ%' THEN 15 ELSE 0 END AS score_residential_zone,
        CASE WHEN COALESCE(pt.within_800m_train, FALSE) OR COALESCE(pt.within_800m_tram, FALSE) THEN 20 ELSE 0 END AS score_pt_proximity,
        CASE WHEN COALESCE(pt.within_400m_bus, FALSE) THEN 10 ELSE 0 END AS score_bus_proximity,
        CASE WHEN COALESCE(ac.within_800m_ac, FALSE) THEN 15 ELSE 0 END AS score_activity_centre,
        CASE WHEN COALESCE(ac.within_800m_mac, FALSE) THEN 10 ELSE 0 END AS score_metro_ac,
        CASE WHEN e.below_min_area_500 THEN 25 ELSE 0 END AS score_small_lot,
        CASE WHEN e.below_min_area_300 THEN 10 ELSE 0 END AS score_very_small_lot,
        CASE WHEN e.below_frontage_15m THEN 20 ELSE 0 END AS score_narrow_frontage,
        CASE WHEN e.narrow_lot THEN 10 ELSE 0 END AS score_very_narrow,
        CASE WHEN e.compactness_index < 0.3 THEN 15 ELSE 0 END AS score_poor_shape,
        CASE WHEN e.elongation_index > 3 THEN 10 ELSE 0 END AS score_elongated,
        CASE WHEN e.is_internal_lot THEN 20 ELSE 0 END AS score_internal_lot,
        CASE WHEN e.adjacent_same_zone_count > 0 THEN 15 ELSE 0 END AS score_good_adjacency,
        CASE WHEN e.has_long_shared_boundary THEN 10 ELSE 0 END AS score_long_shared_boundary,
        CASE WHEN COALESCE(rf.has_laneway_access, FALSE) THEN 5 ELSE 0 END AS score_laneway_access,

        -- Individual constraint scores (negative)
        CASE WHEN e.is_isolated_lot THEN -20 ELSE 0 END AS score_isolated,
        CASE WHEN e.area_sqm > 1000 THEN -15 ELSE 0 END AS score_large_lot,
        CASE WHEN COALESCE(rf.is_corner_lot, FALSE) THEN -10 ELSE 0 END AS score_corner_lot,
        -- Overlay constraints from spatial join
        CASE WHEN COALESCE(ov.has_heritage_overlay, 0) = 1 THEN -50 ELSE 0 END AS score_heritage_overlay,
        CASE WHEN COALESCE(ov.has_flood_overlay, 0) = 1 THEN -40 ELSE 0 END AS score_flood_overlay,
        CASE WHEN COALESCE(ov.has_eso_overlay, 0) = 1 THEN -30 ELSE 0 END AS score_eso_overlay,
        CASE WHEN COALESCE(ov.has_vpo_overlay, 0) = 1 THEN -20 ELSE 0 END AS score_vpo_overlay,

        -- Overlay flags for reference
        COALESCE(ov.has_heritage_overlay, 0) = 1 AS has_heritage_overlay,
        COALESCE(ov.has_flood_overlay, 0) = 1 AS has_flood_overlay,
        COALESCE(ov.has_eso_overlay, 0) = 1 AS has_eso_overlay,

        -- Total opportunity score
        (
            CASE WHEN e.zone_code LIKE '%HCTZ%' OR e.zone_code LIKE '%MUZ%' THEN 30 ELSE 0 END +
            CASE WHEN e.zone_code LIKE 'GRZ%' OR e.zone_code LIKE 'NRZ%' THEN 15 ELSE 0 END +
            CASE WHEN COALESCE(pt.within_800m_train, FALSE) OR COALESCE(pt.within_800m_tram, FALSE) THEN 20 ELSE 0 END +
            CASE WHEN COALESCE(pt.within_400m_bus, FALSE) THEN 10 ELSE 0 END +
            CASE WHEN COALESCE(ac.within_800m_ac, FALSE) THEN 15 ELSE 0 END +
            CASE WHEN COALESCE(ac.within_800m_mac, FALSE) THEN 10 ELSE 0 END +
            CASE WHEN e.below_min_area_500 THEN 25 ELSE 0 END +
            CASE WHEN e.below_min_area_300 THEN 10 ELSE 0 END +
            CASE WHEN e.below_frontage_15m THEN 20 ELSE 0 END +
            CASE WHEN e.narrow_lot THEN 10 ELSE 0 END +
            CASE WHEN e.compactness_index < 0.3 THEN 15 ELSE 0 END +
            CASE WHEN e.elongation_index > 3 THEN 10 ELSE 0 END +
            CASE WHEN e.is_internal_lot THEN 20 ELSE 0 END +
            CASE WHEN e.adjacent_same_zone_count > 0 THEN 15 ELSE 0 END +
            CASE WHEN e.has_long_shared_boundary THEN 10 ELSE 0 END +
            CASE WHEN COALESCE(rf.has_laneway_access, FALSE) THEN 5 ELSE 0 END
        ) AS opportunity_score,

        -- Total constraint score
        (
            CASE WHEN e.is_isolated_lot THEN -20 ELSE 0 END +
            CASE WHEN e.area_sqm > 1000 THEN -15 ELSE 0 END +
            CASE WHEN COALESCE(rf.is_corner_lot, FALSE) THEN -10 ELSE 0 END +
            CASE WHEN COALESCE(ov.has_heritage_overlay, 0) = 1 THEN -50 ELSE 0 END +
            CASE WHEN COALESCE(ov.has_flood_overlay, 0) = 1 THEN -40 ELSE 0 END +
            CASE WHEN COALESCE(ov.has_eso_overlay, 0) = 1 THEN -30 ELSE 0 END +
            CASE WHEN COALESCE(ov.has_vpo_overlay, 0) = 1 THEN -20 ELSE 0 END
        ) AS constraint_score,

        -- Final suitability score (opportunity + constraint)
        (
            -- Opportunities
            CASE WHEN e.zone_code LIKE '%HCTZ%' OR e.zone_code LIKE '%MUZ%' THEN 30 ELSE 0 END +
            CASE WHEN e.zone_code LIKE 'GRZ%' OR e.zone_code LIKE 'NRZ%' THEN 15 ELSE 0 END +
            CASE WHEN COALESCE(pt.within_800m_train, FALSE) OR COALESCE(pt.within_800m_tram, FALSE) THEN 20 ELSE 0 END +
            CASE WHEN COALESCE(pt.within_400m_bus, FALSE) THEN 10 ELSE 0 END +
            CASE WHEN COALESCE(ac.within_800m_ac, FALSE) THEN 15 ELSE 0 END +
            CASE WHEN COALESCE(ac.within_800m_mac, FALSE) THEN 10 ELSE 0 END +
            CASE WHEN e.below_min_area_500 THEN 25 ELSE 0 END +
            CASE WHEN e.below_min_area_300 THEN 10 ELSE 0 END +
            CASE WHEN e.below_frontage_15m THEN 20 ELSE 0 END +
            CASE WHEN e.narrow_lot THEN 10 ELSE 0 END +
            CASE WHEN e.compactness_index < 0.3 THEN 15 ELSE 0 END +
            CASE WHEN e.elongation_index > 3 THEN 10 ELSE 0 END +
            CASE WHEN e.is_internal_lot THEN 20 ELSE 0 END +
            CASE WHEN e.adjacent_same_zone_count > 0 THEN 15 ELSE 0 END +
            CASE WHEN e.has_long_shared_boundary THEN 10 ELSE 0 END +
            CASE WHEN COALESCE(rf.has_laneway_access, FALSE) THEN 5 ELSE 0 END +
            -- Constraints
            CASE WHEN e.is_isolated_lot THEN -20 ELSE 0 END +
            CASE WHEN e.area_sqm > 1000 THEN -15 ELSE 0 END +
            CASE WHEN COALESCE(rf.is_corner_lot, FALSE) THEN -10 ELSE 0 END +
            CASE WHEN COALESCE(ov.has_heritage_overlay, 0) = 1 THEN -50 ELSE 0 END +
            CASE WHEN COALESCE(ov.has_flood_overlay, 0) = 1 THEN -40 ELSE 0 END +
            CASE WHEN COALESCE(ov.has_eso_overlay, 0) = 1 THEN -30 ELSE 0 END +
            CASE WHEN COALESCE(ov.has_vpo_overlay, 0) = 1 THEN -20 ELSE 0 END
        ) AS suitability_score

    FROM {catalog_name}.{schema_name}.parcel_edge_topology e
    LEFT JOIN {catalog_name}.{schema_name}.parcel_pt_proximity pt ON e.parcel_id = pt.parcel_id
    LEFT JOIN {catalog_name}.{schema_name}.parcel_activity_centre_proximity ac ON e.parcel_id = ac.parcel_id
    LEFT JOIN {catalog_name}.{schema_name}.parcel_road_frontage rf ON e.parcel_id = rf.parcel_id
    LEFT JOIN overlay_flags ov ON e.parcel_id = ov.parcel_id
""")

print("Created parcel_suitability_scores table")

# COMMAND ----------

# Display suitability scores ranked
catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

display(spark.sql(f"""
    SELECT
        parcel_id,
        zone_code,
        lga_name,
        area_sqm,
        compactness_index,
        num_adjacent_parcels,
        nearest_any_pt_m,
        opportunity_score,
        constraint_score,
        suitability_score,
        -- Suitability tier
        CASE
            WHEN suitability_score >= 100 THEN 'Tier 1 - Excellent'
            WHEN suitability_score >= 80 THEN 'Tier 2 - Very Good'
            WHEN suitability_score >= 60 THEN 'Tier 3 - Good'
            WHEN suitability_score >= 40 THEN 'Tier 4 - Moderate'
            ELSE 'Tier 5 - Low'
        END AS suitability_tier
    FROM {catalog_name}.{schema_name}.parcel_suitability_scores
    ORDER BY suitability_score DESC
    LIMIT 100
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Score Breakdown Analysis

# COMMAND ----------

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

# Detailed score breakdown for top candidates
display(spark.sql(f"""
    SELECT
        parcel_id,
        zone_code,
        suitability_score,
        -- Opportunity breakdown
        score_hctz_zone,
        score_residential_zone,
        score_pt_proximity,
        score_bus_proximity,
        score_small_lot,
        score_very_small_lot,
        score_narrow_frontage,
        score_very_narrow,
        score_poor_shape,
        score_elongated,
        score_internal_lot,
        score_good_adjacency,
        score_long_shared_boundary,
        -- Constraint breakdown
        score_isolated,
        score_large_lot
    FROM {catalog_name}.{schema_name}.parcel_suitability_scores
    ORDER BY suitability_score DESC
    LIMIT 50
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Summary Statistics by Zone

# COMMAND ----------

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

# Summary by zone
display(spark.sql(f"""
    SELECT
        zone_code,
        COUNT(*) AS parcel_count,
        ROUND(AVG(suitability_score), 2) AS avg_score,
        ROUND(MIN(suitability_score), 2) AS min_score,
        ROUND(MAX(suitability_score), 2) AS max_score,
        SUM(CASE WHEN suitability_score >= 80 THEN 1 ELSE 0 END) AS high_priority_count,
        SUM(CASE WHEN suitability_score >= 60 AND suitability_score < 80 THEN 1 ELSE 0 END) AS medium_priority_count,
        SUM(CASE WHEN suitability_score < 60 THEN 1 ELSE 0 END) AS low_priority_count
    FROM {catalog_name}.{schema_name}.parcel_suitability_scores
    WHERE zone_code IS NOT NULL
    GROUP BY zone_code
    ORDER BY avg_score DESC
    LIMIT 30
"""))

# COMMAND ----------

# Matplotlib visualization of suitability scores by zone
import matplotlib.pyplot as plt
import numpy as np

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

zone_stats = spark.sql(f"""
    SELECT
        zone_code,
        COUNT(*) AS parcel_count,
        ROUND(AVG(suitability_score), 2) AS avg_score,
        SUM(CASE WHEN suitability_score >= 80 THEN 1 ELSE 0 END) AS high_priority,
        SUM(CASE WHEN suitability_score >= 60 AND suitability_score < 80 THEN 1 ELSE 0 END) AS medium_priority,
        SUM(CASE WHEN suitability_score < 60 THEN 1 ELSE 0 END) AS low_priority
    FROM {catalog_name}.{schema_name}.parcel_suitability_scores
    WHERE zone_code IS NOT NULL
    GROUP BY zone_code
    ORDER BY avg_score DESC
    LIMIT 15
""").toPandas()

if len(zone_stats) > 0:
    fig, axes = plt.subplots(1, 2, figsize=(16, 6))

    # Chart 1: Average suitability score by zone (horizontal bar)
    ax1 = axes[0]
    colors = plt.cm.RdYlGn(np.linspace(0.2, 0.8, len(zone_stats)))[::-1]  # Red to green
    bars = ax1.barh(zone_stats['zone_code'], zone_stats['avg_score'], color=colors)
    ax1.set_xlabel('Average Suitability Score', fontsize=12)
    ax1.set_ylabel('Zone Code', fontsize=12)
    ax1.set_title('Suitability Score by Zone', fontsize=14, fontweight='bold')
    ax1.axvline(x=60, color='orange', linestyle='--', alpha=0.7, label='Good threshold (60)')
    ax1.axvline(x=80, color='red', linestyle='--', alpha=0.7, label='High priority threshold (80)')
    ax1.legend(loc='lower right', fontsize=9)

    # Add score labels
    for bar, score in zip(bars, zone_stats['avg_score']):
        ax1.text(bar.get_width() + 1, bar.get_y() + bar.get_height()/2,
                f'{score:.0f}', va='center', fontsize=9)

    # Chart 2: Stacked bar showing priority distribution
    ax2 = axes[1]
    x = np.arange(len(zone_stats))
    width = 0.7

    p1 = ax2.bar(x, zone_stats['high_priority'], width, label='High Priority (≥80)', color='#d7191c')
    p2 = ax2.bar(x, zone_stats['medium_priority'], width, bottom=zone_stats['high_priority'],
                 label='Medium Priority (60-79)', color='#fdae61')
    p3 = ax2.bar(x, zone_stats['low_priority'], width,
                 bottom=zone_stats['high_priority'] + zone_stats['medium_priority'],
                 label='Low Priority (<60)', color='#1a9641')

    ax2.set_xlabel('Zone Code', fontsize=12)
    ax2.set_ylabel('Parcel Count', fontsize=12)
    ax2.set_title('Priority Distribution by Zone', fontsize=14, fontweight='bold')
    ax2.set_xticks(x)
    ax2.set_xticklabels(zone_stats['zone_code'], rotation=45, ha='right')
    ax2.legend(loc='upper right', fontsize=9)

    plt.tight_layout()
    plt.show()

    # Additional chart: Score distribution histogram
    fig2, ax3 = plt.subplots(figsize=(12, 5))

    score_dist = spark.sql(f"""
        SELECT suitability_score
        FROM {catalog_name}.{schema_name}.parcel_suitability_scores
    """).toPandas()

    ax3.hist(score_dist['suitability_score'], bins=30, edgecolor='black', alpha=0.7, color='steelblue')
    ax3.axvline(x=score_dist['suitability_score'].mean(), color='red', linestyle='--',
                linewidth=2, label=f'Mean: {score_dist["suitability_score"].mean():.1f}')
    ax3.axvline(x=score_dist['suitability_score'].median(), color='orange', linestyle='--',
                linewidth=2, label=f'Median: {score_dist["suitability_score"].median():.1f}')
    ax3.axvline(x=80, color='darkred', linestyle=':', linewidth=2, label='High Priority (80)')
    ax3.axvline(x=60, color='darkorange', linestyle=':', linewidth=2, label='Good (60)')

    ax3.set_xlabel('Suitability Score', fontsize=12)
    ax3.set_ylabel('Number of Parcels', fontsize=12)
    ax3.set_title('Distribution of Suitability Scores Across All Parcels', fontsize=14, fontweight='bold')
    ax3.legend(loc='upper right', fontsize=10)

    plt.tight_layout()
    plt.show()
else:
    print("No zone statistics available for visualization")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Identify Best Consolidation Opportunities

# COMMAND ----------

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

# ============================================================================
# BEST CONSOLIDATION PAIRS: Find Adjacent Parcels to Merge Together
# ============================================================================
# The ultimate output: pairs of neighboring parcels that should be considered
# for consolidation. This combines:
# - Individual suitability scores for each parcel
# - The shared boundary length between them
# - Combined area (what you'd get after merging)
# - Zone compatibility (same zone = easier approval)
#
# COMBINED SCORE interpretation:
# - 150+: Excellent - both parcels score highly, strong case for consolidation
# - 120-149: Very Good - high combined potential
# - 100-119: Good - worth considering
# - <100: Moderate - may have constraints on one or both sides
#
# PRACTICAL CONSIDERATIONS:
# - Same zone pairs are prioritized (simpler planning process)
# - Longer shared boundaries make consolidation more practical
# - Combined area should still be reasonable for the zone (not too large)
# ============================================================================
display(spark.sql(f"""
    WITH scored_adjacency AS (
        SELECT
            a.parcel_1,
            a.parcel_2,
            a.shared_boundary_m,
            s1.suitability_score AS score_1,
            s2.suitability_score AS score_2,
            s1.area_sqm AS area_1,
            s2.area_sqm AS area_2,
            s1.zone_code AS zone_1,
            s2.zone_code AS zone_2,
            -- Combined metrics
            s1.suitability_score + s2.suitability_score AS combined_score,
            s1.area_sqm + s2.area_sqm AS combined_area_sqm
        FROM {catalog_name}.{schema_name}.parcel_adjacency a
        JOIN {catalog_name}.{schema_name}.parcel_suitability_scores s1 ON a.parcel_1 = s1.parcel_id
        JOIN {catalog_name}.{schema_name}.parcel_suitability_scores s2 ON a.parcel_2 = s2.parcel_id
        WHERE a.shared_boundary_m > 0
    )
    SELECT
        parcel_1,
        parcel_2,
        zone_1,
        zone_2,
        ROUND(shared_boundary_m, 2) AS shared_boundary_m,
        score_1,
        score_2,
        combined_score,
        ROUND(area_1, 2) AS area_1_sqm,
        ROUND(area_2, 2) AS area_2_sqm,
        ROUND(combined_area_sqm, 2) AS combined_area_sqm,
        -- Consolidation recommendation
        CASE
            WHEN combined_score >= 150 AND zone_1 = zone_2 THEN 'Excellent - High priority, same zone'
            WHEN combined_score >= 120 AND zone_1 = zone_2 THEN 'Very Good - Strong candidate, same zone'
            WHEN combined_score >= 100 THEN 'Good - Worth considering'
            ELSE 'Moderate'
        END AS consolidation_recommendation
    FROM scored_adjacency
    ORDER BY combined_score DESC, shared_boundary_m DESC
    LIMIT 100
"""))

# COMMAND ----------

# AI-powered analysis of best consolidation opportunities
catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

consolidation_summary = spark.sql(f"""
    WITH scored_adjacency AS (
        SELECT
            a.parcel_1,
            a.parcel_2,
            a.shared_boundary_m,
            s1.suitability_score AS score_1,
            s2.suitability_score AS score_2,
            s1.area_sqm AS area_1,
            s2.area_sqm AS area_2,
            s1.zone_code AS zone_1,
            s2.zone_code AS zone_2,
            s1.lga_name AS lga_name,
            s1.suitability_score + s2.suitability_score AS combined_score,
            s1.area_sqm + s2.area_sqm AS combined_area_sqm
        FROM {catalog_name}.{schema_name}.parcel_adjacency a
        JOIN {catalog_name}.{schema_name}.parcel_suitability_scores s1 ON a.parcel_1 = s1.parcel_id
        JOIN {catalog_name}.{schema_name}.parcel_suitability_scores s2 ON a.parcel_2 = s2.parcel_id
        WHERE a.shared_boundary_m > 0
    ),
    consolidation_stats AS (
        SELECT
            COUNT(*) AS total_pairs,
            SUM(CASE WHEN combined_score >= 150 AND zone_1 = zone_2 THEN 1 ELSE 0 END) AS excellent_pairs,
            SUM(CASE WHEN combined_score >= 120 AND combined_score < 150 AND zone_1 = zone_2 THEN 1 ELSE 0 END) AS very_good_pairs,
            SUM(CASE WHEN combined_score >= 100 AND combined_score < 120 THEN 1 ELSE 0 END) AS good_pairs,
            ROUND(AVG(combined_score), 1) AS avg_combined_score,
            ROUND(AVG(shared_boundary_m), 1) AS avg_shared_boundary_m,
            ROUND(AVG(combined_area_sqm), 1) AS avg_combined_area_sqm,
            MAX(combined_score) AS max_combined_score,
            ROUND(SUM(combined_area_sqm) / 10000, 2) AS total_area_hectares,
            COUNT(DISTINCT lga_name) AS num_lgas,
            COLLECT_LIST(DISTINCT zone_1) AS zones_involved
        FROM scored_adjacency
        WHERE zone_1 = zone_2  -- Same zone pairs
    )
    SELECT ai_query(
        'databricks-claude-opus-4-6',
        CONCAT(
            'You are a Victorian land use planning analyst specializing in site consolidation. ',
            'Analyze the following consolidation opportunity statistics and provide a brief, insightful summary (3-4 paragraphs) of the consolidation potential. ',
            'Include: (1) Overall assessment of the consolidation pipeline quality, ',
            '(2) Key metrics that stand out and what they mean for planners, ',
            '(3) Practical recommendations for prioritizing which pairs to consolidate first. ',
            'Use specific numbers from the data. Keep language clear and actionable. ',
            'Data: ', TO_JSON(STRUCT(
                total_pairs,
                excellent_pairs,
                very_good_pairs,
                good_pairs,
                avg_combined_score,
                avg_shared_boundary_m,
                avg_combined_area_sqm,
                max_combined_score,
                total_area_hectares,
                num_lgas,
                zones_involved
            ))
        )
    ) AS consolidation_analysis_summary
    FROM consolidation_stats
""")

# Display the ai_query result with proper markdown rendering
from IPython.display import display, Markdown, HTML

print("=" * 80)
print("AI ANALYSIS: CONSOLIDATION OPPORTUNITIES")
print("=" * 80)

result_df = consolidation_summary.toPandas()
if len(result_df) > 0:
    summary_text = result_df['consolidation_analysis_summary'].iloc[0]
    display(Markdown(summary_text))
else:
    print("No summary generated")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Export Final Results

# COMMAND ----------

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

# Create final output table with geometry for visualization
spark.sql(f"""
    CREATE OR REPLACE TABLE {catalog_name}.{schema_name}.consolidation_candidates AS
    SELECT
        parcel_id,
        plan_number,
        lot_number,
        zone_code,
        zone_description,
        lga_code,
        lga_name,
        geometry,
        centroid_lon,
        centroid_lat,
        area_sqm,
        perimeter_m,
        compactness_index,
        aspect_ratio,
        elongation_index,
        num_adjacent_parcels,
        longest_shared_boundary_m,
        adjacent_same_zone_count,
        nearest_any_pt_m,
        opportunity_score,
        constraint_score,
        suitability_score,
        CASE
            WHEN suitability_score >= 100 THEN 'Tier 1 - Excellent'
            WHEN suitability_score >= 80 THEN 'Tier 2 - Very Good'
            WHEN suitability_score >= 60 THEN 'Tier 3 - Good'
            WHEN suitability_score >= 40 THEN 'Tier 4 - Moderate'
            ELSE 'Tier 5 - Low'
        END AS suitability_tier,
        RANK() OVER (ORDER BY suitability_score DESC) AS rank
    FROM {catalog_name}.{schema_name}.parcel_suitability_scores
""")

print(f"Created consolidation_candidates table")
print(f"Query with: SELECT * FROM {catalog_name}.{schema_name}.consolidation_candidates ORDER BY rank")

# COMMAND ----------

# Final summary
catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

display(spark.sql(f"""
    SELECT
        suitability_tier,
        COUNT(*) AS parcel_count,
        ROUND(AVG(area_sqm), 2) AS avg_area_sqm,
        ROUND(AVG(suitability_score), 2) AS avg_score,
        ROUND(SUM(area_sqm), 2) AS total_area_sqm
    FROM {catalog_name}.{schema_name}.consolidation_candidates
    GROUP BY suitability_tier
    ORDER BY
        CASE suitability_tier
            WHEN 'Tier 1 - Excellent' THEN 1
            WHEN 'Tier 2 - Very Good' THEN 2
            WHEN 'Tier 3 - Good' THEN 3
            WHEN 'Tier 4 - Moderate' THEN 4
            ELSE 5
        END
"""))

# COMMAND ----------

# Matplotlib visualization of suitability tiers
import matplotlib.pyplot as plt
import numpy as np

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

tier_stats = spark.sql(f"""
    SELECT
        suitability_tier,
        COUNT(*) AS parcel_count,
        ROUND(AVG(area_sqm), 2) AS avg_area_sqm,
        ROUND(SUM(area_sqm), 2) AS total_area_sqm,
        ROUND(AVG(suitability_score), 2) AS avg_score
    FROM {catalog_name}.{schema_name}.consolidation_candidates
    GROUP BY suitability_tier
    ORDER BY
        CASE suitability_tier
            WHEN 'Tier 1 - Excellent' THEN 1
            WHEN 'Tier 2 - Very Good' THEN 2
            WHEN 'Tier 3 - Good' THEN 3
            WHEN 'Tier 4 - Moderate' THEN 4
            ELSE 5
        END
""").toPandas()

if len(tier_stats) > 0:
    fig, axes = plt.subplots(1, 3, figsize=(18, 5))

    # Define tier colors matching the map legend
    tier_colors = ['#d7191c', '#fdae61', '#ffffbf', '#a6d96a', '#1a9641']

    # Chart 1: Parcel count by tier (pie chart)
    ax1 = axes[0]
    wedges, texts, autotexts = ax1.pie(
        tier_stats['parcel_count'],
        labels=tier_stats['suitability_tier'].str.replace('Tier [0-9] - ', '', regex=True),
        autopct='%1.1f%%',
        colors=tier_colors[:len(tier_stats)],
        explode=[0.05 if 'Excellent' in t else 0 for t in tier_stats['suitability_tier']],
        shadow=True
    )
    ax1.set_title('Parcel Distribution by Suitability Tier', fontsize=12, fontweight='bold')

    # Chart 2: Parcel count bar chart
    ax2 = axes[1]
    bars = ax2.bar(
        range(len(tier_stats)),
        tier_stats['parcel_count'],
        color=tier_colors[:len(tier_stats)],
        edgecolor='black'
    )
    ax2.set_xticks(range(len(tier_stats)))
    ax2.set_xticklabels(
        tier_stats['suitability_tier'].str.replace('Tier [0-9] - ', '', regex=True),
        rotation=30, ha='right'
    )
    ax2.set_ylabel('Number of Parcels', fontsize=11)
    ax2.set_title('Consolidation Candidate Count by Tier', fontsize=12, fontweight='bold')

    # Add count labels
    for bar, count in zip(bars, tier_stats['parcel_count']):
        ax2.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 5,
                f'{int(count):,}', ha='center', va='bottom', fontsize=10)

    # Chart 3: Total area by tier
    ax3 = axes[2]
    tier_stats['total_area_ha'] = tier_stats['total_area_sqm'] / 10000  # Convert to hectares
    bars3 = ax3.bar(
        range(len(tier_stats)),
        tier_stats['total_area_ha'],
        color=tier_colors[:len(tier_stats)],
        edgecolor='black'
    )
    ax3.set_xticks(range(len(tier_stats)))
    ax3.set_xticklabels(
        tier_stats['suitability_tier'].str.replace('Tier [0-9] - ', '', regex=True),
        rotation=30, ha='right'
    )
    ax3.set_ylabel('Total Area (hectares)', fontsize=11)
    ax3.set_title('Total Land Area by Tier', fontsize=12, fontweight='bold')

    # Add area labels
    for bar, area in zip(bars3, tier_stats['total_area_ha']):
        ax3.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.5,
                f'{area:.1f} ha', ha='center', va='bottom', fontsize=10)

    plt.tight_layout()
    plt.show()

    # Summary statistics text
    total_parcels = tier_stats['parcel_count'].sum()
    high_priority = tier_stats[tier_stats['suitability_tier'].isin(['Tier 1 - Excellent', 'Tier 2 - Very Good'])]['parcel_count'].sum()

    print(f"\n{'='*60}")
    print(f"CONSOLIDATION CANDIDATE SUMMARY")
    print(f"{'='*60}")
    print(f"Total Candidates: {total_parcels:,} parcels")
    print(f"High Priority (Tier 1-2): {high_priority:,} parcels ({100*high_priority/total_parcels:.1f}%)")
    print(f"Total Land Area: {tier_stats['total_area_sqm'].sum()/10000:.1f} hectares")
    print(f"{'='*60}")
else:
    print("No tier statistics available for visualization")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Top Consolidation Candidates for Review

# COMMAND ----------

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

# Show top 50 candidates with all details
display(spark.sql(f"""
    SELECT
        rank,
        parcel_id,
        plan_number,
        lot_number,
        zone_code,
        lga_name,
        ROUND(area_sqm, 2) AS area_sqm,
        ROUND(compactness_index, 3) AS compactness,
        num_adjacent_parcels,
        ROUND(longest_shared_boundary_m, 2) AS longest_shared_m,
        adjacent_same_zone_count AS same_zone_neighbors,
        ROUND(nearest_any_pt_m, 0) AS nearest_pt_m,
        suitability_score,
        suitability_tier,
        centroid_lon,
        centroid_lat
    FROM {catalog_name}.{schema_name}.consolidation_candidates
    ORDER BY rank
    LIMIT 50
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Visualize Suitability Scores with Folium
# MAGIC
# MAGIC Interactive map showing parcels colored by their suitability tier.

# COMMAND ----------

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

# Get top consolidation candidates with geometry for visualization
candidates_df = spark.sql(f"""
    SELECT
        parcel_id,
        zone_code,
        lga_name,
        area_sqm,
        suitability_score,
        suitability_tier,
        opportunity_score,
        constraint_score,
        num_adjacent_parcels,
        nearest_any_pt_m,
        centroid_lon,
        centroid_lat,
        ST_AsGeoJSON(ST_Transform(geometry, 4326)) AS geojson
    FROM {catalog_name}.{schema_name}.consolidation_candidates
    WHERE geometry IS NOT NULL
      AND centroid_lon IS NOT NULL
    ORDER BY suitability_score DESC
    LIMIT 100
""")

candidates = candidates_df.toPandas()
print(f"Retrieved {len(candidates)} parcels for visualization")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Folium Map: Suitability Score Heatmap
# MAGIC
# MAGIC Parcels colored by suitability tier from red (Tier 1 - Excellent) to blue (Tier 5 - Low).

# COMMAND ----------

import folium
import json

if len(candidates) > 0:
    # Create map centered on parcels
    center_lat = candidates['centroid_lat'].mean()
    center_lon = candidates['centroid_lon'].mean()

    m = folium.Map(
        location=[center_lat, center_lon],
        zoom_start=15,
        tiles='CartoDB positron'
    )

    # Color scale for suitability tiers
    tier_colors = {
        'Tier 1 - Excellent': '#d7191c',   # Dark red
        'Tier 2 - Very Good': '#fdae61',   # Orange
        'Tier 3 - Good': '#ffffbf',        # Yellow
        'Tier 4 - Moderate': '#a6d96a',    # Light green
        'Tier 5 - Low': '#1a9641'          # Dark green
    }

    # Add parcels
    for _, row in candidates.iterrows():
        try:
            geojson = json.loads(row['geojson'])
            color = tier_colors.get(row['suitability_tier'], '#808080')

            tooltip = f"""
                <b>Parcel:</b> {row['parcel_id']}<br>
                <b>Zone:</b> {row['zone_code']}<br>
                <b>Area:</b> {row['area_sqm']:.0f} sqm<br>
                <b>Score:</b> {row['suitability_score']}<br>
                <b>Tier:</b> {row['suitability_tier']}<br>
                <b>Neighbors:</b> {row['num_adjacent_parcels']}<br>
                <b>Nearest PT:</b> {row['nearest_any_pt_m']:.0f}m
            """

            folium.GeoJson(
                geojson,
                style_function=lambda x, c=color: {
                    'fillColor': c,
                    'color': '#333333',
                    'weight': 1,
                    'fillOpacity': 0.7
                },
                tooltip=folium.Tooltip(tooltip)
            ).add_to(m)
        except:
            pass

    # Add legend
    legend_html = '''
    <div style="position: fixed;
                bottom: 50px; left: 50px; width: 200px; height: 150px;
                border:2px solid grey; z-index:9999; font-size:14px;
                background-color:white; padding: 10px;
                border-radius: 5px;">
        <b>Suitability Tier</b><br>
        <i style="background:#d7191c; width:18px; height:18px; display:inline-block;"></i> Tier 1 - Excellent<br>
        <i style="background:#fdae61; width:18px; height:18px; display:inline-block;"></i> Tier 2 - Very Good<br>
        <i style="background:#ffffbf; width:18px; height:18px; display:inline-block;"></i> Tier 3 - Good<br>
        <i style="background:#a6d96a; width:18px; height:18px; display:inline-block;"></i> Tier 4 - Moderate<br>
        <i style="background:#1a9641; width:18px; height:18px; display:inline-block;"></i> Tier 5 - Low
    </div>
    '''
    m.get_root().html.add_child(folium.Element(legend_html))

    # Use displayHTML for proper rendering in Databricks
    displayHTML(m._repr_html_())
else:
    print("No candidates found for visualization")

# COMMAND ----------

# Export consolidation candidates for target LGA to Folium HTML (filtered to avoid OOM)
import folium
import json

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

# Use Casey LGA - Metro Melbourne area with good Activity Centre coverage
target_lga = "CASEY"

# Verify the LGA exists in consolidation candidates
lga_check = spark.sql(f"""
    SELECT COUNT(*) AS cnt
    FROM {catalog_name}.{schema_name}.consolidation_candidates
    WHERE lga_name = '{target_lga}'
""").collect()[0]['cnt']

if lga_check == 0:
    print(f"WARNING: {target_lga} not found. Falling back to most common LGA.")
    target_lga = "CASEY"  # Default to Casey LGA

print(f"Exporting consolidation candidates for LGA: {target_lga} ({lga_check:,} parcels)")

# Get consolidation candidates filtered by LGA
# Limit to top 2000 parcels to avoid OOM
all_candidates_df = spark.sql(f"""
    SELECT
        parcel_id,
        zone_code,
        lga_name,
        area_sqm,
        suitability_score,
        suitability_tier,
        opportunity_score,
        constraint_score,
        num_adjacent_parcels,
        nearest_any_pt_m,
        centroid_lon,
        centroid_lat,
        ST_AsGeoJSON(ST_Transform(geometry, 4326)) AS geojson
    FROM {catalog_name}.{schema_name}.consolidation_candidates
    WHERE geometry IS NOT NULL
      AND centroid_lon IS NOT NULL
      AND lga_name = '{target_lga}'
    ORDER BY suitability_score DESC
    LIMIT 2000
""")

all_candidates = all_candidates_df.toPandas()
print(f"Retrieved {len(all_candidates)} parcels for full Folium export")

if len(all_candidates) > 0:
    # Create map centered on parcels
    center_lat = all_candidates['centroid_lat'].mean()
    center_lon = all_candidates['centroid_lon'].mean()

    m_full = folium.Map(
        location=[center_lat, center_lon],
        zoom_start=14,
        tiles='CartoDB positron'
    )

    # Color scale for suitability tiers
    tier_colors = {
        'Tier 1 - Excellent': '#d7191c',
        'Tier 2 - Very Good': '#fdae61',
        'Tier 3 - Good': '#ffffbf',
        'Tier 4 - Moderate': '#a6d96a',
        'Tier 5 - Low': '#1a9641'
    }

    # Add ALL parcels
    for _, row in all_candidates.iterrows():
        try:
            geojson = json.loads(row['geojson'])
            color = tier_colors.get(row['suitability_tier'], '#808080')

            tooltip = f"""
                <b>Parcel:</b> {row['parcel_id']}<br>
                <b>Zone:</b> {row['zone_code']}<br>
                <b>Area:</b> {row['area_sqm']:.0f} sqm<br>
                <b>Score:</b> {row['suitability_score']}<br>
                <b>Tier:</b> {row['suitability_tier']}<br>
                <b>Neighbors:</b> {row['num_adjacent_parcels']}<br>
                <b>Nearest PT:</b> {row['nearest_any_pt_m']:.0f}m
            """

            folium.GeoJson(
                geojson,
                style_function=lambda x, c=color: {
                    'fillColor': c,
                    'color': '#333333',
                    'weight': 1,
                    'fillOpacity': 0.7
                },
                tooltip=folium.Tooltip(tooltip)
            ).add_to(m_full)
        except:
            pass

    # Add legend
    legend_html = '''
    <div style="position: fixed;
                bottom: 50px; left: 50px; width: 200px; height: 150px;
                border:2px solid grey; z-index:9999; font-size:14px;
                background-color:white; padding: 10px;
                border-radius: 5px;">
        <b>Suitability Tier</b><br>
        <i style="background:#d7191c; width:18px; height:18px; display:inline-block;"></i> Tier 1 - Excellent<br>
        <i style="background:#fdae61; width:18px; height:18px; display:inline-block;"></i> Tier 2 - Very Good<br>
        <i style="background:#ffffbf; width:18px; height:18px; display:inline-block;"></i> Tier 3 - Good<br>
        <i style="background:#a6d96a; width:18px; height:18px; display:inline-block;"></i> Tier 4 - Moderate<br>
        <i style="background:#1a9641; width:18px; height:18px; display:inline-block;"></i> Tier 5 - Low
    </div>
    '''
    m_full.get_root().html.add_child(folium.Element(legend_html))

    # Save to UC Volume
    volume_path = f"/Volumes/{catalog_name}/{schema_name}/source"
    import os
    os.makedirs(f"{volume_path}/visualizations", exist_ok=True)
    folium_html_path = f"{volume_path}/visualizations/folium_consolidation_candidates_{target_lga.replace(' ', '_')}.html"
    m_full.save(folium_html_path)
    print(f"Folium map for {target_lga} saved to: {folium_html_path}")
    print(f"Total parcels exported: {len(all_candidates):,}")
else:
    print("No candidates available for export")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Kepler.gl: Interactive Exploration of Consolidation Candidates
# MAGIC
# MAGIC Use Kepler.gl for more advanced geospatial exploration with filtering and 3D views.

# COMMAND ----------

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

# Get data for Kepler visualization
kepler_df = spark.sql(f"""
    SELECT
        parcel_id,
        zone_code,
        lga_name,
        area_sqm,
        compactness_index,
        num_adjacent_parcels,
        longest_shared_boundary_m,
        nearest_any_pt_m,
        opportunity_score,
        constraint_score,
        suitability_score,
        suitability_tier,
        centroid_lon,
        centroid_lat,
        ST_AsGeoJSON(ST_Transform(geometry, 4326)) AS geojson
    FROM {catalog_name}.{schema_name}.consolidation_candidates
    WHERE geometry IS NOT NULL
    ORDER BY suitability_score DESC
    LIMIT 500
""")

kepler_data = kepler_df.toPandas()
print(f"Retrieved {len(kepler_data)} parcels for Kepler visualization")

# COMMAND ----------

from keplergl import KeplerGl
import json

if len(kepler_data) > 0:
    # Convert GeoJSON strings to geometry objects for Kepler
    features = []
    for _, row in kepler_data.iterrows():
        try:
            geom = json.loads(row['geojson'])
            feature = {
                "type": "Feature",
                "geometry": geom,
                "properties": {
                    "parcel_id": row['parcel_id'],
                    "zone_code": row['zone_code'],
                    "lga_name": row['lga_name'],
                    "area_sqm": float(row['area_sqm']) if row['area_sqm'] else 0,
                    "compactness_index": float(row['compactness_index']) if row['compactness_index'] else 0,
                    "num_adjacent_parcels": int(row['num_adjacent_parcels']) if row['num_adjacent_parcels'] else 0,
                    "longest_shared_boundary_m": float(row['longest_shared_boundary_m']) if row['longest_shared_boundary_m'] else 0,
                    "nearest_any_pt_m": float(row['nearest_any_pt_m']) if row['nearest_any_pt_m'] else 0,
                    "opportunity_score": int(row['opportunity_score']) if row['opportunity_score'] else 0,
                    "suitability_score": int(row['suitability_score']) if row['suitability_score'] else 0,
                    "suitability_tier": row['suitability_tier']
                }
            }
            features.append(feature)
        except:
            pass

    geojson_collection = {
        "type": "FeatureCollection",
        "features": features
    }

    # Create Kepler map with configuration
    config = {
        "version": "v1",
        "config": {
            "mapState": {
                "latitude": kepler_data['centroid_lat'].mean(),
                "longitude": kepler_data['centroid_lon'].mean(),
                "zoom": 14
            },
            "visState": {
                "layers": [{
                    "type": "geojson",
                    "config": {
                        "dataId": "consolidation_candidates",
                        "label": "Consolidation Candidates",
                        "color": [255, 153, 31],
                        "columns": {"geojson": "geometry"},
                        "isVisible": True,
                        "colorField": {"name": "suitability_score", "type": "integer"},
                        "colorScale": "quantize"
                    }
                }]
            }
        }
    }

    kepler_map = KeplerGl(height=600, config=config)
    kepler_map.add_data(data=geojson_collection, name="consolidation_candidates")

    # Save Kepler HTML to UC Volume to avoid 10MB output limit
    volume_path = f"/Volumes/{catalog_name}/{schema_name}/source"
    kepler_html_path = f"{volume_path}/visualizations/kepler_consolidation.html"

    import os
    os.makedirs(f"{volume_path}/visualizations", exist_ok=True)

    kepler_map.save_to_html(file_name=kepler_html_path)
    print(f"Kepler map saved to: {kepler_html_path}")
    print("Open this file in a browser to view the interactive map.")
else:
    print("No data available for Kepler visualization")

# COMMAND ----------

# Export consolidation candidates for target LGA to Kepler.gl HTML (filtered to avoid OOM)
from keplergl import KeplerGl
import json

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

# Use target_lga from previous cell (or get it again if running independently)
try:
    _ = target_lga
except NameError:
    target_lga = "CASEY"  # Default to Casey LGA

print(f"Exporting Kepler visualization for LGA: {target_lga}")

# Get data for Kepler visualization filtered by LGA (limit to 2000 to avoid OOM)
kepler_full_df = spark.sql(f"""
    SELECT
        parcel_id,
        zone_code,
        lga_name,
        area_sqm,
        compactness_index,
        num_adjacent_parcels,
        longest_shared_boundary_m,
        nearest_any_pt_m,
        opportunity_score,
        constraint_score,
        suitability_score,
        suitability_tier,
        centroid_lon,
        centroid_lat,
        ST_AsGeoJSON(ST_Transform(geometry, 4326)) AS geojson
    FROM {catalog_name}.{schema_name}.consolidation_candidates
    WHERE geometry IS NOT NULL
      AND lga_name = '{target_lga}'
    ORDER BY suitability_score DESC
    LIMIT 2000
""")

kepler_full_data = kepler_full_df.toPandas()
print(f"Retrieved {len(kepler_full_data)} parcels for full Kepler export")

if len(kepler_full_data) > 0:
    # Convert GeoJSON strings to geometry objects for Kepler
    features = []
    for _, row in kepler_full_data.iterrows():
        try:
            geom = json.loads(row['geojson'])
            feature = {
                "type": "Feature",
                "geometry": geom,
                "properties": {
                    "parcel_id": row['parcel_id'],
                    "zone_code": row['zone_code'],
                    "lga_name": row['lga_name'],
                    "area_sqm": float(row['area_sqm']) if row['area_sqm'] else 0,
                    "compactness_index": float(row['compactness_index']) if row['compactness_index'] else 0,
                    "num_adjacent_parcels": int(row['num_adjacent_parcels']) if row['num_adjacent_parcels'] else 0,
                    "longest_shared_boundary_m": float(row['longest_shared_boundary_m']) if row['longest_shared_boundary_m'] else 0,
                    "nearest_any_pt_m": float(row['nearest_any_pt_m']) if row['nearest_any_pt_m'] else 0,
                    "opportunity_score": int(row['opportunity_score']) if row['opportunity_score'] else 0,
                    "suitability_score": int(row['suitability_score']) if row['suitability_score'] else 0,
                    "suitability_tier": row['suitability_tier']
                }
            }
            features.append(feature)
        except:
            pass

    geojson_collection = {
        "type": "FeatureCollection",
        "features": features
    }

    # Create Kepler map with configuration
    config = {
        "version": "v1",
        "config": {
            "mapState": {
                "latitude": kepler_full_data['centroid_lat'].mean(),
                "longitude": kepler_full_data['centroid_lon'].mean(),
                "zoom": 13
            },
            "visState": {
                "layers": [{
                    "type": "geojson",
                    "config": {
                        "dataId": "all_consolidation_candidates",
                        "label": "All Consolidation Candidates",
                        "color": [255, 153, 31],
                        "columns": {"geojson": "geometry"},
                        "isVisible": True,
                        "colorField": {"name": "suitability_score", "type": "integer"},
                        "colorScale": "quantize"
                    }
                }]
            }
        }
    }

    kepler_map_full = KeplerGl(height=600, config=config)
    kepler_map_full.add_data(data=geojson_collection, name="all_consolidation_candidates")

    # Save Kepler HTML to UC Volume
    volume_path = f"/Volumes/{catalog_name}/{schema_name}/source"
    kepler_full_html_path = f"{volume_path}/visualizations/kepler_consolidation_{target_lga.replace(' ', '_')}.html"
    kepler_map_full.save_to_html(file_name=kepler_full_html_path)
    print(f"Kepler map for {target_lga} saved to: {kepler_full_html_path}")
    print(f"Total parcels exported: {len(features):,}")
else:
    print("No data available for Kepler export")

# COMMAND ----------

# MAGIC %md
# MAGIC ### PyDeck 3D Visualization: Suitability Score Towers
# MAGIC
# MAGIC 3D column chart where height represents suitability score.

# COMMAND ----------

import pydeck as pdk

if len(candidates) > 0:
    # Prepare data for pydeck
    pydeck_data = candidates.copy()
    pydeck_data['elevation'] = pydeck_data['suitability_score'] * 3  # Scale for visibility

    # Color based on suitability tier
    def get_tier_color(tier):
        colors = {
            'Tier 1 - Excellent': [215, 25, 28, 200],
            'Tier 2 - Very Good': [253, 174, 97, 200],
            'Tier 3 - Good': [255, 255, 191, 200],
            'Tier 4 - Moderate': [166, 217, 106, 200],
            'Tier 5 - Low': [26, 150, 65, 200]
        }
        return colors.get(tier, [128, 128, 128, 200])

    pydeck_data['color'] = pydeck_data['suitability_tier'].apply(get_tier_color)

    # Create the deck
    layer = pdk.Layer(
        "ColumnLayer",
        data=pydeck_data,
        get_position=["centroid_lon", "centroid_lat"],
        get_elevation="elevation",
        elevation_scale=1,
        radius=10,
        get_fill_color="color",
        pickable=True,
        auto_highlight=True
    )

    view_state = pdk.ViewState(
        latitude=pydeck_data['centroid_lat'].mean(),
        longitude=pydeck_data['centroid_lon'].mean(),
        zoom=14,
        pitch=45,
        bearing=0
    )

    deck = pdk.Deck(
        layers=[layer],
        initial_view_state=view_state,
        tooltip={
            "html": "<b>Parcel:</b> {parcel_id}<br/>"
                    "<b>Zone:</b> {zone_code}<br/>"
                    "<b>Score:</b> {suitability_score}<br/>"
                    "<b>Tier:</b> {suitability_tier}<br/>"
                    "<b>Area:</b> {area_sqm:.0f} sqm",
            "style": {"backgroundColor": "steelblue", "color": "white"}
        }
    )

    display(deck)
else:
    print("No data available for 3D visualization")

# COMMAND ----------

# Export consolidation candidates for target LGA to PyDeck 3D HTML (filtered to avoid OOM)
import pydeck as pdk

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

# Use target_lga from previous cell (or get it again if running independently)
try:
    _ = target_lga
except NameError:
    target_lga = "CASEY"  # Default to Casey LGA

print(f"Exporting PyDeck 3D visualization for LGA: {target_lga}")

# Get candidates for 3D visualization filtered by LGA (limit to 2000 to avoid OOM)
all_pydeck_df = spark.sql(f"""
    SELECT
        parcel_id,
        zone_code,
        lga_name,
        area_sqm,
        suitability_score,
        suitability_tier,
        centroid_lon,
        centroid_lat
    FROM {catalog_name}.{schema_name}.consolidation_candidates
    WHERE centroid_lon IS NOT NULL
      AND centroid_lat IS NOT NULL
      AND lga_name = '{target_lga}'
    ORDER BY suitability_score DESC
    LIMIT 2000
""")

all_pydeck_data = all_pydeck_df.toPandas()
print(f"Retrieved {len(all_pydeck_data)} parcels for full PyDeck export")

if len(all_pydeck_data) > 0:
    # Prepare data for pydeck
    all_pydeck_data['elevation'] = all_pydeck_data['suitability_score'] * 3  # Scale for visibility

    # Color based on suitability tier
    def get_tier_color(tier):
        colors = {
            'Tier 1 - Excellent': [215, 25, 28, 200],
            'Tier 2 - Very Good': [253, 174, 97, 200],
            'Tier 3 - Good': [255, 255, 191, 200],
            'Tier 4 - Moderate': [166, 217, 106, 200],
            'Tier 5 - Low': [26, 150, 65, 200]
        }
        return colors.get(tier, [128, 128, 128, 200])

    all_pydeck_data['color'] = all_pydeck_data['suitability_tier'].apply(get_tier_color)

    # Create the deck
    layer = pdk.Layer(
        "ColumnLayer",
        data=all_pydeck_data,
        get_position=["centroid_lon", "centroid_lat"],
        get_elevation="elevation",
        elevation_scale=1,
        radius=10,
        get_fill_color="color",
        pickable=True,
        auto_highlight=True
    )

    view_state = pdk.ViewState(
        latitude=all_pydeck_data['centroid_lat'].mean(),
        longitude=all_pydeck_data['centroid_lon'].mean(),
        zoom=13,
        pitch=45,
        bearing=0
    )

    deck_full = pdk.Deck(
        layers=[layer],
        initial_view_state=view_state,
        tooltip={
            "html": "<b>Parcel:</b> {parcel_id}<br/>"
                    "<b>Zone:</b> {zone_code}<br/>"
                    "<b>Score:</b> {suitability_score}<br/>"
                    "<b>Tier:</b> {suitability_tier}<br/>"
                    "<b>Area:</b> {area_sqm:.0f} sqm",
            "style": {"backgroundColor": "steelblue", "color": "white"}
        }
    )

    # Save to UC Volume
    volume_path = f"/Volumes/{catalog_name}/{schema_name}/source"
    pydeck_html_path = f"{volume_path}/visualizations/pydeck_consolidation_3d_{target_lga.replace(' ', '_')}.html"
    deck_full.to_html(pydeck_html_path)
    print(f"PyDeck 3D map for {target_lga} saved to: {pydeck_html_path}")
    print(f"Total parcels exported: {len(all_pydeck_data):,}")
else:
    print("No data available for 3D export")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Scalable Geospatial Operations with Pandas UDFs
# MAGIC
# MAGIC For external API calls (routing, geocoding, etc.), use Pandas UDFs to maintain Spark's parallelism.
# MAGIC This pattern allows you to call external services at scale across the cluster.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Example: Scalable Walking Distance Calculation using OSRM
# MAGIC
# MAGIC This example shows how to use Pandas UDFs to call external routing APIs at scale.
# MAGIC OSRM (Open Source Routing Machine) provides free routing - you can use the demo server or host your own.

# COMMAND ----------

from pyspark.sql.functions import pandas_udf, col
from pyspark.sql.types import DoubleType, StructType, StructField
import pandas as pd

# Define a Pandas UDF for calculating walking distance to nearest PT stop
# This runs in parallel across the Spark cluster
@pandas_udf(DoubleType())
def calc_walking_distance_osrm(
    origin_lon: pd.Series,
    origin_lat: pd.Series,
    dest_lon: pd.Series,
    dest_lat: pd.Series
) -> pd.Series:
    """
    Calculate walking distance using OSRM routing API.
    This Pandas UDF runs in parallel across Spark workers.

    For production, consider:
    - Hosting your own OSRM instance for Victoria
    - Using batch requests to reduce API calls
    - Adding caching for repeated routes
    """
    import requests

    def get_walking_distance(o_lon, o_lat, d_lon, d_lat):
        try:
            # OSRM demo server - for production, host your own
            url = f"http://router.project-osrm.org/route/v1/foot/{o_lon},{o_lat};{d_lon},{d_lat}"
            response = requests.get(url, params={"overview": "false"}, timeout=5)
            if response.status_code == 200:
                data = response.json()
                if data.get("routes"):
                    return float(data["routes"][0]["distance"])  # Distance in meters
        except Exception:
            pass
        return None

    # Apply to each row in the partition
    return pd.Series([
        get_walking_distance(o_lon, o_lat, d_lon, d_lat)
        for o_lon, o_lat, d_lon, d_lat in zip(origin_lon, origin_lat, dest_lon, dest_lat)
    ])

# Note: The UDF above is defined but not executed yet - it will run when called on a DataFrame

# COMMAND ----------

# MAGIC %md
# MAGIC ### Example: Using the Pandas UDF for Routing at Scale
# MAGIC
# MAGIC Uncomment and run the cell below to calculate actual walking distances.
# MAGIC Note: The OSRM demo server has rate limits - for production, host your own instance.

# COMMAND ----------

# Example usage (commented out to avoid hitting rate limits on demo server):
#
# catalog_name = dbutils.widgets.get("catalog_name")
# schema_name = dbutils.widgets.get("schema_name")
#
# # Get sample parcels with their nearest PT stop coordinates
# sample_routes = spark.sql(f"""
#     SELECT
#         c.parcel_id,
#         c.centroid_lon AS origin_lon,
#         c.centroid_lat AS origin_lat,
#         pt.lon AS dest_lon,
#         pt.lat AS dest_lat
#     FROM {catalog_name}.{schema_name}.consolidation_candidates c
#     CROSS JOIN (
#         SELECT
#             ST_X(geometry) AS lon,
#             ST_Y(geometry) AS lat
#         FROM {catalog_name}.{schema_name}.pt_stops
#         WHERE MODE = 'Train'
#         LIMIT 1
#     ) pt
#     WHERE c.centroid_lon IS NOT NULL
#     LIMIT 50
# """)
#
# # Apply the Pandas UDF - this runs in parallel across the cluster
# routes_with_distance = sample_routes.withColumn(
#     "walking_distance_m",
#     calc_walking_distance_osrm(
#         col("origin_lon"), col("origin_lat"),
#         col("dest_lon"), col("dest_lat")
#     )
# )
#
# display(routes_with_distance)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scaling Spatial Joins: Hybrid Geometry-H3 Approach
# MAGIC
# MAGIC For large-scale spatial joins, the **hybrid geometry-H3 approach** provides significant performance gains.
# MAGIC Instead of expensive geometry-to-geometry comparisons, use H3 hexagonal indexes for initial filtering.
# MAGIC
# MAGIC **Reference:** [Spatial Analytics at Any Scale with H3 and Photon](https://www.databricks.com/blog/2022/12/13/spatial-analytics-any-scale-h3-and-photon.html)
# MAGIC
# MAGIC #### Key H3 Functions in Databricks SQL:
# MAGIC ```sql
# MAGIC -- Convert point to H3 index
# MAGIC h3_pointash3(geom_wkt, resolution)
# MAGIC
# MAGIC -- Tessellate polygon/linestring into H3 cells (returns array)
# MAGIC h3_tessellateaswkb(geom_wkt, resolution)
# MAGIC
# MAGIC -- Polyfill polygon with H3 cells
# MAGIC h3_polyfillash3(geom_wkt, resolution)
# MAGIC ```
# MAGIC
# MAGIC #### Example: Creating H3-indexed Silver Table
# MAGIC ```sql
# MAGIC -- For point geometries (e.g., PT stops)
# MAGIC CREATE TABLE silver_pt_stops AS
# MAGIC SELECT *, h3_pointash3(ST_AsText(ST_Transform(geometry, 4326)), 9) AS h3
# MAGIC FROM bronze_pt_stops;
# MAGIC
# MAGIC -- For polygon geometries (e.g., parcels) - tessellate and explode
# MAGIC CREATE TABLE silver_parcels AS
# MAGIC SELECT parcel_id, geometry,
# MAGIC        inline(h3_tessellateaswkb(ST_AsText(ST_Transform(geometry, 4326)), 9))
# MAGIC FROM bronze_parcels;
# MAGIC ```
# MAGIC
# MAGIC #### H3 Join Pattern (2-stage):
# MAGIC ```sql
# MAGIC -- Stage 1: Fast H3 join (filters candidates)
# MAGIC -- Stage 2: Precise geometry verification (ST_Contains/ST_Intersects)
# MAGIC SELECT p.parcel_id, z.zone_code
# MAGIC FROM silver_parcels p
# MAGIC JOIN silver_zones z ON p.h3 = z.h3  -- Fast H3 equality join
# MAGIC WHERE ST_Contains(z.geometry, ST_Centroid(p.geometry))  -- Precise check
# MAGIC ```
# MAGIC
# MAGIC **Benefits:**
# MAGIC - H3 joins use standard equality predicates (highly optimized)
# MAGIC - Reduces candidate pairs before expensive geometry operations
# MAGIC - Works with Photon for additional acceleration
# MAGIC
# MAGIC **Example Implementation:** [Exploring VicMaps Data](https://github.com/danny-db/Spatial-Analytics-Test-Drive/blob/main/vector/01.%20Exploring%20VicMaps%20Data%20(Skipped%20data%20ingestion).ipynb)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Folium Map: Best Consolidation Pairs
# MAGIC
# MAGIC Visualize the top consolidation pairs - adjacent parcels with high combined scores.

# COMMAND ----------

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

# Get top consolidation pairs with geometry
pairs_df = spark.sql(f"""
    WITH scored_adjacency AS (
        SELECT
            a.parcel_1,
            a.parcel_2,
            a.shared_boundary_m,
            s1.suitability_score AS score_1,
            s2.suitability_score AS score_2,
            s1.suitability_score + s2.suitability_score AS combined_score,
            s1.zone_code AS zone_1,
            s2.zone_code AS zone_2,
            ST_AsGeoJSON(ST_Transform(s1.geometry, 4326)) AS geojson_1,
            ST_AsGeoJSON(ST_Transform(s2.geometry, 4326)) AS geojson_2,
            s1.centroid_lon AS lon_1,
            s1.centroid_lat AS lat_1,
            s2.centroid_lon AS lon_2,
            s2.centroid_lat AS lat_2,
            ST_AsGeoJSON(ST_Transform(ST_Intersection(ST_Boundary(s1.geometry), ST_Boundary(s2.geometry)), 4326)) AS shared_boundary_geojson
        FROM {catalog_name}.{schema_name}.parcel_adjacency a
        JOIN {catalog_name}.{schema_name}.consolidation_candidates s1 ON a.parcel_1 = s1.parcel_id
        JOIN {catalog_name}.{schema_name}.consolidation_candidates s2 ON a.parcel_2 = s2.parcel_id
        WHERE a.shared_boundary_m > 0
    )
    SELECT *
    FROM scored_adjacency
    WHERE zone_1 = zone_2  -- Same zone pairs are best for consolidation
    ORDER BY combined_score DESC
    LIMIT 15
""")

consolidation_pairs = pairs_df.toPandas()
print(f"Retrieved {len(consolidation_pairs)} consolidation pairs for visualization")

# COMMAND ----------

import folium
import json

if len(consolidation_pairs) > 0:
    # Create map
    center_lat = consolidation_pairs['lat_1'].mean()
    center_lon = consolidation_pairs['lon_1'].mean()

    m = folium.Map(
        location=[center_lat, center_lon],
        zoom_start=16,
        tiles='CartoDB positron'
    )

    # Color by combined score
    def get_pair_color(score):
        if score >= 150:
            return '#d7191c'  # Dark red - excellent
        elif score >= 120:
            return '#fdae61'  # Orange - very good
        elif score >= 100:
            return '#ffffbf'  # Yellow - good
        else:
            return '#a6d96a'  # Light green - moderate

    for idx, row in consolidation_pairs.iterrows():
        color = get_pair_color(row['combined_score'])

        # Add first parcel
        try:
            geojson_1 = json.loads(row['geojson_1'])
            folium.GeoJson(
                geojson_1,
                style_function=lambda x, c=color: {
                    'fillColor': c,
                    'color': '#333333',
                    'weight': 2,
                    'fillOpacity': 0.6
                },
                tooltip=f"Parcel 1: {row['parcel_1']}<br>Score: {row['score_1']}<br>Zone: {row['zone_1']}"
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
                    'color': '#333333',
                    'weight': 2,
                    'fillOpacity': 0.6
                },
                tooltip=f"Parcel 2: {row['parcel_2']}<br>Score: {row['score_2']}<br>Zone: {row['zone_2']}"
            ).add_to(m)
        except:
            pass

        # Add shared boundary highlight
        try:
            if row['shared_boundary_geojson']:
                shared_geom = json.loads(row['shared_boundary_geojson'])
                folium.GeoJson(
                    shared_geom,
                    style_function=lambda x: {
                        'color': '#0000ff',
                        'weight': 5,
                        'opacity': 0.9
                    },
                    tooltip=f"Shared: {row['shared_boundary_m']:.1f}m<br>Combined Score: {row['combined_score']}"
                ).add_to(m)
        except:
            pass

        # Add label at midpoint
        mid_lat = (row['lat_1'] + row['lat_2']) / 2
        mid_lon = (row['lon_1'] + row['lon_2']) / 2
        folium.Marker(
            [mid_lat, mid_lon],
            icon=folium.DivIcon(
                html=f'<div style="font-size: 10px; font-weight: bold; color: blue;">#{idx+1}</div>'
            )
        ).add_to(m)

    # Add legend
    legend_html = '''
    <div style="position: fixed;
                bottom: 50px; left: 50px; width: 220px; height: 130px;
                border:2px solid grey; z-index:9999; font-size:14px;
                background-color:white; padding: 10px;
                border-radius: 5px;">
        <b>Consolidation Priority</b><br>
        <i style="background:#d7191c; width:18px; height:18px; display:inline-block;"></i> Excellent (150+)<br>
        <i style="background:#fdae61; width:18px; height:18px; display:inline-block;"></i> Very Good (120-149)<br>
        <i style="background:#ffffbf; width:18px; height:18px; display:inline-block;"></i> Good (100-119)<br>
        <i style="background:blue; width:18px; height:3px; display:inline-block;"></i> Shared Boundary
    </div>
    '''
    m.get_root().html.add_child(folium.Element(legend_html))

    # Use displayHTML for proper rendering in Databricks
    displayHTML(m._repr_html_())
else:
    print("No consolidation pairs found for visualization")

# COMMAND ----------

# Export consolidation pairs for target LGA to Folium HTML (filtered to avoid OOM)
import folium
import json

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

# Use target_lga from previous cell (or get it again if running independently)
try:
    _ = target_lga
except NameError:
    target_lga = "CASEY"  # Default to Casey LGA

print(f"Exporting consolidation pairs for LGA: {target_lga}")

# Get consolidation pairs filtered by LGA (limit to top 500 pairs with high scores to avoid OOM)
all_pairs_df = spark.sql(f"""
    WITH scored_adjacency AS (
        SELECT
            a.parcel_1,
            a.parcel_2,
            a.shared_boundary_m,
            s1.suitability_score AS score_1,
            s2.suitability_score AS score_2,
            s1.suitability_score + s2.suitability_score AS combined_score,
            s1.zone_code AS zone_1,
            s2.zone_code AS zone_2,
            ST_AsGeoJSON(ST_Transform(s1.geometry, 4326)) AS geojson_1,
            ST_AsGeoJSON(ST_Transform(s2.geometry, 4326)) AS geojson_2,
            s1.centroid_lon AS lon_1,
            s1.centroid_lat AS lat_1,
            s2.centroid_lon AS lon_2,
            s2.centroid_lat AS lat_2,
            ST_AsGeoJSON(ST_Transform(ST_Intersection(ST_Boundary(s1.geometry), ST_Boundary(s2.geometry)), 4326)) AS shared_boundary_geojson
        FROM {catalog_name}.{schema_name}.parcel_adjacency a
        JOIN {catalog_name}.{schema_name}.consolidation_candidates s1 ON a.parcel_1 = s1.parcel_id
        JOIN {catalog_name}.{schema_name}.consolidation_candidates s2 ON a.parcel_2 = s2.parcel_id
        WHERE a.shared_boundary_m > 5
          AND s1.lga_name = '{target_lga}'
          AND s1.suitability_score >= 40
          AND s2.suitability_score >= 40
    )
    SELECT *
    FROM scored_adjacency
    WHERE zone_1 = zone_2  -- Same zone pairs are best for consolidation
    ORDER BY combined_score DESC
    LIMIT 500
""")

all_consolidation_pairs = all_pairs_df.toPandas()
print(f"Retrieved {len(all_consolidation_pairs)} consolidation pairs for full export")

if len(all_consolidation_pairs) > 0:
    # Create map
    center_lat = all_consolidation_pairs['lat_1'].mean()
    center_lon = all_consolidation_pairs['lon_1'].mean()

    m_pairs = folium.Map(
        location=[center_lat, center_lon],
        zoom_start=14,
        tiles='CartoDB positron'
    )

    # Color by combined score
    def get_pair_color(score):
        if score >= 150:
            return '#d7191c'  # Dark red - excellent
        elif score >= 120:
            return '#fdae61'  # Orange - very good
        elif score >= 100:
            return '#ffffbf'  # Yellow - good
        else:
            return '#a6d96a'  # Light green - moderate

    # Track processed parcels to avoid duplicates
    processed_parcels = set()

    for idx, row in all_consolidation_pairs.iterrows():
        color = get_pair_color(row['combined_score'])

        # Add first parcel if not already processed
        if row['parcel_1'] not in processed_parcels:
            try:
                geojson_1 = json.loads(row['geojson_1'])
                folium.GeoJson(
                    geojson_1,
                    style_function=lambda x, c=color: {
                        'fillColor': c,
                        'color': '#333333',
                        'weight': 1,
                        'fillOpacity': 0.6
                    },
                    tooltip=f"Parcel: {row['parcel_1']}<br>Score: {row['score_1']}<br>Zone: {row['zone_1']}"
                ).add_to(m_pairs)
                processed_parcels.add(row['parcel_1'])
            except:
                pass

        # Add second parcel if not already processed
        if row['parcel_2'] not in processed_parcels:
            try:
                geojson_2 = json.loads(row['geojson_2'])
                folium.GeoJson(
                    geojson_2,
                    style_function=lambda x, c=color: {
                        'fillColor': c,
                        'color': '#333333',
                        'weight': 1,
                        'fillOpacity': 0.6
                    },
                    tooltip=f"Parcel: {row['parcel_2']}<br>Score: {row['score_2']}<br>Zone: {row['zone_2']}"
                ).add_to(m_pairs)
                processed_parcels.add(row['parcel_2'])
            except:
                pass

        # Add shared boundary highlight for top pairs
        if row['combined_score'] >= 100:
            try:
                if row['shared_boundary_geojson']:
                    shared_geom = json.loads(row['shared_boundary_geojson'])
                    folium.GeoJson(
                        shared_geom,
                        style_function=lambda x: {
                            'color': '#0000ff',
                            'weight': 3,
                            'opacity': 0.8
                        },
                        tooltip=f"Shared: {row['shared_boundary_m']:.1f}m<br>Combined Score: {row['combined_score']}"
                    ).add_to(m_pairs)
            except:
                pass

    # Add legend
    legend_html = '''
    <div style="position: fixed;
                bottom: 50px; left: 50px; width: 220px; height: 130px;
                border:2px solid grey; z-index:9999; font-size:14px;
                background-color:white; padding: 10px;
                border-radius: 5px;">
        <b>Consolidation Priority</b><br>
        <i style="background:#d7191c; width:18px; height:18px; display:inline-block;"></i> Excellent (150+)<br>
        <i style="background:#fdae61; width:18px; height:18px; display:inline-block;"></i> Very Good (120-149)<br>
        <i style="background:#ffffbf; width:18px; height:18px; display:inline-block;"></i> Good (100-119)<br>
        <i style="background:blue; width:18px; height:3px; display:inline-block;"></i> Shared Boundary
    </div>
    '''
    m_pairs.get_root().html.add_child(folium.Element(legend_html))

    # Save to UC Volume
    volume_path = f"/Volumes/{catalog_name}/{schema_name}/source"
    pairs_html_path = f"{volume_path}/visualizations/folium_consolidation_pairs_{target_lga.replace(' ', '_')}.html"
    m_pairs.save(pairs_html_path)
    print(f"Consolidation pairs map for {target_lga} saved to: {pairs_html_path}")
    print(f"Total pairs exported: {len(all_consolidation_pairs):,}")
    print(f"Unique parcels in visualization: {len(processed_parcels):,}")
else:
    print("No consolidation pairs available for export")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC This notebook implemented a **multi-criteria suitability scoring model** for site consolidation:
# MAGIC
# MAGIC ### Scoring Factors
# MAGIC
# MAGIC **Opportunities (Positive Score):**
# MAGIC | Factor | Points |
# MAGIC |--------|--------|
# MAGIC | HCTZ/MUZ Zone | +30 |
# MAGIC | GRZ/NRZ Zone | +15 |
# MAGIC | Within 800m of Train/Tram | +20 |
# MAGIC | Within 400m of Bus | +10 |
# MAGIC | Below 500sqm | +25 |
# MAGIC | Below 300sqm | +10 (additional) |
# MAGIC | Below 15m frontage | +20 |
# MAGIC | Narrow lot (<10m) | +10 (additional) |
# MAGIC | Poor compactness (<0.3) | +15 |
# MAGIC | High elongation (>3) | +10 |
# MAGIC | Internal lot | +20 |
# MAGIC | Adjacent same-zone parcels | +15 |
# MAGIC | Long shared boundary (>15m) | +10 |
# MAGIC
# MAGIC **Constraints (Negative Score):**
# MAGIC | Factor | Points |
# MAGIC |--------|--------|
# MAGIC | Isolated lot | -20 |
# MAGIC | Large lot (>1000sqm) | -15 |
# MAGIC | Heritage overlay | -50 |
# MAGIC | Flood overlay | -40 |
# MAGIC | ESO overlay | -30 |
# MAGIC
# MAGIC ### Output Tables
# MAGIC - `parcel_pt_proximity` - Distance to PT stops
# MAGIC - `parcel_suitability_scores` - Individual and total scores
# MAGIC - `consolidation_candidates` - Final ranked output with geometry
