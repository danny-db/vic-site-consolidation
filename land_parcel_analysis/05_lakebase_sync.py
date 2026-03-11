# Databricks notebook source
# MAGIC %md
# MAGIC # Stage 5: Data Export & Verification
# MAGIC
# MAGIC This notebook verifies the consolidation candidate data and creates
# MAGIC derived views for the Databricks App visualization layer.
# MAGIC
# MAGIC ## What it does:
# MAGIC 1. Verifies `consolidation_candidates` table has data
# MAGIC 2. Creates `consolidation_pairs` view for the best adjacent parcel pairs
# MAGIC 3. Prints summary statistics for verification

# COMMAND ----------

# No additional pip packages needed (pure Spark SQL)

# COMMAND ----------

# Create widgets
dbutils.widgets.text("catalog_name", "danny_catalog", "Catalog Name")
dbutils.widgets.text("schema_name", "dtp_hackathon", "Schema Name")
dbutils.widgets.text("lakebase_project_name", "vic-site-consolidation", "Lakebase Project Name")

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

print(f"Using: {catalog_name}.{schema_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Verify Consolidation Candidates

# COMMAND ----------

total = spark.sql(f"SELECT COUNT(*) AS cnt FROM {catalog_name}.{schema_name}.consolidation_candidates").first()["cnt"]
print(f"Total consolidation candidates: {total:,}")

if total == 0:
    raise ValueError("consolidation_candidates table is empty! Check upstream pipeline stages.")

# Tier distribution
print("\nTier distribution:")
tier_df = spark.sql(f"""
    SELECT suitability_tier, COUNT(*) AS cnt,
           ROUND(AVG(suitability_score), 1) AS avg_score
    FROM {catalog_name}.{schema_name}.consolidation_candidates
    GROUP BY suitability_tier
    ORDER BY avg_score DESC
""")
display(tier_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Growth-Area Constraint Verification

# COMMAND ----------

# Verify growth-area penalties are applied
print("Growth-area LGA analysis:")
growth_df = spark.sql(f"""
    SELECT
        is_growth_area_lga,
        COUNT(*) AS cnt,
        ROUND(AVG(suitability_score), 1) AS avg_score,
        ROUND(AVG(score_growth_area_lga), 1) AS avg_lga_penalty,
        ROUND(AVG(score_recent_subdivision), 1) AS avg_subdiv_penalty,
        ROUND(AVG(score_mass_subdivision), 1) AS avg_mass_penalty
    FROM {catalog_name}.{schema_name}.consolidation_candidates
    GROUP BY is_growth_area_lga
    ORDER BY is_growth_area_lga
""")
display(growth_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Create Consolidation Pairs View

# COMMAND ----------

# Create a view for the best consolidation pairs (adjacent parcels with high combined scores)
spark.sql(f"""
    CREATE OR REPLACE VIEW {catalog_name}.{schema_name}.consolidation_pairs AS
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

pairs_count = spark.sql(f"SELECT COUNT(*) AS cnt FROM {catalog_name}.{schema_name}.consolidation_pairs").first()["cnt"]
print(f"Total consolidation pairs: {pairs_count:,}")

# Top pairs
print("\nTop 10 consolidation pairs:")
display(spark.sql(f"""
    SELECT * FROM {catalog_name}.{schema_name}.consolidation_pairs
    ORDER BY combined_score DESC
    LIMIT 10
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Top LGAs for Consolidation

# COMMAND ----------

print("Top LGAs by average suitability score (non-growth-area):")
display(spark.sql(f"""
    SELECT
        lga_name,
        COUNT(*) AS parcel_count,
        ROUND(AVG(suitability_score), 1) AS avg_score,
        SUM(CASE WHEN suitability_tier = 'Tier 1 - Excellent' THEN 1 ELSE 0 END) AS tier1_count,
        SUM(CASE WHEN suitability_tier = 'Tier 2 - Very Good' THEN 1 ELSE 0 END) AS tier2_count,
        is_growth_area_lga
    FROM {catalog_name}.{schema_name}.consolidation_candidates
    GROUP BY lga_name, is_growth_area_lga
    HAVING COUNT(*) >= 100
    ORDER BY avg_score DESC
    LIMIT 20
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC This notebook:
# MAGIC 1. Verified `consolidation_candidates` table has data
# MAGIC 2. Validated growth-area constraint penalties are applied
# MAGIC 3. Created `consolidation_pairs` view for the app's pairs endpoint
# MAGIC 4. Summarized top LGAs and tier distribution
# MAGIC
# MAGIC The Databricks App (`app/`) connects directly to the SQL Warehouse
# MAGIC to serve interactive maps from these Unity Catalog tables.
