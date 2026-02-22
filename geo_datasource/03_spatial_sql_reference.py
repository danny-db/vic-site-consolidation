# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks Spatial SQL Reference
# MAGIC
# MAGIC This notebook demonstrates the native **Databricks Spatial SQL** functions for geospatial analysis.
# MAGIC These functions are optimized for performance and work directly with `GEOMETRY` and `GEOGRAPHY` types.
# MAGIC
# MAGIC **Requirements:** Databricks Runtime 17.1+ or Databricks SQL
# MAGIC
# MAGIC **Documentation:** [ST Geospatial Functions](https://docs.databricks.com/sql/language-manual/sql-ref-st-geospatial-functions.html)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup: Configure Unity Catalog Location

# COMMAND ----------

# Create widgets for catalog and schema names
dbutils.widgets.text("catalog_name", "danny_catalog", "Catalog Name")
dbutils.widgets.text("schema_name", "dtp_hackathon", "Schema Name")

# Get widget values
catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

print(f"Using catalog: {catalog_name}")
print(f"Using schema: {schema_name}")

# COMMAND ----------

# Create catalog and schema if they don't exist
spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name}")

print(f"Catalog '{catalog_name}' is ready")
print(f"Schema '{catalog_name}.{schema_name}' is ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup: Create Sample Datasets
# MAGIC
# MAGIC We'll create sample datasets to demonstrate Spatial SQL functions:
# MAGIC 1. **Countries** - Polygons representing country boundaries
# MAGIC 2. **Cities** - Points representing city locations
# MAGIC 3. **Routes** - LineStrings representing travel routes

# COMMAND ----------

# Create sample countries (simplified polygon boundaries)
countries_data = [
    ("Australia", "POLYGON ((113 -44, 154 -44, 154 -10, 113 -10, 113 -44))", 25690000, 1400000),
    ("New Zealand", "POLYGON ((166 -47, 179 -47, 179 -34, 166 -34, 166 -47))", 5100000, 250000),
    ("Indonesia", "POLYGON ((95 -11, 141 -11, 141 6, 95 6, 95 -11))", 273500000, 1200000),
    ("Papua New Guinea", "POLYGON ((141 -12, 156 -12, 156 -1, 141 -1, 141 -12))", 9000000, 30000),
]

df_countries = spark.createDataFrame(
    countries_data,
    ["country_name", "geometry_wkt", "population", "gdp_millions"]
)
df_countries.createOrReplaceTempView("countries")

print("Countries table created:")
display(df_countries)

# COMMAND ----------

# Create sample cities with point geometries
cities_data = [
    ("Melbourne", "Australia", "POINT (144.9631 -37.8136)", 5000000),
    ("Sydney", "Australia", "POINT (151.2093 -33.8688)", 5300000),
    ("Brisbane", "Australia", "POINT (153.0251 -27.4698)", 2500000),
    ("Perth", "Australia", "POINT (115.8605 -31.9505)", 2100000),
    ("Adelaide", "Australia", "POINT (138.6007 -34.9285)", 1400000),
    ("Auckland", "New Zealand", "POINT (174.7633 -36.8485)", 1600000),
    ("Wellington", "New Zealand", "POINT (174.7762 -41.2866)", 215000),
    ("Jakarta", "Indonesia", "POINT (106.8456 -6.2088)", 10500000),
    ("Bali", "Indonesia", "POINT (115.1889 -8.4095)", 4300000),
    ("Port Moresby", "Papua New Guinea", "POINT (147.1803 -9.4438)", 380000),
]

df_cities = spark.createDataFrame(
    cities_data,
    ["city_name", "country", "geometry_wkt", "population"]
)
df_cities.createOrReplaceTempView("cities")

print("Cities table created:")
display(df_cities)

# COMMAND ----------

# Create sample routes (LineStrings)
routes_data = [
    ("Melbourne-Sydney", "LINESTRING (144.9631 -37.8136, 147.5 -36.0, 151.2093 -33.8688)"),
    ("Sydney-Brisbane", "LINESTRING (151.2093 -33.8688, 153.0251 -27.4698)"),
    ("Melbourne-Adelaide", "LINESTRING (144.9631 -37.8136, 138.6007 -34.9285)"),
    ("Auckland-Wellington", "LINESTRING (174.7633 -36.8485, 174.7762 -41.2866)"),
    ("Jakarta-Bali", "LINESTRING (106.8456 -6.2088, 115.1889 -8.4095)"),
]

df_routes = spark.createDataFrame(routes_data, ["route_name", "geometry_wkt"])
df_routes.createOrReplaceTempView("routes")

print("Routes table created:")
display(df_routes)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Import/Export Functions
# MAGIC
# MAGIC Convert between WKT/WKB strings and native GEOMETRY types.
# MAGIC
# MAGIC **Note:** The native `GEOMETRY` type cannot be displayed directly in query results, but can be stored in Delta tables.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Convert WKT to different formats
# MAGIC SELECT
# MAGIC     city_name,
# MAGIC     geometry_wkt AS original_wkt,
# MAGIC     ST_AsWKT(ST_GeomFromWKT(geometry_wkt)) AS back_to_wkt,
# MAGIC     ST_AsBinary(ST_GeomFromWKT(geometry_wkt)) AS as_wkb,
# MAGIC     ST_AsGeoJSON(ST_GeomFromWKT(geometry_wkt)) AS as_geojson
# MAGIC FROM cities
# MAGIC LIMIT 3

# COMMAND ----------

# MAGIC %md
# MAGIC ### Materialize GEOMETRY to Delta Tables
# MAGIC
# MAGIC Store native GEOMETRY columns in Delta tables for optimized spatial queries.

# COMMAND ----------

# Get widget values for table creation
catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

# Create cities table with native GEOMETRY column
spark.sql(f"""
    CREATE OR REPLACE TABLE {catalog_name}.{schema_name}.cities_geo AS
    SELECT
        city_name,
        country,
        ST_GeomFromWKT(geometry_wkt, 4326) AS geometry,
        population
    FROM cities
""")

# Create countries table with native GEOMETRY column
spark.sql(f"""
    CREATE OR REPLACE TABLE {catalog_name}.{schema_name}.countries_geo AS
    SELECT
        country_name,
        ST_GeomFromWKT(geometry_wkt, 4326) AS geometry,
        population,
        gdp_millions
    FROM countries
""")

# Create routes table with native GEOMETRY column
spark.sql(f"""
    CREATE OR REPLACE TABLE {catalog_name}.{schema_name}.routes_geo AS
    SELECT
        route_name,
        ST_GeomFromWKT(geometry_wkt, 4326) AS geometry
    FROM routes
""")

print(f"Created tables with native GEOMETRY columns:")
print(f"  - {catalog_name}.{schema_name}.cities_geo")
print(f"  - {catalog_name}.{schema_name}.countries_geo")
print(f"  - {catalog_name}.{schema_name}.routes_geo")

# COMMAND ----------

# Verify the table schema - GEOMETRY type is stored natively
catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

print("Cities table schema:")
spark.sql(f"DESCRIBE {catalog_name}.{schema_name}.cities_geo").show(truncate=False)

# COMMAND ----------

# Query from materialized table - convert GEOMETRY back to WKT for display
catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

df_from_table = spark.sql(f"""
    SELECT
        city_name,
        country,
        ST_AsWKT(geometry) AS geometry_wkt,
        ST_X(geometry) AS longitude,
        ST_Y(geometry) AS latitude,
        population
    FROM {catalog_name}.{schema_name}.cities_geo
""")

display(df_from_table)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Accessor Functions
# MAGIC
# MAGIC Extract coordinates and properties from geometries.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Extract X/Y coordinates from points
# MAGIC SELECT
# MAGIC     city_name,
# MAGIC     country,
# MAGIC     ST_X(ST_GeomFromWKT(geometry_wkt)) AS longitude,
# MAGIC     ST_Y(ST_GeomFromWKT(geometry_wkt)) AS latitude,
# MAGIC     ST_GeometryType(ST_GeomFromWKT(geometry_wkt)) AS geometry_type,
# MAGIC     ST_SRID(ST_GeomFromWKT(geometry_wkt, 4326)) AS srid
# MAGIC FROM cities

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Get bounding box coordinates for polygons
# MAGIC SELECT
# MAGIC     country_name,
# MAGIC     ST_XMin(ST_GeomFromWKT(geometry_wkt)) AS min_longitude,
# MAGIC     ST_YMin(ST_GeomFromWKT(geometry_wkt)) AS min_latitude,
# MAGIC     ST_XMax(ST_GeomFromWKT(geometry_wkt)) AS max_longitude,
# MAGIC     ST_YMax(ST_GeomFromWKT(geometry_wkt)) AS max_latitude
# MAGIC FROM countries

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Get centroid of polygons
# MAGIC SELECT
# MAGIC     country_name,
# MAGIC     ST_AsWKT(ST_Centroid(ST_GeomFromWKT(geometry_wkt))) AS centroid_wkt,
# MAGIC     ST_X(ST_Centroid(ST_GeomFromWKT(geometry_wkt))) AS centroid_longitude,
# MAGIC     ST_Y(ST_Centroid(ST_GeomFromWKT(geometry_wkt))) AS centroid_latitude
# MAGIC FROM countries

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Get start and end points of linestrings
# MAGIC SELECT
# MAGIC     route_name,
# MAGIC     ST_AsWKT(ST_StartPoint(ST_GeomFromWKT(geometry_wkt))) AS start_point,
# MAGIC     ST_AsWKT(ST_EndPoint(ST_GeomFromWKT(geometry_wkt))) AS end_point,
# MAGIC     ST_NPoints(ST_GeomFromWKT(geometry_wkt)) AS num_points
# MAGIC FROM routes

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Measurement Functions
# MAGIC
# MAGIC Calculate area, length, perimeter, and distances.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Calculate area and perimeter of country polygons (in square degrees for unprojected data)
# MAGIC SELECT
# MAGIC     country_name,
# MAGIC     population,
# MAGIC     ROUND(ST_Area(ST_GeomFromWKT(geometry_wkt)), 2) AS area_sq_degrees,
# MAGIC     ROUND(ST_Perimeter(ST_GeomFromWKT(geometry_wkt)), 2) AS perimeter_degrees
# MAGIC FROM countries
# MAGIC ORDER BY area_sq_degrees DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Calculate length of routes (in degrees for unprojected data)
# MAGIC SELECT
# MAGIC     route_name,
# MAGIC     ROUND(ST_Length(ST_GeomFromWKT(geometry_wkt)), 4) AS length_degrees
# MAGIC FROM routes
# MAGIC ORDER BY length_degrees DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Calculate Cartesian distance between geometries
# MAGIC SELECT
# MAGIC     c1.city_name AS city1,
# MAGIC     c2.city_name AS city2,
# MAGIC     ROUND(ST_Distance(
# MAGIC         ST_GeomFromWKT(c1.geometry_wkt),
# MAGIC         ST_GeomFromWKT(c2.geometry_wkt)
# MAGIC     ), 4) AS cartesian_distance_degrees
# MAGIC FROM cities c1
# MAGIC CROSS JOIN cities c2
# MAGIC WHERE c1.city_name = 'Melbourne' AND c1.city_name != c2.city_name
# MAGIC ORDER BY cartesian_distance_degrees

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Calculate spherical distance in kilometers (great circle distance)
# MAGIC -- ST_DistanceSphere returns meters, divide by 1000 for kilometers
# MAGIC SELECT
# MAGIC     c1.city_name AS from_city,
# MAGIC     c2.city_name AS to_city,
# MAGIC     ROUND(
# MAGIC         ST_DistanceSphere(
# MAGIC             ST_GeomFromWKT(c1.geometry_wkt),
# MAGIC             ST_GeomFromWKT(c2.geometry_wkt)
# MAGIC         ) / 1000,
# MAGIC         2
# MAGIC     ) AS distance_km
# MAGIC FROM cities c1
# MAGIC CROSS JOIN cities c2
# MAGIC WHERE c1.city_name = 'Melbourne' AND c1.city_name != c2.city_name
# MAGIC ORDER BY distance_km

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Topological Relationship Functions
# MAGIC
# MAGIC Test spatial relationships between geometries.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check which cities are contained within which countries
# MAGIC SELECT
# MAGIC     ci.city_name,
# MAGIC     co.country_name,
# MAGIC     ST_Contains(
# MAGIC         ST_GeomFromWKT(co.geometry_wkt),
# MAGIC         ST_GeomFromWKT(ci.geometry_wkt)
# MAGIC     ) AS city_in_country
# MAGIC FROM cities ci
# MAGIC CROSS JOIN countries co
# MAGIC WHERE ST_Contains(
# MAGIC     ST_GeomFromWKT(co.geometry_wkt),
# MAGIC     ST_GeomFromWKT(ci.geometry_wkt)
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check if geometries intersect
# MAGIC SELECT
# MAGIC     c1.country_name AS country1,
# MAGIC     c2.country_name AS country2,
# MAGIC     ST_Intersects(
# MAGIC         ST_GeomFromWKT(c1.geometry_wkt),
# MAGIC         ST_GeomFromWKT(c2.geometry_wkt)
# MAGIC     ) AS intersects,
# MAGIC     ST_Touches(
# MAGIC         ST_GeomFromWKT(c1.geometry_wkt),
# MAGIC         ST_GeomFromWKT(c2.geometry_wkt)
# MAGIC     ) AS touches
# MAGIC FROM countries c1
# MAGIC CROSS JOIN countries c2
# MAGIC WHERE c1.country_name < c2.country_name

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Find cities within a certain distance (using ST_DWithin)
# MAGIC -- Find all cities within 10 degrees of Melbourne
# MAGIC SELECT
# MAGIC     c1.city_name AS reference_city,
# MAGIC     c2.city_name AS nearby_city,
# MAGIC     c2.country,
# MAGIC     ROUND(ST_Distance(
# MAGIC         ST_GeomFromWKT(c1.geometry_wkt),
# MAGIC         ST_GeomFromWKT(c2.geometry_wkt)
# MAGIC     ), 2) AS distance_degrees
# MAGIC FROM cities c1
# MAGIC CROSS JOIN cities c2
# MAGIC WHERE c1.city_name = 'Melbourne'
# MAGIC   AND c1.city_name != c2.city_name
# MAGIC   AND ST_DWithin(
# MAGIC         ST_GeomFromWKT(c1.geometry_wkt),
# MAGIC         ST_GeomFromWKT(c2.geometry_wkt),
# MAGIC         10  -- within 10 degrees
# MAGIC       )
# MAGIC ORDER BY distance_degrees

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Geometry Processing Functions
# MAGIC
# MAGIC Create new geometries through spatial operations.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create buffer around cities (e.g., 2 degree buffer)
# MAGIC SELECT
# MAGIC     city_name,
# MAGIC     ST_AsWKT(ST_GeomFromWKT(geometry_wkt)) AS original_point,
# MAGIC     ST_AsWKT(ST_Buffer(ST_GeomFromWKT(geometry_wkt), 2)) AS buffer_2_degrees
# MAGIC FROM cities
# MAGIC WHERE city_name = 'Melbourne'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Get envelope (bounding box) of geometries
# MAGIC SELECT
# MAGIC     route_name,
# MAGIC     geometry_wkt AS original_linestring,
# MAGIC     ST_AsWKT(ST_Envelope(ST_GeomFromWKT(geometry_wkt))) AS bounding_box
# MAGIC FROM routes

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Get convex hull of a geometry
# MAGIC SELECT
# MAGIC     country_name,
# MAGIC     ST_AsWKT(ST_ConvexHull(ST_GeomFromWKT(geometry_wkt))) AS convex_hull
# MAGIC FROM countries
# MAGIC WHERE country_name = 'Australia'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Simplify geometries (useful for reducing complexity)
# MAGIC SELECT
# MAGIC     country_name,
# MAGIC     ST_NPoints(ST_GeomFromWKT(geometry_wkt)) AS original_points,
# MAGIC     ST_NPoints(ST_Simplify(ST_GeomFromWKT(geometry_wkt), 5)) AS simplified_points,
# MAGIC     ST_AsWKT(ST_Simplify(ST_GeomFromWKT(geometry_wkt), 5)) AS simplified_geometry
# MAGIC FROM countries

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Overlay Functions
# MAGIC
# MAGIC Compute intersection, union, and difference of geometries.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create two overlapping polygons for demonstration
# MAGIC WITH overlapping_polygons AS (
# MAGIC     SELECT
# MAGIC         'Region A' AS name,
# MAGIC         'POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))' AS geometry_wkt
# MAGIC     UNION ALL
# MAGIC     SELECT
# MAGIC         'Region B' AS name,
# MAGIC         'POLYGON ((5 5, 15 5, 15 15, 5 15, 5 5))' AS geometry_wkt
# MAGIC )
# MAGIC SELECT
# MAGIC     ST_AsWKT(ST_Intersection(
# MAGIC         ST_GeomFromWKT(a.geometry_wkt),
# MAGIC         ST_GeomFromWKT(b.geometry_wkt)
# MAGIC     )) AS intersection_geometry,
# MAGIC     ST_AsWKT(ST_Union(
# MAGIC         ST_GeomFromWKT(a.geometry_wkt),
# MAGIC         ST_GeomFromWKT(b.geometry_wkt)
# MAGIC     )) AS union_geometry,
# MAGIC     ST_AsWKT(ST_Difference(
# MAGIC         ST_GeomFromWKT(a.geometry_wkt),
# MAGIC         ST_GeomFromWKT(b.geometry_wkt)
# MAGIC     )) AS difference_a_minus_b
# MAGIC FROM overlapping_polygons a, overlapping_polygons b
# MAGIC WHERE a.name = 'Region A' AND b.name = 'Region B'

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Coordinate Reference System (CRS) Functions
# MAGIC
# MAGIC Transform geometries between coordinate systems.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Transform from WGS84 (EPSG:4326) to Web Mercator (EPSG:3857)
# MAGIC SELECT
# MAGIC     city_name,
# MAGIC     geometry_wkt AS wgs84_wkt,
# MAGIC     ST_AsWKT(
# MAGIC         ST_Transform(
# MAGIC             ST_GeomFromWKT(geometry_wkt, 4326),  -- Source CRS: WGS84
# MAGIC             3857                                  -- Target CRS: Web Mercator
# MAGIC         )
# MAGIC     ) AS web_mercator_wkt
# MAGIC FROM cities
# MAGIC WHERE city_name IN ('Melbourne', 'Sydney')

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Get and set SRID
# MAGIC SELECT
# MAGIC     city_name,
# MAGIC     ST_SRID(ST_GeomFromWKT(geometry_wkt)) AS default_srid,
# MAGIC     ST_SRID(ST_GeomFromWKT(geometry_wkt, 4326)) AS explicit_srid,
# MAGIC     ST_SRID(ST_SetSRID(ST_GeomFromWKT(geometry_wkt), 4326)) AS set_srid
# MAGIC FROM cities
# MAGIC LIMIT 3

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Aggregation Functions
# MAGIC
# MAGIC Aggregate multiple geometries.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Get the union of all city points per country
# MAGIC SELECT
# MAGIC     country,
# MAGIC     COUNT(*) AS num_cities,
# MAGIC     ST_AsWKT(ST_Union_Agg(ST_GeomFromWKT(geometry_wkt))) AS all_cities_union
# MAGIC FROM cities
# MAGIC GROUP BY country

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Get the bounding box (envelope) of all cities per country
# MAGIC SELECT
# MAGIC     country,
# MAGIC     ST_AsWKT(ST_Envelope_Agg(ST_GeomFromWKT(geometry_wkt))) AS cities_bounding_box
# MAGIC FROM cities
# MAGIC GROUP BY country

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Geometry Constructors
# MAGIC
# MAGIC Create geometries programmatically.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create points using ST_Point
# MAGIC SELECT
# MAGIC     'Custom Point' AS name,
# MAGIC     ST_AsWKT(ST_Point(144.9631, -37.8136)) AS point_wkt,
# MAGIC     ST_AsWKT(ST_Point(144.9631, -37.8136, 4326)) AS point_with_srid

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a line from multiple points using ST_MakeLine
# MAGIC WITH city_points AS (
# MAGIC     SELECT
# MAGIC         ST_GeomFromWKT(geometry_wkt) AS geom,
# MAGIC         city_name
# MAGIC     FROM cities
# MAGIC     WHERE country = 'Australia'
# MAGIC     ORDER BY ST_Y(ST_GeomFromWKT(geometry_wkt))  -- Order by latitude
# MAGIC )
# MAGIC SELECT
# MAGIC     ST_AsWKT(ST_MakeLine(COLLECT_LIST(geom))) AS connected_cities_line
# MAGIC FROM city_points

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Practical Example: Spatial Analysis
# MAGIC
# MAGIC Combining multiple Spatial SQL functions for real-world analysis.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Comprehensive spatial analysis: Find cities, calculate distances, and identify relationships
# MAGIC WITH city_analysis AS (
# MAGIC     SELECT
# MAGIC         ci.city_name,
# MAGIC         ci.country,
# MAGIC         ci.population,
# MAGIC         ST_X(ST_GeomFromWKT(ci.geometry_wkt)) AS longitude,
# MAGIC         ST_Y(ST_GeomFromWKT(ci.geometry_wkt)) AS latitude,
# MAGIC         -- Distance from Melbourne in km
# MAGIC         ROUND(
# MAGIC             ST_DistanceSphere(
# MAGIC                 ST_GeomFromWKT(ci.geometry_wkt),
# MAGIC                 ST_Point(144.9631, -37.8136)
# MAGIC             ) / 1000,
# MAGIC             2
# MAGIC         ) AS distance_from_melbourne_km
# MAGIC     FROM cities ci
# MAGIC )
# MAGIC SELECT
# MAGIC     city_name,
# MAGIC     country,
# MAGIC     FORMAT_NUMBER(population, 0) AS population,
# MAGIC     ROUND(longitude, 4) AS longitude,
# MAGIC     ROUND(latitude, 4) AS latitude,
# MAGIC     distance_from_melbourne_km,
# MAGIC     CASE
# MAGIC         WHEN distance_from_melbourne_km = 0 THEN 'Reference City'
# MAGIC         WHEN distance_from_melbourne_km < 1000 THEN 'Nearby'
# MAGIC         WHEN distance_from_melbourne_km < 3000 THEN 'Regional'
# MAGIC         ELSE 'Far'
# MAGIC     END AS distance_category
# MAGIC FROM city_analysis
# MAGIC ORDER BY distance_from_melbourne_km

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary: Spatial SQL Function Reference
# MAGIC
# MAGIC ### Import/Export
# MAGIC | Function | Description |
# MAGIC |----------|-------------|
# MAGIC | `ST_GeomFromWKT(wkt, srid)` | Create GEOMETRY from WKT string |
# MAGIC | `ST_AsWKT(geom)` | Convert GEOMETRY to WKT string |
# MAGIC | `ST_AsBinary(geom)` | Convert GEOMETRY to WKB binary |
# MAGIC | `ST_AsGeoJSON(geom)` | Convert GEOMETRY to GeoJSON |
# MAGIC
# MAGIC ### Accessors
# MAGIC | Function | Description |
# MAGIC |----------|-------------|
# MAGIC | `ST_X(point)` | Get X coordinate (longitude) |
# MAGIC | `ST_Y(point)` | Get Y coordinate (latitude) |
# MAGIC | `ST_XMin/XMax/YMin/YMax(geom)` | Get bounding box coordinates |
# MAGIC | `ST_Centroid(geom)` | Get centroid point |
# MAGIC | `ST_StartPoint/EndPoint(line)` | Get endpoints of linestring |
# MAGIC | `ST_NPoints(geom)` | Count number of points |
# MAGIC
# MAGIC ### Measurements
# MAGIC | Function | Description |
# MAGIC |----------|-------------|
# MAGIC | `ST_Area(geom)` | Calculate area |
# MAGIC | `ST_Length(geom)` | Calculate length |
# MAGIC | `ST_Perimeter(geom)` | Calculate perimeter |
# MAGIC | `ST_Distance(geom1, geom2)` | Cartesian distance |
# MAGIC | `ST_DistanceSphere(geom1, geom2)` | Spherical distance (meters) |
# MAGIC
# MAGIC ### Relationships
# MAGIC | Function | Description |
# MAGIC |----------|-------------|
# MAGIC | `ST_Contains(geom1, geom2)` | Does geom1 contain geom2? |
# MAGIC | `ST_Within(geom1, geom2)` | Is geom1 within geom2? |
# MAGIC | `ST_Intersects(geom1, geom2)` | Do geometries intersect? |
# MAGIC | `ST_DWithin(geom1, geom2, dist)` | Within distance? |
# MAGIC
# MAGIC ### Processing
# MAGIC | Function | Description |
# MAGIC |----------|-------------|
# MAGIC | `ST_Buffer(geom, distance)` | Create buffer |
# MAGIC | `ST_Envelope(geom)` | Get bounding box |
# MAGIC | `ST_ConvexHull(geom)` | Get convex hull |
# MAGIC | `ST_Simplify(geom, tolerance)` | Simplify geometry |
# MAGIC | `ST_Intersection(g1, g2)` | Geometry intersection |
# MAGIC | `ST_Union(g1, g2)` | Geometry union |
# MAGIC | `ST_Difference(g1, g2)` | Geometry difference |
# MAGIC
# MAGIC ### CRS Functions
# MAGIC | Function | Description |
# MAGIC |----------|-------------|
# MAGIC | `ST_Transform(geom, target_srid)` | Reproject geometry |
# MAGIC | `ST_SetSRID(geom, srid)` | Set SRID |
# MAGIC | `ST_SRID(geom)` | Get SRID |
# MAGIC
# MAGIC ### Aggregations
# MAGIC | Function | Description |
# MAGIC |----------|-------------|
# MAGIC | `ST_Union_Agg(geom)` | Union all geometries |
# MAGIC | `ST_Envelope_Agg(geom)` | Bounding box of all geometries |
