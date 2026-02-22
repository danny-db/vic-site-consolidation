# Databricks notebook source
# MAGIC %md
# MAGIC # Using the Geospatial Custom Data Source
# MAGIC
# MAGIC This notebook demonstrates how to use the custom PySpark geospatial data source to read various geospatial file formats.
# MAGIC
# MAGIC ## Prerequisites
# MAGIC First, run the data source definition notebook to register the data source.

# COMMAND ----------

# MAGIC %run ./01_geo_datasource_definition

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup: Configure Unity Catalog Location
# MAGIC
# MAGIC Configure the catalog, schema, and volume where sample data will be stored.

# COMMAND ----------

# Create widgets for catalog, schema, and volume names
dbutils.widgets.text("catalog_name", "danny_catalog", "Catalog Name")
dbutils.widgets.text("schema_name", "dtp_hackathon", "Schema Name")
dbutils.widgets.text("volume_name", "source", "Volume Name")

# Get widget values
catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")
volume_name = dbutils.widgets.get("volume_name")

print(f"Using catalog: {catalog_name}")
print(f"Using schema: {schema_name}")
print(f"Using volume: {volume_name}")

# COMMAND ----------

# Create catalog, schema, and volume if they don't exist
spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog_name}.{schema_name}.{volume_name}")

print(f"Catalog '{catalog_name}' is ready")
print(f"Schema '{catalog_name}.{schema_name}' is ready")
print(f"Volume '{catalog_name}.{schema_name}.{volume_name}' is ready")

# Define the volume path for use throughout the notebook
volume_path = f"/Volumes/{catalog_name}/{schema_name}/{volume_name}"
print(f"\nVolume path: {volume_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 1: Reading a Shapefile (.shp)
# MAGIC
# MAGIC Shapefiles are one of the most common geospatial formats. They consist of multiple files (.shp, .shx, .dbf, .prj).

# COMMAND ----------

# MAGIC %md
# MAGIC ### Download Sample Data
# MAGIC Let's download some sample geospatial data for testing and store it in the Unity Catalog Volume.

# COMMAND ----------

import urllib.request
import zipfile
import os
import shutil

# Get the volume path from widgets
volume_path = f"/Volumes/{dbutils.widgets.get('catalog_name')}/{dbutils.widgets.get('schema_name')}/{dbutils.widgets.get('volume_name')}"
geo_samples_path = f"{volume_path}/geo_samples"

# Create the geo_samples directory in the volume
os.makedirs(geo_samples_path, exist_ok=True)

# Download Natural Earth countries shapefile
shapefile_path = f"{geo_samples_path}/ne_110m_admin_0_countries.shp"
if not os.path.exists(shapefile_path):
    print("Downloading Natural Earth countries shapefile...")

    # Download to temp location first
    temp_zip = "/tmp/countries.zip"
    temp_extract = "/tmp/countries_extract"

    urllib.request.urlretrieve(
        "https://naciscdn.org/naturalearth/110m/cultural/ne_110m_admin_0_countries.zip",
        temp_zip
    )

    # Extract
    os.makedirs(temp_extract, exist_ok=True)
    with zipfile.ZipFile(temp_zip, 'r') as zip_ref:
        zip_ref.extractall(temp_extract)

    # Copy all extracted files to the volume
    for f in os.listdir(temp_extract):
        src = os.path.join(temp_extract, f)
        dst = os.path.join(geo_samples_path, f)
        shutil.copy2(src, dst)
        print(f"Copied {f} to volume")

    # Cleanup temp files
    os.remove(temp_zip)
    shutil.rmtree(temp_extract)

    print("Download complete!")
else:
    print("Sample shapefile already exists in volume.")

# List files in the volume
print(f"\nFiles in {geo_samples_path}:")
for f in os.listdir(geo_samples_path):
    print(f"  {f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read Shapefile Using the Custom Data Source

# COMMAND ----------

# Get the volume path
volume_path = f"/Volumes/{dbutils.widgets.get('catalog_name')}/{dbutils.widgets.get('schema_name')}/{dbutils.widgets.get('volume_name')}"
shapefile_path = f"{volume_path}/geo_samples/ne_110m_admin_0_countries.shp"

# Read the shapefile using our custom data source
df_shp = (spark.read
    .format("geo")
    .option("path", shapefile_path)
    .load()
)

# Show the schema
print("Schema:")
df_shp.printSchema()

# COMMAND ----------

# Show sample data
display(df_shp.select("geometry_type", "NAME", "CONTINENT", "POP_EST", "GDP_MD").limit(10))

# COMMAND ----------

# Count records
print(f"Total countries: {df_shp.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Analyze Geospatial Data with Spark SQL

# COMMAND ----------

# Register as temp view for SQL queries
df_shp.createOrReplaceTempView("countries")

# Query using Spark SQL
display(spark.sql("""
    SELECT
        CONTINENT,
        COUNT(*) as country_count,
        SUM(POP_EST) as total_population,
        SUM(GDP_MD) as total_gdp
    FROM countries
    WHERE CONTINENT IS NOT NULL
    GROUP BY CONTINENT
    ORDER BY total_population DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 2: Reading a GeoJSON File

# COMMAND ----------

# Create a sample GeoJSON file in the volume
import json

volume_path = f"/Volumes/{dbutils.widgets.get('catalog_name')}/{dbutils.widgets.get('schema_name')}/{dbutils.widgets.get('volume_name')}"
geojson_path = f"{volume_path}/geo_samples/sample_points.geojson"

geojson_data = {
    "type": "FeatureCollection",
    "features": [
        {
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [144.9631, -37.8136]},
            "properties": {"name": "Melbourne", "country": "Australia", "population": 5000000}
        },
        {
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [151.2093, -33.8688]},
            "properties": {"name": "Sydney", "country": "Australia", "population": 5300000}
        },
        {
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [153.0251, -27.4698]},
            "properties": {"name": "Brisbane", "country": "Australia", "population": 2500000}
        },
        {
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [115.8605, -31.9505]},
            "properties": {"name": "Perth", "country": "Australia", "population": 2100000}
        },
        {
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [138.6007, -34.9285]},
            "properties": {"name": "Adelaide", "country": "Australia", "population": 1400000}
        }
    ]
}

with open(geojson_path, 'w') as f:
    json.dump(geojson_data, f, indent=2)

print(f"GeoJSON file created at: {geojson_path}")

# COMMAND ----------

# Read GeoJSON file
volume_path = f"/Volumes/{dbutils.widgets.get('catalog_name')}/{dbutils.widgets.get('schema_name')}/{dbutils.widgets.get('volume_name')}"
geojson_path = f"{volume_path}/geo_samples/sample_points.geojson"

df_geojson = (spark.read
    .format("geo")
    .option("path", geojson_path)
    .load()
)

display(df_geojson)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 3: Working with Geometry

# COMMAND ----------

# MAGIC %md
# MAGIC ### Extract Coordinates from WKT

# COMMAND ----------

# For point geometries, extract coordinates using Spatial SQL functions
# ST_GeomFromWKT converts WKT string to GEOMETRY type
# ST_X and ST_Y extract the X (longitude) and Y (latitude) coordinates

df_geojson.createOrReplaceTempView("cities")

df_with_coords = spark.sql("""
    SELECT
        name,
        ST_X(ST_GeomFromWKT(geometry)) AS longitude,
        ST_Y(ST_GeomFromWKT(geometry)) AS latitude,
        population
    FROM cities
    WHERE geometry_type = 'Point'
""")

display(df_with_coords)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 4: Reprojecting Data to Different CRS

# COMMAND ----------

# Check the CRS of the shapefile
volume_path = f"/Volumes/{dbutils.widgets.get('catalog_name')}/{dbutils.widgets.get('schema_name')}/{dbutils.widgets.get('volume_name')}"
shapefile_path = f"{volume_path}/geo_samples/ne_110m_admin_0_countries.shp"

original_crs = get_geo_crs(shapefile_path)
print(f"Original CRS: {original_crs}")

# COMMAND ----------

# Read with reprojection to Web Mercator (EPSG:3857)
volume_path = f"/Volumes/{dbutils.widgets.get('catalog_name')}/{dbutils.widgets.get('schema_name')}/{dbutils.widgets.get('volume_name')}"
shapefile_path = f"{volume_path}/geo_samples/ne_110m_admin_0_countries.shp"

df_reprojected = (spark.read
    .format("geo")
    .option("path", shapefile_path)
    .option("crs", "EPSG:3857")  # Web Mercator projection
    .load()
)

# Show the reprojected geometry (coordinates will be in meters)
display(df_reprojected.select("NAME", "geometry").limit(3))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Alternative: Reprojection using Spatial SQL ST_Transform
# MAGIC
# MAGIC You can also reproject geometry using `ST_Transform` in Spatial SQL. This is useful when you want to reproject data that's already loaded in a DataFrame.

# COMMAND ----------

# Reprojection using Spatial SQL ST_Transform
# ST_GeomFromWKT converts WKT to GEOMETRY with SRID 4326 (WGS84)
# ST_Transform reprojects from source CRS to target CRS (EPSG:3857 Web Mercator)
# ST_AsWKT converts the result back to WKT string for display

df_shp.createOrReplaceTempView("countries_wgs84")

df_reprojected_sql = spark.sql("""
    SELECT
        NAME,
        geometry AS original_wkt,
        ST_AsWKT(
            ST_Transform(
                ST_GeomFromWKT(geometry, 4326),  -- Source: WGS84 (EPSG:4326)
                3857                              -- Target: Web Mercator (EPSG:3857)
            )
        ) AS reprojected_wkt
    FROM countries_wgs84
    LIMIT 3
""")

print("Reprojection using ST_Transform (EPSG:4326 -> EPSG:3857):")
display(df_reprojected_sql)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 5: Using WKB Format Instead of WKT

# COMMAND ----------

# Read with WKB (Well-Known Binary) format for geometry
volume_path = f"/Volumes/{dbutils.widgets.get('catalog_name')}/{dbutils.widgets.get('schema_name')}/{dbutils.widgets.get('volume_name')}"
geojson_path = f"{volume_path}/geo_samples/sample_points.geojson"

df_wkb = (spark.read
    .format("geo")
    .option("path", geojson_path)
    .option("geometry_format", "wkb")
    .load()
)

# WKB is a hexadecimal representation of the geometry
display(df_wkb)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 6: Reading Multi-Layer Files (GDB)
# MAGIC
# MAGIC For File Geodatabases and other multi-layer formats, you can specify which layer to read.

# COMMAND ----------

# Example: List layers in a file (if you have a GDB)
# layers = list_geo_layers("/path/to/your/file.gdb")
# print(f"Available layers: {layers}")

# Read a specific layer:
# df_layer = (spark.read
#     .format("geo")
#     .option("path", "/path/to/your/file.gdb")
#     .option("layer", "layer_name")
#     .load()
# )

print("For multi-layer files like GDB:")
print("1. Use list_geo_layers('/path/to/file.gdb') to see available layers")
print("2. Use .option('layer', 'layer_name') to read a specific layer")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 7: Saving Geospatial Data to Delta Table

# COMMAND ----------

# Get catalog and schema from widgets
catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")
table_name = f"{catalog_name}.{schema_name}.geo_countries"

# Save the countries data to a Delta table in Unity Catalog
# Note: Geometry is stored as WKT string which can be converted back later
(df_shp
    .select("geometry", "geometry_type", "NAME", "CONTINENT", "POP_EST", "GDP_MD")
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(table_name)
)

print(f"Data saved to Delta table '{table_name}'")

# COMMAND ----------

# Query the Delta table
catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

display(spark.sql(f"SELECT * FROM {catalog_name}.{schema_name}.geo_countries WHERE CONTINENT = 'Oceania'"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary of Options
# MAGIC
# MAGIC | Option | Description | Default |
# MAGIC |--------|-------------|---------|
# MAGIC | `path` | Path to the geospatial file (required) | - |
# MAGIC | `layer` | Layer name for multi-layer formats (GDB, GPKG) | None (reads first layer) |
# MAGIC | `geometry_format` | Output format for geometry: 'wkt' or 'wkb' | 'wkt' |
# MAGIC | `crs` | Target CRS to reproject data to (e.g., 'EPSG:4326') | None (keeps original) |
# MAGIC
# MAGIC ## Supported File Formats
# MAGIC
# MAGIC | Format | Extension | Notes |
# MAGIC |--------|-----------|-------|
# MAGIC | Shapefile | .shp | Most common format |
# MAGIC | GeoJSON | .geojson, .json | Web-friendly format |
# MAGIC | File Geodatabase | .gdb | Esri format, may have multiple layers |
# MAGIC | MapInfo TAB | .tab | MapInfo format |
# MAGIC | GeoPackage | .gpkg | Modern OGC standard |
# MAGIC | KML | .kml | Google Earth format |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clean Up (Optional)

# COMMAND ----------

# Remove sample data from volume
# import shutil
# volume_path = f"/Volumes/{dbutils.widgets.get('catalog_name')}/{dbutils.widgets.get('schema_name')}/{dbutils.widgets.get('volume_name')}"
# shutil.rmtree(f"{volume_path}/geo_samples", ignore_errors=True)

# Drop the Delta table
# catalog_name = dbutils.widgets.get("catalog_name")
# schema_name = dbutils.widgets.get("schema_name")
# spark.sql(f"DROP TABLE IF EXISTS {catalog_name}.{schema_name}.geo_countries")

# Remove widgets
# dbutils.widgets.removeAll()
