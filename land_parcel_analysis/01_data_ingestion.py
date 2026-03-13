# Databricks notebook source
# MAGIC %md
# MAGIC # Land Parcel Data Ingestion
# MAGIC
# MAGIC This notebook ingests geospatial datasets for the **Site Consolidation Suitability Analysis** using the custom geo data source.
# MAGIC
# MAGIC ## Datasets to Ingest:
# MAGIC 1. **Vicmap Property** - Parcel polygons (primary dataset)
# MAGIC 2. **Vicmap Planning Zones** - Zone polygons (HCTZ, GRZ, etc.)
# MAGIC 3. **Vicmap Planning Overlays** - Constraint overlays (Heritage, Flood, ESO)
# MAGIC 4. **Public Transport Stops** - PT node locations
# MAGIC 5. **Road Network** - Road lines for frontage analysis
# MAGIC 6. **Activity Centres** - Plan Melbourne activity centres

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup: Load Custom Geo Data Source

# COMMAND ----------

# MAGIC %run ../geo_datasource/01_geo_datasource_definition

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configure Unity Catalog Location

# COMMAND ----------

# Create widgets for catalog, schema, and volume names
dbutils.widgets.text("catalog_name", "danny_catalog", "Catalog Name")
dbutils.widgets.text("schema_name", "dtp_hackathon", "Schema Name")
dbutils.widgets.text("volume_name", "source", "Volume Name")

# Get notebook widget values
catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")
volume_name = dbutils.widgets.get("volume_name")

print(f"Catalog: {catalog_name}")
print(f"Schema: {schema_name}")
print(f"Volume: {volume_name}")

# COMMAND ----------

# Create catalog, schema, and volume if they don't exist
spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog_name}.{schema_name}.{volume_name}")

# Define paths
volume_path = f"/Volumes/{catalog_name}/{schema_name}/{volume_name}"
raw_data_path = f"{volume_path}/land_parcel_raw"

import os
os.makedirs(raw_data_path, exist_ok=True)

print(f"Volume path: {volume_path}")
print(f"Raw data path: {raw_data_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Download Vicmap Property - Parcel Polygons
# MAGIC
# MAGIC This is the **foundation dataset** containing polygon geometries for every land parcel in Victoria.
# MAGIC
# MAGIC **Source:** https://discover.data.vic.gov.au/dataset/vicmap-property-parcel-polygon-with-parcel-detail

# COMMAND ----------

import urllib.request
import zipfile
import os
import shutil

def download_and_extract(url: str, dataset_name: str, raw_data_path: str):
    """
    Download a file (ZIP or direct) to the raw data path.
    - For ZIP files: downloads and extracts to a folder
    - For direct files (GeoJSON, SHP, etc.): downloads directly to the folder
    Returns the path to the destination folder.
    """
    dest_path = f"{raw_data_path}/{dataset_name}"

    # Check if already downloaded
    if os.path.exists(dest_path) and len(os.listdir(dest_path)) > 0:
        print(f"{dataset_name} already exists at {dest_path}")
        return dest_path

    os.makedirs(dest_path, exist_ok=True)

    # Get filename from URL
    url_filename = url.split('/')[-1].split('?')[0]  # Remove query params
    url_filename_lower = url_filename.lower()

    # Check for direct file extensions (non-zip geospatial formats)
    direct_file_extensions = ['.geojson', '.json', '.shp', '.gpkg', '.kml', '.gml']
    is_direct_file = any(url_filename_lower.endswith(ext) for ext in direct_file_extensions)

    print(f"Downloading {dataset_name}...")

    try:
        if is_direct_file:
            # Download direct file (GeoJSON, SHP, etc.)
            dest_file = f"{dest_path}/{url_filename}"
            urllib.request.urlretrieve(url, dest_file)
            print(f"Downloaded to {dest_file}")
        else:
            # Download and extract ZIP file
            temp_zip = f"/tmp/{dataset_name}.zip"
            urllib.request.urlretrieve(url, temp_zip)

            print(f"Extracting {dataset_name}...")
            with zipfile.ZipFile(temp_zip, 'r') as zip_ref:
                zip_ref.extractall(dest_path)

            os.remove(temp_zip)
            print(f"Downloaded and extracted to {dest_path}")

        # List files
        for root, dirs, files in os.walk(dest_path):
            for f in files:
                print(f"  {os.path.join(root, f)}")

    except Exception as e:
        print(f"Error downloading {dataset_name}: {str(e)}")
        print("You may need to download this dataset manually from Data.Vic")

    return dest_path

# COMMAND ----------

# Download from DataShare
download_and_extract("https://s3.ap-southeast-2.amazonaws.com/cl-isd-prd-datashare-s3-delivery/Order_SUXOLL.zip", "Property_Parcel_Polygon", f"{raw_data_path}") 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Manual Download Instructions
# MAGIC
# MAGIC Some Victorian Government datasets require manual download due to API restrictions.
# MAGIC
# MAGIC **Steps:**
# MAGIC 1. Go to the Data.Vic portal links below
# MAGIC 2. Download the **Shapefile (SHP)** or **File Geodatabase (GDB)** format
# MAGIC 3. Upload to the volume path shown above
# MAGIC
# MAGIC | Dataset | Download Link |
# MAGIC |---------|---------------|
# MAGIC | Vicmap Property - Parcel Polygon | [Data.Vic](https://discover.data.vic.gov.au/dataset/vicmap-property-parcel-polygon-with-parcel-detail) |
# MAGIC | Vicmap Planning - Zone Polygon | [Data.Vic](https://discover.data.vic.gov.au/dataset/vicmap-planning-planning-scheme-zone-polygon) |
# MAGIC | Vicmap Planning - Overlay Polygon | [Data.Vic](https://discover.data.vic.gov.au/dataset/vicmap-planning-planning-scheme-overlay-polygon) |
# MAGIC | Vicmap Transport - Road Line | [Data.Vic](https://discover.data.vic.gov.au/dataset/vicmap-transport-road-line) |
# MAGIC | PT Stops | [Transport Open Data](https://opendata.transport.vic.gov.au/dataset/public-transport-lines-and-stops) |
# MAGIC | Activity Centres | [Planning.vic.gov.au](https://www.planning.vic.gov.au/guides-and-resources/strategies-and-initiatives/plan-melbourne/the-map-room) |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Ingest Parcel Data Using Custom Geo Data Source
# MAGIC
# MAGIC The custom geo data source is **optimized for large files**:
# MAGIC - Uses **Fiona streaming reads** (memory efficient)
# MAGIC - **Partitioned reading** with configurable chunk size
# MAGIC - Processes one feature at a time instead of loading entire file
# MAGIC
# MAGIC Use the `chunk_size` option to control memory usage:
# MAGIC ```python
# MAGIC spark.read.format("geo").option("path", "...").option("chunk_size", 5000).load()
# MAGIC ```

# COMMAND ----------

# Helper function to find shapefile in a directory
def find_shapefile(directory: str, pattern: str = None) -> str:
    """Find the first .shp file in a directory, optionally matching a pattern."""
    for root, dirs, files in os.walk(directory):
        for f in files:
            if f.endswith('.shp'):
                if pattern is None or pattern.lower() in f.lower():
                    return os.path.join(root, f)
    return None

# Helper function to find GDB in a directory
def find_gdb(directory: str) -> str:
    """Find the first .gdb folder in a directory."""
    for root, dirs, files in os.walk(directory):
        for d in dirs:
            if d.endswith('.gdb'):
                return os.path.join(root, d)
    return None

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ingest Parcel Polygons

# COMMAND ----------

# Get paths from widgets
volume_path = f"/Volumes/{dbutils.widgets.get('catalog_name')}/{dbutils.widgets.get('schema_name')}/{dbutils.widgets.get('volume_name')}"
raw_data_path = f"{volume_path}/land_parcel_raw"

# Look for parcel data (matches the download_and_extract dataset_name)
parcel_path = f"{raw_data_path}/Property_Parcel_Polygon"

# Check if file exists
if os.path.exists(parcel_path):
    shp_file = find_shapefile(parcel_path)
    gdb_file = find_gdb(parcel_path)

    if shp_file:
        print(f"Found shapefile: {shp_file}")

        # Get feature count using helper function (memory efficient)
        feature_count = get_geo_count(shp_file)
        print(f"Total features: {feature_count:,}")

        # Use smaller chunk_size for large files to reduce memory per partition
        chunk_size = 5000 if feature_count > 100000 else 10000
        print(f"Using chunk_size: {chunk_size}")

        df_parcels = (spark.read
            .format("geo")
            .option("path", shp_file)
            .option("chunk_size", chunk_size)
            .load()
        )
        df_parcels.printSchema()
        print("\nSample data (first 10 rows):")
        display(df_parcels.limit(10))

    elif gdb_file:
        print(f"Found GDB: {gdb_file}")
        layers = list_geo_layers(gdb_file)
        print(f"Available layers: {layers}")

        # Load the first layer
        layer_name = layers[0] if layers else None
        if layer_name:
            print(f"Loading layer: {layer_name}")
            feature_count = get_geo_count(gdb_file, layer=layer_name)
            print(f"Total features: {feature_count:,}")
            chunk_size = 5000 if feature_count > 100000 else 10000

            df_parcels = (spark.read
                .format("geo")
                .option("path", gdb_file)
                .option("layer", layer_name)
                .option("chunk_size", chunk_size)
                .load()
            )
            df_parcels.printSchema()
            print("\nSample data (first 10 rows):")
            display(df_parcels.limit(10))
    else:
        print(f"No supported geospatial file found in {parcel_path}")
        print("Supported formats: .shp, .gdb, .geojson, .gpkg, .tab")
else:
    print(f"Directory does not exist: {parcel_path}")
    print("Please create the directory and upload the Vicmap Property dataset.")
    print(f"\nExpected structure:")
    print(f"  {parcel_path}/")
    print(f"    *.shp (and associated .dbf, .shx, .prj files)")
    print(f"    OR *.gdb/ folder")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ingest Planning Zones

# COMMAND ----------

# Download from DataShare
download_and_extract("https://s3.ap-southeast-2.amazonaws.com/cl-isd-prd-datashare-s3-delivery/Order_MYALPH.zip", "vicmap_planning_zone", f"{raw_data_path}") 

# COMMAND ----------

# Look for planning zone data
zone_path = f"{raw_data_path}/vicmap_planning_zone"

if os.path.exists(zone_path):
    shp_file = find_shapefile(zone_path)
    gdb_file = find_gdb(zone_path)

    if shp_file:
        print(f"Found shapefile: {shp_file}")
        feature_count = get_geo_count(shp_file)
        print(f"Total features: {feature_count:,}")
        chunk_size = 5000 if feature_count > 100000 else 10000

        df_zones = (spark.read
            .format("geo")
            .option("path", shp_file)
            .option("chunk_size", chunk_size)
            .load()
        )
        df_zones.printSchema()
        display(df_zones.limit(10))

    elif gdb_file:
        print(f"Found GDB: {gdb_file}")
        layers = list_geo_layers(gdb_file)
        print(f"Available layers: {layers}")

        # Load the first layer (or specify the layer you need)
        layer_name = layers[0] if layers else None
        if layer_name:
            print(f"Loading layer: {layer_name}")
            feature_count = get_geo_count(gdb_file, layer=layer_name)
            print(f"Total features: {feature_count:,}")
            chunk_size = 5000 if feature_count > 100000 else 10000

            df_zones = (spark.read
                .format("geo")
                .option("path", gdb_file)
                .option("layer", layer_name)
                .option("chunk_size", chunk_size)
                .load()
            )
            df_zones.printSchema()
            display(df_zones.limit(10))
else:
    print(f"Planning zones not found at: {zone_path}")
    print("Please upload Vicmap Planning Zone data.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ingest Planning Overlays

# COMMAND ----------

# Download from DataShare
download_and_extract("https://s3.ap-southeast-2.amazonaws.com/cl-isd-prd-datashare-s3-delivery/xxx.zip", "vicmap_planning_overlay", f"{raw_data_path}") 

# COMMAND ----------

# Look for planning overlay data
overlay_path = f"{raw_data_path}/vicmap_planning_overlay"

if os.path.exists(overlay_path):
    shp_file = find_shapefile(overlay_path)
    gdb_file = find_gdb(overlay_path)

    if shp_file:
        print(f"Found shapefile: {shp_file}")
        feature_count = get_geo_count(shp_file)
        print(f"Total features: {feature_count:,}")
        chunk_size = 5000 if feature_count > 100000 else 10000

        df_overlays = (spark.read
            .format("geo")
            .option("path", shp_file)
            .option("chunk_size", chunk_size)
            .load()
        )
        df_overlays.printSchema()
        display(df_overlays.limit(10))

    elif gdb_file:
        print(f"Found GDB: {gdb_file}")
        layers = list_geo_layers(gdb_file)
        print(f"Available layers: {layers}")

        # Load the first layer (or specify the layer you need)
        layer_name = layers[0] if layers else None
        if layer_name:
            print(f"Loading layer: {layer_name}")
            feature_count = get_geo_count(gdb_file, layer=layer_name)
            print(f"Total features: {feature_count:,}")
            chunk_size = 5000 if feature_count > 100000 else 10000

            df_overlays = (spark.read
                .format("geo")
                .option("path", gdb_file)
                .option("layer", layer_name)
                .option("chunk_size", chunk_size)
                .load()
            )
            df_overlays.printSchema()
            display(df_overlays.limit(10))
else:
    print(f"Planning overlays not found at: {overlay_path}")
    print("Please upload Vicmap Planning Overlay data.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ingest Road Network

# COMMAND ----------

# Download from DataShare
download_and_extract("https://s3.ap-southeast-2.amazonaws.com/cl-isd-prd-datashare-s3-delivery/Order_8DZE2H.zip", "vicmap_transport_road", f"{raw_data_path}") 

# COMMAND ----------

# Look for road data
road_path = f"{raw_data_path}/vicmap_transport_road"

if os.path.exists(road_path):
    shp_file = find_shapefile(road_path)
    gdb_file = find_gdb(road_path)

    if shp_file:
        print(f"Found shapefile: {shp_file}")
        feature_count = get_geo_count(shp_file)
        print(f"Total features: {feature_count:,}")
        chunk_size = 5000 if feature_count > 100000 else 10000

        df_roads = (spark.read
            .format("geo")
            .option("path", shp_file)
            .option("chunk_size", chunk_size)
            .load()
        )
        df_roads.printSchema()
        display(df_roads.limit(10))

    elif gdb_file:
        print(f"Found GDB: {gdb_file}")
        layers = list_geo_layers(gdb_file)
        print(f"Available layers: {layers}")

        layer_name = layers[0] if layers else None
        if layer_name:
            print(f"Loading layer: {layer_name}")
            feature_count = get_geo_count(gdb_file, layer=layer_name)
            print(f"Total features: {feature_count:,}")
            chunk_size = 5000 if feature_count > 100000 else 10000

            df_roads = (spark.read
                .format("geo")
                .option("path", gdb_file)
                .option("layer", layer_name)
                .option("chunk_size", chunk_size)
                .load()
            )
            df_roads.printSchema()
            display(df_roads.limit(10))
else:
    print(f"Road network not found at: {road_path}")
    print("Please upload Vicmap Transport Road data.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ingest Public Transport Stops

# COMMAND ----------

# Download from DataShare
download_and_extract("https://opendata.transport.vic.gov.au/dataset/6d36dfd9-8693-4552-8a03-05eb29a391fd/resource/afa7b823-0c8b-47a1-bc40-ada565f684c7/download/public_transport_stops.geojson", "pt_stops", f"{raw_data_path}") 

# COMMAND ----------

# PT stops are typically in GeoJSON format
pt_stops_path = f"{raw_data_path}/pt_stops"

if os.path.exists(pt_stops_path):
    # Look for GeoJSON file
    for f in os.listdir(pt_stops_path):
        if f.endswith('.geojson') or f.endswith('.json'):
            geojson_file = os.path.join(pt_stops_path, f)
            print(f"Found GeoJSON: {geojson_file}")

            df_pt_stops = (spark.read
                .format("geo")
                .option("path", geojson_file)
                .load()
            )
            df_pt_stops.printSchema()
            display(df_pt_stops.limit(10))
            break
else:
    print(f"PT stops not found at: {pt_stops_path}")
    print("Please upload PT stops GeoJSON data.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Materialize to Delta Tables with Native GEOMETRY
# MAGIC
# MAGIC Once data is loaded, materialize to Delta tables with native GEOMETRY columns for optimized spatial queries.

# COMMAND ----------

from pyspark.sql.functions import expr

def materialize_geo_table(df, table_name: str, catalog: str, schema: str, geometry_col: str = "geometry", source_srid: int = 3111):
    """
    Materialize a DataFrame with WKT geometry to a Delta table with native GEOMETRY column.

    Args:
        df: DataFrame with WKT geometry column
        table_name: Name of the target table
        catalog: Unity Catalog name
        schema: Schema name
        geometry_col: Name of the WKT geometry column (default: "geometry")
        source_srid: SRID of the source data (default: 3111 for VicGrid GDA94)
    """
    full_table_name = f"{catalog}.{schema}.{table_name}"

    # Convert WKT to native GEOMETRY with correct source SRID
    # VicGrid GDA94 (EPSG:3111) is the standard for Victorian government data
    df_with_geom = (df
        .withColumn("geom", expr(f"ST_GeomFromWKT({geometry_col}, {source_srid})"))
        .drop(geometry_col)
        .withColumnRenamed("geom", "geometry")
    )

    df_with_geom.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(full_table_name)

    print(f"Created table: {full_table_name} (SRID: {source_srid})")
    return full_table_name

# COMMAND ----------

# MAGIC %md
# MAGIC ### Materialize Parcels Table
# MAGIC
# MAGIC Run this cell after uploading and loading the parcel data.

# COMMAND ----------

# Uncomment and run after loading parcel data
catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

if 'df_parcels' in dir():
    materialize_geo_table(df_parcels, "parcels", catalog_name, schema_name)
    print("Parcels table created with native GEOMETRY column")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Materialize Planning Zones Table
# MAGIC
# MAGIC Converts the planning zones DataFrame to a Delta table with native GEOMETRY column.
# MAGIC This table contains zone polygons (GRZ, NRZ, HCTZ, etc.) for spatial joins with parcels.

# COMMAND ----------

if 'df_zones' in dir():
    materialize_geo_table(df_zones, "zones", catalog_name, schema_name)
    print("Planning Zones table created with native GEOMETRY column")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Materialize Planning Overlays Table
# MAGIC
# MAGIC Converts the planning overlays DataFrame to a Delta table with native GEOMETRY column.
# MAGIC This table contains overlay polygons (Heritage, Flood, ESO, etc.) used as constraints in scoring.

# COMMAND ----------

if 'df_overlays' in dir():
    materialize_geo_table(df_overlays, "overlays", catalog_name, schema_name)
    print("Planning Overlays table created with native GEOMETRY column")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Materialize Roads Table
# MAGIC
# MAGIC Converts the roads DataFrame to a Delta table with native GEOMETRY column.
# MAGIC This table contains road linestrings used for frontage analysis and edge topology.

# COMMAND ----------

if 'df_roads' in dir():
    materialize_geo_table(df_roads, "roads", catalog_name, schema_name)
    print("Roads table created with native GEOMETRY column")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Materialize PT Stops Table
# MAGIC
# MAGIC Converts the PT stops DataFrame to a Delta table with native GEOMETRY column.
# MAGIC This table contains point locations for train stations, tram stops, and bus stops
# MAGIC used to calculate proximity scores for HCTZ eligibility.
# MAGIC
# MAGIC **Note:** PT stops GeoJSON from opendata.transport.vic.gov.au is in WGS84 (EPSG:4326).

# COMMAND ----------

if 'df_pt_stops' in dir():
    # PT stops GeoJSON is typically in WGS84 (EPSG:4326)
    materialize_geo_table(df_pt_stops, "pt_stops", catalog_name, schema_name, source_srid=4326)
    print("PT Stops table created with native GEOMETRY column (SRID: 4326)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ingest Activity Centres (Plan Melbourne)
# MAGIC
# MAGIC Activity Centres data from Plan Melbourne Map Room.
# MAGIC
# MAGIC **Download from:** https://www.planning.vic.gov.au/guides-and-resources/strategies-and-initiatives/plan-melbourne/the-map-room

# COMMAND ----------

# manual download right now, due to geofencing, got 403 error

# download_and_extract("https://www.planning.vic.gov.au/__data/assets/file/0037/628498/plan-melbourne-shape-files.zip", "activity_centres", f"{raw_data_path}") 

# COMMAND ----------

# Look for activity centres data
activity_centres_path = f"{raw_data_path}/activity_centres"

if os.path.exists(activity_centres_path):
    shp_file = find_shapefile(activity_centres_path)
    gdb_file = find_gdb(activity_centres_path)

    if shp_file:
        print(f"Found shapefile: {shp_file}")
        feature_count = get_geo_count(shp_file)
        print(f"Total features: {feature_count:,}")

        df_activity_centres = (spark.read
            .format("geo")
            .option("path", shp_file)
            .load()
        )
        df_activity_centres.printSchema()
        display(df_activity_centres.limit(10))

    elif gdb_file:
        print(f"Found GDB: {gdb_file}")
        layers = list_geo_layers(gdb_file)
        print(f"Available layers: {layers}")

        layer_name = layers[0] if layers else None
        if layer_name:
            print(f"Loading layer: {layer_name}")
            df_activity_centres = (spark.read
                .format("geo")
                .option("path", gdb_file)
                .option("layer", layer_name)
                .load()
            )
            df_activity_centres.printSchema()
            display(df_activity_centres.limit(10))
else:
    print(f"Activity Centres not found at: {activity_centres_path}")
    print("Download from: https://www.planning.vic.gov.au/guides-and-resources/strategies-and-initiatives/plan-melbourne/the-map-room")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Materialize Activity Centres Table

# COMMAND ----------

if 'df_activity_centres' in dir():
    materialize_geo_table(df_activity_centres, "activity_centres", catalog_name, schema_name)
    print("Activity Centres table created with native GEOMETRY column")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC This notebook provides:
# MAGIC
# MAGIC 1. **Custom Geo Data Source** - Easy ingestion of SHP, GDB, GeoJSON files
# MAGIC 2. **Manual Download Instructions** - For Victorian Government datasets
# MAGIC 3. **Helper Functions** - To find and load geospatial files
# MAGIC 4. **Materialization** - Convert to Delta tables with native GEOMETRY
# MAGIC
# MAGIC ### Next Steps
# MAGIC - **02_geometric_features.py** - Compute lot area, perimeter, compactness, aspect ratio
# MAGIC - **03_edge_topology.py** - Compute frontages, shared boundaries, corner lots
# MAGIC - **04_suitability_scoring.py** - Multi-criteria consolidation scoring

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Upload Checklist
# MAGIC
# MAGIC Upload datasets to these paths in the UC Volume:
# MAGIC
# MAGIC | Dataset | Upload Path |
# MAGIC |---------|-------------|
# MAGIC | Vicmap Property | `{volume_path}/land_parcel_raw/vicmap_property/` |
# MAGIC | Vicmap Planning Zones | `{volume_path}/land_parcel_raw/vicmap_planning_zone/` |
# MAGIC | Vicmap Planning Overlays | `{volume_path}/land_parcel_raw/vicmap_planning_overlay/` |
# MAGIC | Vicmap Transport Roads | `{volume_path}/land_parcel_raw/vicmap_transport_road/` |
# MAGIC | PT Stops | `{volume_path}/land_parcel_raw/pt_stops/` |
# MAGIC | Activity Centres | `{volume_path}/land_parcel_raw/activity_centres/` |