# Geospatial Custom PySpark Data Source

A custom PySpark data source for reading geospatial files in Databricks.

## Overview

This module provides a custom PySpark `DataSource` that can read various geospatial file formats using GeoPandas under the hood. It converts geospatial data to Spark DataFrames with geometry stored as WKT (Well-Known Text) or WKB (Well-Known Binary) strings.

## Supported Formats

| Format | Extension | Notes |
|--------|-----------|-------|
| Shapefile | `.shp` | Most common format |
| GeoJSON | `.geojson`, `.json` | Web-friendly format |
| File Geodatabase | `.gdb` | Esri format, may have multiple layers |
| MapInfo TAB | `.tab` | MapInfo format |
| GeoPackage | `.gpkg` | Modern OGC standard |
| KML | `.kml` | Google Earth format |

## Requirements

- Databricks Runtime 15.4 LTS or above
- Python packages: `geopandas`, `pyshp`, `fiona`, `pyproj`, `shapely`

## Notebooks

### 1. `01_geo_datasource_definition.py`
Defines the custom PySpark data source. Run this notebook first to register the `geo` data source.

### 2. `02_geo_datasource_usage.py`
Demonstrates how to use the geo data source with various examples.

### 3. `03_geo_utilities.py`
Provides utility UDFs for common geospatial operations on Spark DataFrames.

## Quick Start

### Step 1: Run the definition notebook
```python
%run ./01_geo_datasource_definition
```

### Step 2: Read a shapefile
```python
df = (spark.read
    .format("geo")
    .option("path", "/path/to/your/file.shp")
    .load()
)
df.show()
```

## Options

| Option | Description | Default |
|--------|-------------|---------|
| `path` | Path to the geospatial file (required) | - |
| `layer` | Layer name for multi-layer formats (GDB, GPKG) | None (reads first layer) |
| `geometry_format` | Output format: `wkt` or `wkb` | `wkt` |
| `crs` | Target CRS to reproject to (e.g., `EPSG:4326`) | None (keeps original) |

## Examples

### Reading a Shapefile
```python
df = spark.read.format("geo").option("path", "/data/states.shp").load()
```

### Reading GeoJSON
```python
df = spark.read.format("geo").option("path", "/data/points.geojson").load()
```

### Reading a Specific Layer from GDB
```python
# List available layers
layers = list_geo_layers("/data/mydata.gdb")
print(layers)

# Read a specific layer
df = (spark.read
    .format("geo")
    .option("path", "/data/mydata.gdb")
    .option("layer", "parcels")
    .load()
)
```

### Reprojecting Data
```python
# Reproject to Web Mercator
df = (spark.read
    .format("geo")
    .option("path", "/data/states.shp")
    .option("crs", "EPSG:3857")
    .load()
)
```

### Using WKB Format
```python
df = (spark.read
    .format("geo")
    .option("path", "/data/states.shp")
    .option("geometry_format", "wkb")
    .load()
)
```

## Schema

The resulting DataFrame will have:
- `geometry` (string): The geometry in WKT or WKB format
- `geometry_type` (string): The type of geometry (Point, Polygon, etc.)
- All attributes from the source file with appropriate Spark types

## Utility Functions

After running `03_geo_utilities.py`, you have access to UDFs for:

### Basic Geometry
- `geo_area()` - Calculate area
- `geo_length()` - Calculate length/perimeter
- `geo_centroid()` - Get centroid as WKT
- `geo_centroid_x()`, `geo_centroid_y()` - Get centroid coordinates

### Spatial Relationships
- `geo_contains()` - Check if geometry contains another
- `geo_intersects()` - Check if geometries intersect
- `geo_within()` - Check if geometry is within another
- `geo_distance()` - Calculate distance between geometries

### Geometry Manipulation
- `geo_buffer()` - Create buffer around geometry
- `geo_simplify()` - Simplify geometry
- `geo_convex_hull()` - Get convex hull
- `geo_envelope()` - Get bounding box
- `geo_intersection()` - Get intersection of two geometries
- `geo_union()` - Get union of two geometries

### Bounding Box
- `geo_bounds()` - Get [minx, miny, maxx, maxy]
- `geo_min_x()`, `geo_min_y()`, `geo_max_x()`, `geo_max_y()`

### Distance
- `geo_haversine_distance()` - Great circle distance in km

## Converting Back to GeoPandas

```python
import geopandas as gpd
from shapely import wkt

# Convert Spark DataFrame to Pandas
pdf = df.toPandas()

# Convert WKT to geometry
pdf['geometry'] = pdf['geometry'].apply(wkt.loads)

# Create GeoDataFrame
gdf = gpd.GeoDataFrame(pdf, geometry='geometry', crs="EPSG:4326")
```

## Saving to Delta

```python
# Save with geometry as WKT string
df.write.format("delta").mode("overwrite").saveAsTable("geo_data")
```

## Limitations

- The data source reads the entire file on the driver node before distributing
- For very large files, consider pre-partitioning or using dedicated geospatial tools
- Complex geometry operations should use GeoPandas for best performance
