# Databricks notebook source
# MAGIC %md
# MAGIC # Geospatial Custom PySpark Data Source
# MAGIC
# MAGIC This notebook defines a custom PySpark DataSource for reading geospatial files including:
# MAGIC - **Shapefiles (.shp)**
# MAGIC - **GeoJSON (.geojson, .json)**
# MAGIC - **File Geodatabase (.gdb)**
# MAGIC - **MapInfo TAB (.tab)**
# MAGIC - **GeoPackage (.gpkg)**
# MAGIC
# MAGIC **Optimized for large files** using Fiona for streaming reads (memory efficient).
# MAGIC
# MAGIC ## Requirements
# MAGIC - Databricks Runtime 15.4 LTS or above
# MAGIC - Install required packages: `geopandas`, `pyshp`, `fiona`

# COMMAND ----------

# MAGIC %pip install geopandas pyshp fiona pyproj shapely --quiet

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define the Geospatial DataSource

# COMMAND ----------

from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, LongType, BooleanType
from pyspark.sql import Row
from typing import Iterator, Tuple, Any, List
import os

class GeoPartition(InputPartition):
    """
    Represents a partition of the geospatial file.
    For chunked reading, each partition handles a range of features.
    """
    def __init__(self, start_idx: int, end_idx: int):
        self.start_idx = start_idx
        self.end_idx = end_idx


class GeoDataSource(DataSource):
    """
    A PySpark custom data source for reading geospatial files.

    Optimized for large files using Fiona streaming reads.

    Supported formats:
    - Shapefile (.shp)
    - GeoJSON (.geojson, .json)
    - File Geodatabase (.gdb)
    - MapInfo TAB (.tab)
    - GeoPackage (.gpkg)
    - KML (.kml)

    Options:
    - path: Path to the geospatial file (required)
    - layer: Layer name for multi-layer formats like GDB (optional)
    - geometry_format: 'wkt' or 'wkb' (default: 'wkt')
    - crs: Target CRS to reproject to (e.g., 'EPSG:4326') (optional)
    - chunk_size: Number of features per partition (default: 10000)
    """

    @classmethod
    def name(cls) -> str:
        return "geo"

    def schema(self) -> str:
        """
        Infer schema from the geospatial file using Fiona (memory efficient).
        """
        return self._infer_schema()

    def _infer_schema(self) -> StructType:
        """
        Infer the schema from the geospatial file using Fiona.
        Only reads metadata and first feature, very memory efficient.
        """
        import fiona

        path = self.options.get("path")
        if not path:
            raise ValueError("The 'path' option is required for the geo data source")

        layer = self.options.get("layer")

        try:
            # Open with Fiona to read schema
            with fiona.open(path, layer=layer) as src:
                fiona_schema = src.schema
        except Exception as e:
            raise ValueError(f"Failed to read geospatial file at '{path}': {str(e)}")

        # Build Spark schema
        fields = []

        # Add geometry column as string (WKT or WKB)
        fields.append(StructField("geometry", StringType(), True))
        fields.append(StructField("geometry_type", StringType(), True))

        # Map Fiona types to Spark types
        fiona_to_spark = {
            'int': IntegerType(),
            'int32': IntegerType(),
            'int64': LongType(),
            'float': DoubleType(),
            'str': StringType(),
            'bool': BooleanType(),
        }

        # Add attribute columns from Fiona schema
        for prop_name, prop_type in fiona_schema['properties'].items():
            # Fiona types can be like 'int:10' or 'str:80', extract base type
            base_type = prop_type.split(':')[0] if ':' in prop_type else prop_type
            spark_type = fiona_to_spark.get(base_type, StringType())
            fields.append(StructField(prop_name, spark_type, True))

        return StructType(fields)

    def reader(self, schema: StructType) -> "GeoDataSourceReader":
        return GeoDataSourceReader(schema, self.options)


class GeoDataSourceReader(DataSourceReader):
    """
    Reader implementation using Fiona for streaming reads.
    Memory efficient - does not load entire file into memory.
    """

    def __init__(self, schema: StructType, options: dict):
        self.schema = schema
        self.options = options

    def partitions(self) -> List[InputPartition]:
        """
        Create partitions for parallel reading.
        Each partition handles a chunk of features.
        """
        import fiona

        path = self.options.get("path")
        layer = self.options.get("layer")
        chunk_size = int(self.options.get("chunk_size", 10000))

        # Get total feature count
        with fiona.open(path, layer=layer) as src:
            total_features = len(src)

        # Create partitions
        partitions = []
        for start in range(0, total_features, chunk_size):
            end = min(start + chunk_size, total_features)
            partitions.append(GeoPartition(start, end))

        return partitions

    def read(self, partition: GeoPartition) -> Iterator[Tuple]:
        """
        Read a partition of the geospatial file using Fiona streaming.
        Memory efficient - only loads one feature at a time.
        """
        import fiona
        from shapely.geometry import shape
        from pyproj import Transformer, CRS
        import warnings
        warnings.filterwarnings('ignore')

        path = self.options.get("path")
        layer = self.options.get("layer")
        geometry_format = self.options.get("geometry_format", "wkt").lower()
        target_crs = self.options.get("crs")

        # Get column names from schema (excluding geometry and geometry_type)
        schema_cols = [f.name for f in self.schema.fields if f.name not in ('geometry', 'geometry_type')]

        # Setup CRS transformer if needed
        transformer = None
        with fiona.open(path, layer=layer) as src:
            if target_crs and src.crs:
                try:
                    source_crs = CRS.from_user_input(src.crs)
                    dest_crs = CRS.from_user_input(target_crs)
                    transformer = Transformer.from_crs(source_crs, dest_crs, always_xy=True)
                except Exception:
                    transformer = None

        # Read features in the partition range
        with fiona.open(path, layer=layer) as src:
            # Skip to partition start
            for i, feature in enumerate(src):
                if i < partition.start_idx:
                    continue
                if i >= partition.end_idx:
                    break

                values = []

                # Process geometry
                geom = feature.get('geometry')
                if geom:
                    try:
                        shapely_geom = shape(geom)

                        # Transform if needed
                        if transformer:
                            from shapely.ops import transform
                            shapely_geom = transform(transformer.transform, shapely_geom)

                        if geometry_format == "wkb":
                            values.append(shapely_geom.wkb_hex)
                        else:
                            values.append(shapely_geom.wkt)
                        values.append(shapely_geom.geom_type)
                    except Exception:
                        values.append(None)
                        values.append(None)
                else:
                    values.append(None)
                    values.append(None)

                # Process properties
                props = feature.get('properties', {})
                for col in schema_cols:
                    val = props.get(col)
                    # Handle None and NaN
                    if val is None:
                        values.append(None)
                    elif isinstance(val, float) and (val != val):  # NaN check
                        values.append(None)
                    else:
                        values.append(val)

                yield tuple(values)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Register the Data Source
# MAGIC
# MAGIC Register the geospatial data source so it can be used with `spark.read.format("geo")`.

# COMMAND ----------

# Register the data source
spark.dataSource.register(GeoDataSource)

print("Geospatial data source 'geo' has been registered successfully!")
print("\nUsage:")
print('  spark.read.format("geo").option("path", "/path/to/file.shp").load()')
print("\nOptions:")
print("  - path: Path to the geospatial file (required)")
print("  - layer: Layer name for multi-layer formats (optional)")
print("  - geometry_format: 'wkt' or 'wkb' (default: 'wkt')")
print("  - crs: Target CRS to reproject to (optional)")
print("  - chunk_size: Features per partition for large files (default: 10000)")
print("\nSupported formats: .shp, .geojson, .json, .gdb, .tab, .gpkg, .kml")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Function: List Layers in a File
# MAGIC
# MAGIC For multi-layer formats like GDB, use this function to list available layers.

# COMMAND ----------

def list_geo_layers(path: str) -> list:
    """
    List all layers in a geospatial file (useful for GDB, GPKG, etc.)

    Args:
        path: Path to the geospatial file

    Returns:
        List of layer names
    """
    import fiona

    try:
        layers = fiona.listlayers(path)
        return layers
    except Exception as e:
        print(f"Error listing layers: {str(e)}")
        return []

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Function: Get CRS Information

# COMMAND ----------

def get_geo_crs(path: str, layer: str = None) -> str:
    """
    Get the CRS (Coordinate Reference System) of a geospatial file.

    Args:
        path: Path to the geospatial file
        layer: Layer name for multi-layer formats (optional)

    Returns:
        CRS string
    """
    import fiona

    try:
        with fiona.open(path, layer=layer) as src:
            return str(src.crs)
    except Exception as e:
        return f"Error: {str(e)}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Function: Get Feature Count

# COMMAND ----------

def get_geo_count(path: str, layer: str = None) -> int:
    """
    Get the number of features in a geospatial file (memory efficient).

    Args:
        path: Path to the geospatial file
        layer: Layer name for multi-layer formats (optional)

    Returns:
        Number of features
    """
    import fiona

    try:
        with fiona.open(path, layer=layer) as src:
            return len(src)
    except Exception as e:
        print(f"Error counting features: {str(e)}")
        return -1

# COMMAND ----------

# MAGIC %md
# MAGIC ## Export for Use in Other Notebooks
# MAGIC
# MAGIC To use this data source in other notebooks, run this notebook first using `%run`.

# COMMAND ----------

# Export the classes and functions for use in other notebooks
__all__ = ['GeoDataSource', 'GeoDataSourceReader', 'GeoPartition', 'list_geo_layers', 'get_geo_crs', 'get_geo_count']
