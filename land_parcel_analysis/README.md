# Land Parcel Site Consolidation Analysis

This solution implements a **data-driven method for identifying land parcels in Victoria that are good candidates for site consolidation** using Databricks Spatial SQL.

## Problem Overview

The Victorian Government's *Housing Statement* and *Plan for Victoria* set ambitious housing targets. One key mechanism is enabling **medium-density housing (3–6 storeys)** through the **Housing Choice and Transport Zone (HCTZ)**, which incentivises lot consolidation by allowing greater building height on larger consolidated sites.

## Solution Architecture

```
land_parcel_analysis/
├── 01_data_ingestion.py           # Ingest geospatial data with native GEOMETRY type
├── 02_geometric_features.py       # Compute parcel shape metrics
├── 03_edge_topology.py            # Compute adjacency and shared boundaries
├── 03a_road_frontage_analysis.py  # Compute road frontage features (NEW)
├── 03b_activity_centre_proximity.py # Compute Activity Centre proximity (NEW)
├── 04_suitability_scoring.py      # Multi-criteria consolidation scoring
└── README.md
```

## Notebook Execution Order

Run notebooks in this order:
1. `01_data_ingestion.py` - Load all source datasets
2. `02_geometric_features.py` - Compute shape metrics
3. `03_edge_topology.py` - Compute adjacency features
4. `03a_road_frontage_analysis.py` - Compute road frontage features
5. `03b_activity_centre_proximity.py` - Compute Activity Centre proximity
6. `04_suitability_scoring.py` - Generate final suitability scores

## Coordinate Reference Systems (CRS)

This solution uses the following coordinate systems:

| CRS | EPSG Code | Usage |
|-----|-----------|-------|
| **VicGrid GDA94** | 3111 | Storage and metric calculations (native Victorian government CRS) |
| **WGS84** | 4326 | Web visualization (Folium, Kepler.gl, PyDeck) |

**Important**: All Victorian government spatial data (Vicmap) is published in VicGrid GDA94 (EPSG:3111). This projected CRS uses meters as units, making it ideal for area and distance calculations. Web maps require WGS84 (EPSG:4326) lat/lon coordinates.

---

## Spark Optimizations & Scalability Patterns

### 1. Native Geometry Storage with Unity Catalog

Data is stored using Databricks' native `GEOMETRY` type with proper SRID metadata:

```python
def materialize_geo_table(df, table_name, catalog, schema, geometry_col="geometry", source_srid=3111):
    """
    Materialize DataFrame with WKT geometry to Delta table with native GEOMETRY column.

    Key optimization: Store geometry with correct SRID for downstream spatial operations.
    """
    df_with_geom = (df
        .withColumn("geom", expr(f"ST_GeomFromWKT({geometry_col}, {source_srid})"))
        .drop(geometry_col)
        .withColumnRenamed("geom", "geometry")
    )
    df_with_geom.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.{table_name}")
```

**Benefits:**
- Native GEOMETRY type enables spatial indexing (H3-based)
- SRID metadata preserved for correct transformations
- Optimized storage format in Delta Lake

### 2. Predicate Pushdown with Spatial Filters

Filter data early to reduce shuffle and computation:

```sql
-- Filter by LGA before expensive spatial operations
WITH sample_parcels AS (
    SELECT parcel_id, zone_code, geometry
    FROM parcel_geometric_features
    WHERE lga_name = 'MELBOURNE'  -- Predicate pushdown
      AND geometry IS NOT NULL
      AND (zone_code LIKE 'GRZ%' OR zone_code LIKE 'NRZ%')
)
```

**Benefits:**
- Reduces data volume before cross-joins
- Leverages Delta Lake statistics for partition pruning
- Minimizes memory pressure on executors

### 3. Optimized Adjacency Computation

Computing parcel adjacency is inherently expensive (O(n²) potential pairs). We optimize using:

```sql
CREATE OR REPLACE TABLE parcel_adjacency AS
WITH sample_parcels AS (
    SELECT parcel_id, zone_code, geometry
    FROM parcel_geometric_features
    WHERE lga_name = '{sample_lga}'  -- Scope reduction
      AND geometry IS NOT NULL
)
SELECT
    p1.parcel_id AS parcel_1,
    p2.parcel_id AS parcel_2,
    p1.zone_code AS zone_1,
    p2.zone_code AS zone_2,
    ST_Touches(p1.geometry, p2.geometry) AS touches,
    ROUND(
        ST_Length(
            ST_Transform(
                ST_Intersection(ST_Boundary(p1.geometry), ST_Boundary(p2.geometry)),
                3111  -- VicGrid for meter calculations
            )
        ),
        2
    ) AS shared_boundary_m
FROM sample_parcels p1
CROSS JOIN sample_parcels p2
WHERE p1.parcel_id < p2.parcel_id  -- Avoid duplicates (n² → n²/2)
  AND ST_Intersects(p1.geometry, p2.geometry)  -- Spatial filter first
```

**Optimization techniques:**
1. **Scope reduction**: Filter to single LGA before cross-join
2. **Symmetry elimination**: `p1.parcel_id < p2.parcel_id` halves comparisons
3. **Spatial predicate first**: `ST_Intersects` filter before expensive `ST_Intersection`
4. **Lazy boundary extraction**: Only compute shared boundary when intersects

### 4. Broadcast Joins for Small Reference Tables

For PT stops and overlay lookups, use broadcast joins:

```sql
-- PT stops is small (~10K records) - ideal for broadcast
WITH parcel_centroids AS (
    SELECT parcel_id, ST_SetSRID(ST_Point(centroid_lon, centroid_lat), 4326) AS centroid
    FROM parcel_edge_topology
    WHERE centroid_lon IS NOT NULL
),
pt_distances AS (
    SELECT /*+ BROADCAST(pt) */
        p.parcel_id,
        pt.MODE AS mode,
        ROUND(ST_DistanceSphere(p.centroid, pt.geometry), 2) AS distance_m
    FROM parcel_centroids p
    CROSS JOIN pt_stops pt
)
```

**Benefits:**
- Avoids shuffle of large parcel table
- PT stops replicated to all executors
- Ideal for reference data < 100MB

### 5. Pandas UDFs for External API Calls

For operations requiring external services (routing, geocoding), maintain Spark parallelism:

```python
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import DoubleType
import pandas as pd

@pandas_udf(DoubleType())
def calc_walking_distance_osrm(
    origin_lon: pd.Series, origin_lat: pd.Series,
    dest_lon: pd.Series, dest_lat: pd.Series
) -> pd.Series:
    """
    Calculate walking distance using OSRM routing API.
    Runs in parallel across Spark workers - each partition processed independently.
    """
    import requests

    def get_walking_distance(o_lon, o_lat, d_lon, d_lat):
        try:
            url = f"http://router.project-osrm.org/route/v1/foot/{o_lon},{o_lat};{d_lon},{d_lat}"
            response = requests.get(url, params={"overview": "false"}, timeout=5)
            if response.status_code == 200:
                data = response.json()
                if data.get("routes"):
                    return float(data["routes"][0]["distance"])
        except Exception:
            pass
        return None

    return pd.Series([
        get_walking_distance(o_lon, o_lat, d_lon, d_lat)
        for o_lon, o_lat, d_lon, d_lat in zip(origin_lon, origin_lat, dest_lon, dest_lat)
    ])
```

**Key patterns:**
- **Vectorized processing**: Operates on pandas Series, not individual rows
- **Partition parallelism**: Each Spark partition calls API independently
- **Fault tolerance**: Handles API failures gracefully per-row
- **Scalability**: Add more workers = more parallel API calls

### 6. CRS Transformation Strategy

Geometry stored in projected CRS (3111) for calculations, transformed on-demand for visualization:

```sql
-- Storage: VicGrid (EPSG:3111) - meters for accurate calculations
CREATE TABLE parcel_geometric_features AS
SELECT
    parcel_id,
    geometry,  -- Stored in EPSG:3111
    ROUND(ST_Area(geometry), 2) AS area_sqm,  -- Native units are meters
    ROUND(ST_Perimeter(geometry), 2) AS perimeter_m
FROM parcels;

-- Visualization: Transform to WGS84 only when needed
SELECT
    parcel_id,
    ST_X(ST_Centroid(ST_Transform(geometry, 4326))) AS centroid_lon,
    ST_Y(ST_Centroid(ST_Transform(geometry, 4326))) AS centroid_lat,
    ST_AsGeoJSON(ST_Transform(geometry, 4326)) AS geojson  -- For web maps
FROM parcel_geometric_features
LIMIT 100;  -- Always limit for visualization
```

**Benefits:**
- Accurate metric calculations (area, distance) using projected CRS
- Transformation only at visualization time (not stored)
- Avoids repeated transformations during analysis

### 7. Visualization Data Reduction

Databricks has a 10MB output limit. Strategies to handle large geospatial visualizations:

```python
# Strategy 1: Limit sample size
sample_df = spark.sql("""
    SELECT * FROM consolidation_candidates
    WHERE geometry IS NOT NULL
    ORDER BY suitability_score DESC
    LIMIT 100  -- Cap for visualization
""")

# Strategy 2: Save to UC Volume for large outputs
volume_path = f"/Volumes/{catalog}/{schema}/source/visualizations"
kepler_map.save_to_html(file_name=f"{volume_path}/kepler_consolidation.html")
print(f"Map saved to: {volume_path}/kepler_consolidation.html")

# Strategy 3: Use displayHTML for Folium
displayHTML(m._repr_html_())  # Proper rendering in Databricks
```

---

## Notebooks

### 01_data_ingestion.py
Ingests geospatial datasets using the custom `geo` data source:
- Vicmap Property (parcel polygons) - EPSG:3111
- Vicmap Planning Zones (HCTZ, GRZ, etc.) - EPSG:3111
- Public Transport Stops - EPSG:4326 (GeoJSON)

**Key function:**
```python
materialize_geo_table(df, table_name, catalog, schema, geometry_col, source_srid=3111)
```

### 02_geometric_features.py
Computes parcel geometric attributes using Spatial SQL:

| Feature | Spatial SQL | Why It Matters |
|---------|-------------|----------------|
| Area | `ST_Area(geometry)` | Parcels below minimum need consolidation |
| Perimeter | `ST_Perimeter(geometry)` | Relates to setback exposure |
| Compactness | `4π × area / perimeter²` | Irregular shapes waste buildable area |
| Aspect Ratio | `width / depth` from bounding box | Narrow lots are less efficient |
| Elongation | `max(w,d) / min(w,d)` | Elongated lots harder to develop |

### 03_edge_topology.py
Computes edge topology features using optimized spatial joins:

| Feature | Spatial SQL | Why It Matters |
|---------|-------------|----------------|
| Adjacent Parcels | `ST_Touches(p1.geometry, p2.geometry)` | Consolidation potential |
| Shared Boundary Length | `ST_Length(ST_Intersection(ST_Boundary(p1), ST_Boundary(p2)))` | Quality of consolidation |
| Same-Zone Neighbors | Count where `zone_1 = zone_2` | Planning compatibility |
| Internal Lot | Shared boundary / perimeter > 0.8 | Limited street access |

### 03a_road_frontage_analysis.py
Computes road frontage features by intersecting parcel boundaries with road network:

| Feature | Spatial SQL | Why It Matters |
|---------|-------------|----------------|
| Number of Road Frontages | Count distinct roads touching boundary | Corner lots have 2+ |
| Total Frontage Length | Sum of road-facing edges | Must meet minimum requirements |
| Frontage by Road Class | Breakdown by arterial/local/laneway | Access and planning rules |
| Corner Lot Indicator | `num_road_frontages >= 2` | Different setback rules |
| Laneway Access | Has laneway frontage | Secondary access improves potential |
| Frontage-to-Perimeter Ratio | Road frontage / total perimeter | Low = internal/battleaxe lot |

### 03b_activity_centre_proximity.py
Computes proximity to Activity Centres (using train stations as proxy):

| Feature | Description | Why It Matters |
|---------|-------------|----------------|
| Nearest MAC Distance | Distance to Metropolitan Activity Centre | Policy priority areas |
| Nearest Major Distance | Distance to Major Activity Centre | Growth corridor locations |
| Within 800m AC | Walk-up distance to any centre | Accessibility premium |
| Activity Centre Tier | Metropolitan/Major/Neighbourhood | Higher tier = more growth |

### 04_suitability_scoring.py
Multi-criteria scoring model with weighted factors:

**Opportunities (Positive Score):**
| Factor | Points | Rationale |
|--------|--------|-----------|
| HCTZ/MUZ Zone | +30 | Policy priority area |
| GRZ/NRZ Zone | +15 | Residential consolidation |
| Within 800m Train/Tram | +20 | Transit-oriented development |
| Within 400m Bus | +10 | Public transport access |
| Within 800m Activity Centre | +15 | Near commercial/retail hub |
| Within 800m Metro AC | +10 | Near major regional hub |
| Below 500sqm | +25 | Below minimum site area |
| Below 300sqm | +10 | Critical undersized lot |
| Below 15m frontage | +20 | Below minimum frontage |
| Poor compactness (<0.3) | +15 | Inefficient shape |
| High elongation (>3) | +10 | Hard to develop efficiently |
| Internal lot | +20 | Limited development potential alone |
| Same-zone neighbors | +15 | Compatible consolidation |
| Long shared boundary (>15m) | +10 | Easy to merge |
| Laneway access | +5 | Secondary access potential |

**Constraints (Negative Score):**
| Factor | Points | Rationale |
|--------|--------|-----------|
| Isolated lot | -20 | No consolidation partners |
| Large lot (>1000sqm) | -15 | Already suitable size |
| Corner lot | -10 | Strategic value, less need to consolidate |
| Heritage overlay (HO) | -50 | Preservation constraint |
| Flood overlay (LSIO/SBO) | -40 | Development risk |
| ESO overlay | -30 | Environmental constraint |
| VPO overlay | -20 | Vegetation protection |

---

## Key Spatial SQL Functions Used

```sql
-- Geometry creation with SRID
ST_GeomFromWKT(wkt_string, 3111)        -- Parse WKT with CRS
ST_SetSRID(ST_Point(lon, lat), 4326)    -- Create point with CRS

-- Coordinate transformation
ST_Transform(geometry, 4326)             -- VicGrid → WGS84
ST_Transform(geometry, 3111)             -- WGS84 → VicGrid

-- Metric calculations (use projected CRS)
ST_Area(geometry)                        -- Square meters in EPSG:3111
ST_Perimeter(geometry)                   -- Meters in EPSG:3111
ST_Length(linestring)                    -- Line length in CRS units

-- Spatial predicates (optimized by Spark)
ST_Intersects(geom1, geom2)              -- Any intersection
ST_Touches(geom1, geom2)                 -- Share boundary only
ST_Contains(polygon, point)              -- Point-in-polygon

-- Geometry operations
ST_Boundary(polygon)                     -- Extract ring as linestring
ST_Intersection(geom1, geom2)            -- Shared geometry
ST_Centroid(geometry)                    -- Center point

-- Bounding box (for aspect ratio)
ST_XMin(geometry), ST_XMax(geometry)
ST_YMin(geometry), ST_YMax(geometry)

-- Distance calculations
ST_DistanceSphere(geom1, geom2)          -- Meters on spheroid (WGS84)

-- Export for visualization
ST_AsGeoJSON(ST_Transform(geometry, 4326))  -- GeoJSON in WGS84
ST_X(point), ST_Y(point)                 -- Extract coordinates
```

---

## Data Sources

| Dataset | Source | Native CRS | Format |
|---------|--------|------------|--------|
| Vicmap Property | [Data.Vic](https://discover.data.vic.gov.au/dataset/vicmap-property-parcel-polygon-with-parcel-detail) | EPSG:3111 | SHP/GDB |
| Vicmap Planning Zones | [Data.Vic](https://discover.data.vic.gov.au/dataset/vicmap-planning-planning-scheme-zone-polygon) | EPSG:3111 | SHP/GDB |
| Vicmap Planning Overlays | [Data.Vic](https://discover.data.vic.gov.au/dataset/vicmap-planning-planning-scheme-overlay-polygon) | EPSG:3111 | SHP/GDB |
| PT Stops | [Transport Open Data](https://opendata.transport.vic.gov.au/dataset/public-transport-lines-and-stops) | EPSG:4326 | GeoJSON |

---

## Output Tables

| Table | Description | Key Columns |
|-------|-------------|-------------|
| `parcel_geometric_features` | Base parcel metrics | geometry, area_sqm, compactness_index |
| `parcel_adjacency` | Pairwise relationships | parcel_1, parcel_2, shared_boundary_m |
| `parcel_edge_topology` | Topology features | num_adjacent_parcels, is_internal_lot |
| `parcel_pt_proximity` | PT distances | nearest_train_m, within_800m_train |
| `parcel_suitability_scores` | Scoring breakdown | opportunity_score, constraint_score |
| `consolidation_candidates` | Final ranked output | suitability_score, suitability_tier, rank |

---

## Requirements

- **Databricks Runtime**: 15.4 LTS+ (for Spatial SQL support)
- **Unity Catalog**: Enabled for managed tables
- **Custom geo data source**: From `../geo_datasource/`
- **Python packages**: folium, keplergl, pydeck (installed via `%pip install`)

---

## Visualization

Three visualization libraries are used, each with strengths:

| Library | Best For | Output Method |
|---------|----------|---------------|
| **Folium** | Interactive polygon maps with tooltips | `displayHTML(m._repr_html_())` |
| **Kepler.gl** | Large dataset exploration, filtering | Save to UC Volume HTML |
| **PyDeck** | 3D visualizations, column charts | `display(deck)` |

All visualizations require geometry transformation to WGS84:
```sql
ST_AsGeoJSON(ST_Transform(geometry, 4326)) AS geojson
```
