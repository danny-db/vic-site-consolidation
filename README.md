# Spatial Analytics for Victoria's Housing Challenge

**Identifying land parcels suitable for site consolidation using Databricks Spatial SQL and Victorian open geospatial data.**

Victoria's [Housing Statement](https://www.vic.gov.au/housing-statement) and [Plan for Victoria](https://www.planning.vic.gov.au/guides-and-resources/strategies-and-initiatives/plan-melbourne) set ambitious targets for new housing supply. A core strategy is enabling **medium-density development (3-6 storeys)** in well-connected locations through the new **Housing Choice and Transport Zone (HCTZ)**. This project uses spatial analytics on Databricks to score every land parcel by its potential for **site consolidation** &mdash; merging adjacent small lots into larger, more developable sites.

---

## Table of Contents

- [Background: The Problem](#background-the-problem)
- [Key Domain Concepts](#key-domain-concepts)
- [Solution Overview](#solution-overview)
- [Project Structure](#project-structure)
- [Getting Started](#getting-started)
  - [Prerequisites](#prerequisites)
  - [Cluster Setup](#cluster-setup)
  - [Data Preparation](#data-preparation)
  - [Running the Notebooks](#running-the-notebooks)
- [Datasets](#datasets)
- [Analysis Pipeline](#analysis-pipeline)
  - [Phase 1: Feature Engineering](#phase-1-feature-engineering)
  - [Phase 2: Suitability Scoring](#phase-2-suitability-scoring)
- [Databricks Spatial SQL Primer](#databricks-spatial-sql-primer)
- [Output Tables](#output-tables)
- [Visualization](#visualization)
- [Architecture Decisions](#architecture-decisions)
- [References](#references)
- [License](#license)

---

## Background: The Problem

Most of Melbourne's suburban land is subdivided into small lots (300-600 sqm) that were designed for single detached houses. These lots are often **too small or too narrow** to efficiently build multi-storey housing. Developers regularly need to **consolidate two or more adjacent parcels** into a single larger site before they can construct medium-density housing that meets frontage, setback, and floor-plate requirements.

The HCTZ explicitly incentivises this: lots within 800m of a train/tram stop or 400m of a high-frequency bus route gain access to 3-6 storey height limits, with **greater height allowed on larger consolidated sites**. The question becomes: **which parcels across Victoria are the best candidates for consolidation?**

This project answers that question by:

1. Computing geometric and topological features for every land parcel
2. Enriching parcels with planning zone, overlay, transport, and activity centre context
3. Scoring each parcel using a weighted multi-criteria model
4. Ranking and visualising consolidation candidates on interactive maps

---

## Key Domain Concepts

If you're new to Victorian planning or spatial analysis, here are the terms you'll encounter throughout the codebase.

### Site Consolidation

Combining two or more adjacent small land parcels into a single larger development site. A consolidated site can achieve better floor-plate efficiency, meet minimum frontage requirements, and yield more dwellings under height controls.

### Housing Choice and Transport Zone (HCTZ)

A new residential zone introduced in 2023. It targets areas within:
- **800 metres** of a train or tram stop
- **400 metres** of a high-frequency bus route

The zone allows 3-6 storey development and ties building height to site size, directly incentivising lot consolidation.

### Activity Centres

Hubs of retail, employment, services, and housing. They are classified into tiers:
- **Metropolitan Activity Centres** (e.g., Box Hill, Sunshine) &mdash; major regional hubs
- **Major Activity Centres** &mdash; significant suburban centres
- **Neighbourhood Activity Centres** &mdash; local shops and services

Parcels near activity centres are prime consolidation targets because planning policy directs growth here.

### Planning Overlays

Additional controls layered on top of zones that constrain what can be built:

| Overlay | Code | Impact on Consolidation |
|---------|------|------------------------|
| Heritage Overlay | HO | Restricts demolition/alteration &mdash; major constraint |
| Land Subject to Inundation | LSIO | Flood-prone land &mdash; development risk |
| Special Building Overlay | SBO | Flood-prone land &mdash; development risk |
| Environmental Significance | ESO | Protects sensitive environments |
| Vegetation Protection | VPO | Protects significant trees |

### Coordinate Reference Systems (CRS)

All Victorian government spatial data uses **VicGrid GDA94 (EPSG:3111)**, a projected CRS with metre units &mdash; ideal for area and distance calculations. Web maps require **WGS84 (EPSG:4326)** latitude/longitude. This project stores data in EPSG:3111 and transforms to EPSG:4326 only for visualisation.

---

## Solution Overview

```
                    Victorian Open Data
                    (Vicmap, Transport, Planning)
                            |
                            v
               +------------------------+
               |   Data Ingestion       |  Load shapefiles, GeoJSON via
               |   (Custom Geo Source)  |  custom PySpark DataSource
               +------------------------+
                            |
                            v
               +------------------------+
               |  Geometric Features    |  Area, perimeter, compactness,
               |  (Spatial SQL)         |  aspect ratio, elongation
               +------------------------+
                            |
                            v
               +------------------------+
               |  Edge Topology         |  Adjacency, shared boundaries,
               |  (Spatial SQL)         |  road frontage, corner lots
               +------------------------+
                            |
                            v
               +------------------------+
               |  Proximity Analysis    |  PT stops, activity centres,
               |  (Spatial SQL)         |  road classification
               +------------------------+
                            |
                            v
               +------------------------+
               |  Suitability Scoring   |  Weighted multi-criteria model
               |  (SQL + Python)        |  Opportunities vs Constraints
               +------------------------+
                            |
                            v
               +------------------------+
               |  Visualisation         |  Folium, Kepler.gl, PyDeck
               |  (Interactive Maps)    |  interactive web maps
               +------------------------+
```

---

## Project Structure

```
vic-site-consolidation/
├── README.md
├── CLAUDE.md                              # AI assistant context (domain guide + dataset inventory)
├── geo_datasource/                        # Custom PySpark geospatial data source
│   ├── README.md
│   ├── 01_geo_datasource_definition.py    # DataSource registration (run first)
│   ├── 02_geo_datasource_usage.py         # Usage examples
│   └── 03_spatial_sql_reference.py        # Spatial SQL function reference
│
└── land_parcel_analysis/                  # Core analysis pipeline
    ├── README.md
    ├── 01_data_ingestion.py               # Ingest Vicmap + PT data into Unity Catalog
    ├── 02_geometric_features.py           # Compute parcel shape metrics
    ├── 03_edge_topology.py                # Compute adjacency + shared boundaries
    ├── 03a_road_frontage_analysis.py      # Road frontage classification
    ├── 03b_activity_centre_proximity.py   # Activity Centre distance scoring
    └── 04_suitability_scoring.py          # Final multi-criteria scoring model
```

All `.py` files are **Databricks notebooks** (using `# Databricks notebook source` and `# MAGIC %md` conventions). Import them into your Databricks workspace to run.

---

## Getting Started

### Prerequisites

| Requirement | Minimum Version | Notes |
|-------------|----------------|-------|
| **Databricks Workspace** | Any cloud (AWS, Azure, GCP) | Unity Catalog must be enabled |
| **Serverless Compute** | Environment v4 | Recommended &mdash; no cluster management needed |
| **Unity Catalog** | Enabled | For managed Delta tables with native GEOMETRY type |
| **Python Packages** | See below | Installed via `%pip` in notebooks |

Python packages installed automatically by the notebooks:

```
geopandas pyshp fiona pyproj shapely folium keplergl pydeck
```

### Compute Setup

**Recommended: Serverless (preferred)**

1. Attach notebooks to **Serverless** compute
2. Select **Environment v4** (required for Spatial SQL `ST_*` functions)
3. Use the **High Memory (32 GB)** option &mdash; spatial joins and cross-joins for adjacency analysis are memory-intensive
4. No cluster provisioning, init scripts, or library configuration required

**Alternative: Classic Compute**

If serverless is not available in your workspace:

1. Create a cluster with **Databricks Runtime 15.4 LTS** or later
2. Recommended: a memory-optimised instance type (e.g., **Standard_E4ds_v5** / 32 GB) for adjacency computations
3. Enable **Unity Catalog** access on the cluster

### Data Preparation

1. **Download the datasets** listed in the [Datasets](#datasets) section below
2. **Upload to Unity Catalog Volumes** (recommended) or DBFS:

```python
# Example: Upload to a UC Volume
# In the Databricks UI: Catalog > your_catalog > your_schema > Volumes > Create Volume
# Then upload files via UI or CLI:
databricks fs cp ./vicmap_property.shp dbfs:/Volumes/your_catalog/your_schema/source/vicmap_property.shp
```

3. **Update notebook paths** &mdash; each notebook has a configuration cell at the top where you set:

```python
catalog = "your_catalog"
schema  = "your_schema"
source_volume = f"/Volumes/{catalog}/{schema}/source"
```

### Running the Notebooks

Import the notebooks into your Databricks workspace and run in order:

| Step | Notebook | What It Does | Approx. Time |
|------|----------|--------------|-------------|
| 0 | `geo_datasource/01_geo_datasource_definition.py` | Registers the custom `geo` data source | < 1 min |
| 1 | `land_parcel_analysis/01_data_ingestion.py` | Loads Vicmap shapefiles into Delta tables with native GEOMETRY | 5-15 min |
| 2 | `land_parcel_analysis/02_geometric_features.py` | Computes area, perimeter, compactness, aspect ratio | 5-10 min |
| 3 | `land_parcel_analysis/03_edge_topology.py` | Computes adjacency and shared boundaries | 10-30 min* |
| 3a | `land_parcel_analysis/03a_road_frontage_analysis.py` | Classifies road frontages | 5-10 min |
| 3b | `land_parcel_analysis/03b_activity_centre_proximity.py` | Distances to activity centres | 5-10 min |
| 4 | `land_parcel_analysis/04_suitability_scoring.py` | Generates scored + ranked output | 5-10 min |

*Edge topology uses a cross-join filtered by `ST_Intersects`, which is computationally intensive. Processing is scoped to one LGA at a time for manageability.

---

## Datasets

All datasets are freely available under Creative Commons Attribution 4.0 licensing.

### Primary: Land Parcels

| Dataset | Source | Format | CRS |
|---------|--------|--------|-----|
| [Vicmap Property - Parcel Polygon](https://discover.data.vic.gov.au/dataset/vicmap-property-parcel-polygon-with-parcel-detail) | Data.Vic | SHP/GDB | EPSG:3111 |

This is the **foundation dataset** &mdash; polygon geometries for every land parcel in Victoria with lot/plan identifiers, LGA codes, and freehold/crown status.

### Planning Context

| Dataset | Source | Format | CRS |
|---------|--------|--------|-----|
| [Vicmap Planning - Zone Polygon](https://discover.data.vic.gov.au/dataset/vicmap-planning-planning-scheme-zone-polygon) | Data.Vic | SHP/GDB | EPSG:3111 |
| [Vicmap Planning - Overlay Polygon](https://discover.data.vic.gov.au/dataset/vicmap-planning-planning-scheme-overlay-polygon) | Data.Vic | SHP/GDB | EPSG:3111 |

Zones identify HCTZ, GRZ, RGZ, NRZ, etc. Overlays flag heritage, flood, environmental, and vegetation constraints.

### Transport

| Dataset | Source | Format | CRS |
|---------|--------|--------|-----|
| [PT Stops (all modes)](https://opendata.transport.vic.gov.au/dataset/public-transport-lines-and-stops) | Transport Open Data | GeoJSON | EPSG:4326 |
| [GTFS Schedule](https://opendata.transport.vic.gov.au/dataset/gtfs-schedule) | Transport Open Data | GTFS (CSV) | EPSG:4326 |
| [Vicmap Transport - Road Line](https://discover.data.vic.gov.au/dataset/vicmap-transport-road-line) | Data.Vic | SHP/GDB | EPSG:3111 |

PT stops determine HCTZ eligibility (800m train/tram, 400m bus). GTFS data identifies high-frequency bus routes. Road lines classify frontage type.

### Other Context Layers

| Dataset | Source | Format | CRS |
|---------|--------|--------|-----|
| [Plan Melbourne Activity Centres](https://www.planning.vic.gov.au/guides-and-resources/strategies-and-initiatives/plan-melbourne/the-map-room) | Planning Vic | SHP/TAB | Various |
| [Vicmap Address](https://discover.data.vic.gov.au/dataset/vicmap-address-address-point) | Data.Vic | SHP/GDB | EPSG:3111 |
| [Vicmap Elevation / 30m DEM](https://data2.cerdi.edu.au/dataset/vvg_vicdem_30m_merged_clipped_3857) | CeRDI | GeoTIFF | EPSG:3857 |
| [Open Space](https://discover.data.vic.gov.au/dataset/open-space) | Data.Vic | SHP/GeoJSON | Various |

---

## Analysis Pipeline

### Phase 1: Feature Engineering

#### Geometric Attributes (Notebook 02)

Computed from parcel polygon geometry using Spatial SQL:

| Feature | SQL Function | Interpretation |
|---------|-------------|----------------|
| Lot area (sqm) | `ST_Area(geometry)` | Below 500 sqm = needs consolidation |
| Perimeter (m) | `ST_Perimeter(geometry)` | Relates to setback exposure |
| Compactness index | `4 * PI() * area / perimeter^2` | 1.0 = circle (ideal), < 0.3 = irregular |
| Aspect ratio | `width / depth` from bounding box | Narrow lots are less efficient |
| Elongation index | `max(w,d) / min(w,d)` | > 3.0 = hard to develop |

#### Edge Topology (Notebook 03)

Computed by intersecting parcel boundaries with neighbours:

| Feature | How It's Computed | Interpretation |
|---------|-------------------|----------------|
| Adjacent parcels | `ST_Touches(p1, p2)` | More neighbours = more consolidation options |
| Shared boundary (m) | `ST_Length(ST_Intersection(boundary1, boundary2))` | > 15m = easy to merge |
| Same-zone neighbours | Count where `zone_1 = zone_2` | Compatible consolidation |
| Internal lot flag | Shared boundary / perimeter > 0.8 | Limited street access |

#### Road Frontage (Notebook 03a)

Computed by intersecting parcel boundaries with road network:

| Feature | Interpretation |
|---------|----------------|
| Number of road frontages | Corner lots have 2+; internal lots may have 0 |
| Frontage length by road class | Arterial vs local vs laneway |
| Corner lot indicator | Different setback rules apply |
| Laneway access | Secondary access improves development potential |
| Frontage-to-perimeter ratio | Low = mostly enclosed by other parcels |

#### Proximity (Notebook 03b)

Distances from parcel centroid to nearest points of interest:

| Feature | Threshold | Significance |
|---------|-----------|-------------|
| Distance to train/tram | 800m | HCTZ eligibility |
| Distance to bus stop | 400m | HCTZ eligibility (if high-frequency) |
| Distance to activity centre | 800m | Growth area proximity |

### Phase 2: Suitability Scoring

Notebook 04 combines all features into a weighted multi-criteria score.

**Opportunity factors (positive score):**

| Factor | Points | Rationale |
|--------|--------|-----------|
| HCTZ/MUZ zone | +30 | Policy priority area |
| GRZ/NRZ zone | +15 | Residential consolidation |
| Within 800m train/tram | +20 | Transit-oriented development |
| Within 400m bus | +10 | Public transport access |
| Within 800m activity centre | +15 | Near commercial/retail hub |
| Below 500 sqm | +25 | Below minimum site area |
| Below 300 sqm | +10 | Critically undersized |
| Below 15m frontage | +20 | Below minimum frontage |
| Poor compactness (< 0.3) | +15 | Inefficient shape |
| High elongation (> 3) | +10 | Hard to develop efficiently |
| Internal lot | +20 | Limited development potential alone |
| Same-zone neighbours | +15 | Compatible consolidation |
| Long shared boundary (> 15m) | +10 | Easy to merge |

**Constraint factors (negative score):**

| Factor | Points | Rationale |
|--------|--------|-----------|
| Heritage overlay (HO) | -50 | Preservation constraint |
| Flood overlay (LSIO/SBO) | -40 | Development risk |
| ESO overlay | -30 | Environmental constraint |
| VPO overlay | -20 | Vegetation protection |
| Isolated lot (no neighbours) | -20 | No consolidation partners |
| Large lot (> 1000 sqm) | -15 | Already developable size |
| Corner lot | -10 | Strategic value, less need |

**Final score** = sum of opportunity points + sum of constraint points. Parcels are ranked and classified into tiers: Excellent, Good, Moderate, Low, Unsuitable.

---

## Databricks Spatial SQL Primer

If you're new to spatial analysis on Databricks, here are the key concepts.

### What is Spatial SQL?

Databricks Runtime 15.4+ includes built-in `ST_*` (spatial) SQL functions that operate directly on a native `GEOMETRY` column type. This means you can do geospatial analysis at scale using familiar SQL &mdash; no external libraries or UDFs needed for core operations.

### Native GEOMETRY Type

Databricks stores geometry using a native binary column type with SRID (Spatial Reference ID) metadata:

```sql
-- Create a geometry from Well-Known Text (WKT)
SELECT ST_GeomFromWKT('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))', 3111) AS geom;

-- Create a point
SELECT ST_SetSRID(ST_Point(144.9631, -37.8136), 4326) AS melbourne_cbd;
```

### Essential Functions Used in This Project

```sql
-- Measurements (use projected CRS like EPSG:3111 for metre results)
ST_Area(geometry)                         -- Area in CRS units (sqm for 3111)
ST_Perimeter(geometry)                    -- Perimeter in CRS units (m for 3111)
ST_Length(linestring)                     -- Line length in CRS units

-- Spatial relationships (the workhorse predicates)
ST_Intersects(geom1, geom2)              -- Do geometries share any space?
ST_Touches(geom1, geom2)                 -- Do they share only a boundary?
ST_Contains(polygon, point)              -- Is the point inside the polygon?

-- Geometry operations
ST_Boundary(polygon)                     -- Extract ring as linestring
ST_Intersection(geom1, geom2)            -- Shared geometry between two
ST_Centroid(geometry)                     -- Centre point
ST_Transform(geometry, target_srid)      -- Reproject between CRS

-- Bounding box
ST_XMin(geometry), ST_XMax(geometry)     -- X extent
ST_YMin(geometry), ST_YMax(geometry)     -- Y extent

-- Distance
ST_DistanceSphere(geom1, geom2)          -- Great-circle distance in metres (WGS84 inputs)

-- Export for visualisation
ST_AsGeoJSON(geometry)                   -- Convert to GeoJSON string
ST_X(point), ST_Y(point)                -- Extract lon/lat from point
```

### Custom Geo Data Source

This project includes a custom PySpark `DataSource` (in `geo_datasource/`) that reads Shapefiles, GeoJSON, GDB, TAB, GPKG, and KML files directly into Spark DataFrames:

```python
# Register the data source (run once per session)
%run ./geo_datasource/01_geo_datasource_definition

# Read a shapefile
df = spark.read.format("geo").option("path", "/Volumes/catalog/schema/source/parcels.shp").load()

# Read a specific layer from a File Geodatabase
df = spark.read.format("geo").option("path", "/data/planning.gdb").option("layer", "zones").load()

# Reproject on read
df = spark.read.format("geo").option("path", "/data/parcels.shp").option("crs", "EPSG:4326").load()
```

### Materialising to Delta with Native Geometry

After loading via the custom data source (which outputs WKT strings), convert to native GEOMETRY:

```python
df_with_geom = (df
    .withColumn("geom", expr("ST_GeomFromWKT(geometry, 3111)"))
    .drop("geometry")
    .withColumnRenamed("geom", "geometry")
)
df_with_geom.write.mode("overwrite").saveAsTable("catalog.schema.table_name")
```

---

## Output Tables

The pipeline produces these Delta tables in Unity Catalog:

| Table | Description | Key Columns |
|-------|-------------|-------------|
| `parcel_geometric_features` | Base parcel metrics | `geometry`, `area_sqm`, `compactness_index`, `elongation_index` |
| `parcel_adjacency` | Pairwise parcel relationships | `parcel_1`, `parcel_2`, `shared_boundary_m`, `touches` |
| `parcel_edge_topology` | Aggregated topology features | `num_adjacent_parcels`, `is_internal_lot`, `centroid_lon/lat` |
| `parcel_pt_proximity` | Public transport distances | `nearest_train_m`, `within_800m_train`, `nearest_bus_m` |
| `parcel_road_frontage` | Road frontage features | `num_road_frontages`, `total_frontage_m`, `is_corner_lot` |
| `parcel_suitability_scores` | Full scoring breakdown | `opportunity_score`, `constraint_score`, `suitability_score` |
| `consolidation_candidates` | Final ranked output | `suitability_score`, `suitability_tier`, `rank` |

---

## Visualization

Three visualisation libraries are demonstrated, each with different strengths:

| Library | Best For | How to Render in Databricks |
|---------|----------|----------------------------|
| **Folium** | Interactive polygon maps with tooltips | `displayHTML(m._repr_html_())` |
| **Kepler.gl** | Large dataset exploration and filtering | Save to UC Volume as HTML |
| **PyDeck** | 3D visualisations and column charts | `display(deck)` |

All visualisations require geometry in WGS84:

```sql
SELECT
    parcel_id,
    suitability_score,
    ST_AsGeoJSON(ST_Transform(geometry, 4326)) AS geojson
FROM consolidation_candidates
WHERE suitability_tier = 'Excellent'
LIMIT 500;
```

---

## Architecture Decisions

| Decision | Rationale |
|----------|-----------|
| **Serverless with High Memory (32 GB)** | Spatial joins are memory-intensive; serverless eliminates cluster management overhead |
| **Environment v4** | Required for native Spatial SQL `ST_*` functions on serverless |
| **Store in EPSG:3111 (VicGrid)** | Metre-based CRS for accurate area/distance; native CRS of all Vicmap data |
| **Native GEOMETRY type** | Enables spatial indexing and pushdown in Databricks; avoids WKT string parsing |
| **LGA-scoped adjacency** | Cross-join is O(n^2); scoping to one LGA keeps it tractable |
| **Symmetry elimination** | `p1.parcel_id < p2.parcel_id` halves adjacency comparisons |
| **Broadcast joins for PT stops** | PT stops (~10K records) are small enough to broadcast to all executors |
| **Custom DataSource over GeoPandas** | Fiona-based streaming reads are more memory-efficient for large shapefiles |
| **Weighted scoring over ML** | Transparent, interpretable, and tuneable by domain experts |

---

## References

- [Victorian Housing Statement](https://www.vic.gov.au/housing-statement)
- [Housing Choice and Transport Zone (HCTZ)](https://landchecker.com.au/functionalities/housing-choice-and-transport-zone-hctz-development-in-victoria/)
- [Vicmap Property](https://www.land.vic.gov.au/maps-and-spatial/spatial-data/vicmap-catalogue/vicmap-property)
- [Vicmap Planning](https://discover.data.vic.gov.au/dataset/vicmap-planning)
- [Plan Melbourne Activity Centres](https://www.planning.vic.gov.au/guides-and-resources/strategies-and-initiatives/plan-melbourne/the-map-room)
- [Databricks Spatial SQL Documentation](https://docs.databricks.com/en/sql/language-manual/sql-ref-functions-builtin.html)
- [Data.Vic Open Data Portal](https://discover.data.vic.gov.au)
- [Transport Open Data Victoria](https://opendata.transport.vic.gov.au)

---

## License

This project is provided under the [MIT License](LICENSE). All source datasets are licensed under [Creative Commons Attribution 4.0](https://creativecommons.org/licenses/by/4.0/) by the Victorian Government.
