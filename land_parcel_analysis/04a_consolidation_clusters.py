# Databricks notebook source
# MAGIC %md
# MAGIC # Stage 4a: Consolidation Clusters (Graph Analysis)
# MAGIC
# MAGIC Discovers **multi-parcel consolidation clusters** using graph algorithms on the
# MAGIC existing `parcel_adjacency` data. While the main pipeline finds pairwise
# MAGIC consolidation (parcel A + B), real-world developers consolidate 3-5+ lots.
# MAGIC
# MAGIC **Approach:**
# MAGIC 1. Build a NetworkX graph from filtered adjacency edges
# MAGIC 2. Find connected components (theoretical maximum sites)
# MAGIC 3. Apply Louvain community detection for developer-realistic clusters
# MAGIC 4. Compute betweenness centrality to identify strategic "hub" parcels
# MAGIC
# MAGIC **Inputs:** `parcel_adjacency`, `consolidation_candidates`
# MAGIC **Outputs:** `consolidation_clusters`, `consolidation_cluster_stats`

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup

# COMMAND ----------

dbutils.widgets.text("catalog_name", "danny_catalog", "Catalog Name")
dbutils.widgets.text("schema_name", "dtp_hackathon", "Schema Name")

CATALOG = dbutils.widgets.get("catalog_name")
SCHEMA = dbutils.widgets.get("schema_name")

print(f"Using: {CATALOG}.{SCHEMA}")

# COMMAND ----------

# Verify input tables exist
for table in ["parcel_adjacency", "consolidation_candidates"]:
    count = spark.sql(f"SELECT COUNT(*) AS cnt FROM {CATALOG}.{SCHEMA}.{table}").first()["cnt"]
    print(f"  {table}: {count:,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Collect Filtered Adjacency Data
# MAGIC
# MAGIC Same filters as `consolidation_pairs_sync` in notebook 05:
# MAGIC - `shared_boundary > 5m`
# MAGIC - Both parcels have `suitability_score >= 40`
# MAGIC - Same zone

# COMMAND ----------

CATALOG = dbutils.widgets.get("catalog_name")
SCHEMA = dbutils.widgets.get("schema_name")

edges_df = spark.sql(f"""
    SELECT
        a.parcel_1,
        a.parcel_2,
        ROUND(a.shared_boundary_m, 2) AS shared_boundary_m,
        s1.suitability_score AS score_1,
        s2.suitability_score AS score_2,
        s1.zone_code AS zone_code,
        s1.lga_name AS lga_name,
        s1.area_sqm AS area_1,
        s2.area_sqm AS area_2,
        s1.centroid_lon AS lon_1,
        s1.centroid_lat AS lat_1,
        s2.centroid_lon AS lon_2,
        s2.centroid_lat AS lat_2
    FROM {CATALOG}.{SCHEMA}.parcel_adjacency a
    JOIN {CATALOG}.{SCHEMA}.consolidation_candidates s1 ON a.parcel_1 = s1.parcel_id
    JOIN {CATALOG}.{SCHEMA}.consolidation_candidates s2 ON a.parcel_2 = s2.parcel_id
    WHERE a.shared_boundary_m > 5
      AND s1.suitability_score >= 40
      AND s2.suitability_score >= 40
      AND s1.zone_code = s2.zone_code
""")

edge_count = edges_df.count()
print(f"Filtered edges: {edge_count:,}")

# Collect to driver for NetworkX (small enough for single-node processing)
edges = edges_df.collect()
print(f"Collected {len(edges):,} edges to driver")

# COMMAND ----------

# Build node attribute lookup from edges.
# zone_code is the same for both parcels (filtered by same zone in the query).
# lga_name in the edge row comes from parcel_1's join, so parcel_2 gets an
# initial value from parcel_1's LGA. We correct parcel_2's lga below.
node_attrs = {}
for row in edges:
    for suffix, pid in [("1", row.parcel_1), ("2", row.parcel_2)]:
        if pid not in node_attrs:
            node_attrs[pid] = {
                "score": row[f"score_{suffix}"],
                "zone_code": row.zone_code,
                "lga_name": row.lga_name,
                "area_sqm": row[f"area_{suffix}"],
                "centroid_lon": row[f"lon_{suffix}"],
                "centroid_lat": row[f"lat_{suffix}"],
            }

# Correct parcel_2's lga_name with its own candidate record
parcel2_lga = spark.sql(f"""
    SELECT DISTINCT a.parcel_2 AS parcel_id, s2.lga_name, s2.zone_code
    FROM {CATALOG}.{SCHEMA}.parcel_adjacency a
    JOIN {CATALOG}.{SCHEMA}.consolidation_candidates s1 ON a.parcel_1 = s1.parcel_id
    JOIN {CATALOG}.{SCHEMA}.consolidation_candidates s2 ON a.parcel_2 = s2.parcel_id
    WHERE a.shared_boundary_m > 5
      AND s1.suitability_score >= 40
      AND s2.suitability_score >= 40
      AND s1.zone_code = s2.zone_code
""").collect()

for row in parcel2_lga:
    if row.parcel_id in node_attrs:
        node_attrs[row.parcel_id]["lga_name"] = row.lga_name
        node_attrs[row.parcel_id]["zone_code"] = row.zone_code

print(f"Built attributes for {len(node_attrs):,} unique parcels")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Build NetworkX Graph

# COMMAND ----------

import networkx as nx

G = nx.Graph()

# Add nodes with attributes
for pid, attrs in node_attrs.items():
    G.add_node(pid, **attrs)

# Add edges with shared boundary as weight
for row in edges:
    G.add_edge(row.parcel_1, row.parcel_2, weight=row.shared_boundary_m)

print(f"Graph: {G.number_of_nodes():,} nodes, {G.number_of_edges():,} edges")
print(f"Density: {nx.density(G):.6f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Connected Components
# MAGIC
# MAGIC Find contiguous parcel groups — the theoretical maximum consolidation sites.

# COMMAND ----------

components = list(nx.connected_components(G))
print(f"Connected components: {len(components):,}")

# Size distribution
from collections import Counter
size_dist = Counter(len(c) for c in components)
print("\nComponent size distribution:")
for size in sorted(size_dist.keys()):
    print(f"  Size {size:>4}: {size_dist[size]:>6} components")

# Assign component IDs (sorted by size descending)
components_sorted = sorted(components, key=len, reverse=True)
node_component = {}
for comp_id, comp in enumerate(components_sorted):
    for node in comp:
        node_component[node] = comp_id

print(f"\nLargest component: {len(components_sorted[0]):,} parcels")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Louvain Community Detection
# MAGIC
# MAGIC Finds natural clusters based on boundary weight density.
# MAGIC Resolution 1.5 biases toward 3-8 parcel groups (developer-realistic).

# COMMAND ----------

communities = nx.community.louvain_communities(G, weight='weight', resolution=1.5, seed=42)
print(f"Louvain communities: {len(communities):,}")

# Size distribution
comm_size_dist = Counter(len(c) for c in communities)
print("\nCluster size distribution:")
for size in sorted(comm_size_dist.keys()):
    print(f"  Size {size:>4}: {comm_size_dist[size]:>6} clusters")

# Assign cluster IDs (sorted by size descending)
communities_sorted = sorted(communities, key=len, reverse=True)
node_cluster = {}
for cluster_id, comm in enumerate(communities_sorted):
    for node in comm:
        node_cluster[node] = cluster_id

multi_parcel = sum(1 for c in communities_sorted if len(c) >= 2)
print(f"\nMulti-parcel clusters (size >= 2): {multi_parcel:,}")
print(f"Clusters with 3+ parcels: {sum(1 for c in communities_sorted if len(c) >= 3):,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Centrality Analysis
# MAGIC
# MAGIC Betweenness centrality identifies "hub" parcels — strategic acquisition targets
# MAGIC that connect multiple neighbours. Computed per-component (skip singletons).

# COMMAND ----------

centrality = {}

# Compute betweenness centrality per component (more efficient than whole graph)
for comp in components_sorted:
    if len(comp) < 3:
        # Assign 0 centrality to small components
        for node in comp:
            centrality[node] = 0.0
        continue
    subgraph = G.subgraph(comp)
    bc = nx.betweenness_centrality(subgraph, weight='weight')
    centrality.update(bc)

# Determine hub threshold (top 5% of non-zero centrality)
non_zero = [v for v in centrality.values() if v > 0]
if non_zero:
    import numpy as np
    hub_threshold = np.percentile(non_zero, 95)
    hub_count = sum(1 for v in centrality.values() if v >= hub_threshold)
    print(f"Centrality stats (non-zero): min={min(non_zero):.4f}, max={max(non_zero):.4f}, median={np.median(non_zero):.4f}")
    print(f"Hub threshold (95th percentile): {hub_threshold:.4f}")
    print(f"Hub parcels: {hub_count:,}")
else:
    hub_threshold = 1.0  # No hubs if all zero
    print("No non-zero centrality values found")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Write Output Tables

# COMMAND ----------

# MAGIC %md
# MAGIC ### Table 1: `consolidation_clusters`
# MAGIC
# MAGIC One row per parcel with cluster assignment and centrality metrics.

# COMMAND ----------

import numpy as np
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType

# Pre-compute cluster-level aggregates
cluster_areas = {}
cluster_scores = {}
for cluster_id, comm in enumerate(communities_sorted):
    areas = [node_attrs[n]["area_sqm"] for n in comm if n in node_attrs]
    scores = [node_attrs[n]["score"] for n in comm if n in node_attrs]
    cluster_areas[cluster_id] = sum(areas) if areas else 0.0
    cluster_scores[cluster_id] = {
        "avg": float(np.mean(scores)) if scores else 0.0,
        "max": int(max(scores)) if scores else 0,
    }

# Build rows
rows = []
for pid, attrs in node_attrs.items():
    cluster_id = node_cluster.get(pid, -1)
    comp_id = node_component.get(pid, -1)
    cluster_size = len(communities_sorted[cluster_id]) if cluster_id >= 0 and cluster_id < len(communities_sorted) else 1
    bc = centrality.get(pid, 0.0)
    is_hub = bc >= hub_threshold and bc > 0

    rows.append((
        pid,
        int(cluster_id),
        int(comp_id),
        int(cluster_size),
        round(cluster_areas.get(cluster_id, 0.0), 2),
        round(cluster_scores.get(cluster_id, {}).get("avg", 0.0), 2),
        int(cluster_scores.get(cluster_id, {}).get("max", 0)),
        bool(is_hub),
        round(float(bc), 6),
        int(attrs["score"]),
        str(attrs["zone_code"]),
        str(attrs["lga_name"]),
        round(float(attrs["area_sqm"]), 2),
        round(float(attrs["centroid_lon"]), 6),
        round(float(attrs["centroid_lat"]), 6),
    ))

schema = StructType([
    StructField("parcel_id", StringType(), False),
    StructField("cluster_id", IntegerType(), False),
    StructField("component_id", IntegerType(), False),
    StructField("cluster_size", IntegerType(), False),
    StructField("cluster_combined_area_sqm", DoubleType(), True),
    StructField("cluster_avg_score", DoubleType(), True),
    StructField("cluster_max_score", IntegerType(), True),
    StructField("is_hub_parcel", BooleanType(), False),
    StructField("betweenness_centrality", DoubleType(), False),
    StructField("suitability_score", IntegerType(), False),
    StructField("zone_code", StringType(), True),
    StructField("lga_name", StringType(), True),
    StructField("area_sqm", DoubleType(), True),
    StructField("centroid_lon", DoubleType(), True),
    StructField("centroid_lat", DoubleType(), True),
])

clusters_sdf = spark.createDataFrame(rows, schema)

clusters_sdf.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.consolidation_clusters")

total = spark.sql(f"SELECT COUNT(*) AS cnt FROM {CATALOG}.{SCHEMA}.consolidation_clusters").first()["cnt"]
print(f"Written consolidation_clusters: {total:,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Table 2: `consolidation_cluster_stats`
# MAGIC
# MAGIC Aggregate stats per cluster (only clusters with 2+ parcels).

# COMMAND ----------

CATALOG = dbutils.widgets.get("catalog_name")
SCHEMA = dbutils.widgets.get("schema_name")

spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.consolidation_cluster_stats AS
    WITH cluster_agg AS (
        SELECT
            cluster_id,
            COUNT(*) AS member_count,
            ROUND(SUM(area_sqm), 2) AS combined_area_sqm,
            ROUND(AVG(suitability_score), 2) AS avg_suitability_score,
            MIN(suitability_score) AS min_suitability_score,
            MAX(suitability_score) AS max_suitability_score,
            -- Dominant zone: most common zone_code in cluster
            FIRST(zone_code) AS dominant_zone,
            -- Dominant LGA: most common lga_name in cluster
            FIRST(lga_name) AS dominant_lga,
            SUM(CASE WHEN is_hub_parcel THEN 1 ELSE 0 END) AS hub_parcel_count,
            ROUND(AVG(centroid_lon), 6) AS centroid_lon,
            ROUND(AVG(centroid_lat), 6) AS centroid_lat,
            ROUND(MIN(centroid_lon), 6) AS bbox_min_lon,
            ROUND(MAX(centroid_lon), 6) AS bbox_max_lon,
            ROUND(MIN(centroid_lat), 6) AS bbox_min_lat,
            ROUND(MAX(centroid_lat), 6) AS bbox_max_lat
        FROM {CATALOG}.{SCHEMA}.consolidation_clusters
        GROUP BY cluster_id
        HAVING COUNT(*) >= 2
    ),
    -- Get dominant zone/lga by mode (most frequent)
    zone_mode AS (
        SELECT cluster_id, zone_code,
            ROW_NUMBER() OVER (PARTITION BY cluster_id ORDER BY COUNT(*) DESC) AS rn
        FROM {CATALOG}.{SCHEMA}.consolidation_clusters
        GROUP BY cluster_id, zone_code
    ),
    lga_mode AS (
        SELECT cluster_id, lga_name,
            ROW_NUMBER() OVER (PARTITION BY cluster_id ORDER BY COUNT(*) DESC) AS rn
        FROM {CATALOG}.{SCHEMA}.consolidation_clusters
        GROUP BY cluster_id, lga_name
    ),
    -- Sum internal boundary lengths per cluster
    internal_edges AS (
        SELECT
            c1.cluster_id,
            ROUND(SUM(a.shared_boundary_m), 2) AS total_internal_boundary_m
        FROM {CATALOG}.{SCHEMA}.parcel_adjacency a
        JOIN {CATALOG}.{SCHEMA}.consolidation_clusters c1 ON a.parcel_1 = c1.parcel_id
        JOIN {CATALOG}.{SCHEMA}.consolidation_clusters c2 ON a.parcel_2 = c2.parcel_id
        WHERE c1.cluster_id = c2.cluster_id
        GROUP BY c1.cluster_id
    )
    SELECT
        ca.cluster_id,
        ca.member_count,
        ca.combined_area_sqm,
        ca.avg_suitability_score,
        ca.min_suitability_score,
        ca.max_suitability_score,
        zm.zone_code AS dominant_zone,
        lm.lga_name AS dominant_lga,
        ca.hub_parcel_count,
        COALESCE(ie.total_internal_boundary_m, 0.0) AS total_internal_boundary_m,
        ca.centroid_lon,
        ca.centroid_lat,
        ca.bbox_min_lon,
        ca.bbox_max_lon,
        ca.bbox_min_lat,
        ca.bbox_max_lat
    FROM cluster_agg ca
    LEFT JOIN zone_mode zm ON ca.cluster_id = zm.cluster_id AND zm.rn = 1
    LEFT JOIN lga_mode lm ON ca.cluster_id = lm.cluster_id AND lm.rn = 1
    LEFT JOIN internal_edges ie ON ca.cluster_id = ie.cluster_id
    ORDER BY ca.member_count DESC, ca.combined_area_sqm DESC
""")

stats_count = spark.sql(f"SELECT COUNT(*) AS cnt FROM {CATALOG}.{SCHEMA}.consolidation_cluster_stats").first()["cnt"]
print(f"Written consolidation_cluster_stats: {stats_count:,} clusters (with 2+ parcels)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Validation

# COMMAND ----------

CATALOG = dbutils.widgets.get("catalog_name")
SCHEMA = dbutils.widgets.get("schema_name")

# Referential integrity: all cluster parcels must exist in candidates
orphans = spark.sql(f"""
    SELECT COUNT(*) AS cnt
    FROM {CATALOG}.{SCHEMA}.consolidation_clusters cl
    LEFT JOIN {CATALOG}.{SCHEMA}.consolidation_candidates cc ON cl.parcel_id = cc.parcel_id
    WHERE cc.parcel_id IS NULL
""").first()["cnt"]

assert orphans == 0, f"Found {orphans} cluster parcels not in consolidation_candidates!"
print(f"Referential integrity check passed (0 orphans)")

# Row count matches graph node count
cluster_rows = spark.sql(f"SELECT COUNT(*) AS cnt FROM {CATALOG}.{SCHEMA}.consolidation_clusters").first()["cnt"]
print(f"Cluster table rows: {cluster_rows:,} (expected: {G.number_of_nodes():,} graph nodes)")
assert cluster_rows == G.number_of_nodes(), f"Row count mismatch: {cluster_rows} vs {G.number_of_nodes()}"

# Multi-parcel cluster summary
print("\nMulti-parcel cluster summary:")
display(spark.sql(f"""
    SELECT
        cluster_size,
        COUNT(*) AS num_parcels,
        COUNT(DISTINCT cluster_id) AS num_clusters,
        ROUND(AVG(suitability_score), 1) AS avg_score,
        ROUND(AVG(area_sqm), 1) AS avg_area_sqm
    FROM {CATALOG}.{SCHEMA}.consolidation_clusters
    WHERE cluster_size >= 2
    GROUP BY cluster_size
    ORDER BY cluster_size
"""))

# COMMAND ----------

# Top 20 largest clusters
print("Top 20 largest clusters:")
display(spark.sql(f"""
    SELECT *
    FROM {CATALOG}.{SCHEMA}.consolidation_cluster_stats
    ORDER BY member_count DESC, combined_area_sqm DESC
    LIMIT 20
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Visualization

# COMMAND ----------

import matplotlib.pyplot as plt

stats_rows = spark.sql(f"""
    SELECT member_count, COUNT(*) AS num_clusters, ROUND(AVG(combined_area_sqm), 0) AS avg_combined_area
    FROM {CATALOG}.{SCHEMA}.consolidation_cluster_stats
    GROUP BY member_count
    ORDER BY member_count
""").collect()

sizes = [r.member_count for r in stats_rows]
counts = [r.num_clusters for r in stats_rows]
avg_areas = [r.avg_combined_area for r in stats_rows]

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))

# Cluster size distribution
ax1.bar(sizes, counts, color='steelblue', edgecolor='white')
ax1.set_xlabel('Cluster Size (parcels)')
ax1.set_ylabel('Number of Clusters')
ax1.set_title('Consolidation Cluster Size Distribution')
ax1.set_xticks(sizes)

# Average combined area by cluster size
ax2.bar(sizes, avg_areas, color='darkorange', edgecolor='white')
ax2.set_xlabel('Cluster Size (parcels)')
ax2.set_ylabel('Avg Combined Area (sqm)')
ax2.set_title('Average Combined Area by Cluster Size')
ax2.set_xticks(sizes)

plt.tight_layout()
plt.savefig('/tmp/cluster_analysis.png', dpi=150, bbox_inches='tight')
plt.show()
print("Saved visualization to /tmp/cluster_analysis.png")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. City2Graph Future Work (Reference Only)
# MAGIC
# MAGIC The following sketch shows how [City2Graph](https://github.com/city2graph/city2graph)
# MAGIC could extend this analysis to heterogeneous graphs (parcels + roads + PT stops)
# MAGIC for GNN-based suitability prediction. Not executed — just a reference.
# MAGIC
# MAGIC ```python
# MAGIC # --- City2Graph + PyTorch Geometric (future enhancement) ---
# MAGIC # pip install city2graph torch-geometric
# MAGIC #
# MAGIC # import city2graph as c2g
# MAGIC # import geopandas as gpd
# MAGIC # from torch_geometric.data import HeteroData
# MAGIC #
# MAGIC # # Load parcel polygons as GeoDataFrame
# MAGIC # parcels_gdf = gpd.read_parquet("parcels.parquet")
# MAGIC # roads_gdf = gpd.read_parquet("roads.parquet")
# MAGIC # pt_stops_gdf = gpd.read_parquet("pt_stops.parquet")
# MAGIC #
# MAGIC # # Build heterogeneous graph
# MAGIC # # Nodes: parcels, road segments, PT stops
# MAGIC # # Edges: parcel-parcel (adjacency), parcel-road (frontage), parcel-PT (proximity)
# MAGIC # graph = c2g.spatial_weights_to_graph(parcels_gdf, method="queen")
# MAGIC #
# MAGIC # # Convert to PyTorch Geometric HeteroData for GNN training
# MAGIC # data = HeteroData()
# MAGIC # data['parcel'].x = parcel_features_tensor  # geometric + scoring features
# MAGIC # data['parcel', 'adjacent_to', 'parcel'].edge_index = adjacency_edges
# MAGIC # data['parcel', 'fronts', 'road'].edge_index = frontage_edges
# MAGIC # data['parcel', 'near', 'pt_stop'].edge_index = proximity_edges
# MAGIC #
# MAGIC # # Train a GNN to predict consolidation suitability
# MAGIC # # This could learn non-linear feature interactions that the
# MAGIC # # additive scoring model misses
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC | Output | Description |
# MAGIC |--------|-------------|
# MAGIC | `consolidation_clusters` | Every graph parcel with cluster assignment & centrality |
# MAGIC | `consolidation_cluster_stats` | Aggregate stats per multi-parcel cluster |
# MAGIC
# MAGIC Key metrics to check:
# MAGIC - Meaningful quantity of multi-parcel clusters (size >= 3)
# MAGIC - Hub parcels have higher-than-average connectivity
# MAGIC - Top clusters in expected areas (inner Melbourne suburbs)
