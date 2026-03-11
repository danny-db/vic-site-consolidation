-- PostGIS Schema Setup for VIC Site Consolidation
-- Run this against the Lakebase PostgreSQL instance after sync tables are created

-- Enable PostGIS
CREATE EXTENSION IF NOT EXISTS postgis;

-- Spatial view: candidates with proper PostGIS geometry
CREATE OR REPLACE VIEW candidates_geo AS
SELECT
    *,
    ST_SetSRID(ST_GeomFromText(geometry_wkt), 4326)::geometry AS geom,
    ST_SetSRID(ST_MakePoint(centroid_lon, centroid_lat), 4326)::geography AS centroid_geog
FROM consolidation_candidates_for_sync
WHERE geometry_wkt IS NOT NULL;

-- Materialized view: fast point lookups with spatial index
CREATE MATERIALIZED VIEW IF NOT EXISTS candidates_points AS
SELECT
    parcel_id, plan_number, lot_number, zone_code, lga_name,
    centroid_lon, centroid_lat, area_sqm, compactness_index,
    num_adjacent_parcels, nearest_any_pt_m, is_growth_area_lga,
    lots_in_plan, opportunity_score, constraint_score,
    suitability_score, suitability_tier, rank,
    ST_SetSRID(ST_MakePoint(centroid_lon, centroid_lat), 4326)::geography AS centroid_geog
FROM consolidation_candidates_for_sync
WHERE centroid_lon IS NOT NULL;

-- Spatial GIST index for proximity queries
CREATE INDEX IF NOT EXISTS idx_candidates_points_geog
ON candidates_points USING GIST (centroid_geog);

-- B-tree indexes for common filters
CREATE INDEX IF NOT EXISTS idx_candidates_tier ON candidates_points (suitability_tier);
CREATE INDEX IF NOT EXISTS idx_candidates_lga ON candidates_points (lga_name);
CREATE INDEX IF NOT EXISTS idx_candidates_score ON candidates_points (suitability_score DESC);
CREATE INDEX IF NOT EXISTS idx_candidates_zone ON candidates_points (zone_code);

-- Refresh materialized view
REFRESH MATERIALIZED VIEW candidates_points;

-- Verify
SELECT suitability_tier, COUNT(*) AS cnt
FROM candidates_points
GROUP BY suitability_tier
ORDER BY cnt DESC;
