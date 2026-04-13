"""Vector tile (MVT) endpoint for parcel polygons."""
from fastapi import APIRouter, Query
from fastapi.responses import Response
from typing import Optional, List
from db import fetch_one

router = APIRouter(prefix="/api/tiles", tags=["tiles"])

# Properties to include in tiles — keep lean for small tile sizes
MVT_PROPERTIES = """
    parcel_id, zone_code, lga_name, area_sqm::int AS area_sqm,
    suitability_score::int AS suitability_score, suitability_tier,
    num_adjacent_parcels, nearest_any_pt_m::int AS nearest_any_pt_m,
    is_growth_area_lga, compactness_index::real AS compactness_index
"""

# Empty MVT tile (single-layer, zero features)
EMPTY_TILE = b""


@router.get("/{z}/{x}/{y}.pbf")
async def get_tile(
    z: int,
    x: int,
    y: int,
    tier: Optional[List[str]] = Query(None),
    lga: Optional[str] = Query(None),
    min_score: Optional[int] = Query(None),
    exclude_growth_areas: bool = Query(False),
):
    """Return a Mapbox Vector Tile (MVT) for the given z/x/y coordinates."""
    # Build dynamic WHERE filters
    # ST_TileEnvelope returns EPSG:3857; our geom is EPSG:4326.
    # Use ST_Transform to compare in 4326 for the spatial index hit.
    conditions = [
        "geom IS NOT NULL",
        "ST_Intersects(geom, ST_Transform(ST_TileEnvelope($1, $2, $3), 4326))",
    ]
    params: list = [z, x, y]
    idx = 4

    if tier:
        if len(tier) == 1:
            conditions.append(f"suitability_tier = ${idx}")
            params.append(tier[0])
            idx += 1
        else:
            placeholders = ", ".join(f"${idx + i}" for i in range(len(tier)))
            conditions.append(f"suitability_tier IN ({placeholders})")
            params.extend(tier)
            idx += len(tier)
    if lga:
        conditions.append(f"lga_name = ${idx}")
        params.append(lga)
        idx += 1
    if min_score is not None:
        conditions.append(f"suitability_score >= ${idx}")
        params.append(min_score)
        idx += 1
    if exclude_growth_areas:
        conditions.append("is_growth_area_lga = false")

    # ST_AsMVTGeom needs geometry + bounds in EPSG:3857 (web mercator).
    # Use ST_MakeValid to fix any degenerate geometries after transform.
    # clip_geom=false avoids tile-boundary clipping artifacts on small parcels;
    # MapLibre handles clipping client-side.
    geom_3857 = "ST_MakeValid(ST_Transform(geom, 3857))"
    tile_env = "ST_TileEnvelope($1, $2, $3)"
    geom_expr = f"ST_AsMVTGeom({geom_3857}, {tile_env}, 4096, 256, false)"
    if z < 10:
        geom_expr = f"ST_AsMVTGeom(ST_Simplify({geom_3857}, 50), {tile_env}, 4096, 256, false)"
        if not tier:
            conditions.append("suitability_tier != 'Tier 5 - Low'")
    elif z < 12:
        geom_expr = f"ST_AsMVTGeom(ST_Simplify({geom_3857}, 10), {tile_env}, 4096, 256, false)"

    where = " AND ".join(conditions)

    query = f"""
        SELECT ST_AsMVT(tile, 'parcels', 4096, 'geom') AS mvt
        FROM (
            SELECT {geom_expr} AS geom, {MVT_PROPERTIES}
            FROM candidates_mvt
            WHERE {where}
        ) AS tile
        WHERE geom IS NOT NULL
    """

    row = await fetch_one(query, *params)
    mvt_data = row["mvt"] if row and row.get("mvt") else EMPTY_TILE

    # Convert memoryview to bytes if needed
    if isinstance(mvt_data, memoryview):
        mvt_data = bytes(mvt_data)

    return Response(
        content=mvt_data,
        media_type="application/x-protobuf",
        headers={
            "Cache-Control": "public, max-age=300",
            "Access-Control-Allow-Origin": "*",
        },
    )
