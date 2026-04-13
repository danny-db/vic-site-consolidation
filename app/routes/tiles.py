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
    conditions = ["geom IS NOT NULL", "ST_Intersects(geom, ST_TileEnvelope($1, $2, $3))"]
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

    # At low zooms, skip Tier 5 (2.8M low-value parcels) and simplify geometry
    geom_expr = "ST_AsMVTGeom(geom, ST_TileEnvelope($1, $2, $3), 4096, 64, true)"
    if z < 10:
        geom_expr = "ST_AsMVTGeom(ST_Simplify(geom, 0.0005), ST_TileEnvelope($1, $2, $3), 4096, 64, true)"
        if not tier:
            conditions.append("suitability_tier != 'Tier 5 - Low'")
    elif z < 12:
        geom_expr = "ST_AsMVTGeom(ST_Simplify(geom, 0.0001), ST_TileEnvelope($1, $2, $3), 4096, 64, true)"

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
