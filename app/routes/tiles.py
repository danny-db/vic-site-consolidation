"""Vector tile (MVT) endpoint for parcel polygons."""
import time
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

EMPTY_TILE = b""

# In-memory LRU tile cache: key -> (bytes, timestamp)
_tile_cache: dict[str, tuple[bytes, float]] = {}
TILE_CACHE_TTL = 600  # 10 minutes
TILE_CACHE_MAX = 2000  # max cached tiles


def _cache_key(z: int, x: int, y: int, tier, lga, min_score, exclude_growth_areas) -> str:
    tier_str = ",".join(sorted(tier)) if tier else ""
    return f"{z}/{x}/{y}|t={tier_str}|l={lga or ''}|s={min_score}|e={exclude_growth_areas}"


def _cache_get(key: str) -> bytes | None:
    if key in _tile_cache:
        data, ts = _tile_cache[key]
        if time.time() - ts < TILE_CACHE_TTL:
            return data
        del _tile_cache[key]
    return None


def _cache_put(key: str, data: bytes):
    # Evict oldest entries if cache is full
    if len(_tile_cache) >= TILE_CACHE_MAX:
        oldest_keys = sorted(_tile_cache, key=lambda k: _tile_cache[k][1])[:TILE_CACHE_MAX // 4]
        for k in oldest_keys:
            del _tile_cache[k]
    _tile_cache[key] = (data, time.time())


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
    # Check cache first
    key = _cache_key(z, x, y, tier, lga, min_score, exclude_growth_areas)
    cached = _cache_get(key)
    if cached is not None:
        return Response(
            content=cached,
            media_type="application/x-protobuf",
            headers={"Cache-Control": "public, max-age=300", "X-Tile-Cache": "HIT"},
        )

    # Build WHERE filters using pre-computed geom_3857 column —
    # no per-request ST_Transform needed, and GIST index on geom_3857
    # allows direct intersection with ST_TileEnvelope (both in 3857).
    conditions = [
        "geom_3857 IS NOT NULL",
        "geom_3857 && ST_TileEnvelope($1, $2, $3)",
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

    # Use pre-computed geom_3857 directly — no ST_Transform or ST_MakeValid needed.
    # clip_geom=false avoids tile-boundary clipping artifacts on small parcels.
    tile_env = "ST_TileEnvelope($1, $2, $3)"
    geom_expr = f"ST_AsMVTGeom(geom_3857, {tile_env}, 4096, 256, false)"
    if z < 10:
        geom_expr = f"ST_AsMVTGeom(ST_Simplify(geom_3857, 50), {tile_env}, 4096, 256, false)"
        if not tier:
            conditions.append("suitability_tier != 'Tier 5 - Low'")
    elif z < 12:
        geom_expr = f"ST_AsMVTGeom(ST_Simplify(geom_3857, 10), {tile_env}, 4096, 256, false)"

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

    if isinstance(mvt_data, memoryview):
        mvt_data = bytes(mvt_data)

    # Cache the tile
    _cache_put(key, mvt_data)

    return Response(
        content=mvt_data,
        media_type="application/x-protobuf",
        headers={"Cache-Control": "public, max-age=300", "X-Tile-Cache": "MISS"},
    )
