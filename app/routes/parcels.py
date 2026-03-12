"""Parcel API endpoints."""
import json
import hashlib
import time
from fastapi import APIRouter, Query, HTTPException, Request
from fastapi.responses import JSONResponse, Response
from typing import Optional, List
from db import fetch_all, fetch_one
from models import Parcel, ParcelDetail

router = APIRouter(prefix="/api/parcels", tags=["parcels"])

PARCEL_COLUMNS = """
    parcel_id, plan_number, lot_number, zone_code, lga_name,
    centroid_lon, centroid_lat, area_sqm, compactness_index,
    num_adjacent_parcels, nearest_any_pt_m, is_growth_area_lga,
    lots_in_plan, opportunity_score, constraint_score,
    suitability_score, suitability_tier, rank
"""

# In-memory GeoJSON cache: key -> (etag, json_bytes, timestamp)
_geojson_cache: dict[str, tuple[str, bytes, float]] = {}
CACHE_TTL = 600  # 10 minutes


def _build_filters(zone, tiers, lga, min_score, exclude_growth_areas=False):
    """Build WHERE conditions and params from filter arguments.

    `tiers` can be a list of tier strings (multi-select).
    """
    conditions = []
    params = []
    idx = 1
    if zone:
        conditions.append(f"zone_code = ${idx}")
        params.append(zone)
        idx += 1
    if tiers:
        if len(tiers) == 1:
            conditions.append(f"suitability_tier = ${idx}")
            params.append(tiers[0])
            idx += 1
        else:
            placeholders = ", ".join(f"${idx + i}" for i in range(len(tiers)))
            conditions.append(f"suitability_tier IN ({placeholders})")
            params.extend(tiers)
            idx += len(tiers)
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
    return conditions, params, idx


@router.get("", response_model=list[Parcel])
async def list_parcels(
    zone: Optional[str] = Query(None, description="Filter by zone_code"),
    tier: Optional[List[str]] = Query(None, description="Filter by suitability_tier (repeatable)"),
    lga: Optional[str] = Query(None, description="Filter by lga_name"),
    min_score: Optional[int] = Query(None, description="Minimum suitability score"),
    limit: int = Query(1000, le=5000),
    offset: int = Query(0, ge=0),
):
    """List parcels with optional filters."""
    conditions, params, idx = _build_filters(zone, tier, lga, min_score)
    where = "WHERE " + " AND ".join(conditions) if conditions else ""
    params.extend([limit, offset])

    query = f"""
        SELECT {PARCEL_COLUMNS}
        FROM consolidation_candidates_sync
        {where}
        ORDER BY suitability_score DESC
        LIMIT ${idx} OFFSET ${idx + 1}
    """
    return await fetch_all(query, *params)


def _cache_key(tiers, lga, min_score, exclude_growth_areas, limit) -> str:
    """Build a deterministic cache key from query params."""
    parts = [
        "tiers=" + (",".join(sorted(tiers)) if tiers else ""),
        f"lga={lga or ''}",
        f"min_score={min_score if min_score is not None else ''}",
        f"exclude={exclude_growth_areas}",
        f"limit={limit}",
    ]
    return "|".join(parts)


@router.get("/geojson")
async def parcels_geojson(
    request: Request,
    zone: Optional[str] = Query(None),
    tier: Optional[List[str]] = Query(None, description="Tier filter (repeatable, e.g. ?tier=Tier+1&tier=Tier+2)"),
    lga: Optional[str] = Query(None),
    min_score: Optional[int] = Query(None),
    exclude_growth_areas: bool = Query(False),
    limit: int = Query(200000, le=200000),
):
    """Return parcels as GeoJSON FeatureCollection with polygon geometry.

    Responses are cached server-side and support ETag/If-None-Match for
    browser caching — subsequent loads of the same query are instant.
    """
    key = _cache_key(tier, lga, min_score, exclude_growth_areas, limit)

    # Check server-side cache
    if key in _geojson_cache:
        etag, body_bytes, ts = _geojson_cache[key]
        if time.time() - ts < CACHE_TTL:
            # Check If-None-Match from browser
            if request.headers.get("if-none-match") == etag:
                return Response(status_code=304)
            return Response(
                content=body_bytes,
                media_type="application/json",
                headers={"ETag": etag, "Cache-Control": "private, max-age=300"},
            )
        else:
            del _geojson_cache[key]

    # Cache miss — query DB
    conditions, params, idx = _build_filters(zone, tier, lga, min_score, exclude_growth_areas)
    where = "WHERE geometry_wkt IS NOT NULL"
    if conditions:
        where += " AND " + " AND ".join(conditions)
    params.append(limit)

    query = f"""
        SELECT parcel_id, plan_number, lot_number, zone_code, lga_name,
               centroid_lon, centroid_lat, area_sqm, compactness_index,
               num_adjacent_parcels, nearest_any_pt_m, is_growth_area_lga,
               lots_in_plan, opportunity_score, constraint_score,
               suitability_score, suitability_tier, rank,
               ST_AsGeoJSON(ST_GeomFromText(geometry_wkt, 4326))::json AS geojson
        FROM consolidation_candidates_sync
        {where}
        ORDER BY suitability_score DESC
        LIMIT ${idx}
    """
    rows = await fetch_all(query, *params)

    features = []
    for row in rows:
        geojson = row.pop("geojson", None)
        if geojson is None:
            continue
        if isinstance(geojson, str):
            geojson = json.loads(geojson)
        features.append({
            "type": "Feature",
            "geometry": geojson,
            "properties": row,
        })

    body = json.dumps({"type": "FeatureCollection", "features": features})
    body_bytes = body.encode()
    etag = '"' + hashlib.md5(body_bytes[:4096]).hexdigest()[:16] + '"'

    # Store in server-side cache
    _geojson_cache[key] = (etag, body_bytes, time.time())

    return Response(
        content=body_bytes,
        media_type="application/json",
        headers={"ETag": etag, "Cache-Control": "private, max-age=300"},
    )


@router.get("/nearby", response_model=list[Parcel])
async def nearby_parcels(
    lat: float = Query(..., description="Latitude"),
    lng: float = Query(..., description="Longitude"),
    radius_km: float = Query(1.0, le=10, description="Search radius in km"),
    limit: int = Query(200, le=2000),
):
    """Find parcels near a point using PostGIS spatial query."""
    query = f"""
        SELECT {PARCEL_COLUMNS}
        FROM candidates_geo
        WHERE ST_DWithin(
            location,
            ST_SetSRID(ST_MakePoint($1, $2), 4326)::geography,
            $3
        )
        ORDER BY ST_Distance(
            location,
            ST_SetSRID(ST_MakePoint($1, $2), 4326)::geography
        )
        LIMIT $4
    """
    return await fetch_all(query, lng, lat, radius_km * 1000, limit)


@router.get("/{parcel_id}", response_model=ParcelDetail)
async def get_parcel(parcel_id: str):
    """Get detailed info for a single parcel."""
    query = """
        SELECT parcel_id, plan_number, lot_number, zone_code, zone_description,
               lga_name, centroid_lon, centroid_lat,
               area_sqm, perimeter_m, compactness_index, aspect_ratio,
               elongation_index, num_adjacent_parcels,
               longest_shared_boundary_m, adjacent_same_zone_count,
               nearest_any_pt_m, is_growth_area_lga, lots_in_plan,
               score_growth_area_lga, score_recent_subdivision, score_mass_subdivision,
               opportunity_score, constraint_score,
               suitability_score, suitability_tier, rank,
               geometry_wkt
        FROM consolidation_candidates_sync
        WHERE parcel_id = $1
        LIMIT 1
    """
    row = await fetch_one(query, parcel_id)
    if not row:
        raise HTTPException(status_code=404, detail="Parcel not found")
    return row
