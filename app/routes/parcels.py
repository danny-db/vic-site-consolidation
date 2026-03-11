"""Parcel API endpoints."""
from fastapi import APIRouter, Query
from typing import Optional
from ..db import get_pool
from ..models import Parcel, ParcelDetail

router = APIRouter(prefix="/api/parcels", tags=["parcels"])


@router.get("", response_model=list[Parcel])
async def list_parcels(
    zone: Optional[str] = Query(None, description="Filter by zone_code"),
    tier: Optional[str] = Query(None, description="Filter by suitability_tier"),
    lga: Optional[str] = Query(None, description="Filter by lga_name"),
    min_score: Optional[int] = Query(None, description="Minimum suitability score"),
    limit: int = Query(1000, le=5000),
    offset: int = Query(0, ge=0),
):
    """List parcels with optional filters."""
    pool = await get_pool()
    conditions = []
    params = []
    idx = 1

    if zone:
        conditions.append(f"zone_code = ${idx}")
        params.append(zone)
        idx += 1
    if tier:
        conditions.append(f"suitability_tier = ${idx}")
        params.append(tier)
        idx += 1
    if lga:
        conditions.append(f"lga_name = ${idx}")
        params.append(lga)
        idx += 1
    if min_score is not None:
        conditions.append(f"suitability_score >= ${idx}")
        params.append(min_score)
        idx += 1

    where = "WHERE " + " AND ".join(conditions) if conditions else ""
    params.extend([limit, offset])

    query = f"""
        SELECT parcel_id, plan_number, lot_number, zone_code, lga_name,
               centroid_lon, centroid_lat, area_sqm, compactness_index,
               num_adjacent_parcels, nearest_any_pt_m, is_growth_area_lga,
               lots_in_plan, opportunity_score, constraint_score,
               suitability_score, suitability_tier, rank
        FROM candidates_points
        {where}
        ORDER BY suitability_score DESC
        LIMIT ${idx} OFFSET ${idx + 1}
    """

    rows = await pool.fetch(query, *params)
    return [dict(r) for r in rows]


@router.get("/nearby", response_model=list[Parcel])
async def nearby_parcels(
    lat: float = Query(..., description="Latitude"),
    lng: float = Query(..., description="Longitude"),
    radius_km: float = Query(1.0, le=10, description="Search radius in km"),
    limit: int = Query(200, le=2000),
):
    """Find parcels near a point using PostGIS spatial query."""
    pool = await get_pool()
    query = """
        SELECT parcel_id, plan_number, lot_number, zone_code, lga_name,
               centroid_lon, centroid_lat, area_sqm, compactness_index,
               num_adjacent_parcels, nearest_any_pt_m, is_growth_area_lga,
               lots_in_plan, opportunity_score, constraint_score,
               suitability_score, suitability_tier, rank
        FROM candidates_points
        WHERE ST_DWithin(
            centroid_geog,
            ST_SetSRID(ST_MakePoint($1, $2), 4326)::geography,
            $3
        )
        ORDER BY suitability_score DESC
        LIMIT $4
    """
    rows = await pool.fetch(query, lng, lat, radius_km * 1000, limit)
    return [dict(r) for r in rows]


@router.get("/{parcel_id}", response_model=ParcelDetail)
async def get_parcel(parcel_id: str):
    """Get detailed info for a single parcel."""
    pool = await get_pool()
    query = """
        SELECT *
        FROM candidates_geo
        WHERE parcel_id = $1
        LIMIT 1
    """
    row = await pool.fetchrow(query, parcel_id)
    if not row:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail="Parcel not found")
    return dict(row)
