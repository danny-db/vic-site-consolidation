"""Parcel API endpoints."""
from fastapi import APIRouter, Query, HTTPException
from typing import Optional
from ..db import fetch_all, fetch_one
from ..models import Parcel, ParcelDetail

router = APIRouter(prefix="/api/parcels", tags=["parcels"])

PARCEL_COLUMNS = """
    parcel_id, plan_number, lot_number, zone_code, lga_name,
    centroid_lon, centroid_lat, area_sqm, compactness_index,
    num_adjacent_parcels, nearest_any_pt_m, is_growth_area_lga,
    lots_in_plan, opportunity_score, constraint_score,
    suitability_score, suitability_tier, rank
"""


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
        SELECT {PARCEL_COLUMNS}
        FROM consolidation_candidates_sync
        {where}
        ORDER BY suitability_score DESC
        LIMIT ${idx} OFFSET ${idx + 1}
    """
    return await fetch_all(query, *params)


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
