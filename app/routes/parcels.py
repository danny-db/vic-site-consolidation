"""Parcel API endpoints."""
import math
from fastapi import APIRouter, Query, HTTPException
from typing import Optional
from ..db import execute_query, execute_query_one, get_table
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
def list_parcels(
    zone: Optional[str] = Query(None, description="Filter by zone_code"),
    tier: Optional[str] = Query(None, description="Filter by suitability_tier"),
    lga: Optional[str] = Query(None, description="Filter by lga_name"),
    min_score: Optional[int] = Query(None, description="Minimum suitability score"),
    limit: int = Query(1000, le=5000),
    offset: int = Query(0, ge=0),
):
    """List parcels with optional filters."""
    table = get_table("consolidation_candidates")
    conditions = []
    params = {}

    if zone:
        conditions.append("zone_code = %(zone)s")
        params["zone"] = zone
    if tier:
        conditions.append("suitability_tier = %(tier)s")
        params["tier"] = tier
    if lga:
        conditions.append("lga_name = %(lga)s")
        params["lga"] = lga
    if min_score is not None:
        conditions.append("suitability_score >= %(min_score)s")
        params["min_score"] = min_score

    where = "WHERE " + " AND ".join(conditions) if conditions else ""
    params["limit"] = limit
    params["offset"] = offset

    query = f"""
        SELECT {PARCEL_COLUMNS}
        FROM {table}
        {where}
        ORDER BY suitability_score DESC
        LIMIT %(limit)s OFFSET %(offset)s
    """
    return execute_query(query, params)


@router.get("/nearby", response_model=list[Parcel])
def nearby_parcels(
    lat: float = Query(..., description="Latitude"),
    lng: float = Query(..., description="Longitude"),
    radius_km: float = Query(1.0, le=10, description="Search radius in km"),
    limit: int = Query(200, le=2000),
):
    """Find parcels near a point using Haversine distance."""
    table = get_table("consolidation_candidates")

    # Bounding box pre-filter for performance
    lat_delta = radius_km / 111.0
    lng_delta = radius_km / (111.0 * math.cos(math.radians(lat)))

    params = {
        "lat": lat, "lng": lng,
        "lat_min": lat - lat_delta, "lat_max": lat + lat_delta,
        "lng_min": lng - lng_delta, "lng_max": lng + lng_delta,
        "radius_km": radius_km, "limit": limit,
    }

    query = f"""
        SELECT {PARCEL_COLUMNS} FROM (
            SELECT {PARCEL_COLUMNS},
                   6371.0 * ACOS(
                       LEAST(1.0,
                           SIN(RADIANS(centroid_lat)) * SIN(RADIANS(%(lat)s)) +
                           COS(RADIANS(centroid_lat)) * COS(RADIANS(%(lat)s)) *
                           COS(RADIANS(centroid_lon - %(lng)s))
                       )
                   ) AS distance_km
            FROM {table}
            WHERE centroid_lat BETWEEN %(lat_min)s AND %(lat_max)s
              AND centroid_lon BETWEEN %(lng_min)s AND %(lng_max)s
              AND centroid_lat IS NOT NULL
              AND centroid_lon IS NOT NULL
        ) t
        WHERE distance_km <= %(radius_km)s
        ORDER BY distance_km
        LIMIT %(limit)s
    """
    return execute_query(query, params)


@router.get("/{parcel_id}", response_model=ParcelDetail)
def get_parcel(parcel_id: str):
    """Get detailed info for a single parcel."""
    table = get_table("consolidation_candidates")
    query = f"""
        SELECT parcel_id, plan_number, lot_number, zone_code, zone_description,
               lga_name, centroid_lon, centroid_lat,
               ROUND(area_sqm, 2) AS area_sqm,
               ROUND(perimeter_m, 2) AS perimeter_m,
               ROUND(compactness_index, 4) AS compactness_index,
               ROUND(aspect_ratio, 4) AS aspect_ratio,
               ROUND(elongation_index, 4) AS elongation_index,
               num_adjacent_parcels,
               ROUND(longest_shared_boundary_m, 2) AS longest_shared_boundary_m,
               adjacent_same_zone_count,
               nearest_any_pt_m, is_growth_area_lga, lots_in_plan,
               score_growth_area_lga, score_recent_subdivision, score_mass_subdivision,
               opportunity_score, constraint_score,
               suitability_score, suitability_tier, rank,
               ST_AsText(ST_Transform(geometry, 'EPSG:7899', 'EPSG:4326')) AS geometry_wkt
        FROM {table}
        WHERE parcel_id = %(parcel_id)s
        LIMIT 1
    """
    row = execute_query_one(query, {"parcel_id": parcel_id})
    if not row:
        raise HTTPException(status_code=404, detail="Parcel not found")
    return row
