"""Consolidation candidate endpoints."""
from fastapi import APIRouter, Query
from typing import Optional
from ..db import execute_query, get_table
from ..models import Parcel, ConsolidationPair

router = APIRouter(prefix="/api/candidates", tags=["candidates"])

PARCEL_COLUMNS = """
    parcel_id, plan_number, lot_number, zone_code, lga_name,
    centroid_lon, centroid_lat, area_sqm, compactness_index,
    num_adjacent_parcels, nearest_any_pt_m, is_growth_area_lga,
    lots_in_plan, opportunity_score, constraint_score,
    suitability_score, suitability_tier, rank
"""


@router.get("", response_model=list[Parcel])
def top_candidates(
    tier: Optional[str] = Query(None, description="Filter by tier"),
    lga: Optional[str] = Query(None, description="Filter by LGA"),
    exclude_growth_areas: bool = Query(False, description="Exclude growth-area LGAs"),
    limit: int = Query(500, le=5000),
):
    """Get top consolidation candidates."""
    table = get_table("consolidation_candidates")
    conditions = []
    params = {}

    if tier:
        conditions.append("suitability_tier = %(tier)s")
        params["tier"] = tier
    if lga:
        conditions.append("lga_name = %(lga)s")
        params["lga"] = lga
    if exclude_growth_areas:
        conditions.append("is_growth_area_lga = false")

    where = "WHERE " + " AND ".join(conditions) if conditions else ""
    params["limit"] = limit

    query = f"""
        SELECT {PARCEL_COLUMNS}
        FROM {table}
        {where}
        ORDER BY suitability_score DESC
        LIMIT %(limit)s
    """
    return execute_query(query, params)


@router.get("/pairs", response_model=list[ConsolidationPair])
def consolidation_pairs(
    lga: Optional[str] = Query(None, description="Filter by LGA"),
    min_combined_score: int = Query(100, description="Min combined score"),
    limit: int = Query(100, le=1000),
):
    """Get best consolidation pairs (adjacent parcels with high combined scores)."""
    table = get_table("consolidation_pairs")
    conditions = ["combined_score >= %(min_combined_score)s"]
    params = {"min_combined_score": min_combined_score}

    if lga:
        conditions.append("lga_name = %(lga)s")
        params["lga"] = lga

    params["limit"] = limit
    where = "WHERE " + " AND ".join(conditions)

    query = f"""
        SELECT parcel_1, parcel_2, shared_boundary_m, score_1, score_2,
               combined_score, zone_1, zone_2, lga_name,
               lon_1, lat_1, lon_2, lat_2, combined_area_sqm
        FROM {table}
        {where}
        ORDER BY combined_score DESC
        LIMIT %(limit)s
    """
    return execute_query(query, params)
