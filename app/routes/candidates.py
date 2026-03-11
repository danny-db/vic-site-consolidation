"""Consolidation candidate endpoints."""
from fastapi import APIRouter, Query
from typing import Optional
from ..db import get_pool
from ..models import Parcel, ConsolidationPair

router = APIRouter(prefix="/api/candidates", tags=["candidates"])


@router.get("", response_model=list[Parcel])
async def top_candidates(
    tier: Optional[str] = Query(None, description="Filter by tier"),
    lga: Optional[str] = Query(None, description="Filter by LGA"),
    exclude_growth_areas: bool = Query(False, description="Exclude growth-area LGAs"),
    limit: int = Query(500, le=5000),
):
    """Get top consolidation candidates."""
    pool = await get_pool()
    conditions = []
    params = []
    idx = 1

    if tier:
        conditions.append(f"suitability_tier = ${idx}")
        params.append(tier)
        idx += 1
    if lga:
        conditions.append(f"lga_name = ${idx}")
        params.append(lga)
        idx += 1
    if exclude_growth_areas:
        conditions.append("is_growth_area_lga = false")

    where = "WHERE " + " AND ".join(conditions) if conditions else ""
    params.append(limit)

    query = f"""
        SELECT parcel_id, plan_number, lot_number, zone_code, lga_name,
               centroid_lon, centroid_lat, area_sqm, compactness_index,
               num_adjacent_parcels, nearest_any_pt_m, is_growth_area_lga,
               lots_in_plan, opportunity_score, constraint_score,
               suitability_score, suitability_tier, rank
        FROM candidates_points
        {where}
        ORDER BY suitability_score DESC
        LIMIT ${idx}
    """

    rows = await pool.fetch(query, *params)
    return [dict(r) for r in rows]


@router.get("/pairs", response_model=list[ConsolidationPair])
async def consolidation_pairs(
    lga: Optional[str] = Query(None, description="Filter by LGA"),
    min_combined_score: int = Query(100, description="Min combined score"),
    limit: int = Query(100, le=1000),
):
    """Get best consolidation pairs (adjacent parcels with high combined scores)."""
    pool = await get_pool()

    # Get the catalog/schema from environment or use defaults
    import os
    catalog = os.environ.get("UC_CATALOG", "danny_catalog")
    schema = os.environ.get("UC_SCHEMA", "dtp_hackathon")

    conditions = [f"combined_score >= $1"]
    params = [min_combined_score]
    idx = 2

    if lga:
        conditions.append(f"lga_name = ${idx}")
        params.append(lga)
        idx += 1

    params.append(limit)
    where = "WHERE " + " AND ".join(conditions)

    query = f"""
        SELECT parcel_1, parcel_2, shared_boundary_m, score_1, score_2,
               combined_score, zone_1, zone_2, lga_name,
               lon_1, lat_1, lon_2, lat_2, combined_area_sqm
        FROM "{catalog}"."{schema}"."adjacency_pairs_for_sync"
        {where}
        ORDER BY combined_score DESC
        LIMIT ${idx}
    """

    rows = await pool.fetch(query, *params)
    return [dict(r) for r in rows]
