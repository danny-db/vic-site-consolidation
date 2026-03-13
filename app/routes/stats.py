"""Statistics endpoints."""
from fastapi import APIRouter
from db import fetch_all
from models import TierStats, LGAStats

router = APIRouter(prefix="/api/stats", tags=["stats"])


@router.get("/tiers", response_model=list[TierStats])
async def tier_stats():
    """Get summary statistics by suitability tier."""
    return await fetch_all("""
        SELECT
            suitability_tier,
            COUNT(*) AS parcel_count,
            ROUND(AVG(suitability_score)::numeric, 2) AS avg_score,
            ROUND(AVG(area_sqm)::numeric, 2) AS avg_area_sqm,
            ROUND(SUM(area_sqm)::numeric, 2) AS total_area_sqm
        FROM consolidation_candidates_sync
        GROUP BY suitability_tier
        ORDER BY
            CASE suitability_tier
                WHEN 'Tier 1 - Excellent' THEN 1
                WHEN 'Tier 2 - Very Good' THEN 2
                WHEN 'Tier 3 - Good' THEN 3
                WHEN 'Tier 4 - Moderate' THEN 4
                ELSE 5
            END
    """)


@router.get("/lgas", response_model=list[LGAStats])
async def lga_stats():
    """Get summary statistics by LGA."""
    return await fetch_all("""
        SELECT
            lga_name,
            COUNT(*) AS parcel_count,
            ROUND(AVG(suitability_score)::numeric, 2) AS avg_score,
            SUM(CASE WHEN suitability_score >= 80 THEN 1 ELSE 0 END) AS high_priority_count,
            lga_name IN ('CASEY','CARDINIA','WYNDHAM','MELTON','HUME','WHITTLESEA','MITCHELL') AS growth_area
        FROM consolidation_candidates_sync
        WHERE lga_name IS NOT NULL
        GROUP BY lga_name
        ORDER BY avg_score DESC
    """)
