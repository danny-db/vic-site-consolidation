"""Pydantic models for API responses."""
from pydantic import BaseModel
from typing import Optional


class Parcel(BaseModel):
    parcel_id: str
    plan_number: Optional[str] = None
    lot_number: Optional[str] = None
    zone_code: Optional[str] = None
    lga_name: Optional[str] = None
    centroid_lon: float
    centroid_lat: float
    area_sqm: Optional[float] = None
    compactness_index: Optional[float] = None
    num_adjacent_parcels: Optional[int] = None
    nearest_any_pt_m: Optional[float] = None
    is_growth_area_lga: Optional[bool] = None
    lots_in_plan: Optional[int] = None
    opportunity_score: Optional[int] = None
    constraint_score: Optional[int] = None
    suitability_score: Optional[int] = None
    suitability_tier: Optional[str] = None
    rank: Optional[int] = None


class ParcelDetail(Parcel):
    zone_description: Optional[str] = None
    perimeter_m: Optional[float] = None
    aspect_ratio: Optional[float] = None
    elongation_index: Optional[float] = None
    longest_shared_boundary_m: Optional[float] = None
    adjacent_same_zone_count: Optional[int] = None
    score_growth_area_lga: Optional[int] = None
    score_recent_subdivision: Optional[int] = None
    score_mass_subdivision: Optional[int] = None
    geometry_wkt: Optional[str] = None


class ConsolidationPair(BaseModel):
    parcel_1: str
    parcel_2: str
    shared_boundary_m: float
    score_1: int
    score_2: int
    combined_score: int
    zone_1: Optional[str] = None
    zone_2: Optional[str] = None
    lga_name: Optional[str] = None
    lon_1: float
    lat_1: float
    lon_2: float
    lat_2: float
    combined_area_sqm: Optional[float] = None


class TierStats(BaseModel):
    suitability_tier: str
    parcel_count: int
    avg_score: float
    avg_area_sqm: float
    total_area_sqm: float


class LGAStats(BaseModel):
    lga_name: str
    parcel_count: int
    avg_score: float
    high_priority_count: int
    growth_area: bool


class HealthResponse(BaseModel):
    status: str
    total_candidates: int
    tier_count: int
