from pydantic import BaseModel
from typing import Dict, Optional

class Group(BaseModel):
    owner_id: str
    work_type: str
    size_bin: str

class Diagnostics(BaseModel):
    empirical_samples: int
    w_empirical: float
    spacing_profile: str
    rounding_unit_est: int
    centrality_weight: float

class EstimateResponse(BaseModel):
    bid_no: str
    ord: int
    base: int
    range_pct: float
    group: Group
    E_quantiles: Dict[str, float]
    diagnostics: Diagnostics