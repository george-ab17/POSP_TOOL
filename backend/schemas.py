from pydantic import BaseModel
from typing import List, Optional, Any


class CompanyPayout(BaseModel):
    company_name: Optional[str] = None
    company: Optional[str] = None
    rank: Optional[int] = None
    payout_percentage: Optional[float] = None
    payout: Optional[float] = None
    final_payout: Optional[float] = None
    conditions: Optional[str] = None
    state: Optional[str] = None
    rto_code: Optional[str] = None
    vehicle_category: Optional[str] = None
    vehicle_type: Optional[str] = None
    policy_type: Optional[str] = None
    business_type: Optional[str] = None
    cpa_cover: Optional[str] = None
    trailer: Optional[str] = None
    make: Optional[str] = None
    model: Optional[str] = None


class PayoutResponse(BaseModel):
    status: str
    message: Optional[str] = None
    rto_code: Optional[str] = None
    top_3_payouts: List[CompanyPayout] = []
    top_5_payouts: List[CompanyPayout] = []
    total_companies: int = 0
