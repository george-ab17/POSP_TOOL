from pydantic import BaseModel
from typing import List, Optional


class CompanyPayout(BaseModel):
    rank: int
    company_name: str
    conditions: Optional[str] = None
    payout_percentage: float


class PayoutResponse(BaseModel):
    status: str
    message: str
    rto_code: str
    top_3_payouts: List[CompanyPayout]
    total_companies: int
