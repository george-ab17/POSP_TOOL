"""
Pydantic schemas for request/response validation
Defines data models for the POSP Payout Checker
"""

from pydantic import BaseModel, Field
from typing import List, Optional
from decimal import Decimal
from datetime import date

# ==================== REQUEST MODELS ====================

class PayoutRequest(BaseModel):
    """Model for incoming payout check request"""
    state: str = Field(..., description="State code (TN, KL, KA, etc)")
    rto_number: str = Field(..., description="RTO code")
    vehicle_category: str = Field(..., description="Two-Wheeler, Private Car, GCV, PCV, Misc")
    vehicle_type: str = Field(..., description="Bike, Scooter, Taxi, Auto, etc")
    fuel_type: str = Field(..., description="Petrol, Diesel, CNG, Electric")
    cc_slab: Optional[str] = Field(None, description="CC slab for two-wheelers")
    seating_capacity: Optional[str] = Field(None, description="For PCV vehicles")
    gvw_slab: Optional[str] = Field(None, description="For GCV vehicles")
    watt_slab: Optional[str] = Field(None, description="For electric vehicles")
    vehicle_age: str = Field(..., description="Vehicle age category")
    policy_type: str = Field(..., description="Package, SATP, SAOD, Bundle")
    business_type: str = Field(..., description="New, Rollover")
    claim_status: str = Field(..., description="Claim, No Claim")
    ncb_slab: str = Field(..., description="NCB percentage")
    cpa_cover: str = Field(..., description="CPA cover yes/no")
    zero_dep: str = Field(..., description="Zero depreciation yes/no")
    trailer: str = Field(..., description="Trailer attached yes/no")
    # Backend-only fields (not in UI form, populated during import)
    make_model: Optional[str] = Field(None, description="Make/model restrictions (comma-separated or * for any)")
    effective_from: Optional[date] = Field(None, description="Rule effective from date")
    effective_to: Optional[date] = Field(None, description="Rule effective to date")
    
    class Config:
        json_schema_extra = {
            "example": {
                "state": "TN",
                "rto_number": "01",
                "vehicle_category": "two-wheeler",
                "vehicle_type": "bike",
                "fuel_type": "petrol",
                "cc_slab": "upto-150",
                "seating_capacity": "",
                "gvw_slab": "",
                "watt_slab": "",
                "vehicle_age": "upto-5-years",
                "policy_type": "package",
                "business_type": "new",
                "claim_status": "no-claim",
                "ncb_slab": "0",
                "cpa_cover": "no",
                "zero_dep": "no",
                "trailer": "no"
            }
        }

# ==================== RESPONSE MODELS ====================

class CompanyPayout(BaseModel):
    """Individual company payout information"""
    rank: int = Field(..., description="1, 2, or 3")
    company_name: str = Field(..., description="Insurance company name")
    payout_percentage: Decimal = Field(..., description="Commission percentage (e.g., 0.55)")
    payout_amount: str = Field(..., description="Formatted percentage (e.g., '55%')")
    conditions: Optional[str] = Field(None, description="Special conditions (e.g., 'Commission on OD')")
    
    class Config:
        json_schema_extra = {
            "example": {
                "rank": 1,
                "company_name": "Shriram",
                "payout_percentage": "0.55",
                "payout_amount": "55%",
                "conditions": "Commission on OD"
            }
        }

class PayoutResponse(BaseModel):
    """API response for payout check"""
    status: str = Field(..., description="success or error")
    message: str = Field(..., description="Response message")
    rto_code: str = Field(..., description="Combined RTO code")
    top_3_payouts: List[CompanyPayout] = Field(default_factory=list, description="Top 3 payouts")
    total_companies: int = Field(default=0, description="Total companies found")
    
    class Config:
        json_schema_extra = {
            "example": {
                "status": "success",
                "message": "Top 3 payouts found",
                "rto_code": "TN01",
                "top_3_payouts": [
                    {
                        "rank": 1,
                        "company_name": "Shriram",
                        "payout_percentage": 0.55,
                        "payout_amount": "55%"
                    },
                    {
                        "rank": 2,
                        "company_name": "Royal",
                        "payout_percentage": 0.48,
                        "payout_amount": "48%"
                    },
                    {
                        "rank": 3,
                        "company_name": "ICICI",
                        "payout_percentage": 0.45,
                        "payout_amount": "45%"
                    }
                ],
                "total_companies": 12
            }
        }

class ErrorResponse(BaseModel):
    """Error response model"""
    status: str = Field(default="error")
    message: str = Field(..., description="Error message")
    error_code: Optional[str] = Field(None, description="Error code for debugging")

print("[SCHEMAS] Pydantic schemas loaded successfully")
