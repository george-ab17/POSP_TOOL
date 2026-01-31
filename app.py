from fastapi import FastAPI, Form
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.responses import JSONResponse
from database import (
    init_connection_pool, get_top_5_payouts, log_query, test_connection,
    get_distinct_states, get_distinct_rtos, get_distinct_vehicle_categories,
    get_distinct_vehicle_types, get_distinct_fuel_types, get_distinct_policy_types,
    get_distinct_business_types, get_distinct_vehicle_ages, get_distinct_cc_slabs,
    get_distinct_gvw_slabs, get_distinct_watt_slabs, get_distinct_seating_capacities,
    get_distinct_ncb_slabs, get_distinct_cpa_covers, get_distinct_zero_depreciation,
    get_distinct_trailers, get_distinct_makes
)
from schemas import PayoutResponse, CompanyPayout
from config import API_HOST, API_PORT
import os

# Initialize FastAPI app
app = FastAPI(
    title="POSP Payout Checker",
    description="Insurance Payout Commission Lookup System"
)

# Initialize database connection pool on startup
@app.on_event("startup")
async def startup_event():
    """Initialize database connection on app startup only when enabled.

    Use environment variable `DB_AUTO_CONNECT` (true|false). Default: true.
    When disabled the app will serve the UI and endpoints but will not
    attempt to connect to MySQL until you explicitly enable/import data.
    """
    auto_connect = os.getenv("DB_AUTO_CONNECT", "true").lower() in ("1", "true", "yes")
    print("[APP] Starting up - DB auto-connect=%s" % auto_connect)
    if auto_connect:
        print("[APP] Initializing database connection...")
        init_connection_pool()
        if not test_connection():
            print("[APP] ⚠️  WARNING: Database connection test failed. Check MySQL is running.")
        else:
            print("[APP] ✅ Database connection ready")
    else:
        print("[APP] ✅ Running in UI-only mode (DB disabled). Connect DB after Excel is ready.")

# Mount static files for serving HTML, CSS, JS, images
app.mount("/static", StaticFiles(directory="."), name="static")

@app.get("/", response_class=HTMLResponse)
async def entry_page():
    with open("entry.html", "r", encoding="utf-8") as f:
        return HTMLResponse(f.read())

@app.get("/form", response_class=HTMLResponse)
async def get_form():
    with open("index.html", "r", encoding="utf-8") as f:
        return HTMLResponse(f.read())

# ==================== DROPDOWN DATA ENDPOINTS ====================
# These endpoints return distinct values from the database for UI dropdown population

@app.get("/api/states")
async def get_states():
    """Get all distinct states"""
    return {"states": get_distinct_states()}

@app.get("/api/rtos/{state}")
async def get_rtos(state: str):
    """Get all RTO codes for a state"""
    return {"rtos": get_distinct_rtos(state)}

@app.get("/api/vehicle-categories")
async def get_vehicle_categories():
    """Get all vehicle categories"""
    return {"categories": get_distinct_vehicle_categories()}

@app.get("/api/vehicle-types")
async def get_vehicle_types(category: str = None):
    """Get all vehicle types (optionally filtered by category)"""
    return {"types": get_distinct_vehicle_types(category)}

@app.get("/api/fuel-types")
async def get_fuel_types(vehicle_type: str = None):
    """Get all fuel types (optionally filtered by vehicle type)"""
    return {"fuels": get_distinct_fuel_types(vehicle_type)}

@app.get("/api/policy-types")
async def get_policy_types():
    """Get all policy types"""
    policies = [p for p in get_distinct_policy_types() if str(p).strip().lower() != 'all']
    return {"policies": policies}

@app.get("/api/business-types")
async def get_business_types():
    """Get all business types (excluding 'All')"""
    business = [b for b in get_distinct_business_types() if str(b).strip().lower() != 'all']
    return {"business_types": business}

@app.get("/api/vehicle-ages")
async def get_vehicle_ages():
    """Get all vehicle ages (excluding 'All')"""
    return {"ages": get_distinct_vehicle_ages()}

@app.get("/api/cc-slabs")
async def get_cc_slabs():
    """Get all CC slabs"""
    return {"cc_slabs": get_distinct_cc_slabs()}

@app.get("/api/gvw-slabs")
async def get_gvw_slabs():
    """Get all GVW slabs"""
    return {"gvw_slabs": get_distinct_gvw_slabs()}

@app.get("/api/watt-slabs")
async def get_watt_slabs():
    """Get all Watt slabs"""
    return {"watt_slabs": get_distinct_watt_slabs()}

@app.get("/api/seating-capacities")
async def get_seating_capacities():
    """Get all seating capacities"""
    return {"capacities": get_distinct_seating_capacities()}

@app.get("/api/ncb-slabs")
async def get_ncb_slabs():
    """Get all NCB slabs"""
    return {"ncb_slabs": get_distinct_ncb_slabs()}

@app.get("/api/cpa-covers")
async def get_cpa_covers():
    """Get all CPA covers"""
    return {"cpa_covers": get_distinct_cpa_covers()}

@app.get("/api/zero-depreciation")
async def get_zero_depreciation():
    """Get all zero depreciation options"""
    return {"options": get_distinct_zero_depreciation()}

@app.get("/api/trailers")
async def get_trailers():
    """Get all trailer options"""
    return {"trailers": get_distinct_trailers()}

@app.get("/api/makes")
async def get_makes(vehicle_type: str = None):
    """Get all makes (optionally filtered by vehicle type)"""
    makes = [m for m in get_distinct_makes(vehicle_type) if str(m).strip().lower() not in ('all', 'all make')]
    return {"makes": makes}

@app.post("/check-payout")
async def check_payout(
    state: str = Form(...),
    rto_number: str = Form(...),
    vehicle_category: str = Form(...),
    vehicle_type: str = Form(...),
    fuel_type: str = Form(...),
    cc_slab: str = Form(None),
    seating_capacity: str = Form(None),
    gvw_slab: str = Form(None),
    watt_slab: str = Form(None),
    vehicle_age: str = Form(...),
    policy_type: str = Form(...),
    business_type: str = Form(...),
    ncb_slab: str = Form(...),
    cpa_cover: str = Form(...),
    zero_dep: str = Form(...),
    trailer: str = Form(...),
    make: str = Form(None),
    model: str = Form(None),
):
    """
    Check payout for given parameters using ALL parameters to match database records
    """
    try:
        # Construct RTO code
        rto_code = f"{state}{rto_number}" if state and rto_number else "N/A"

        print(f"[API] Payout check request - State: {state}, RTO: {rto_code}, Vehicle: {vehicle_type}, Fuel: {fuel_type}")

        auto_connect = os.getenv("DB_AUTO_CONNECT", "true").lower() in ("1", "true", "yes")

        # If DB auto-connect is disabled, do not call database. Serve UI-only response.
        if not auto_connect:
            print("[API] DB disabled - returning UI-only response")
            return PayoutResponse(
                status="ui_only",
                message="Database connection is disabled. Import cleaned Excel data and enable DB to get payout results.",
                rto_code=rto_code,
                top_3_payouts=[],
                total_companies=0
            )

        # Query database using all parameters
        payouts = get_top_5_payouts(
            state=state,
            rto_code=rto_code,
            vehicle_type=vehicle_type,
            fuel_type=fuel_type,
            policy_type=policy_type,
            vehicle_age=vehicle_age,
            business_type=business_type,
            vehicle_category=vehicle_category,
            cc_slab=cc_slab,
            gvw_slab=gvw_slab,
            watt_slab=watt_slab,
            seating_capacity=seating_capacity,
            ncb_slab=ncb_slab,
            cpa_cover=cpa_cover,
            zero_depreciation=zero_dep,
            trailer=trailer,
            make=make,
            model=model
        )

        # Log this query for analytics
        log_query(state, rto_code, vehicle_type, fuel_type, policy_type, len(payouts))

        # Prepare response
        if payouts:
            response = PayoutResponse(
                status="success",
                message=f"Found {len(payouts)} payout(s) - Top {len(payouts)} insurers by commission",
                rto_code=rto_code,
                top_3_payouts=[CompanyPayout(**p) for p in payouts],
                total_companies=len(payouts)
            )
            print(f"[API] ✅ Found {len(payouts)} payouts")
        else:
            response = PayoutResponse(
                status="no_data",
                message="No matching payout data found for this combination. Database may be empty.",
                rto_code=rto_code,
                top_3_payouts=[],
                total_companies=0
            )
            print(f"[API] ⚠️  No payouts found for this combination")

        return response

    except Exception as e:
        print(f"[API ERROR] {str(e)}")
        return PayoutResponse(
            status="error",
            message=f"Error processing request: {str(e)}",
            rto_code="",
            top_3_payouts=[],
            total_companies=0
        )
