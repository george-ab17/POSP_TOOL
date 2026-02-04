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
    get_distinct_trailers, get_distinct_makes, get_distinct_models
)
from schemas import PayoutResponse, CompanyPayout
from config import API_HOST, API_PORT, STATE_CODE_MAP, STATE_DISPLAY_NAMES, VEHICLE_CATEGORY_MAP
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
            print("[APP] WARNING: Database connection test failed. Check MySQL is running.")
        else:
            print("[APP] Database connection ready")
    else:
        print("[APP] Running in UI-only mode (DB disabled). Connect DB after Excel is ready.")

# Mount static files for serving HTML, CSS, JS, images
app.mount("/static", StaticFiles(directory="."), name="static")

@app.get("/", response_class=HTMLResponse)
async def entry_page():
    """Serve the entry page (landing page)"""
    with open("entry.html", "r", encoding="utf-8") as f:
        return HTMLResponse(f.read())

@app.get("/form", response_class=HTMLResponse)
async def get_form():
    """Serve the main payout checker form"""
    with open("index.html", "r", encoding="utf-8") as f:
        return HTMLResponse(f.read())

@app.get("/diagnostic", response_class=HTMLResponse)
async def get_diagnostic():
    """Serve the API diagnostic tool"""
    with open("diagnostic.html", "r", encoding="utf-8") as f:
        return HTMLResponse(f.read())

# ==================== DROPDOWN DATA ENDPOINTS ====================
# These endpoints return distinct values from the database for UI dropdown population

@app.get("/api/states")
async def get_states():
    """Get all distinct states (with display names)"""
    return {"states": get_distinct_states()}

@app.get("/api/state-code/{display_name}")
async def get_state_code(display_name: str):
    """Convert display name to state code"""
    state_code = STATE_CODE_MAP.get(display_name, display_name)
    return {"code": state_code}

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
async def get_policy_types(vehicle_type: str = None, fuel_type: str = None):
    """Get all policy types (optionally filtered by vehicle_type and fuel_type)"""
    policies = [p for p in get_distinct_policy_types(vehicle_type, fuel_type) if str(p).strip().lower() != 'all']
    return {"policies": policies}

@app.get("/api/business-types")
async def get_business_types(vehicle_type: str = None, fuel_type: str = None):
    """Get all business types (optionally filtered by vehicle_type and fuel_type)"""
    return {"business_types": get_distinct_business_types(vehicle_type, fuel_type)}

@app.get("/api/vehicle-ages")
async def get_vehicle_ages():
    """Get all vehicle ages"""
    # Return numeric age choices 1..50 for the UI to select a specific age
    ages = [str(i) for i in range(1, 51)]
    return {"ages": ages}

@app.get("/api/cc-slabs")
async def get_cc_slabs(vehicle_type: str = None, fuel_type: str = None):
    """Get all CC slabs (optionally filtered by vehicle_type and fuel_type)"""
    return {"cc_slabs": get_distinct_cc_slabs(vehicle_type, fuel_type)}

@app.get("/api/gvw-slabs")
async def get_gvw_slabs(vehicle_type: str = None):
    """Get all GVW slabs (optionally filtered by vehicle_type)"""
    return {"gvw_slabs": get_distinct_gvw_slabs(vehicle_type)}

@app.get("/api/watt-slabs")
async def get_watt_slabs(vehicle_type: str = None, fuel_type: str = None):
    """Get all Watt slabs (optionally filtered by vehicle_type and fuel_type)"""
    return {"watt_slabs": get_distinct_watt_slabs(vehicle_type, fuel_type)}

@app.get("/api/seating-capacities")
async def get_seating_capacities(vehicle_type: str = None, fuel_type: str = None):
    """Get all seating capacities (optionally filtered by vehicle_type and fuel_type)"""
    return {"capacities": get_distinct_seating_capacities(vehicle_type, fuel_type)}

@app.get("/api/ncb-slabs")
async def get_ncb_slabs(vehicle_type: str = None, fuel_type: str = None):
    """Get all NCB slabs (optionally filtered by vehicle_type and fuel_type)"""
    return {"ncb_slabs": get_distinct_ncb_slabs(vehicle_type, fuel_type)}

@app.get("/api/cpa-covers")
async def get_cpa_covers(vehicle_type: str = None, fuel_type: str = None):
    """Get all CPA covers (optionally filtered by vehicle_type and fuel_type)"""
    return {"cpa_covers": get_distinct_cpa_covers(vehicle_type, fuel_type)}

@app.get("/api/zero-depreciation")
async def get_zero_depreciation(vehicle_type: str = None, fuel_type: str = None):
    """Get all zero depreciation options (optionally filtered by vehicle_type and fuel_type)"""
    return {"options": get_distinct_zero_depreciation(vehicle_type, fuel_type)}

@app.get("/api/trailers")
async def get_trailers(vehicle_type: str = None):
    """Get all trailer options (optionally filtered by vehicle_type)"""
    return {"trailers": get_distinct_trailers(vehicle_type)}

@app.get("/api/makes")
async def get_makes(vehicle_type: str = None):
    """Get all makes (optionally filtered by vehicle type)"""
    makes = get_distinct_makes(vehicle_type)
    return {"makes": makes}

@app.get("/api/models")
async def get_models(make: str = None, vehicle_type: str = None):
    """Get models for a specific make (optionally filtered by vehicle_type)"""
    models = get_distinct_models(make, vehicle_type)
    return {"models": models}

@app.post("/check-payout")
async def check_payout(
    state: str = Form(...),
    rto_number: str = Form(...),
    vehicle_category: str = Form(...),
    vehicle_type: str = Form(None),
    fuel_type: str = Form(None),  # Optional: hidden for PCV buses
    cc_slab: str = Form(None),
    seating_capacity: str = Form(None),
    gvw_slab: str = Form(None),
    gvw_value: str = Form(None),
    watt_slab: str = Form(None),
    vehicle_age: str = Form(...),
    policy_type: str = Form(...),
    business_type: str = Form(...),
    ncb_slab: str = Form(...),
    cpa_cover: str = Form(...),
    zero_dep: str = Form(...),
    trailer: str = Form(None),
    make: str = Form(None),
    model: str = Form(None),
):
    """
    Check payout for given parameters using ALL parameters to match database records
    """
    try:
        # DEBUG: Log all received parameters
        print(f"[API] Received form data:")
        print(f"  state={state}, rto_number={rto_number}, vehicle_category={vehicle_category}")
        print(f"  vehicle_type={vehicle_type}, fuel_type={fuel_type}")
        print(f"  policy_type={policy_type}, vehicle_age={vehicle_age}")
        print(f"  business_type={business_type}, ncb_slab={ncb_slab}")
        print(f"  cpa_cover={cpa_cover}, zero_dep={zero_dep}")
        print(f"  cc_slab={cc_slab}, watt_slab={watt_slab}")
        print(f"  gvw_slab={gvw_slab}, gvw_value={gvw_value}")
        print(f"  seating_capacity={seating_capacity}, trailer={trailer}")
        print(f"  make={make}, model={model}")
        
        # Convert display name to state code if needed
        state_code = STATE_CODE_MAP.get(state, state)
        
        # RTO code for display (combined format)
        rto_code_display = f"{state_code}{rto_number}" if state_code and rto_number else "N/A"
        # RTO code for query (just the number as stored in DB)
        # Strip state prefix if present (e.g., 'PY-02' → '02', '01' stays '01')
        rto_code = rto_number
        if rto_code and '-' in rto_code:
            rto_code = rto_code.split('-', 1)[1]  # Extract everything after the first hyphen
        if not rto_code or rto_code.strip() == '':
            rto_code = "N/A"

        print(f"[API] Payout check request - State: {state} ({state_code}), RTO: {rto_code_display}, Vehicle: {vehicle_type}, Fuel: {fuel_type}")

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

        # Map vehicle category display name to code if needed (DB may store code)
        vehicle_category_code = VEHICLE_CATEGORY_MAP.get(vehicle_category, vehicle_category)

        # Query database using all parameters
        payouts = get_top_5_payouts(
            state=state_code,
            rto_code=rto_code,
            vehicle_type=vehicle_type,
            fuel_type=fuel_type,
            policy_type=policy_type,
            vehicle_age=vehicle_age,
            business_type=business_type,
            vehicle_category=vehicle_category_code,
            cc_slab=cc_slab,
            gvw_slab=gvw_slab,
            gvw_value=gvw_value,
            watt_slab=watt_slab,
            seating_capacity=seating_capacity,
            ncb_slab=ncb_slab,
            cpa_cover=cpa_cover,
            zero_depreciation=zero_dep,
            trailer=trailer or None,
            make=make if make and str(make).strip().lower() not in ('other', '') else None,
            model=model if model and str(model).strip().lower() not in ('other', '') else None
        )

        # Log this query for analytics
        log_query(state_code, rto_code, vehicle_type, fuel_type, policy_type, len(payouts))

        # Prepare response
        if payouts:
            response = PayoutResponse(
                status="success",
                message=f"Found {len(payouts)} payout(s) - Top {len(payouts)} insurers by commission",
                rto_code=rto_code_display,
                top_3_payouts=[CompanyPayout(**p) for p in payouts],
                total_companies=len(payouts)
            )
            print(f"[API] Found {len(payouts)} payouts")
        else:
            response = PayoutResponse(
                status="no_data",
                message="No matching payout data found for this combination. Database may be empty.",
                rto_code=rto_code_display,
                top_3_payouts=[],
                total_companies=0
            )
            print(f"[API] No payouts found for this combination")

        return response

    except Exception as e:
        import traceback
        print(f"[API ERROR] {str(e)}")
        print(f"[API ERROR] Traceback:")
        traceback.print_exc()
        return PayoutResponse(
            status="error",
            message=f"Error processing request: {str(e)}",
            rto_code="",
            top_3_payouts=[],
            total_companies=0
        )
