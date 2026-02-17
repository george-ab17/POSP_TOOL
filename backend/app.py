"""Purpose: FastAPI entrypoint that serves UI pages, dropdown APIs, and payout result API."""

from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.responses import JSONResponse
from fastapi.responses import RedirectResponse
from starlette.middleware.sessions import SessionMiddleware
from pathlib import Path

from .database import (
    init_connection_pool, get_top_5_payouts, log_query, test_connection,
    get_distinct_states, get_distinct_rto_options, get_distinct_vehicle_categories,
    get_distinct_vehicle_types, get_distinct_fuel_types, get_distinct_policy_types,
    get_distinct_business_types, get_distinct_vehicle_ages, get_distinct_cc_slabs,
    get_distinct_gvw_slabs, get_distinct_watt_slabs, get_distinct_seating_capacities,
    get_distinct_ncb_slabs, get_distinct_cpa_covers, get_distinct_zero_depreciation,
    get_distinct_trailers, get_distinct_makes, get_distinct_models
)
from .schemas import PayoutResponse, CompanyPayout
from .config import API_HOST, API_PORT, STATE_CODE_MAP, STATE_DISPLAY_NAMES, VEHICLE_CATEGORY_MAP
import os
import logging

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s"
)
logger = logging.getLogger(__name__)

# Initialize FastAPI app
app = FastAPI(
    title="POSP Payout Checker",
    description="Insurance Payout Commission Lookup System"
)

SESSION_SECRET = os.getenv("SESSION_SECRET_KEY", "change-this-session-secret")
LOGIN_USER_ID = os.getenv("APP_USER_ID")
LOGIN_PASSWORD = os.getenv("APP_PASSWORD")
app.add_middleware(SessionMiddleware, secret_key=SESSION_SECRET, same_site="lax")

# Initialize database connection pool on startup
@app.on_event("startup")
async def startup_event():
    """Initialize database connection on app startup only when enabled.

    Use environment variable `DB_AUTO_CONNECT` (true|false). Default: true.
    When disabled the app will serve the UI and endpoints but will not
    attempt to connect to MySQL until you explicitly enable/import data.
    """
    auto_connect = os.getenv("DB_AUTO_CONNECT", "true").lower() in ("1", "true", "yes")
    logger.info("Starting up - DB auto-connect=%s", auto_connect)
    if auto_connect:
        logger.info("Initializing database connection...")
        init_connection_pool()
        if not test_connection():
            logger.warning("Database connection test failed. Check MySQL is running.")
        else:
            logger.info("Database connection ready")
    else:
        logger.info("Running in UI-only mode (DB disabled). Connect DB after Excel is ready.")

ROOT = Path(__file__).resolve().parents[1]
FRONTEND_DIR = ROOT / "frontend"

# Mount static files for serving HTML, CSS, JS, images
app.mount("/static", StaticFiles(directory=str(FRONTEND_DIR)), name="static")

@app.get("/", response_class=HTMLResponse)
async def entry_page(request: Request):
    """Serve the entry page (landing page)"""
    if request.session.get("authenticated"):
        return RedirectResponse(url="/form", status_code=303)
    with open(FRONTEND_DIR / "entry.html", "r", encoding="utf-8") as f:
        html = f.read()
    login_error = request.query_params.get("error", "")
    html = html.replace("__LOGIN_ERROR__", login_error)
    return HTMLResponse(html)


@app.post("/login")
async def login(request: Request, user_id: str = Form(...), password: str = Form(...)):
    """Authenticate user from entry page."""
    if not LOGIN_USER_ID or not LOGIN_PASSWORD:
        return RedirectResponse(
            url="/?error=Login%20is%20disabled.%20Set%20APP_USER_ID%20and%20APP_PASSWORD%20in%20.env",
            status_code=303,
        )
    input_user_id = (user_id or "").strip()
    input_password = (password or "").strip()
    expected_user_id = (LOGIN_USER_ID or "").strip()
    expected_password = (LOGIN_PASSWORD or "").strip()
    if input_user_id == expected_user_id and input_password == expected_password:
        request.session["authenticated"] = True
        request.session["user_id"] = input_user_id
        return RedirectResponse(url="/form", status_code=303)
    return RedirectResponse(url="/?error=Invalid%20User%20ID%20or%20Password", status_code=303)


@app.get("/logout")
async def logout(request: Request):
    request.session.clear()
    return RedirectResponse(url="/", status_code=303)

@app.get("/form", response_class=HTMLResponse)
async def get_form(request: Request):
    """Serve the main payout checker form"""
    if not request.session.get("authenticated"):
        return RedirectResponse(url="/?error=Please%20login%20to%20continue", status_code=303)
    with open(FRONTEND_DIR / "index.html", "r", encoding="utf-8") as f:
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
    rto_options = [
        opt for opt in get_distinct_rto_options(state)
        if str(opt.get("code", "")).strip().lower() != "others"
    ]
    return {
        "rtos": [
            opt.get("code")
            for opt in rto_options
            if opt.get("code") and str(opt.get("code")).strip().lower() != "others"
        ],
        "rto_options": rto_options,
    }

@app.get("/api/vehicle-categories")
async def get_vehicle_categories():
    """Get all vehicle categories"""
    return {"categories": get_distinct_vehicle_categories()}

@app.get("/api/vehicle-types")
async def get_vehicle_types(category: str = None):
    """Get all vehicle types (optionally filtered by category)"""
    return {"types": get_distinct_vehicle_types(category)}

@app.get("/api/fuel-types")
async def get_fuel_types(vehicle_type: str = None, category: str = None):
    """Get all fuel types (optionally filtered by vehicle type)"""
    return {"fuels": get_distinct_fuel_types(vehicle_type, category)}

@app.get("/api/policy-types")
async def get_policy_types(vehicle_type: str = None, fuel_type: str = None, category: str = None):
    """Get all policy types (optionally filtered by vehicle_type and fuel_type)"""
    policies = [p for p in get_distinct_policy_types(vehicle_type, fuel_type, category) if str(p).strip().lower() != 'all']
    return {"policies": policies}

@app.get("/api/business-types")
async def get_business_types(vehicle_type: str = None, fuel_type: str = None, category: str = None):
    """Get all business types (optionally filtered by vehicle_type and fuel_type)"""
    return {"business_types": get_distinct_business_types(vehicle_type, fuel_type, category)}

@app.get("/api/vehicle-ages")
async def get_vehicle_ages():
    """Get all vehicle ages"""
    # Return numeric age choices 1..50 for the UI to select a specific age
    ages = [str(i) for i in range(1, 51)]
    return {"ages": ages}

@app.get("/api/cc-slabs")
async def get_cc_slabs(vehicle_type: str = None, fuel_type: str = None, category: str = None):
    """Get all CC slabs (optionally filtered by vehicle_type and fuel_type)"""
    return {"cc_slabs": get_distinct_cc_slabs(vehicle_type, fuel_type, category)}

@app.get("/api/gvw-slabs")
async def get_gvw_slabs(vehicle_type: str = None):
    """Get all GVW slabs (optionally filtered by vehicle_type)"""
    return {"gvw_slabs": get_distinct_gvw_slabs(vehicle_type)}

@app.get("/api/watt-slabs")
async def get_watt_slabs(vehicle_type: str = None, fuel_type: str = None, category: str = None):
    """Get all Watt slabs (optionally filtered by vehicle_type and fuel_type)"""
    return {"watt_slabs": get_distinct_watt_slabs(vehicle_type, fuel_type, category)}

@app.get("/api/seating-capacities")
async def get_seating_capacities(vehicle_type: str = None, fuel_type: str = None, category: str = None):
    """Get all seating capacities (optionally filtered by vehicle_type and fuel_type)"""
    return {"capacities": get_distinct_seating_capacities(vehicle_type, fuel_type, category)}

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
async def get_makes(vehicle_type: str = None, category: str = None, fuel_type: str = None):
    """Get all makes (optionally filtered by vehicle type)"""
    makes = get_distinct_makes(vehicle_type, category, fuel_type)
    return {"makes": makes}

@app.get("/api/models")
async def get_models(make: str = None, vehicle_type: str = None, category: str = None):
    """Get models for a specific make (optionally filtered by vehicle_type)"""
    models = get_distinct_models(make, vehicle_type, category)
    return {"models": models}

@app.post("/check-payout")
async def check_payout(
    state: str = Form(...),
    rto_number: str = Form(None),
    vehicle_category: str = Form(...),
    vehicle_type: str = Form(None),
    fuel_type: str = Form(None),  # Optional: hidden for PCV buses
    cc_slab: str = Form(None),
    seating_capacity: str = Form(None),
    gvw_slab: str = Form(None),
    gvw_value: str = Form(None),
    watt_slab: str = Form(None),
    vehicle_age: str = Form(None),  # Vehicle age now handled via Conditions field
    policy_type: str = Form(...),
    business_type: str = Form(...),
    ncb_slab: str = Form(None),
    cpa_cover: str = Form(None),
    zero_dep: str = Form(None),
    trailer: str = Form(None),
    make: str = Form(None),
    model: str = Form(None),
):
    """
    Check payout for given parameters using ALL parameters to match database records
    """
    try:
        age_value = (vehicle_age or "").strip()
        policy_value = "".join((policy_type or "").strip().lower().split())
        business_value = (business_type or "").strip().lower()
        is_bundle_policy = policy_value in ("bundle(1+3)", "bundle(1+5)", "bundle(5+5)")
        if age_value and age_value != "1" and business_value == "new" and not is_bundle_policy:
            return PayoutResponse(
                status="error",
                message="Business Type cannot be New when Vehicle Age is not 1.",
                rto_code="",
                top_3_payouts=[],
                top_5_payouts=[],
                total_companies=0
            )
        if (age_value == "1" or is_bundle_policy) and business_value != "new":
            return PayoutResponse(
                status="error",
                message="Business Type must be New when Vehicle Age is 1 or Policy Type is Bundle(1+3)/Bundle(1+5).",
                rto_code="",
                top_3_payouts=[],
                top_5_payouts=[],
                total_companies=0
            )

        logger.debug(
            "Payout check request received: state=%s rto_number=%s vehicle_category=%s vehicle_type=%s fuel_type=%s policy_type=%s business_type=%s",
            state,
            rto_number,
            vehicle_category,
            vehicle_type,
            fuel_type,
            policy_type,
            business_type,
        )
        
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

        logger.info(
            "Payout check request - State: %s (%s), RTO: %s, Vehicle: %s, Fuel: %s",
            state,
            state_code,
            rto_code_display,
            vehicle_type,
            fuel_type,
        )

        auto_connect = os.getenv("DB_AUTO_CONNECT", "true").lower() in ("1", "true", "yes")

        # If DB auto-connect is disabled, do not call database. Serve UI-only response.
        if not auto_connect:
            logger.info("DB disabled - returning UI-only response")
            return PayoutResponse(
                status="ui_only",
                message="Database connection is disabled. Import cleaned Excel data and enable DB to get payout results.",
                rto_code=rto_code,
                top_3_payouts=[],
                top_5_payouts=[],
                total_companies=0
            )

        # Map vehicle category display name to code if needed (DB may store code)
        vehicle_category_code = VEHICLE_CATEGORY_MAP.get(vehicle_category, vehicle_category)

        # GVW input validation: allow decimals, enforce supported range.
        if gvw_value is not None and str(gvw_value).strip() != "":
            try:
                gvw_num = float(gvw_value)
            except ValueError:
                return PayoutResponse(
                    status="error",
                    message="GVW Slab (Ton) must be a valid number.",
                    rto_code=rto_code_display,
                    top_3_payouts=[],
                    top_5_payouts=[],
                    total_companies=0
                )
            if gvw_num < 0 or gvw_num > 50:
                return PayoutResponse(
                    status="error",
                    message="GVW highest is 50. If more than 50, enter 50.",
                    rto_code=rto_code_display,
                    top_3_payouts=[],
                    top_5_payouts=[],
                    total_companies=0
                )

        # Query database using all parameters
        payouts = get_top_5_payouts(
            state=state_code,
            rto_code=rto_code,
            vehicle_type=vehicle_type,
            fuel_type=fuel_type if fuel_type and fuel_type.lower() != 'others' else None,  # 'Others' → None (no filter)
            policy_type=policy_type,
            vehicle_age=vehicle_age,
            business_type=business_type,
            vehicle_category=vehicle_category_code,
            cc_slab=cc_slab if cc_slab and cc_slab.lower() != 'others' else None,  # 'Others' → None (no filter)
            gvw_slab=gvw_slab,
            gvw_value=gvw_value,
            watt_slab=watt_slab if watt_slab and watt_slab.lower() != 'others' else None,  # 'Others' → None (no filter)
            seating_capacity=seating_capacity if seating_capacity and seating_capacity.lower() != 'others' else None,  # 'Others' → N/A (wildcard)
            ncb_slab=ncb_slab,
            cpa_cover=cpa_cover,
            zero_depreciation=zero_dep,
            trailer=trailer or None,
            make=make if make and str(make).strip().lower() not in ('other', 'others', '') else None,
            model=model if model and str(model).strip().lower() not in ('other', 'others', '') else None
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
                top_5_payouts=[CompanyPayout(**p) for p in payouts],
                total_companies=len(payouts)
            )
            logger.info("Found %d payouts", len(payouts))
        else:
            response = PayoutResponse(
                status="no_data",
                message="No matching payout data found for this combination. Database may be empty.",
                rto_code=rto_code_display,
                top_3_payouts=[],
                top_5_payouts=[],
                total_companies=0
            )
            logger.info("No payouts found for this combination")

        return response

    except Exception as e:
        logger.exception("Error processing payout request")
        return PayoutResponse(
            status="error",
            message=f"Error processing request: {str(e)}",
            rto_code="",
            top_3_payouts=[],
            top_5_payouts=[],
            total_companies=0
        )
