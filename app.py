from fastapi import FastAPI, Form
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
#python -m uvicorn app:app --reload --host 127.0.0.1 --port 8000
app = FastAPI()

# Mount static files for serving HTML, CSS, JS, images
app.mount("/static", StaticFiles(directory="."), name="static")

@app.get("/")
async def entry_page():
    with open("entry.html", "r") as f:
        return HTMLResponse(f.read())

@app.get("/form", response_class=HTMLResponse)
async def get_form():
    with open("index.html", "r") as f:
        return f.read()

@app.post("/check-payout")
async def check_payout(
    state: str = Form(...),
    rto_number: str = Form(...),
    vehicle_category: str = Form(...),
    vehicle_type: str = Form(...),
    fuel_type: str = Form(...),
    cc_slab: str = Form(None),
    seating_capacity: str = Form(None),
    gvw_slab: str = Form(...),
    watt_slab: str = Form(...),
    vehicle_age: str = Form(...),
    policy_type: str = Form(...),
    business_type: str = Form(...),
    claim_status: str = Form(...),
    ncb_slab: str = Form(...),
    cpa_cover: str = Form(...),
    zero_dep: str = Form(...),
    financer: str = Form(...),
    trailer: str = Form(...),
):
    # Backend logic placeholder
    # Here you can add your payout calculation logic based on the parameters
    rto_code = f"{state}{rto_number}" if state and rto_number else "N/A"
    
    # For demonstration, return a simple response
    # In real implementation, compute the best payout
    response_message = f"Payout checked for RTO Code: {rto_code}. Selected Vehicle: {vehicle_category} - {vehicle_type}."
    
    # Return JSON response (can be modified to return HTML or redirect)
    return {
        "message": response_message,
        "rto_code": rto_code,
        "selected_parameters": {
            "state": state,
            "vehicle_category": vehicle_category,
            "vehicle_type": vehicle_type,
            "fuel_type": fuel_type,
            "cc_slab": cc_slab,
            "seating_capacity": seating_capacity,
            "gvw_slab": gvw_slab,
            "watt_slab": watt_slab,
            "vehicle_age": vehicle_age,
            "policy_type": policy_type,
            "business_type": business_type,
            "claim_status": claim_status,
            "ncb_slab": ncb_slab,
            "cpa_cover": cpa_cover,
            "zero_dep": zero_dep,
            "financer": financer,
            "trailer": trailer,
        }
    }