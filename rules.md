# POSP Payout Checker - Master Rules

This is the latest single-source rules document for UI + backend matching.

## 1) Run / Import / Validate

1. Start API:
`python -m uvicorn backend.app:app --reload --host 127.0.0.1 --port 8000`

2. Import data:
`python scripts/import_data.py --include-gcv`

3. Validation:
`python tools/validate_all_row_combinations.py`
`python tools/demo_validation_suite.py`

## 2) Common UI Flow (All Categories)

1. Show `State`.
2. Show `RTO Code` only for enabled states: `TN, KA, KL, AP, TS, PY, MH`.
3. Show `Vehicle Category`.
4. Show category-specific fields.
5. Show `Vehicle Age` (1 to 50).
6. Show `Business Type`.
7. Show `Policy Type`.
8. Show top payout results.

## 3) Global Matching Semantics

1. Blank in data means wildcard (applicable to all) for all fields.
2. Exception for `Business_Type`:
   blank means applicable only to `Old`, `Renewal`, `Rollover`.
3. Comma-separated values mean OR match:
   `TN,KL` => matches TN or KL.
4. `Except ...` / `Declined ...` means exclusion:
   row applies to all values except listed values.
5. Exclusion logic is applied for:
   `State`, `RTO_Code`, `Vehicle_Type`, `Make`, `Model` (and related fields).
6. Date window logic is global:
   - if `Date_from` exists -> today must be >= `Date_from`
   - if `Date_till` exists -> today must be <= `Date_till`
7. Payout values are normalized to percentage style (e.g., `33.81`).
8. `State = Others` must include pan-India style rows where state is blank/null/none.

## 4) RTO Rules

1. UI must not show `Others` in RTO dropdown.
2. RTO options are split one-by-one (no combined `TN,KL` style display).
3. `Except` in RTO is exclusion logic against the state’s RTO list.
4. District-based RTO mapping is from `data/extraction/district_rto`.

## 5) Category-wise UI Rules

### A) Two Wheeler
1. Fields:
`State -> RTO -> Category -> Vehicle Type -> Make -> Fuel Type -> (CC/Watt by fuel) -> Business Type -> Policy Type`.
2. `Model` is not shown for Two Wheeler.
3. Fuel slab:
   - EV -> show `Watt Slab`
   - non-EV -> show `CC Slab`

### B) Private Car
1. Fields:
`State -> RTO -> Category -> Make -> Model -> Fuel Type -> (CC/Watt by fuel) -> Business Type -> Policy Type`.
2. Fuel slab:
   - EV -> `Watt Slab`
   - non-EV -> `CC Slab`

### C) PCV
1. Fields:
`State -> RTO -> Category -> Vehicle Type -> Make -> Model -> Fuel Type -> (CC/Watt by fuel) -> Business Type -> Policy Type`.
2. Seating is not explicit UI input; seating-related conditions can appear in output text.
3. For Auto in PCV:
   - do not show `Model`
   - do not show seating input

### D) Misc
1. Fields:
`State -> RTO -> Category -> Vehicle Type -> Vehicle Age -> Business Type -> Policy Type`.
2. `Trailer` shown only when `Vehicle Type = Tractor`.
3. Make/Model can appear when applicable from data.

### E) GCV
1. Subcategories:
`3 WHEELER GOODS`, `4 WHEELER GOODS`, `Flatbed`.
2. `GVW` input shown only for `4 WHEELER GOODS`.
3. `Flatbed`:
   - hide `Make`
   - hide `Model`
   - no GVW required
4. `3 WHEELER GOODS`:
   - GVW not used
5. `4 WHEELER GOODS`:
   - strict numeric GVW range matching (`GVW_Min <= input <= GVW_Max`).

## 6) GVW Rules

1. GVW input accepts decimals (e.g., `12.5`).
2. Allowed range: `0` to `50`.
3. If user enters > 50, show:
`GVW highest is 50. If more than 50, enter 50.`

## 7) Output Rules

1. Show top insurers with payout.
2. Always show `Company + Payout`.
3. Show `Conditions` only when present.
4. If no matching row exists in DB, show `No matching payout data found...`.
5. Conditions are displayed in bold.

## 8) Condition Text Display Normalization (UI)

Use UI display rewrites (data stays unchanged), including:

1. `NCB is there` -> `NCB applicable`
2. `Only System discount` -> `Only system discount applicable`
3. `upto 85% Discount` -> `Discount up to 85%`
4. `upto 90% Discount` -> `Discount up to 90%`
5. `with NCB and CPA` -> `With NCB and CPA`
6. `without NCB and CPA` -> `Without NCB and CPA`
7. `with Nil Deep` -> `With Zero Depreciation`
8. `without NCB upto 15 years` -> `Without NCB, vehicle age up to 15 years`

(and all additional mappings implemented in `frontend/js/index.js`).

## 9) Data Cleaning Standards

1. Company casing must be standardized (e.g., avoid duplicate casing variants).
2. Slab wording standardized (`Below 75 CC`, etc.).
3. Fuel normalization done consistently (EV/non-EV and non-diesel rules).
4. `Declined` treated as equivalent to `Except` for matching logic.
5. Duplicate rows removed.

## 10) Project Structure

1. `backend/` -> API + DB logic
2. `frontend/` -> UI assets
3. `data/extraction/` -> source Excel files
4. `db/` -> schema
5. `scripts/` -> run/import scripts
6. `tools/` -> validation and utility scripts

