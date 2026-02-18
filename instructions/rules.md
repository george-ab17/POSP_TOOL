# POSP Payout Checker - Master Rules

This is the current source of truth for UI behavior, backend matching, and data standards.

## 1) Run / Import

1. Start API:
`python -m uvicorn backend.app:app --reload --host 127.0.0.1 --port 8000`

2. Full import (all categories including GCV):
`python scripts/import_data.py --include-gcv`

3. Append mode (do not truncate existing data):
`python scripts/import_data.py --include-gcv --append`

## 2) Core Flow (UI)

1. Login page (User ID/Password from `.env`):
`APP_USER_ID`, `APP_PASSWORD`
2. Main form:
`State -> RTO (conditional) -> Vehicle Category -> Category-specific fields -> Vehicle Age -> Business Type -> Policy Type -> Check Payout`

## 3) Global Matching Semantics

1. Blank field in row means wildcard (applicable to all), except `Business_Type`.
2. `Business_Type` blank means applicable to:
`Old`, `Renewal`, `Rollover`
3. Comma-separated row values mean OR:
`TN,KL` means TN or KL.
4. `Except ...` / `Declined ...` means exclusion match.
5. Exclusion logic is implemented for:
`State`, `RTO_Code`, `Vehicle_Type`, `Make`, `Model` (and generic comma/exclusion fields).
6. Date validity is global:
`Date_from <= today <= Date_till`
with blank/null date as open-ended.
7. Payout is treated as percentage points (for display).

## 4) State / RTO Rules

1. RTO dropdown is shown only for states:
`TN, KA, KL, AP, MH, TS, PY`
2. RTO dropdown does not show `Others`.
3. RTO names/codes come from:
`data/extraction/district_rto`
4. UI state `Others` is mapped in backend to non-explicit rows:
blank/null/none state rows and exclusion-style state rows.

## 5) Category Rules

### 5.1 Two Wheeler

1. UI order:
`Vehicle Type -> Fuel Type -> Make`
2. Model is hidden.
3. Slab logic:
- EV -> show `Watt Slab`
- Non-EV -> show `CC Slab`
4. EV make-category map applied:
- Ather: Scooter
- Bajaj: Both
- Hero: Bike
- Honda: Scooter
- Piaggio: Scooter

### 5.2 Private Car

1. No separate vehicle type selection.
2. Show:
`Make -> Model -> Fuel Type -> Slab`
3. Slab logic:
- EV -> `Watt Slab`
- Other fuels -> `CC Slab`
4. Curated make-model mapping enforced in UI/backend.

### 5.3 PCV

Vehicle types:
`Auto`, `Taxi`, `Educational Bus`, `Educational Bus under school name`, `Staff Bus`

1. `Auto`
- Show Fuel Type
- Hide Model
- Hide CC/Watt slab

2. `Taxi`
- Hide Make
- Show Model
- Show Fuel Type
- Slab by fuel:
  - EV -> Watt
  - Others -> CC

3. `Educational Bus`, `Educational Bus under school name`, `Staff Bus`
- Hide Make
- Hide Model
- Hide Fuel Type
- Seating is not an input; seating text can appear in output condition.

### 5.4 Misc

1. Primary fields:
`Vehicle Type`, `Vehicle Age`, `Business Type`, `Policy Type`
2. `Trailer` is shown only for `Tractor`.

### 5.5 GCV

Sub-types:
`3 WHEELER GOODS`, `4 WHEELER GOODS`, `Flatbed`

1. `4 WHEELER GOODS`
- Show GVW slab dropdown
- Show Make/Model

2. `3 WHEELER GOODS`
- Hide GVW slab
- Hide Make/Model

3. `Flatbed`
- Hide GVW slab
- Hide Make/Model

4. GVW slab input (UI):
- `0|2.5`
- `2.5|3.5`
- `3.5|7.5`
- `7.5|12`
- `12|20`
- `20|25`
- `25|40`
- `40|MAX`

Backend uses overlap matching against row `gvw_min/gvw_max`.

## 6) Vehicle Age / Business / Policy Lock Rules

UI display labels:
- `New Vehicle`
- `1 Year`, `2 Year`, ... `50 Year`

Underlying value:
- `New Vehicle` label uses value `New`

Rules:

1. If Vehicle Age is `New Vehicle`:
- Business Type is forced to `New` and locked.
- Policy Type remains editable.

2. If Policy Type is Bundle (`Bundle(1+3)`, `Bundle(1+5)`, `Bundle(5+5)`):
- Vehicle Age forced to `New Vehicle` and locked.
- Business Type forced to `New` and locked.
- If user changes policy away from Bundle, locks are released.

3. If Vehicle Age is not `New Vehicle`:
- Business Type `New` is hidden.
- Bundle policies are hidden.

4. Backend validation enforces same rules.

## 7) Results Rules

1. Show top payout rows sorted by payout descending.
2. Always show `Company` + `Payout`.
3. Show condition text only when present.
4. PCV output can prepend seating text in condition line when seating exists in row.
5. Pan-India pairing rule (only these insurers):
- National Insurance
- New India
- Oriental Insurance
- United India

If one of these insurers appears in top set and both `Commission on OD` and `Commission on TP` rows exist for same match, both are shown with same rank number.

## 8) Condition Text Normalization (Display Layer)

UI applies normalization for readability (stored data remains unchanged), including:

1. `NCB is there` -> `NCB applicable`
2. `Commission on OD only` -> `Commission on OD`
3. `Commission on TP only` -> `Commission on TP`
4. `with Nil Deep` -> `With Zero Depreciation`
5. `Only System discount` -> `Only system discount applicable`
6. Discount phrase cleanups and capitalization fixes
7. Chennai/Kerala ID phrasing normalization

(full mappings are implemented in `frontend/js/index.js`)

## 9) Data Quality / Cleaning Rules

1. Standardize insurer casing (no duplicate-case variants).
2. Standardize slab naming (`Below 75 CC`, etc.).
3. Standardize vehicle type naming (`Backho Loader`, `Digger and Boring machine`, `Tanker`).
4. `Declined` and `Except` treated as exclusion logic.
5. Remove exact duplicates.
6. Keep payout values as numeric percentages (`33.81` style).

## 10) UX Rules Implemented

1. Smart auto-select:
If a dropdown has exactly one valid option, it is auto-selected.
2. Preserve prior selection on dependent reload when still valid.
3. Locked dropdown style is applied when Business/Age rules lock fields.
4. RTO, type, fuel, make/model, slabs, trailer, business, policy are all dependency-aware.

## 11) File Map

1. `backend/app.py`
API routes, form handling, validation, login/session.
2. `backend/database.py`
Matching engine, exclusion logic, dropdown distincts, ranking.
3. `frontend/index.html`
Main form layout.
4. `frontend/js/index.js`
All dynamic UI rules and client-side validation.
5. `frontend/css/index.css`
Main page styles.
6. `frontend/entry.html`
Login page UI.
7. `scripts/import_data.py`
Excel-to-MySQL import pipeline.
8. `db/schema.sql`
DB schema.

## 12) Extension Reference

For adding new Excel columns safely, use:
`COLUMN_MAPPING_GUIDE.md`

