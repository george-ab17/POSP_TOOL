# Extra Column Mapping Guide

Use this guide whenever new columns are added in Excel so mapping stays consistent in DB, backend, and UI.

## 1) Column Onboarding Checklist

1. Define the column meaning:
- Filter input?
- Display-only output?
- Ranking factor?
- Date/validity rule?

2. Decide matching type:
- Exact
- Comma-separated OR
- Exclusion (`Except` / `Declined`)
- Numeric range
- Wildcard-with-blank

3. Add to import:
- Ensure column is present in `raw_json` during import.
- If numeric/range, also map helper columns (like `gvw_min`, `age_min` style) when needed.

4. Add to backend filter logic:
- Add input field to `/check-payout` if it is a UI filter.
- Add query condition in `backend/database.py:get_top_5_payouts`.

5. Add to dropdown API (if selectable):
- Create `get_distinct_*` function pattern in `backend/database.py`.
- Expose via `/api/...` route in `backend/app.py`.

6. Add to frontend:
- Add HTML field in `frontend/index.html`.
- Add element/group references in `frontend/js/index.js`.
- Add visibility rule in `applyCategoryVisibility`.
- Add validation in `validateForm`.

7. Add docs:
- Update `rules.md` with behavior.
- Add sample row cases.

## 2) Supported Matching Patterns

## 2.1 Exact Match

Use when value must match exactly.

Example:
`Policy_Type = SATP`

## 2.2 Comma-separated OR

Use when one row applies to many values.

Example:
`State = TN,KL`
matches TN or KL.

## 2.3 Exclusion

Use for values prefixed with `Except` or `Declined`.

Example:
`Model = Except Bolero,Scorpio`
matches every model except Bolero and Scorpio.

## 2.4 Numeric Range

Use helper min/max columns and point-in-range or overlap logic.

Examples:
- vehicle age: `age_min <= selected_age <= age_max`
- gvw slab overlap:
  selected `[a,b]` overlaps row `[x,y]` if `x <= b` and `y >= a`

## 2.5 Wildcard with Blank

Blank/null means applicable to all.

Exception already used:
`Business_Type` blank means only `Old/Renewal/Rollover`.

## 3) Current JSON Key Mapping Pattern

Backend currently maps API keys to JSON keys via `_JSON_KEY_MAP` in `backend/database.py`.

When adding a new filter key:

1. Add entry in `_JSON_KEY_MAP`
2. Choose which filter block it belongs to:
- simple exact/wildcard
- comma-sep
- make/model exclusion style
- numeric range

## 4) New Column Templates

## 4.1 New Select Dropdown Column

1. Import keeps it in `raw_json`
2. Add backend distinct getter:
`get_distinct_<column>()`
3. Add route:
`/api/<column>`
4. Add field in HTML and JS refs
5. Add to validation and submission payload

## 4.2 New Numeric Min/Max Column

1. Add helper columns in `rates` table if performance-critical
2. Parse numbers during import
3. Add range logic in `get_top_5_payouts`
4. Add UI input (number or controlled slab dropdown)

## 4.3 Output-only Column

No UI filter needed.

1. Keep in `raw_json`
2. Extend result select/formatting in backend
3. Render in results table/card in frontend

## 5) Data Normalization Standards

Before import:

1. Company casing standardized
2. Fuel/type spelling standardized
3. Slab labels standardized
4. Exclusion wording standardized (`Except` preferred)
5. Payout kept numeric percentage style

If source uses `%` strings, normalize before import or ensure parser supports `%`.

## 6) Regression Test Matrix (Minimum)

For every new column, test:

1. blank row behavior
2. exact match row
3. comma OR row
4. exclusion row
5. combined row with existing filters (state/rto/age/policy)
6. no-result scenario

## 7) Safe Rollout Strategy

1. Add column in Excel and import parser first.
2. Verify DB contains expected distinct values.
3. Add backend filtering next.
4. Add UI field last.
5. Run sanity tests per vehicle category.
6. Deploy.

