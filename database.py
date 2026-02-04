"""Database helper functions for POSP payout app.

Implements minimal functions referenced by `app.py`:
- init_connection_pool()
- test_connection()
- get_top_5_payouts(...)
- get_distinct_* helpers
- log_query()

This module uses `mysql.connector` and expects DB creds in env:
DB_HOST, DB_PORT, DB_USER, DB_PASS, DB_NAME
"""
import os
from typing import List, Any, Optional
import mysql.connector
from mysql.connector import pooling, Error
from dotenv import load_dotenv

# Load .env if present so DB credentials from workspace are picked up
load_dotenv()

_POOL: Optional[pooling.MySQLConnectionPool] = None

# Mapping of parameter names to correct JSON keys (handle special cases)
_JSON_KEY_MAP = {
    'ncb_slab': 'NCB_Slab',
    'cpa_cover': 'CPA_Cover', 
    'zero_depreciation': 'Zero_Depreciation',
    'cc_slab': 'CC_Slab',
    'gvw_slab': 'GVW_Slab',
    'watt_slab': 'Watt_Slab',
    'seating_capacity': 'Seating_Capacity',
    'fuel_type': 'Fuel_Type',
    'vehicle_type': 'Vehicle_Type',
    'vehicle_category': 'Vehicle_Category',
    'policy_type': 'Policy_Type',
    'business_type': 'Business_Type',
    'trailer': 'Trailer',
    'make': 'Make',
    'model': 'Model',
}

def init_connection_pool(pool_name: str = 'posp_pool', pool_size: int = 5):
    global _POOL
    if _POOL is not None:
        return
    # Support both DB_PASS and DB_PASSWORD environment variable names
    db_pass = os.getenv('DB_PASS') or os.getenv('DB_PASSWORD') or ''
    cfg = {
        'host': os.getenv('DB_HOST', '127.0.0.1'),
        'port': int(os.getenv('DB_PORT', 3306)),
        'user': os.getenv('DB_USER', 'root'),
        'password': db_pass,
        'database': os.getenv('DB_NAME', 'rates_db'),
    }
    _POOL = pooling.MySQLConnectionPool(pool_name=pool_name, pool_size=pool_size, **cfg)


def get_conn():
    if _POOL is None:
        init_connection_pool()
    return _POOL.get_connection()


def test_connection() -> bool:
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute('SELECT 1')
        cur.fetchall()
        cur.close()
        conn.close()
        return True
    except Exception as e:
        print('[DB TEST ERROR]', e)
        return False


def _get_current_import_id(conn) -> Optional[int]:
    cur = conn.cursor()
    cur.execute("SELECT id FROM imports WHERE status='completed' ORDER BY uploaded_at DESC LIMIT 1")
    r = cur.fetchone()
    cur.close()
    return r[0] if r else None


def log_query(state, rto, vehicle_type, fuel_type, policy_type, count):
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("CREATE TABLE IF NOT EXISTS query_log (id BIGINT AUTO_INCREMENT PRIMARY KEY, ts DATETIME DEFAULT CURRENT_TIMESTAMP, state VARCHAR(64), rto VARCHAR(64), vehicle_type VARCHAR(64), fuel_type VARCHAR(64), policy_type VARCHAR(64), result_count INT)")
        cur.execute("INSERT INTO query_log (state, rto, vehicle_type, fuel_type, policy_type, result_count) VALUES (%s,%s,%s,%s,%s,%s)", (state, rto, vehicle_type, fuel_type, policy_type, count))
        conn.commit()
        cur.close()
        conn.close()
    except Error as e:
        print('[DB] log_query error', e)


def _distinct_from_raw(column_name: str, all_imports: bool = False) -> List[Any]:
    """Return distinct values from JSON raw_json column.
    
    By default, returns values from current import only.
    Set all_imports=True to get values from ALL imports (useful for dropdown lists).
    """
    conn = get_conn()
    try:
        import_id = _get_current_import_id(conn)
        cur = conn.cursor()
        path = f"$.{column_name}"
        if import_id and not all_imports:
            sql = "SELECT DISTINCT JSON_UNQUOTE(JSON_EXTRACT(raw_json, %s)) FROM rates WHERE import_id=%s AND JSON_EXTRACT(raw_json, %s) IS NOT NULL"
            cur.execute(sql, (path, import_id, path))
        else:
            sql = "SELECT DISTINCT JSON_UNQUOTE(JSON_EXTRACT(raw_json, %s)) FROM rates WHERE JSON_EXTRACT(raw_json, %s) IS NOT NULL"
            cur.execute(sql, (path, path))
        # Filter out None and the string 'null' / 'none' (JSON null becomes string 'null' when unquoted)
        rows = [r[0] for r in cur.fetchall() if r[0] is not None and str(r[0]).lower() not in ('null', 'none')]
        cur.close()
        return rows
    finally:
        conn.close()


def _split_comma_cell(cell: Any) -> List[str]:
    """Split a cell value by comma, strip each token, return non-empty list. Handles None."""
    if cell is None:
        return []
    s = str(cell).strip()
    if not s:
        return []
    return [t.strip() for t in s.split(',') if t.strip()]


def _distinct_single_values(column_name: str, exclude_tokens: Optional[List[str]] = None, exclude_na: bool = True, all_imports: bool = True) -> List[str]:
    """Return distinct SINGLE values for dropdown: comma-separated cells are split so UI shows
    e.g. 'Petrol' and 'Diesel' as separate options, not 'Petrol,Diesel' as one.
    Excludes empty, 'All', and optional exclude_tokens (case-insensitive).
    By default also excludes 'N/A', but set exclude_na=False to keep 'N/A' (for seating capacity when it's the only value).
    NOTE: 'No' is NOT excluded by default - pass it explicitly in exclude_tokens if needed.
    
    By default (all_imports=True), queries all imports to show all possible dropdown values.
    Set all_imports=False to show values from current import only.
    """
    raw_list = _distinct_from_raw(column_name, all_imports=all_imports)
    exclude = set((exclude_tokens or []) + ['', 'all'])
    if exclude_na:
        exclude.add('n/a')
    seen = set()
    result = []
    for raw in raw_list:
        for token in _split_comma_cell(raw):
            key = token.lower()
            if key in exclude or key in seen:
                continue
            seen.add(key)
            result.append(token)
    return sorted(result, key=lambda x: (x.lower(), x))


def _distinct_single_values_filtered(
    column_name: str,
    filter_column: str,
    filter_value: str,
    exclude_tokens: Optional[List[str]] = None,
    all_imports: bool = True,
) -> List[str]:
    """Like _distinct_single_values but only from rows where filter_column equals filter_value
    OR filter_column (comma-separated) contains filter_value as a whole token
    OR filter_column = 'All' (wildcard: applies to all values).
    
    By default (all_imports=True), queries all imports to show all possible dropdown values.
    Set all_imports=False to show values from current import only.
    """
    conn = get_conn()
    try:
        import_id = _get_current_import_id(conn) if not all_imports else None
        cur = conn.cursor()
        path_col = f"$.{column_name}"
        path_filt = f"$.{filter_column}"
        # Match: filter_column = filter_value OR filter_column comma-list contains filter_value OR filter_column = 'All' (wildcard)
        if import_id:
            sql = (
                "SELECT DISTINCT JSON_UNQUOTE(JSON_EXTRACT(raw_json, %s)) FROM rates "
                "WHERE import_id = %s AND JSON_EXTRACT(raw_json, %s) IS NOT NULL AND "
                "(JSON_UNQUOTE(JSON_EXTRACT(raw_json, %s)) = %s OR "
                "CONCAT(',', REPLACE(REPLACE(TRIM(COALESCE(JSON_UNQUOTE(JSON_EXTRACT(raw_json, %s)), '')), ', ', ','), ' ,', ','), ',') LIKE CONCAT('%,', %s, ',%') OR "
                "LOWER(TRIM(COALESCE(JSON_UNQUOTE(JSON_EXTRACT(raw_json, %s)), ''))) = 'all')"
            )
            cur.execute(
                sql,
                (path_col, import_id, path_col, path_filt, filter_value, path_filt, filter_value, path_filt),
            )
        else:
            sql = (
                "SELECT DISTINCT JSON_UNQUOTE(JSON_EXTRACT(raw_json, %s)) FROM rates "
                "WHERE JSON_EXTRACT(raw_json, %s) IS NOT NULL AND "
                "(JSON_UNQUOTE(JSON_EXTRACT(raw_json, %s)) = %s OR "
                "CONCAT(',', REPLACE(REPLACE(TRIM(COALESCE(JSON_UNQUOTE(JSON_EXTRACT(raw_json, %s)), '')), ', ', ','), ' ,', ','), ',') LIKE CONCAT('%,', %s, ',%') OR "
                "LOWER(TRIM(COALESCE(JSON_UNQUOTE(JSON_EXTRACT(raw_json, %s)), ''))) = 'all')"
            )
            cur.execute(sql, (path_col, path_col, path_filt, filter_value, path_filt, filter_value, path_filt))
        raw_list = [r[0] for r in cur.fetchall() if r[0] is not None and str(r[0]).lower() not in ('null', 'none')]
        cur.close()
        exclude = set((exclude_tokens or []) + ['', 'all', 'n/a', 'no'])
        seen = set()
        result = []
        for raw in raw_list:
            for token in _split_comma_cell(raw):
                key = token.lower()
                if key in exclude or key in seen:
                    continue
                seen.add(key)
                result.append(token)
        return sorted(result, key=lambda x: (x.lower(), x))
    finally:
        conn.close()


def _distinct_with_filters(
    column_name: str,
    filters: List[tuple],  # List of (filter_column, filter_value) tuples
    exclude_tokens: Optional[List[str]] = None,
    exclude_na: bool = True,
    all_imports: bool = True,
) -> List[str]:
    """Get distinct values for column_name filtered by multiple conditions (AND logic).
    Each filter is checked with: 
    - filter_column = filter_value OR 
    - filter_column (comma-separated) contains filter_value OR
    - filter_column = 'All' (wildcard: applies to all values)
    By default excludes 'N/A', but set exclude_na=False to keep it (e.g., for seating capacity).
    
    By default (all_imports=True), queries all imports to show all possible filtered dropdown values.
    Set all_imports=False to show values from current import only.
    """
    conn = get_conn()
    try:
        import_id = _get_current_import_id(conn) if not all_imports else None
        cur = conn.cursor()
        path_col = f"$.{column_name}"
        
        # Build WHERE clause for each filter
        where_conditions = []
        params = []
        
        if import_id:
            where_conditions.append("import_id = %s")
            params.append(import_id)
        
        where_conditions.append(f"JSON_EXTRACT(raw_json, '{path_col}') IS NOT NULL")
        
        for filter_column, filter_value in filters:
            path_filt = f"$.{filter_column}"
            # Match: filter_column = filter_value OR filter_column (comma-list) contains filter_value OR filter_column = 'All' (wildcard)
            where_conditions.append(
                f"(JSON_UNQUOTE(JSON_EXTRACT(raw_json, '{path_filt}')) = %s OR "
                f"CONCAT(',', REPLACE(REPLACE(TRIM(COALESCE(JSON_UNQUOTE(JSON_EXTRACT(raw_json, '{path_filt}')), '')), ', ', ','), ' ,', ','), ',') LIKE CONCAT('%,', %s, ',%') OR "
                f"LOWER(TRIM(COALESCE(JSON_UNQUOTE(JSON_EXTRACT(raw_json, '{path_filt}')), ''))) = 'all')"
            )
            params.append(filter_value)
            params.append(filter_value)
        
        sql = f"SELECT DISTINCT JSON_UNQUOTE(JSON_EXTRACT(raw_json, '{path_col}')) FROM rates WHERE {' AND '.join(where_conditions)}"
        cur.execute(sql, params)
        # Filter out None and JSON-unquoted 'null'/'none' strings
        raw_list = [r[0] for r in cur.fetchall() if r[0] is not None and str(r[0]).lower() not in ('null', 'none')]
        cur.close()
        exclude = set((exclude_tokens or []) + ['', 'all'])
        if exclude_na:
            exclude.add('n/a')
        seen = set()
        result = []
        for raw in raw_list:
            for token in _split_comma_cell(raw):
                key = token.lower()
                if key in exclude or key in seen:
                    continue
                seen.add(key)
                result.append(token)
        return sorted(result, key=lambda x: (x.lower(), x))
    finally:
        conn.close()


def get_distinct_states() -> List[str]:
    # Use config values if present else try to derive from data
    from config import STATE_DISPLAY_NAMES
    if STATE_DISPLAY_NAMES:
        return STATE_DISPLAY_NAMES
    return _distinct_from_raw('State')


def get_distinct_rtos(state: str = None) -> List[str]:
    """Return RTO codes available for a given state.
    
    For a given state, return all RTO codes that are associated with rates for that state,
    plus any RTOs from rates with applies_all_rto=1 for that state.
    """
    conn = get_conn()
    cur = conn.cursor()
    
    if not state:
        # No state specified - return all RTO codes
        cur.execute("SELECT code FROM rto ORDER BY code")
        rows = [r[0] for r in cur.fetchall()]
    else:
        # Get RTOs associated with rates for this state via rate_included_rto
        cur.execute(
            "SELECT DISTINCT rr.code FROM rate_included_rto i "
            "JOIN rto rr ON rr.id = i.rto_id "
            "WHERE i.rate_id IN (SELECT id FROM rates WHERE state_code = %s) "
            "UNION "
            "SELECT DISTINCT rr.code FROM rto rr "
            "WHERE EXISTS (SELECT 1 FROM rates r WHERE r.state_code = %s AND r.applies_all_rto = 1) "
            "ORDER BY code",
            (state, state)
        )
        rows = [r[0] for r in cur.fetchall()]
    
    cur.close()
    conn.close()
    return rows


def get_distinct_vehicle_categories() -> List[str]:
    """Single values only: 'GCV' and 'PCV' as separate options, not 'GCV,PCV'."""
    return _distinct_single_values('Vehicle_Category')


def get_distinct_vehicle_types(category: str = None) -> List[str]:
    """Single values only; when category given, only types for that category (comma-sep match)."""
    if not category:
        return _distinct_single_values('Vehicle_Type')
    return _distinct_single_values_filtered('Vehicle_Type', 'Vehicle_Category', category)
def _fuel_tokens_exist_for_vehicle_type(tokens: List[str], vehicle_type: str) -> List[str]:
    """Validate that fuel type tokens exist as standalone values (not just in combinations).
    
    For Two-Wheeler Scooter: 'Petrol,EV' records exist, but if pure 'EV' Scooter records
    don't exist, don't show 'EV' as a selectable option.
    Returns only tokens that have actual records for the given vehicle type.
    """
    conn = get_conn()
    try:
        cur = conn.cursor()
        validated = []

        for token in tokens:
            token_stripped = str(token).strip()
            # 1) Check if this token exists as standalone Fuel_Type for vehicle_type
            cur.execute("""
                SELECT COUNT(*) FROM rates
                WHERE JSON_UNQUOTE(JSON_EXTRACT(raw_json, '$.Vehicle_Type')) = %s
                AND JSON_UNQUOTE(JSON_EXTRACT(raw_json, '$.Fuel_Type')) = %s
            """, (vehicle_type, token_stripped))
            count = cur.fetchone()[0]
            if count > 0:
                validated.append(token)
                continue

            # 2) If not standalone, check composite records (e.g. 'Petrol,EV') that include this token
            #    and have CC_Slab='All' or Watt_Slab='All' — these indicate applicability to both fuels.
            cur.execute("""
                SELECT COUNT(*) FROM rates
                WHERE JSON_UNQUOTE(JSON_EXTRACT(raw_json, '$.Vehicle_Type')) = %s
                  AND (
                    JSON_UNQUOTE(JSON_EXTRACT(raw_json, '$.Fuel_Type')) = %s OR
                    CONCAT(',', REPLACE(REPLACE(TRIM(COALESCE(JSON_UNQUOTE(JSON_EXTRACT(raw_json, '$.Fuel_Type')), '')), ', ', ','), ' ,', ','), ',') LIKE CONCAT('%,', %s, ',%')
                  )
                  AND (
                    LOWER(TRIM(COALESCE(JSON_UNQUOTE(JSON_EXTRACT(raw_json, '$.CC_Slab')), ''))) = 'all' OR
                    LOWER(TRIM(COALESCE(JSON_UNQUOTE(JSON_EXTRACT(raw_json, '$.Watt_Slab')), ''))) = 'all'
                  )
            """, (vehicle_type, token_stripped, token_stripped))
            comp_count = cur.fetchone()[0]
            if comp_count > 0:
                validated.append(token)

        cur.close()
        return validated
    finally:
        conn.close()



def get_distinct_fuel_types(vehicle_type: str = None) -> List[str]:
    """Single values only: 'Petrol', 'Diesel' as separate options, not 'Petrol,Diesel'.
    When vehicle_type is provided, return only fuels for that vehicle type.
    If filtered result is empty (e.g., only 'All'), fall back to global fuel types."""
    if not vehicle_type:
        return _distinct_single_values('Fuel_Type', exclude_tokens=['all'])
    fuels = _distinct_single_values_filtered('Fuel_Type', 'Vehicle_Type', vehicle_type, exclude_tokens=['all'])
    # If filtered result is empty (vehicle type only has 'All' fuel), return global fuel types
    if not fuels:
        return _distinct_single_values('Fuel_Type', exclude_tokens=['all'])
    # For Two-Wheeler vehicle types with multi-fuel combos, validate that split tokens exist independently
    two_wheeler_types = ('Scooter', 'Bike', 'Motor Cycle', 'Motorcycle')
    if vehicle_type in two_wheeler_types:
        validated = _fuel_tokens_exist_for_vehicle_type(fuels, vehicle_type)
        # Return validated list if any valid fuels found, else return original
        return validated if validated else fuels
    return fuels


def get_distinct_policy_types(vehicle_type: str = None, fuel_type: str = None) -> List[str]:
    """Single values only.
    When vehicle_type and/or fuel_type provided, return only policy types for that combination.
    Do NOT fall back to global - show only what applies to the selected vehicle type."""
    if not vehicle_type and not fuel_type:
        return _distinct_single_values('Policy_Type')
    filters = []
    if vehicle_type:
        filters.append(('Vehicle_Type', vehicle_type))
    if fuel_type:
        filters.append(('Fuel_Type', fuel_type))
    return _distinct_with_filters('Policy_Type', filters)


def get_distinct_business_types(vehicle_type: str = None, fuel_type: str = None) -> List[str]:
    """Single values only.
    When vehicle_type and/or fuel_type provided, return only business types for that combination.
    Do NOT fall back to global - show only what applies to the selected vehicle type."""
    if not vehicle_type and not fuel_type:
        return _distinct_single_values('Business_Type')
    filters = []
    if vehicle_type:
        filters.append(('Vehicle_Type', vehicle_type))
    if fuel_type:
        filters.append(('Fuel_Type', fuel_type))
    return _distinct_with_filters('Business_Type', filters)


def get_distinct_vehicle_ages() -> List[str]:
    return _distinct_from_raw('Vehicle_Age')


def get_distinct_cc_slabs(vehicle_type: str = None, fuel_type: str = None) -> List[str]:
    """Single values only; exclude No/N/A from dropdown.
    When vehicle_type and/or fuel_type provided, return only CC slabs for that combination.
    If filtered result is empty (only 'All' records), fall back to global CC slabs."""
    if not vehicle_type and not fuel_type:
        return _distinct_single_values('CC_Slab', exclude_tokens=['no'])
    filters = []
    if vehicle_type:
        filters.append(('Vehicle_Type', vehicle_type))
    if fuel_type:
        filters.append(('Fuel_Type', fuel_type))
    slabs = _distinct_with_filters('CC_Slab', filters, exclude_tokens=['no'])
    # If filtered result is empty (only 'All' records), return global CC slabs
    if not slabs:
        return _distinct_single_values('CC_Slab', exclude_tokens=['no'])
    return slabs


def get_distinct_gvw_slabs(vehicle_type: str = None) -> List[str]:
    """Single values only; exclude No/N/A.
    When vehicle_type provided, return only GVW slabs for that vehicle type."""
    if not vehicle_type:
        return _distinct_single_values('GVW_Slab', exclude_tokens=['no'])
    return _distinct_single_values_filtered('GVW_Slab', 'Vehicle_Type', vehicle_type, exclude_tokens=['no'])


def get_distinct_watt_slabs(vehicle_type: str = None, fuel_type: str = None) -> List[str]:
    """Single values only; exclude No/N/A.
    When vehicle_type and/or fuel_type provided, return only watt slabs for that combination.
    If filtered result is empty (only 'All' records), fall back to global watt slabs."""
    if not vehicle_type and not fuel_type:
        return _distinct_single_values('Watt_Slab', exclude_tokens=['no'])
    filters = []
    if vehicle_type:
        filters.append(('Vehicle_Type', vehicle_type))
    if fuel_type:
        filters.append(('Fuel_Type', fuel_type))
    slabs = _distinct_with_filters('Watt_Slab', filters, exclude_tokens=['no'])
    # If filtered result is empty (only 'All' records), return global watt slabs
    if not slabs:
        return _distinct_single_values('Watt_Slab', exclude_tokens=['no'])
    return slabs


def get_distinct_seating_capacities(vehicle_type: str = None, fuel_type: str = None) -> List[str]:
    """Single values only; exclude No/N/A.
    When vehicle_type and/or fuel_type provided, return only capacities for that combination.
    If result is N/A-only (meaning seating is not specific/applicable for this vehicle type),
    return only N/A so UI shows 'Other' (which is the N/A representation).
    """
    if not vehicle_type and not fuel_type:
        # Global: exclude N/A but include all seat ranges
        return _distinct_single_values('Seating_Capacity', exclude_tokens=['no'], exclude_na=True)
    filters = []
    if vehicle_type:
        filters.append(('Vehicle_Type', vehicle_type))
    if fuel_type:
        filters.append(('Fuel_Type', fuel_type))
    # Do NOT exclude N/A when filtering by vehicle_type/fuel_type, so we can return N/A if it's the only value
    capacities = _distinct_with_filters('Seating_Capacity', filters, exclude_tokens=['no'], exclude_na=False)
    # If result is N/A-only, keep it (UI will show as "Other")
    # If result is empty, fallback to global distinct (for truly empty cases)
    if not capacities:
        return _distinct_single_values('Seating_Capacity', exclude_tokens=['no'], exclude_na=True)
    return capacities


def get_distinct_ncb_slabs(vehicle_type: str = None, fuel_type: str = None) -> List[str]:
    """Single values only (e.g. Yes, No as separate options).
    When vehicle_type and/or fuel_type provided, return only NCB slabs for that combination.
    If filtered result is empty (only 'All' records), fall back to global NCB slabs."""
    if not vehicle_type and not fuel_type:
        return _distinct_single_values('NCB_Slab')
    filters = []
    if vehicle_type:
        filters.append(('Vehicle_Type', vehicle_type))
    if fuel_type:
        filters.append(('Fuel_Type', fuel_type))
    slabs = _distinct_with_filters('NCB_Slab', filters)
    # If filtered result is empty (only 'All' records), return global NCB slabs
    if not slabs:
        return _distinct_single_values('NCB_Slab')
    return slabs


def get_distinct_cpa_covers(vehicle_type: str = None, fuel_type: str = None) -> List[str]:
    """Single values only.
    When vehicle_type and/or fuel_type provided, return only covers for that combination.
    If filtered result is empty (only 'All' records), fall back to global CPA covers."""
    if not vehicle_type and not fuel_type:
        return _distinct_single_values('CPA_Cover')
    filters = []
    if vehicle_type:
        filters.append(('Vehicle_Type', vehicle_type))
    if fuel_type:
        filters.append(('Fuel_Type', fuel_type))
    covers = _distinct_with_filters('CPA_Cover', filters)
    # If filtered result is empty (only 'All' records), return global CPA covers
    if not covers:
        return _distinct_single_values('CPA_Cover')
    return covers


def get_distinct_zero_depreciation(vehicle_type: str = None, fuel_type: str = None) -> List[str]:
    """Single values only.
    When vehicle_type and/or fuel_type provided, return only options for that combination.
    If filtered result is empty (only 'All' records), fall back to global options."""
    if not vehicle_type and not fuel_type:
        return _distinct_single_values('Zero_Depreciation')
    filters = []
    if vehicle_type:
        filters.append(('Vehicle_Type', vehicle_type))
    if fuel_type:
        filters.append(('Fuel_Type', fuel_type))
    options = _distinct_with_filters('Zero_Depreciation', filters)
    # If filtered result is empty (only 'All' records), return global options
    if not options:
        return _distinct_single_values('Zero_Depreciation')
    return options


def get_distinct_trailers(vehicle_type: str = None) -> List[str]:
    """Single values only (e.g. Yes, No as separate options if both appear in data).
    When vehicle_type provided, return only trailers for that vehicle type.
    If no trailers found for vehicle_type, fallback to global trailers."""
    if not vehicle_type:
        return _distinct_single_values('Trailer')
    
    result = _distinct_single_values_filtered('Trailer', 'Vehicle_Type', vehicle_type)
    # If filtered result is empty (e.g., Tractor only has "No" which is auto-excluded), return global trailers
    if not result:
        return _distinct_single_values('Trailer')
    return result


def _get_except_patterns(field_name: str) -> List[str]:
    """Extract all 'excepted' make/model names from 'except' patterns.
    
    Example: If database has Make = "Except TVS", returns ['TVS']
    
    Returns: List of excepted values
    """
    conn = get_conn()
    try:
        import_id = _get_current_import_id(conn)
        cur = conn.cursor()
        path = f"$.{field_name}"
        
        if import_id:
            sql = f"""
                SELECT DISTINCT 
                    TRIM(SUBSTR(JSON_UNQUOTE(JSON_EXTRACT(raw_json, %s)), 8)) as excepted
                FROM rates 
                WHERE import_id = %s 
                AND (JSON_UNQUOTE(JSON_EXTRACT(raw_json, %s)) LIKE 'Except %%'
                     OR JSON_UNQUOTE(JSON_EXTRACT(raw_json, %s)) LIKE 'except %%')
            """
            cur.execute(sql, (path, import_id, path, path))
        else:
            sql = f"""
                SELECT DISTINCT 
                    TRIM(SUBSTR(JSON_UNQUOTE(JSON_EXTRACT(raw_json, %s)), 8)) as excepted
                FROM rates 
                WHERE (JSON_UNQUOTE(JSON_EXTRACT(raw_json, %s)) LIKE 'Except %%'
                       OR JSON_UNQUOTE(JSON_EXTRACT(raw_json, %s)) LIKE 'except %%')
            """
            cur.execute(sql, (path, path, path))
        
        results = [r[0] for r in cur.fetchall() if r[0]]
        cur.close()
        return results
    except Exception as e:
        print(f'[DB] _get_except_patterns error: {e}')
        return []
    finally:
        conn.close()


def _build_except_match_condition(json_key: str, user_val: str) -> tuple:
    """Build SQL condition that handles "except" patterns in Make/Model fields.
    
    For a user input (e.g., user_val='Mahindra'), this creates a condition that:
    1. Matches exact values: Make = 'Mahindra'
    2. Matches comma-separated values: Make LIKE '%,Mahindra,%'
    3. Matches except patterns (inversely):
       - If database has Make = "Except TVS"
       - And user selects Mahindra
       - Then we MATCH this record (because Mahindra ≠ TVS)
       - But if user selects TVS, we DON'T match (because TVS is excepted)
    
    Returns: (sql_condition_string, params_list)
    """
    key = _JSON_KEY_MAP.get(json_key, json_key.replace('_', ' ').title().replace(' ', '_'))
    user_val_stripped = str(user_val).strip()
    
    # Condition 1: Exact match
    exact_match = f"JSON_UNQUOTE(JSON_EXTRACT(r.raw_json, '$.{key}')) = %s"
    
    # Condition 2: Comma-separated match (e.g., "Petrol,Diesel")
    comma_match = (
        f"CONCAT(',', REPLACE(REPLACE(TRIM(COALESCE(JSON_UNQUOTE(JSON_EXTRACT(r.raw_json, '$.{key}')), '')), ', ', ','), ' ,', ','), ',') "
        f"LIKE CONCAT('%,', TRIM(%s), ',%')"
    )
    
    # Condition 3: Except pattern match (inverse)
    # Match "Except X" pattern where user value is NOT "X"
    # Example: "Except TVS" matches when user selects Bajaj, Mahindra, etc., but NOT TVS
    except_match = (
        f"("
        f"(JSON_UNQUOTE(JSON_EXTRACT(r.raw_json, '$.{key}')) LIKE 'Except %%' "
        f"OR JSON_UNQUOTE(JSON_EXTRACT(r.raw_json, '$.{key}')) LIKE 'except %%') "
        f"AND JSON_UNQUOTE(JSON_EXTRACT(r.raw_json, '$.{key}')) NOT LIKE CONCAT('%%', %s, '%%')"
        f")"
    )
    
    # Combine all conditions with OR
    condition = f"({exact_match} OR {comma_match} OR {except_match})"
    params = [user_val_stripped, user_val_stripped, user_val_stripped]
    
    return condition, params


def get_distinct_makes(vehicle_type: str = None) -> List[str]:
    """Single values only: each make as separate option; exclude 'All', 'All make', 'N/A', and except patterns from display.
    When vehicle_type provided, return only makes for that vehicle type."""
    if not vehicle_type:
        makes = _distinct_single_values('Make', exclude_tokens=['all', 'all make', 'n/a'])
    else:
        makes = _distinct_single_values_filtered('Make', 'Vehicle_Type', vehicle_type, exclude_tokens=['all', 'all make', 'n/a'])
    
    # Filter out "Except X" patterns from dropdown (these are handled internally by except logic)
    return [m for m in makes if not m.lower().startswith('except ')]


def get_distinct_models(make: str = None, vehicle_type: str = None) -> List[str]:
    """Single values only; when make and/or vehicle_type given, filter accordingly.
    If both provided, returns models for that make AND vehicle_type combination.
    Filters out except patterns from dropdown."""
    if not make and not vehicle_type:
        models = _distinct_single_values('Model', exclude_tokens=['all', 'n/a'])
    else:
        filters = []
        if make:
            filters.append(('Make', make))
        if vehicle_type:
            filters.append(('Vehicle_Type', vehicle_type))
        if len(filters) == 1:
            col, val = filters[0]
            models = _distinct_single_values_filtered('Model', col, val, exclude_tokens=['all', 'n/a'])
        else:
            models = _distinct_with_filters('Model', filters, exclude_tokens=['all', 'n/a'])
    
    # Filter out "except X" patterns from dropdown (these are handled internally by except logic)
    return [m for m in models if not m.lower().startswith('except ')]


def get_top_5_payouts(**filters) -> List[dict]:
    """Return top payouts matching given filters. Uses current import batch.

    This implements basic matching: rto included/excluded logic, slab numeric checks (if provided), and flexible matching for other fields using JSON values.
    Handles comma-separated values and 'All' wildcards.
    """
    conn = get_conn()
    cur = conn.cursor(dictionary=True)
    import_id = _get_current_import_id(conn)

    # build base
    params = []
    where_clauses = []
    if import_id:
        where_clauses.append('r.import_id = %s')
        params.append(import_id)

    # State filter (code or display name)
    state = filters.get('state')
    if state and state != 'N/A':
        try:
            from config import STATE_DISPLAY_MAP
            state_display = STATE_DISPLAY_MAP.get(state, state)
            where_clauses.append("(JSON_UNQUOTE(JSON_EXTRACT(r.raw_json, '$.State')) = %s OR JSON_UNQUOTE(JSON_EXTRACT(r.raw_json, '$.State')) = %s)")
            params.extend([state, state_display])
        except Exception:
            where_clauses.append("JSON_UNQUOTE(JSON_EXTRACT(r.raw_json, '$.State')) = %s")
            params.append(state)

    # RTO: 01,02,03 means applicable for 01 OR 02 OR 03 (importer already stores each in rate_included_rto)
    rto = filters.get('rto_code')
    if rto and rto != 'N/A':
        rto_clause = ("(EXISTS (SELECT 1 FROM rate_included_rto i JOIN rto rr ON rr.id = i.rto_id WHERE i.rate_id = r.id AND rr.code = %s) "
                      "OR (r.applies_all_rto = 1 AND NOT EXISTS (SELECT 1 FROM rate_excluded_rto e JOIN rto re ON re.id = e.rto_id WHERE e.rate_id = r.id AND re.code = %s)))")
        where_clauses.append(rto_clause)
        params.extend([rto, rto])

    # Helper: comma-separated match — row matches if user value is one of the comma-separated tokens (whole-token, not substring)
    def _comma_sep_match(json_key: str, user_val: str, allow_all: bool = False) -> None:
        # Use mapping if available, otherwise use title case conversion
        key = _JSON_KEY_MAP.get(json_key, json_key.replace('_', ' ').title().replace(' ', '_'))
        norm = f"CONCAT(',', REPLACE(REPLACE(TRIM(COALESCE(JSON_UNQUOTE(JSON_EXTRACT(r.raw_json, '$.{key}')), '')), ', ', ','), ' ,', ','), ',')"
        cond = f"(JSON_UNQUOTE(JSON_EXTRACT(r.raw_json, '$.{key}')) = %s OR {norm} LIKE CONCAT('%,', TRIM(%s), ',%')"
        if allow_all:
            cond += f" OR LOWER(TRIM(COALESCE(JSON_UNQUOTE(JSON_EXTRACT(r.raw_json, '$.{key}')), ''))) IN ('all', 'all make', 'n/a')"
        cond += ")"
        where_clauses.append(cond)
        params.extend([user_val.strip(), user_val.strip()])

    # Fields that can be comma-separated: Petrol,Diesel means applicable for Petrol OR Diesel (whole-token match)
    # Also: Field='All' means applicable to ALL values for that field (wildcard matching)
    comma_sep_fields = ['fuel_type', 'vehicle_type', 'vehicle_category', 'policy_type', 'business_type', 'ncb_slab', 'cpa_cover', 'zero_depreciation', 'trailer']
    for jf in comma_sep_fields:
        val = filters.get(jf)
        if val and str(val).strip():
            # Enable allow_all for all comma-separated fields (so rows with field='All' will match any user selection)
            _comma_sep_match(jf, str(val).strip(), allow_all=True)

    # Make: handle comma-separated values AND "except" patterns
    # Uses new _build_except_match_condition to properly match "Except TVS" etc.
    make_val = filters.get('make')
    if make_val and str(make_val).strip():
        make_stripped = str(make_val).strip().lower()
        # Check if value is 'all' or 'all make' or 'n/a' - if so, skip filtering
        if make_stripped in ('all', 'all make', 'n/a'):
            pass  # No make filter needed
        else:
            cond, parms = _build_except_match_condition('make', make_val.strip())
            where_clauses.append(cond)
            params.extend(parms)

    # Model: handle comma-separated values AND "except" patterns
    # Uses new _build_except_match_condition to properly match "except Bolero" etc.
    model_val = filters.get('model')
    if model_val and str(model_val).strip():
        model_stripped = str(model_val).strip().lower()
        # Check if value is 'all', 'n/a' or 'other' - if so, skip filtering
        if model_stripped in ('all', 'n/a', 'other'):
            pass  # No model filter needed
        else:
            cond, parms = _build_except_match_condition('model', model_val.strip())
            where_clauses.append(cond)
            params.extend(parms)

    # Slab fields: row matches if No/NULL (not applicable) or exact match OR 'All' (wildcard)
    # NOTE: GVW_Slab is NOT included - GVW uses numeric range matching from gvw_min/gvw_max
    simple_json_filters = ['cc_slab', 'watt_slab', 'seating_capacity']
    for jf in simple_json_filters:
        val = filters.get(jf)
        if val and str(val).strip():
            # Special handling: "Other" in UI represents "N/A" in database for seating_capacity
            if jf == 'seating_capacity' and str(val).strip().lower() == 'other':
                val = 'N/A'
            key = _JSON_KEY_MAP.get(jf, jf.replace('_', ' ').title().replace(' ', '_'))

            # For seating_capacity: treat 'N/A' (and No) as wildcard ONLY for PCV (Passenger Commercial Vehicles)
            # and NOT for vehicle_type 'Auto'. For other categories, do not treat 'N/A' as wildcard.
            if jf == 'seating_capacity':
                vcat = str(filters.get('vehicle_category') or '').lower()
                vtype = str(filters.get('vehicle_type') or '').lower()
                is_pcv = ('pcv' in vcat) or ('passenger' in vcat)
                is_auto = (vtype == 'auto')

                if is_pcv and not is_auto:
                    where_clauses.append(
                        f"(JSON_EXTRACT(r.raw_json, '$.{key}') IS NULL OR TRIM(COALESCE(JSON_UNQUOTE(JSON_EXTRACT(r.raw_json, '$.{key}')), '')) = '' "
                        f"OR LOWER(TRIM(JSON_UNQUOTE(JSON_EXTRACT(r.raw_json, '$.{key}')))) IN ('no', 'n/a', 'all') OR JSON_UNQUOTE(JSON_EXTRACT(r.raw_json, '$.{key}')) = %s)"
                    )
                else:
                    where_clauses.append(
                        f"(JSON_EXTRACT(r.raw_json, '$.{key}') IS NULL OR TRIM(COALESCE(JSON_UNQUOTE(JSON_EXTRACT(r.raw_json, '$.{key}')), '')) = '' "
                        f"OR JSON_UNQUOTE(JSON_EXTRACT(r.raw_json, '$.{key}')) = %s)"
                    )
                params.append(val)
            else:
                where_clauses.append(
                    f"(JSON_EXTRACT(r.raw_json, '$.{key}') IS NULL OR TRIM(COALESCE(JSON_UNQUOTE(JSON_EXTRACT(r.raw_json, '$.{key}')), '')) = '' "
                    f"OR LOWER(TRIM(JSON_UNQUOTE(JSON_EXTRACT(r.raw_json, '$.{key}')))) IN ('no', 'n/a', 'all') OR JSON_UNQUOTE(JSON_EXTRACT(r.raw_json, '$.{key}')) = %s)"
                )
                params.append(val)

    # vehicle_age numeric matching
    vage = filters.get('vehicle_age')
    if vage:
        try:
            age_num = int(vage)
            where_clauses.append('(r.age_min IS NULL OR r.age_min <= %s)')
            where_clauses.append('(r.age_max IS NULL OR r.age_max >= %s)')
            params.extend([age_num, age_num])
        except Exception:
            pass

    # Numeric GVW: row with no range (No/NULL) matches any; row with range matches if user value in range
    gvw = filters.get('gvw_value')
    if gvw:
        try:
            gvw_num = float(gvw)
            where_clauses.append(
                '((r.gvw_min IS NULL AND r.gvw_max IS NULL) OR (r.gvw_min IS NOT NULL AND r.gvw_max IS NOT NULL AND r.gvw_min <= %s AND r.gvw_max >= %s))'
            )
            params.extend([gvw_num, gvw_num])
        except Exception:
            pass

    # date validity: ensure system/server date within date range if date fields present
    # If dates are null/empty, applicable to all dates. If dates have values, check if today is within range.
    # Note: JSON null when unquoted becomes the string 'null', so we must check for that
    # Also: dates may be stored with timestamp (2026-01-15T00:00:00), so extract date part with DATE() or SUBSTRING()
    from datetime import date
    today_str = date.today().isoformat()
    where_clauses.append(f"(JSON_EXTRACT(r.raw_json, '$.Date_from') IS NULL OR TRIM(COALESCE(JSON_UNQUOTE(JSON_EXTRACT(r.raw_json, '$.Date_from')), '')) = '' OR LOWER(TRIM(JSON_UNQUOTE(JSON_EXTRACT(r.raw_json, '$.Date_from')))) = 'null' OR CAST(JSON_UNQUOTE(JSON_EXTRACT(r.raw_json, '$.Date_from')) AS DATE) <= '{today_str}')")
    where_clauses.append(f"(JSON_EXTRACT(r.raw_json, '$.Date_till') IS NULL OR TRIM(COALESCE(JSON_UNQUOTE(JSON_EXTRACT(r.raw_json, '$.Date_till')), '')) = '' OR LOWER(TRIM(JSON_UNQUOTE(JSON_EXTRACT(r.raw_json, '$.Date_till')))) = 'null' OR CAST(JSON_UNQUOTE(JSON_EXTRACT(r.raw_json, '$.Date_till')) AS DATE) >= '{today_str}')")

    # assemble
    where_sql = ' AND '.join(where_clauses) if where_clauses else '1'
    
    # DEBUG: Log the SQL query being executed
    print(f"[DB] get_top_5_payouts SQL:")
    print(f"[DB] WHERE clause ({len(where_clauses)} conditions):")
    for i, cond in enumerate(where_clauses):
        print(f"  {i+1}. {cond}")
    print(f"[DB] Parameters ({len(params)}): {params}")
    
    # Return results grouped by condition, with best payout per (condition, company) combination
    # Separate 'No' conditions into General category
    sql = (
        f"SELECT "
        f"  CASE "
        f"    WHEN TRIM(COALESCE(JSON_UNQUOTE(JSON_EXTRACT(r.raw_json, '$.Conditions')), '')) IN ('', 'No', 'N/A', 'null') THEN 'General' "
        f"    ELSE TRIM(JSON_UNQUOTE(JSON_EXTRACT(r.raw_json, '$.Conditions'))) "
        f"  END AS condition_group, "
        f"  JSON_UNQUOTE(JSON_EXTRACT(r.raw_json, '$.Company')) AS company_name, "
        f"  MAX(r.final_payout) AS final_payout "
        f"FROM rates r "
        f"WHERE {where_sql} "
        f"GROUP BY condition_group, company_name "
        f"ORDER BY condition_group, final_payout DESC"
    )

    cur.execute(sql, tuple(params))
    rows = cur.fetchall()
    
    # Format results: group by condition
    results_by_condition = {}
    for row in rows:
        condition = row.get('condition_group') or 'General'
        company = row.get('company_name') or 'Unknown'
        payout = float(row.get('final_payout') or 0.0) * 100
        
        if condition not in results_by_condition:
            results_by_condition[condition] = []
        results_by_condition[condition].append({
            'company': company,
            'payout': payout
        })
    
    # Build final results: flatten all, sort globally by payout, take top 5
    all_results = []
    for condition in results_by_condition.keys():
        for company_data in results_by_condition[condition]:
            all_results.append({
                'conditions': condition if condition != 'General' else '',
                'company_name': company_data['company'],
                'payout_percentage': company_data['payout']
            })
    
    # Sort globally by payout (highest first), then by company name for tie-breaking
    all_results.sort(key=lambda x: (-x['payout_percentage'], x['company_name']))
    
    # Build final results with top 3 only
    results = []
    for rank, result in enumerate(all_results[:3], start=1):
        results.append({
            'rank': rank,
            'conditions': result['conditions'],
            'company_name': result['company_name'],
            'payout_percentage': result['payout_percentage']
        })

    cur.close()
    conn.close()
    return results
