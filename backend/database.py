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
import logging
import ast
import re
from pathlib import Path
from typing import List, Any, Optional, Dict
import mysql.connector
from mysql.connector import pooling, Error
from dotenv import load_dotenv

# Load .env if present so DB credentials from workspace are picked up
load_dotenv()

logger = logging.getLogger(__name__)

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

_RTO_STATES_WITH_CODES = {'TN', 'KA', 'KL', 'AP', 'MH', 'TS', 'PY'}
_RTO_MASTER_CACHE: Optional[dict] = None
_VEHICLE_TYPE_CANONICAL_MAP = {
    'digger & boring machine': 'Digger and Boring machine',
    'digger and boring machine': 'Digger and Boring machine',
    'bacho loader': 'Backho Loader',
    'backho loader': 'Backho Loader',
    'educational bus under school name': 'Educational Bus',
}
_VEHICLE_TYPE_VARIANTS = {
    'digger and boring machine': ['Digger and Boring machine', 'Digger & Boring machine'],
    'digger & boring machine': ['Digger and Boring machine', 'Digger & Boring machine'],
    'backho loader': ['Backho Loader', 'Bacho Loader'],
    'bacho loader': ['Backho Loader', 'Bacho Loader'],
    'educational bus': ['Educational Bus', 'Educational Bus under school name'],
    'educational bus under school name': ['Educational Bus', 'Educational Bus under school name'],
}


def _normalize_rto_code(code: Any) -> str:
    """Normalize raw RTO code to comparable dropdown code."""
    c = str(code).strip().upper()
    if not c:
        return ""
    if c.isdigit():
        return f"{int(c):02d}"
    m = re.fullmatch(r"(\d+)([A-Z]+)", c)
    if m:
        return f"{int(m.group(1)):02d}{m.group(2)}"
    return c


def _rto_sort_key(code: str):
    """Sort numeric-like codes naturally, then suffix."""
    m = re.fullmatch(r"(\d+)([A-Z]*)", str(code).upper())
    if m:
        return (int(m.group(1)), m.group(2))
    return (10**9, str(code).upper())


def _normalize_vehicle_type_label(value: Any) -> str:
    raw = str(value or '').strip()
    if not raw:
        return ''
    return _VEHICLE_TYPE_CANONICAL_MAP.get(raw.lower(), raw)


def _expand_filter_values(filter_column: str, filter_value: str) -> List[str]:
    """Expand filter value into equivalent aliases for matching."""
    if str(filter_column).strip().lower() != 'vehicle_type':
        return [str(filter_value)]
    key = str(filter_value).strip().lower()
    variants = _VEHICLE_TYPE_VARIANTS.get(key)
    if not variants:
        return [str(filter_value)]
    return variants


def _load_rto_master() -> dict:
    """Read RTO master mapping from data/extraction/district_rto file.

    Returns:
        {
            "TN": {
                "01": "CHENNAI CENTRAL RTO",
                ...
            },
            ...
        }
    """
    global _RTO_MASTER_CACHE
    if _RTO_MASTER_CACHE is not None:
        return _RTO_MASTER_CACHE

    path = Path(__file__).resolve().parents[1] / 'data' / 'extraction' / 'district_rto'
    if not path.exists():
        _RTO_MASTER_CACHE = {}
        return _RTO_MASTER_CACHE

    text = path.read_text(encoding='utf-8', errors='ignore')
    marker = 'const rtoMasterData ='
    start = text.find(marker)
    if start < 0:
        _RTO_MASTER_CACHE = {}
        return _RTO_MASTER_CACHE

    brace_start = text.find('{', start)
    if brace_start < 0:
        _RTO_MASTER_CACHE = {}
        return _RTO_MASTER_CACHE

    depth = 0
    brace_end = -1
    for i in range(brace_start, len(text)):
        ch = text[i]
        if ch == '{':
            depth += 1
        elif ch == '}':
            depth -= 1
            if depth == 0:
                brace_end = i
                break

    if brace_end < 0:
        _RTO_MASTER_CACHE = {}
        return _RTO_MASTER_CACHE

    obj_str = text[brace_start:brace_end + 1]
    try:
        parsed = ast.literal_eval(obj_str)
    except Exception:
        parsed = {}

    data: Dict[str, Dict[str, str]] = {}
    if isinstance(parsed, dict):
        for state, code_map in parsed.items():
            if not isinstance(code_map, dict):
                continue
            sc = str(state).strip().upper()
            if sc not in _RTO_STATES_WITH_CODES:
                continue
            cleaned_map: Dict[str, str] = {}
            for code, district_name in code_map.items():
                c = _normalize_rto_code(code)
                if not c:
                    continue
                name = str(district_name).strip() if district_name is not None else ""
                # Keep the first non-empty district name if duplicates exist.
                if c not in cleaned_map or (not cleaned_map[c] and name):
                    cleaned_map[c] = name
            data[sc] = cleaned_map

    _RTO_MASTER_CACHE = data
    return _RTO_MASTER_CACHE

def init_connection_pool(pool_name: str = 'posp_pool', pool_size: int = 5):
    global _POOL
    if _POOL is not None:
        return
    # Support both DB_PASS and DB_PASSWORD environment variable names
    db_pass = os.getenv('DB_PASS') or os.getenv('DB_PASSWORD') or ''
    db_host = os.getenv('DB_HOST', '127.0.0.1')
    db_port = int(os.getenv('DB_PORT', 3306))
    db_user = os.getenv('DB_USER', 'root')
    db_name = os.getenv('DB_NAME', 'rates_db')

    # Ensure configured database exists before creating the pool.
    bootstrap = mysql.connector.connect(
        host=db_host,
        port=db_port,
        user=db_user,
        password=db_pass,
    )
    cur = bootstrap.cursor()
    cur.execute(f"CREATE DATABASE IF NOT EXISTS `{db_name}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
    bootstrap.commit()
    cur.close()
    bootstrap.close()

    cfg = {
        'host': db_host,
        'port': db_port,
        'user': db_user,
        'password': db_pass,
        'database': db_name,
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
        logger.warning("DB connection test failed", exc_info=True)
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
        logger.warning("log_query error", exc_info=True)


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


def _distinct_single_values(column_name: str, exclude_tokens: Optional[List[str]] = None, exclude_na: bool = True, all_imports: bool = False) -> List[str]:
    """Return distinct SINGLE values for dropdown: comma-separated cells are split so UI shows
    e.g. 'Petrol' and 'Diesel' as separate options, not 'Petrol,Diesel' as one.
    Excludes empty, 'All', and optional exclude_tokens (case-insensitive).
    By default also excludes 'N/A', but set exclude_na=False to keep 'N/A' (for seating capacity when it's the only value).
    NOTE: 'No' is NOT excluded by default - pass it explicitly in exclude_tokens if needed.
    
    By default (all_imports=False), queries current import only.
    Set all_imports=True to show values from all imports.
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
            if key.startswith('except ') or key.startswith('declined '):
                continue
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
    all_imports: bool = False,
) -> List[str]:
    """Like _distinct_single_values but only from rows where filter_column equals filter_value
    OR filter_column (comma-separated) contains filter_value as a whole token
    OR filter_column = 'All' (wildcard: applies to all values).
    
    By default (all_imports=False), queries current import only.
    Set all_imports=True to show values from all imports.
    """
    conn = get_conn()
    try:
        import_id = _get_current_import_id(conn) if not all_imports else None
        cur = conn.cursor()
        path_col = f"$.{column_name}"
        path_filt = f"$.{filter_column}"
        filter_values = _expand_filter_values(filter_column, filter_value)
        norm = (
            f"CONCAT(',', REPLACE(REPLACE(TRIM(COALESCE(JSON_UNQUOTE(JSON_EXTRACT(raw_json, '{path_filt}')), '')), ', ', ','), ' ,', ','), ',')"
        )
        token_checks = []
        params: List[str] = []
        for value in filter_values:
            token_checks.append(
                f"(JSON_UNQUOTE(JSON_EXTRACT(raw_json, '{path_filt}')) = %s OR {norm} LIKE CONCAT('%,', %s, ',%'))"
            )
            params.extend([value, value])
        token_checks.append(
            f"LOWER(TRIM(COALESCE(JSON_UNQUOTE(JSON_EXTRACT(raw_json, '{path_filt}')), ''))) = 'all'"
        )
        match_clause = f"({' OR '.join(token_checks)})"

        if import_id:
            sql = (
                f"SELECT DISTINCT JSON_UNQUOTE(JSON_EXTRACT(raw_json, '{path_col}')) FROM rates "
                f"WHERE import_id = %s AND JSON_EXTRACT(raw_json, '{path_col}') IS NOT NULL AND {match_clause}"
            )
            cur.execute(sql, [import_id, *params])
        else:
            sql = (
                f"SELECT DISTINCT JSON_UNQUOTE(JSON_EXTRACT(raw_json, '{path_col}')) FROM rates "
                f"WHERE JSON_EXTRACT(raw_json, '{path_col}') IS NOT NULL AND {match_clause}"
            )
            cur.execute(sql, params)
        raw_list = [r[0] for r in cur.fetchall() if r[0] is not None and str(r[0]).lower() not in ('null', 'none')]
        cur.close()
        exclude = set((exclude_tokens or []) + ['', 'all', 'n/a', 'no'])
        seen = set()
        result = []
        for raw in raw_list:
            for token in _split_comma_cell(raw):
                key = token.lower()
                if key.startswith('except ') or key.startswith('declined '):
                    continue
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
    all_imports: bool = False,
) -> List[str]:
    """Get distinct values for column_name filtered by multiple conditions (AND logic).
    Each filter is checked with: 
    - filter_column = filter_value OR 
    - filter_column (comma-separated) contains filter_value OR
    - filter_column = 'All' (wildcard: applies to all values)
    By default excludes 'N/A', but set exclude_na=False to keep it (e.g., for seating capacity).
    
    By default (all_imports=False), queries current import only.
    Set all_imports=True to show values from all imports.
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
            values = _expand_filter_values(filter_column, filter_value)
            norm = (
                f"CONCAT(',', REPLACE(REPLACE(TRIM(COALESCE(JSON_UNQUOTE(JSON_EXTRACT(raw_json, '{path_filt}')), '')), ', ', ','), ' ,', ','), ',')"
            )
            checks = []
            for value in values:
                checks.append(
                    f"(JSON_UNQUOTE(JSON_EXTRACT(raw_json, '{path_filt}')) = %s OR {norm} LIKE CONCAT('%,', %s, ',%'))"
                )
                params.extend([value, value])
            checks.append(
                f"LOWER(TRIM(COALESCE(JSON_UNQUOTE(JSON_EXTRACT(raw_json, '{path_filt}')), ''))) = 'all'"
            )
            where_conditions.append(f"({' OR '.join(checks)})")
        
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
                if key.startswith('except ') or key.startswith('declined '):
                    continue
                if key in exclude or key in seen:
                    continue
                seen.add(key)
                result.append(token)
        return sorted(result, key=lambda x: (x.lower(), x))
    finally:
        conn.close()


def get_distinct_states() -> List[str]:
    """Return clean state options derived from data (single tokens only)."""
    from .config import STATE_DISPLAY_MAP

    raw_states = _distinct_from_raw('State')
    seen = set()
    states: List[str] = []

    for raw in raw_states:
        if not raw:
            continue
        raw_str = str(raw).strip()
        if raw_str.lower().startswith('except ') or raw_str.lower().startswith('declined '):
            continue
        for token in _split_comma_cell(raw_str):
            key = token.strip().upper()
            if not key:
                continue
            if key.lower() in ('all', 'n/a'):
                continue
            # Convert state code to UI display name when available.
            display = STATE_DISPLAY_MAP.get(key, token.strip())
            norm_key = display.lower()
            if norm_key in seen:
                continue
            seen.add(norm_key)
            states.append(display)

    states = sorted(states, key=lambda x: x.lower())
    if 'Others' not in states:
        states.append('Others')
    return states


def get_distinct_rtos(state: str = None) -> List[str]:
    """Return RTO codes only for configured states.

    UI rule: show RTO only for TN/KA/KL/AP/MH/TS.
    """
    if not state:
        return []

    code = str(state).strip().upper()
    if code not in _RTO_STATES_WITH_CODES:
        return []

    master = _load_rto_master()
    return sorted(list(master.get(code, {}).keys()), key=_rto_sort_key)


def get_distinct_rto_options(state: str = None) -> List[dict]:
    """Return RTO dropdown options with readable district labels."""
    if not state:
        return []

    code = str(state).strip().upper()
    if code not in _RTO_STATES_WITH_CODES:
        return []

    master = _load_rto_master()
    state_map = master.get(code, {})
    options: List[dict] = []

    for rto_code in sorted(state_map.keys(), key=_rto_sort_key):
        district = (state_map.get(rto_code) or "").strip()
        label = f"{rto_code} - {district}" if district else rto_code
        options.append({
            "code": rto_code,
            "name": district,
            "label": label,
        })
    return options


def get_distinct_vehicle_categories() -> List[str]:
    """Single values only: 'GCV' and 'PCV' as separate options, not 'GCV,PCV'."""
    return _distinct_single_values('Vehicle_Category')


def get_distinct_vehicle_types(category: str = None) -> List[str]:
    """Single values only; when category given, only types for that category (comma-sep match)."""
    if not category:
        raw_types = _distinct_single_values('Vehicle_Type')
    else:
        raw_types = _distinct_single_values_filtered('Vehicle_Type', 'Vehicle_Category', category)

    seen = set()
    normalized = []
    for t in raw_types:
        n = _normalize_vehicle_type_label(t)
        key = n.lower()
        if key in seen:
            continue
        seen.add(key)
        normalized.append(n)
    return normalized
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



def get_distinct_fuel_types(vehicle_type: str = None, category: str = None) -> List[str]:
    """Single values only: 'Petrol', 'Diesel' as separate options, not 'Petrol,Diesel'.
    When vehicle_type is provided, return only fuels for that vehicle type.
    If filtered result is empty (e.g., only 'All'), fall back to global fuel types.
    Always includes 'Others' as a catch-all option."""
    filters = []
    if vehicle_type:
        filters.append(('Vehicle_Type', vehicle_type))
    if category:
        filters.append(('Vehicle_Category', category))

    if not filters:
        fuels = _distinct_single_values('Fuel_Type', exclude_tokens=['all'])
    else:
        if len(filters) == 1:
            col, value = filters[0]
            fuels = _distinct_single_values_filtered('Fuel_Type', col, value, exclude_tokens=['all'])
        else:
            fuels = _distinct_with_filters('Fuel_Type', filters, exclude_tokens=['all'])

        # If filtered result is empty (only 'All'/blank rows), return global fuel types
        if not fuels:
            fuels = _distinct_single_values('Fuel_Type', exclude_tokens=['all'])
        else:
            # For Two-Wheeler vehicle types with multi-fuel combos, validate that split tokens exist independently
            two_wheeler_types = ('Scooter', 'Bike', 'Motor Cycle', 'Motorcycle')
            if vehicle_type in two_wheeler_types:
                validated = _fuel_tokens_exist_for_vehicle_type(fuels, vehicle_type)
                # Return validated list if any valid fuels found, else return original
                fuels = validated if validated else fuels
    
    # Two Wheeler rule: allow only Petrol and EV.
    category_norm = (category or "").strip().lower()
    if "two wheeler" in category_norm:
        allowed = []
        for f in fuels:
            fl = (f or "").strip().lower()
            if fl in ("petrol", "ev"):
                allowed.append(f)
        fuels = allowed

    # Always add 'Others' option at the end
    if 'Others' not in fuels:
        fuels.append('Others')
    
    return fuels


def get_distinct_policy_types(vehicle_type: str = None, fuel_type: str = None, category: str = None) -> List[str]:
    """Single values only.
    When vehicle_type and/or fuel_type provided, return only policy types for that combination.
    Do NOT fall back to global - show only what applies to the selected vehicle type."""
    if not vehicle_type and not fuel_type and not category:
        return _distinct_single_values('Policy_Type')
    filters = []
    if vehicle_type:
        filters.append(('Vehicle_Type', vehicle_type))
    if fuel_type:
        filters.append(('Fuel_Type', fuel_type))
    if category:
        filters.append(('Vehicle_Category', category))
    return _distinct_with_filters('Policy_Type', filters)


def get_distinct_business_types(vehicle_type: str = None, fuel_type: str = None, category: str = None) -> List[str]:
    """Single values only.
    When vehicle_type and/or fuel_type provided, return only business types for that combination.
    Do NOT fall back to global - show only what applies to the selected vehicle type."""
    if not vehicle_type and not fuel_type and not category:
        values = _distinct_single_values('Business_Type')
    else:
        filters = []
        if vehicle_type:
            filters.append(('Vehicle_Type', vehicle_type))
        if fuel_type:
            filters.append(('Fuel_Type', fuel_type))
        if category:
            filters.append(('Vehicle_Category', category))
        values = _distinct_with_filters('Business_Type', filters)
    # Business_Type rule:
    # - Only New / Old are valid UI options.
    # - Blank in data is treated as Old (matching handled in get_top_5_payouts).
    cleaned = []
    seen = set()
    for v in values:
        sv = str(v).strip()
        if not sv:
            continue
        low = sv.lower()
        if low in ('renewal', 'rollover'):
            sv = 'Old'
            low = 'old'
        if ',' in sv:
            parts = [p.strip() for p in sv.split(',') if p.strip()]
            for p in parts:
                pl = p.lower()
                if pl in ('renewal', 'rollover'):
                    p = 'Old'
                    pl = 'old'
                if pl in ('new', 'old') and pl not in seen:
                    seen.add(pl)
                    cleaned.append('New' if pl == 'new' else 'Old')
            continue
        if low in ('new', 'old') and low not in seen:
            seen.add(low)
            cleaned.append('New' if low == 'new' else 'Old')

    # Always keep both New and Old as UI options.
    if 'new' not in seen:
        cleaned.append('New')
    if 'old' not in seen:
        cleaned.append('Old')
    return cleaned


def get_distinct_vehicle_ages() -> List[str]:
    """Return UI vehicle-age options.

    Current data is normalized with age_min/age_max, and many rows do not
    carry raw Vehicle_Age text. The UI expects:
    - New (special option)
    - numeric ages (used to render model years in frontend)
    """
    return ['New'] + [str(i) for i in range(1, 51)]


def get_distinct_cc_slabs(vehicle_type: str = None, fuel_type: str = None, category: str = None) -> List[str]:
    """Single values only; exclude No/N/A from dropdown.
    When vehicle_type and/or fuel_type provided, return only CC slabs for that combination.
    For two-wheelers with no specific CC slabs, return hardcoded distinct values.
    If filtered result is empty (only 'All' records), fall back as appropriate."""
    if not vehicle_type and not fuel_type and not category:
        return _distinct_single_values('CC_Slab', exclude_tokens=['no'])
    
    # Check if this is truly Two Wheeler context
    is_two_wheeler = False
    if vehicle_type:
        vt_lower = vehicle_type.lower().strip()
        two_wheeler_keywords = ('scooter', 'bike', 'motorcycle', 'motor cycle', 'two-wheeler', 'two wheeler', 'twowheeler', 'moped')
        is_two_wheeler = any(kw in vt_lower for kw in two_wheeler_keywords) or vt_lower == 'two wheeler'
    if category and 'two wheeler' in category.lower():
        is_two_wheeler = True

    filters = []
    if vehicle_type:
        filters.append(('Vehicle_Type', vehicle_type))
    if fuel_type:
        filters.append(('Fuel_Type', fuel_type))
    if category:
        filters.append(('Vehicle_Category', category))
    slabs = _distinct_with_filters('CC_Slab', filters, exclude_tokens=['no'])

    # For non-two-wheeler categories (notably PCV Taxi), fuel-specific rows may be sparse.
    # If no slabs found with fuel filter, retry with vehicle_type/category only.
    if not slabs and fuel_type and (vehicle_type or category) and not is_two_wheeler:
        fallback_filters = []
        if vehicle_type:
            fallback_filters.append(('Vehicle_Type', vehicle_type))
        if category:
            fallback_filters.append(('Vehicle_Category', category))
        slabs = _distinct_with_filters('CC_Slab', fallback_filters, exclude_tokens=['no'])

    # Two Wheeler: normalize labels and keep only valid CC bands for selected vehicle type context.
    if is_two_wheeler:
        norm_map = {
            'below 75 cc': 'Below 75 CC',
            'below 75': 'Below 75 CC',
            '75 to 150 cc': '75 to 150 CC',
            '75 to 150': '75 to 150 CC',
            '150 to 350 cc': '150 to 350 CC',
            '150 to 350': '150 to 350 CC',
            'above 350 cc': 'Above 350 CC',
            'above 350': 'Above 350 CC',
        }
        ordered = ['Below 75 CC', '75 to 150 CC', '150 to 350 CC', 'Above 350 CC']
        seen = set()
        normalized = []
        for slab in slabs:
            s = str(slab).strip()
            if not s:
                continue
            # Split any accidental comma-combined values and normalize each token.
            for token in [t.strip() for t in s.split(',') if t.strip()]:
                key = token.lower()
                mapped = norm_map.get(key)
                if mapped and mapped not in seen:
                    seen.add(mapped)
                    normalized.append(mapped)
        # Return normalized in fixed business-friendly order.
        return [x for x in ordered if x in seen]

    # If filtered result is empty, avoid leaking unrelated category slabs.
    if not slabs:
        # For non-two-wheeler categories, do not return global CC slabs.
        # This prevents Two-Wheeler ranges from appearing for other vehicle categories.
        return []
    return slabs


def get_distinct_gvw_slabs(vehicle_type: str = None) -> List[str]:
    """Single values only; exclude No/N/A.
    When vehicle_type provided, return only GVW slabs for that vehicle type."""
    if not vehicle_type:
        return _distinct_single_values('GVW_Slab', exclude_tokens=['no'])
    return _distinct_single_values_filtered('GVW_Slab', 'Vehicle_Type', vehicle_type, exclude_tokens=['no'])


def get_distinct_watt_slabs(vehicle_type: str = None, fuel_type: str = None, category: str = None) -> List[str]:
    """Single values only; exclude No/N/A.
    When vehicle_type and/or fuel_type provided, return only watt slabs for that combination.
    If filtered result is empty (only 'All' records), fall back to global watt slabs."""
    if not vehicle_type and not fuel_type and not category:
        return _distinct_single_values('Watt_Slab', exclude_tokens=['no'])
    filters = []
    if vehicle_type:
        filters.append(('Vehicle_Type', vehicle_type))
    if fuel_type:
        filters.append(('Fuel_Type', fuel_type))
    if category:
        filters.append(('Vehicle_Category', category))
    slabs = _distinct_with_filters('Watt_Slab', filters, exclude_tokens=['no'])
    # If filtered result is empty (only 'All' records), return global watt slabs
    if not slabs:
        return _distinct_single_values('Watt_Slab', exclude_tokens=['no'])
    return slabs


def get_distinct_seating_capacities(vehicle_type: str = None, fuel_type: str = None, category: str = None) -> List[str]:
    """Single values only; exclude No/N/A.
    When vehicle_type and/or fuel_type provided, return only capacities for that combination.
    If result is N/A-only (meaning seating is not specific/applicable for this vehicle type),
    return only N/A so UI shows 'Other' (which is the N/A representation).
    """
    if not vehicle_type and not fuel_type and not category:
        # Global: exclude N/A but include all seat ranges
        return _distinct_single_values('Seating_Capacity', exclude_tokens=['no'], exclude_na=True)
    filters = []
    if vehicle_type:
        filters.append(('Vehicle_Type', vehicle_type))
    if fuel_type:
        filters.append(('Fuel_Type', fuel_type))
    if category:
        filters.append(('Vehicle_Category', category))
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
        logger.warning("_get_except_patterns error", exc_info=True)
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
    
    # Condition: treat NULL/blank/'All' as wildcard (applies to all makes)
    # Match exact value, comma-separated inclusion, OR inverse-except patterns.
    # Examples:
    #  - Make = 'Honda' matches when user selects Honda
    #  - Make = 'Hero,Honda' matches when user selects Honda
    #  - Make = 'Except TVS' matches when user selects any make except TVS
    #  - Make = NULL / '' / 'All' matches any user selection (wildcard)

    # Exact match or wildcard (NULL/blank)
    exact_match = (
        f"(JSON_EXTRACT(r.raw_json, '$.{key}') IS NULL "
        f"OR TRIM(COALESCE(JSON_UNQUOTE(JSON_EXTRACT(r.raw_json, '$.{key}')), '')) = '' "
        f"OR LOWER(TRIM(COALESCE(JSON_UNQUOTE(JSON_EXTRACT(r.raw_json, '$.{key}')), ''))) IN ('null', 'none') "
        f"OR JSON_UNQUOTE(JSON_EXTRACT(r.raw_json, '$.{key}')) = %s)"
    )

    # Comma-separated match (e.g., "Petrol,Diesel")
    comma_match = (
        f"CONCAT(',', REPLACE(REPLACE(TRIM(COALESCE(JSON_UNQUOTE(JSON_EXTRACT(r.raw_json, '$.{key}')), '')), ', ', ','), ' ,', ','), ',') "
        f"LIKE CONCAT('%,', TRIM(%s), ',%')"
    )

    # Exclusion pattern match (inverse): "Except X"/"Declined X" matches when user selection != X
    except_match = (
        f"((JSON_UNQUOTE(JSON_EXTRACT(r.raw_json, '$.{key}')) LIKE 'Except %%' "
        f"OR JSON_UNQUOTE(JSON_EXTRACT(r.raw_json, '$.{key}')) LIKE 'except %%' "
        f"OR JSON_UNQUOTE(JSON_EXTRACT(r.raw_json, '$.{key}')) LIKE 'Declined %%' "
        f"OR JSON_UNQUOTE(JSON_EXTRACT(r.raw_json, '$.{key}')) LIKE 'declined %%') "
        f"AND JSON_UNQUOTE(JSON_EXTRACT(r.raw_json, '$.{key}')) NOT LIKE CONCAT('%%', %s, '%%'))"
    )

    # Also accept explicit 'all'/'all make'/'n/a' tokens as wildcards
    all_tokens_cond = f"LOWER(TRIM(COALESCE(JSON_UNQUOTE(JSON_EXTRACT(r.raw_json, '$.{key}')), ''))) IN ('all', 'all make', 'n/a')"

    condition = f"({exact_match} OR {comma_match} OR {except_match} OR {all_tokens_cond})"
    params = [user_val_stripped, user_val_stripped, user_val_stripped]
    
    return condition, params


def get_distinct_makes(vehicle_type: str = None, category: str = None, fuel_type: str = None) -> List[str]:
    """Single values only: each make as separate option; exclude 'All', 'All make', 'N/A', and except patterns from display.
    When vehicle_type provided, return only makes for that vehicle type.
    Always includes 'Others' as a catch-all option (no duplicates)."""
    if not vehicle_type and not category and not fuel_type:
        makes = _distinct_single_values('Make', exclude_tokens=['all', 'all make', 'n/a'])
    else:
        filters = []
        if vehicle_type:
            filters.append(('Vehicle_Type', vehicle_type))
        if category:
            filters.append(('Vehicle_Category', category))
        if fuel_type:
            filters.append(('Fuel_Type', fuel_type))
        if len(filters) == 1:
            col, val = filters[0]
            makes = _distinct_single_values_filtered('Make', col, val, exclude_tokens=['all', 'all make', 'n/a'])
        else:
            makes = _distinct_with_filters('Make', filters, exclude_tokens=['all', 'all make', 'n/a'])
    
    # Filter out "Except X" patterns from dropdown (these are handled internally by except logic)
    filtered = [m for m in makes if not m.lower().startswith('except ') and not m.lower().startswith('declined ')]
    
    # Deduplicate while preserving order, then add 'Others' only once
    seen = set()
    unique = []
    for m in filtered:
        if m.lower() not in seen:
            seen.add(m.lower())
            unique.append(m)
    
    # Always add 'Others' option at the end (if not already present)
    if 'others' not in seen:
        unique.append('Others')
    
    return unique


def get_distinct_models(make: str = None, vehicle_type: str = None, category: str = None) -> List[str]:
    """Single values only; when make and/or vehicle_type given, filter accordingly.
    If both provided, returns models for that make AND vehicle_type combination.
    Filters out except patterns from dropdown."""
    # Enforce explicit Private Car make->model mapping required by UI rules.
    if category and str(category).strip().lower() == "private car":
        private_car_model_map = {
            "honda": [],
            "hyundai": ["Getz"],
            "kia": [],
            "mahindra": ["Bolero"],
            "maruti": ["Eeco", "Omni"],
            "tata": ["Indica", "Indigo", "Sumo"],
            "toyota": ["Qualis"],
            "others": ["Chevrolet", "GM", "Obsolete Models", "Tavera"],
        }
        mk = str(make or "").strip().lower()
        if mk:
            models = list(private_car_model_map.get(mk, []))
            if not any(str(m).strip().lower() == "others" for m in models):
                models.append("Others")
            return models
        # When make is not selected yet, show union of all mapped models.
        union_models = []
        seen = set()
        for vals in private_car_model_map.values():
            for m in vals:
                k = m.lower()
                if k in seen:
                    continue
                seen.add(k)
                union_models.append(m)
        union_models.append("Others")
        return union_models

    if not make and not vehicle_type and not category:
        models = _distinct_single_values('Model', exclude_tokens=['all', 'n/a'])
    else:
        filters = []
        if make:
            filters.append(('Make', make))
        if vehicle_type:
            filters.append(('Vehicle_Type', vehicle_type))
        if category:
            filters.append(('Vehicle_Category', category))
        if len(filters) == 1:
            col, val = filters[0]
            models = _distinct_single_values_filtered('Model', col, val, exclude_tokens=['all', 'n/a'])
        else:
            models = _distinct_with_filters('Model', filters, exclude_tokens=['all', 'n/a'])
    
    # Filter out "except X" patterns from dropdown (handled in backend matching)
    filtered = [m for m in models if not m.lower().startswith('except ') and not m.lower().startswith('declined ')]
    seen = {m.lower() for m in filtered}
    if 'others' not in seen:
        filtered.append('Others')
    return filtered


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
    # Handle 'Others' as wildcard, 'Except X' patterns, and regular states
    state = filters.get('state')
    if state and state != 'N/A':
        if state.lower() == 'others':
            # 'Others' in UI means non-listed states.
            # Match blank/null states and generic exclusion-style states (Except/Declined ...),
            # but do not match specific explicit states like 'TN' or 'AP,TS'.
            where_clauses.append(
                "(JSON_EXTRACT(r.raw_json, '$.State') IS NULL "
                "OR TRIM(COALESCE(JSON_UNQUOTE(JSON_EXTRACT(r.raw_json, '$.State')), '')) = '' "
                "OR LOWER(TRIM(COALESCE(JSON_UNQUOTE(JSON_EXTRACT(r.raw_json, '$.State')), ''))) IN ('null', 'none') "
                "OR LOWER(TRIM(COALESCE(JSON_UNQUOTE(JSON_EXTRACT(r.raw_json, '$.State')), ''))) LIKE 'except %' "
                "OR LOWER(TRIM(COALESCE(JSON_UNQUOTE(JSON_EXTRACT(r.raw_json, '$.State')), ''))) LIKE 'declined %')"
            )
        else:
            # Use except/multi-value aware matching for State (supports 'Except X' patterns and comma-separated lists)
            cond, parms = _build_except_match_condition('state', state)
            where_clauses.append(cond)
            params.extend(parms)

    # RTO: 01,02,03 means applicable for 01 OR 02 OR 03 (importer already stores each in rate_included_rto)
    # Handle 'Others' as wildcard for unmatched RTOs
    rto = filters.get('rto_code')
    if rto and rto != 'N/A':
        if rto.lower() == 'others':
            # 'Others' RTO: matches records where applies_all_rto=1 with no exclusions
            where_clauses.append("(r.applies_all_rto = 1 AND NOT EXISTS (SELECT 1 FROM rate_excluded_rto WHERE rate_id = r.id))")
        else:
            rto_clause = ("(EXISTS (SELECT 1 FROM rate_included_rto i JOIN rto rr ON rr.id = i.rto_id WHERE i.rate_id = r.id AND rr.code = %s) "
                          "OR (r.applies_all_rto = 1 AND NOT EXISTS (SELECT 1 FROM rate_excluded_rto e JOIN rto re ON re.id = e.rto_id WHERE e.rate_id = r.id AND re.code = %s)))")
            where_clauses.append(rto_clause)
            params.extend([rto, rto])

    # Helper: comma-separated match — row matches if user value is one of the comma-separated tokens (whole-token, not substring)
    def _comma_sep_match(json_key: str, user_val: str, allow_all: bool = False) -> None:
        # Use mapping if available, otherwise use title case conversion
        key = _JSON_KEY_MAP.get(json_key, json_key.replace('_', ' ').title().replace(' ', '_'))
        raw_expr = f"JSON_UNQUOTE(JSON_EXTRACT(r.raw_json, '$.{key}'))"
        norm = f"CONCAT(',', REPLACE(REPLACE(TRIM(COALESCE({raw_expr}, '')), ', ', ','), ' ,', ','), ',')"
        values = _expand_filter_values(key, user_val) if key == 'Vehicle_Type' else [user_val]
        value_checks = []
        for v in values:
            value_checks.append(
                f"({raw_expr} = %s OR {norm} LIKE CONCAT('%,', TRIM(%s), ',%'))"
            )
            params.extend([v.strip(), v.strip()])
        value_match = " OR ".join(value_checks)

        # Support exclusion patterns in comma-sep fields too:
        # Example: Vehicle_Type='Except Ambulance' should match all values except Ambulance.
        exclusion_checks = [f"{raw_expr} NOT LIKE CONCAT('%%', %s, '%%')" for _ in values]
        exclusion_match = " AND ".join(exclusion_checks) if exclusion_checks else "1=1"
        except_match = (
            f"(({raw_expr} LIKE 'Except %%' "
            f"OR {raw_expr} LIKE 'except %%' "
            f"OR {raw_expr} LIKE 'Declined %%' "
            f"OR {raw_expr} LIKE 'declined %%') "
            f"AND {exclusion_match})"
        )
        for v in values:
            params.append(v.strip())

        # Match: exact, comma-separated, NULL/blank (blank = wildcard), or 'all'/'n/a'
        cond = (
            f"(JSON_EXTRACT(r.raw_json, '$.{key}') IS NULL "
            f"OR TRIM(COALESCE(JSON_UNQUOTE(JSON_EXTRACT(r.raw_json, '$.{key}')), '')) = '' "
            f"OR LOWER(TRIM(COALESCE(JSON_UNQUOTE(JSON_EXTRACT(r.raw_json, '$.{key}')), ''))) IN ('null', 'none') "
            f"OR {value_match} "
            f"OR {except_match}"
        )
        if allow_all:
            cond += f" OR LOWER(TRIM(COALESCE(JSON_UNQUOTE(JSON_EXTRACT(r.raw_json, '$.{key}')), ''))) IN ('all', 'all make', 'n/a')"
        cond += ")"
        where_clauses.append(cond)

    # Fields that can be comma-separated: Petrol,Diesel means applicable for Petrol OR Diesel (whole-token match)
    # Also: Field='All' means applicable to ALL values for that field (wildcard matching)
    # NOTE: cc_slab moved from simple_json_filters to here to support composite values like "150 to 350 CC, 75 to 150 CC"
    comma_sep_fields = ['fuel_type', 'vehicle_type', 'vehicle_category', 'policy_type', 'ncb_slab', 'cpa_cover', 'zero_depreciation', 'trailer', 'cc_slab']
    for jf in comma_sep_fields:
        val = filters.get(jf)
        if val and str(val).strip():
            # Enable allow_all for all comma-separated fields (so rows with field='All' will match any user selection)
            _comma_sep_match(jf, str(val).strip(), allow_all=True)

    # Business_Type special rule:
    # - blank in row means applicable to Old only
    # - explicit values should match normally
    # - renewal/rollover inputs are normalized to Old
    business_val = filters.get('business_type')
    if business_val and str(business_val).strip():
        selected = str(business_val).strip()
        selected_low = selected.lower()
        if selected_low in ('renewal', 'rollover'):
            selected = 'Old'
            selected_low = 'old'
        key = _JSON_KEY_MAP['business_type']
        norm = f"CONCAT(',', REPLACE(REPLACE(TRIM(COALESCE(JSON_UNQUOTE(JSON_EXTRACT(r.raw_json, '$.{key}')), '')), ', ', ','), ' ,', ','), ',')"

        common_part = (
            f"(JSON_UNQUOTE(JSON_EXTRACT(r.raw_json, '$.{key}')) = %s "
            f"OR {norm} LIKE CONCAT('%,', TRIM(%s), ',%') "
            f"OR LOWER(TRIM(COALESCE(JSON_UNQUOTE(JSON_EXTRACT(r.raw_json, '$.{key}')), ''))) IN ('all', 'n/a'))"
        )
        params.extend([selected, selected])

        if selected_low == 'old':
            # For these values, blank business rows are also applicable.
            cond = (
                f"(JSON_EXTRACT(r.raw_json, '$.{key}') IS NULL "
                f"OR TRIM(COALESCE(JSON_UNQUOTE(JSON_EXTRACT(r.raw_json, '$.{key}')), '')) = '' "
                f"OR LOWER(TRIM(COALESCE(JSON_UNQUOTE(JSON_EXTRACT(r.raw_json, '$.{key}')), ''))) IN ('null', 'none') "
                f"OR {common_part})"
            )
        else:
            # For New, blank should NOT match.
            cond = common_part
        where_clauses.append(cond)

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
    # NOTE: CC_Slab moved to comma_sep_fields above to support composite values
    simple_json_filters = ['watt_slab', 'seating_capacity']
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
                        f"OR LOWER(TRIM(JSON_UNQUOTE(JSON_EXTRACT(r.raw_json, '$.{key}')))) IN ('no', 'n/a', 'all', 'null', 'none') OR JSON_UNQUOTE(JSON_EXTRACT(r.raw_json, '$.{key}')) = %s)"
                    )
                else:
                    where_clauses.append(
                        f"(JSON_EXTRACT(r.raw_json, '$.{key}') IS NULL OR TRIM(COALESCE(JSON_UNQUOTE(JSON_EXTRACT(r.raw_json, '$.{key}')), '')) = '' "
                        f"OR LOWER(TRIM(JSON_UNQUOTE(JSON_EXTRACT(r.raw_json, '$.{key}')))) IN ('null', 'none') "
                        f"OR JSON_UNQUOTE(JSON_EXTRACT(r.raw_json, '$.{key}')) = %s)"
                    )
                params.append(val)
            else:
                where_clauses.append(
                    f"(JSON_EXTRACT(r.raw_json, '$.{key}') IS NULL OR TRIM(COALESCE(JSON_UNQUOTE(JSON_EXTRACT(r.raw_json, '$.{key}')), '')) = '' "
                    f"OR LOWER(TRIM(JSON_UNQUOTE(JSON_EXTRACT(r.raw_json, '$.{key}')))) IN ('no', 'n/a', 'all', 'null', 'none') OR JSON_UNQUOTE(JSON_EXTRACT(r.raw_json, '$.{key}')) = %s)"
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

    # GVW slab overlap matching:
    # - UI sends gvw_slab as "min|max" (example: "25|40", "40|MAX")
    # - Match rows whose [gvw_min, gvw_max] overlaps selected slab.
    gvw_slab = filters.get('gvw_slab')
    if gvw_slab and str(gvw_slab).strip():
        try:
            slab_raw = str(gvw_slab).strip()
            parts = [p.strip() for p in slab_raw.split('|')]
            if len(parts) == 2:
                slab_min = float(parts[0])
                slab_max = None if parts[1].upper() == 'MAX' else float(parts[1])

                if slab_max is None:
                    # Selected range is [slab_min, +inf)
                    # Overlap with row range exists if row_max is NULL(open-ended) or row_max >= slab_min
                    where_clauses.append(
                        '(r.gvw_min IS NOT NULL AND (r.gvw_max IS NULL OR r.gvw_max >= %s))'
                    )
                    params.append(slab_min)
                else:
                    # Overlap between [row_min, row_max_or_inf] and [slab_min, slab_max]
                    # row_min <= slab_max AND (row_max IS NULL OR row_max >= slab_min)
                    where_clauses.append(
                        '(r.gvw_min IS NOT NULL AND r.gvw_min <= %s AND (r.gvw_max IS NULL OR r.gvw_max >= %s))'
                    )
                    params.extend([slab_max, slab_min])
        except Exception:
            pass

    # Numeric GVW fallback: if explicit numeric value provided, use point-in-range matching
    gvw = filters.get('gvw_value')
    if gvw and not (gvw_slab and str(gvw_slab).strip()):
        try:
            gvw_num = float(gvw)
            vehicle_category_val = str(filters.get('vehicle_category') or '').strip().lower()
            vehicle_type_val = str(filters.get('vehicle_type') or '').strip().lower()
            is_gcv_four_wheeler = (
                'gcv' in vehicle_category_val and
                ('4 wheeler' in vehicle_type_val or '4 wheeler goods' in vehicle_type_val)
            )

            if is_gcv_four_wheeler:
                # For GCV 4-wheeler, enforce strict slab matching:
                # show only payouts whose GVW range contains the entered value.
                where_clauses.append(
                    '(r.gvw_min IS NOT NULL AND r.gvw_max IS NOT NULL AND r.gvw_min <= %s AND r.gvw_max >= %s)'
                )
                params.extend([gvw_num, gvw_num])
            else:
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
    where_clauses.append("(JSON_EXTRACT(r.raw_json, '$.Date_from') IS NULL OR TRIM(COALESCE(JSON_UNQUOTE(JSON_EXTRACT(r.raw_json, '$.Date_from')), '')) = '' OR LOWER(TRIM(JSON_UNQUOTE(JSON_EXTRACT(r.raw_json, '$.Date_from')))) = 'null' OR CAST(JSON_UNQUOTE(JSON_EXTRACT(r.raw_json, '$.Date_from')) AS DATE) <= %s)")
    params.append(today_str)
    where_clauses.append("(JSON_EXTRACT(r.raw_json, '$.Date_till') IS NULL OR TRIM(COALESCE(JSON_UNQUOTE(JSON_EXTRACT(r.raw_json, '$.Date_till')), '')) = '' OR LOWER(TRIM(JSON_UNQUOTE(JSON_EXTRACT(r.raw_json, '$.Date_till')))) = 'null' OR CAST(JSON_UNQUOTE(JSON_EXTRACT(r.raw_json, '$.Date_till')) AS DATE) >= %s)")
    params.append(today_str)

    # assemble
    where_sql = ' AND '.join(where_clauses) if where_clauses else '1'
    
    logger.debug(
        "get_top_5_payouts query built (conditions=%d, params=%d)",
        len(where_clauses),
        len(params),
    )
    vehicle_category_val = str(filters.get('vehicle_category') or '').strip().lower()
    is_pcv_request = ('pcv' in vehicle_category_val) or ('passenger' in vehicle_category_val)

    if is_pcv_request:
        seating_present = (
            "LOWER(TRIM(COALESCE(JSON_UNQUOTE(JSON_EXTRACT(r.raw_json, '$.Seating_Capacity')), ''))) "
            "NOT IN ('', 'no', 'n/a', 'all', 'null')"
        )
        conditions_present = (
            "LOWER(TRIM(COALESCE(JSON_UNQUOTE(JSON_EXTRACT(r.raw_json, '$.Conditions')), ''))) "
            "NOT IN ('', 'no', 'n/a', 'all', 'null')"
        )
        seating_text = (
            "CASE WHEN "
            + seating_present
            + " THEN CONCAT(TRIM(JSON_UNQUOTE(JSON_EXTRACT(r.raw_json, '$.Seating_Capacity'))), ' seating') ELSE '' END"
        )
        conditions_text = (
            "CASE WHEN "
            + conditions_present
            + " THEN CONCAT(CASE WHEN "
            + seating_present
            + " THEN ', ' ELSE '' END, TRIM(JSON_UNQUOTE(JSON_EXTRACT(r.raw_json, '$.Conditions')))) ELSE '' END"
        )
        combined_condition = f"TRIM(CONCAT({seating_text}, {conditions_text}))"
        condition_group_expr = f"CASE WHEN {combined_condition} = '' THEN 'General' ELSE {combined_condition} END"
    else:
        condition_group_expr = (
            "CASE "
            "WHEN TRIM(COALESCE(JSON_UNQUOTE(JSON_EXTRACT(r.raw_json, '$.Conditions')), '')) IN ('', 'No', 'N/A', 'null') THEN 'General' "
            "ELSE TRIM(JSON_UNQUOTE(JSON_EXTRACT(r.raw_json, '$.Conditions'))) "
            "END"
        )

    # Return results grouped by condition, with best payout per (condition, company) combination
    # Separate 'No' conditions into General category
    sql = (
        f"SELECT "
        f"  {condition_group_expr} AS condition_group, "
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
        payout_raw = float(row.get('final_payout') or 0.0)
        payout = payout_raw * 100 if 0 < payout_raw < 1 else payout_raw
        
        if condition not in results_by_condition:
            results_by_condition[condition] = []
        results_by_condition[condition].append({
            'company': company,
            'payout': payout
        })
    
    # Build final results: flatten all and sort globally by payout
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

    # Top-5 insurer grouping logic:
    # - rank by unique insurer (best payout per insurer)
    # - for pan-India insurers, if both Commission on OD and Commission on TP exist
    #   in matched rows, include both rows with SAME rank number
    pan_india_insurers = {
        'national insurance',
        'new india',
        'oriental insurance',
        'united india',
    }

    company_best = []
    seen_companies = set()
    for row in all_results:
        cname = (row.get('company_name') or '').strip()
        ckey = cname.lower()
        if not ckey or ckey in seen_companies:
            continue
        seen_companies.add(ckey)
        company_best.append(cname)
        if len(company_best) >= 5:
            break

    def _condition_type(text: str) -> str:
        t = (text or '').strip().lower()
        if 'commission on od' in t:
            return 'od'
        if 'commission on tp' in t:
            return 'tp'
        return ''

    results = []
    for rank, company_name in enumerate(company_best, start=1):
        company_rows = [r for r in all_results if (r.get('company_name') or '').strip() == company_name]
        if not company_rows:
            continue

        company_key = company_name.strip().lower()
        od_rows = [r for r in company_rows if _condition_type(r.get('conditions', '')) == 'od']
        tp_rows = [r for r in company_rows if _condition_type(r.get('conditions', '')) == 'tp']

        if company_key in pan_india_insurers and od_rows and tp_rows:
            best_od = sorted(od_rows, key=lambda x: (-x['payout_percentage'], x.get('conditions', '')))[0]
            best_tp = sorted(tp_rows, key=lambda x: (-x['payout_percentage'], x.get('conditions', '')))[0]
            pair_rows = sorted([best_od, best_tp], key=lambda x: -x['payout_percentage'])
            for row in pair_rows:
                results.append({
                    'rank': rank,
                    'conditions': row['conditions'],
                    'company_name': company_name,
                    'payout_percentage': row['payout_percentage']
                })
        else:
            best_row = company_rows[0]
            results.append({
                'rank': rank,
                'conditions': best_row['conditions'],
                'company_name': company_name,
                'payout_percentage': best_row['payout_percentage']
            })

    cur.close()
    conn.close()
    return results
