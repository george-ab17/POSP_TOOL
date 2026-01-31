"""
Database operations for POSP Payout Checker
Handles all MySQL database interactions
"""

import mysql.connector
from mysql.connector import Error, pooling
from config import (
    DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME,
    DB_POOL_SIZE, COMMISSION_AFFECTING_PARAMS, REST_OF_CHENNAI
)
from typing import List, Dict, Optional

# ==================== DATABASE CONNECTION POOL ====================

connection_pool = None

def init_connection_pool():
    """Initialize MySQL connection pool"""
    global connection_pool
    try:
        connection_pool = pooling.MySQLConnectionPool(
            pool_name="posp_pool",
            pool_size=DB_POOL_SIZE,
            pool_reset_session=True,
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME
        )
        print("[DATABASE] Connection pool initialized successfully")
        return True
    except Error as e:
        print(f"[DATABASE ERROR] Failed to initialize pool: {e}")
        return False

def get_connection():
    """Get a connection from the pool"""
    global connection_pool
    if connection_pool is None:
        init_connection_pool()
    return connection_pool.get_connection()

# ==================== DATABASE OPERATIONS ====================

def test_connection():
    """Test database connection"""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        cursor.fetchone()
        cursor.close()
        conn.close()
        print("[DATABASE] ✅ Connection test successful")
        return True
    except Error as e:
        print(f"[DATABASE] ❌ Connection test failed: {e}")
        return False

def is_rto_declined(state: str, rto_code: str) -> bool:
    """Check if an RTO is marked as declined (no insurance available)"""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        cursor.execute(
            "SELECT 1 FROM declined_rtos WHERE state = %s AND rto_code = %s LIMIT 1",
            (state, rto_code)
        )
        result = cursor.fetchone()
        
        cursor.close()
        conn.close()
        
        return result is not None
    except Error as e:
        print(f"[DATABASE] Warning: Could not check declined status: {e}")
        return False


def is_param_declined(field_name: str, field_value: str, state: str = None, rto_code: str = None) -> bool:
    """Check if a given parameter (any UI field) is marked as declined/unavailable."""
    try:
        conn = get_connection()
        cursor = conn.cursor()

        # Check generic declined values table first
        query = """
        SELECT 1 FROM declined_values
        WHERE field_name = %s AND field_value = %s
          AND (state IS NULL OR state = %s)
          AND (rto_code IS NULL OR rto_code = %s)
        LIMIT 1
        """
        cursor.execute(query, (field_name, field_value, state, rto_code))
        result = cursor.fetchone()
        cursor.close()
        conn.close()

        if result:
            return True

        # For RTO-specific declines, also check declined_rtos when field is rto_code
        if field_name == 'rto_code' and state and field_value:
            return is_rto_declined(state, field_value)

        return False
    except Error as e:
        print(f"[DATABASE] Warning: Could not check declined parameter: {e}")
        return False

def get_top_5_payouts(
    state: str,
    rto_code: str,
    vehicle_type: str,
    fuel_type: str,
    policy_type: str,
    vehicle_age: str,
    business_type: str = None,
    vehicle_category: str = None,
    cc_slab: Optional[str] = None,
    gvw_slab: Optional[str] = None,
    watt_slab: Optional[str] = None,
    seating_capacity: Optional[str] = None,
    ncb_slab: Optional[str] = None,
    cpa_cover: Optional[str] = None,
    zero_depreciation: Optional[str] = None,
    trailer: Optional[str] = None,
    make: Optional[str] = None,
    model: Optional[str] = None
) -> List[Dict]:
    """
    Get top 5 highest payouts for given parameters (sorted by commission %)
    Uses ALL parameters to find exact matching records
    Automatically filters by current date (date_from <= TODAY <= date_till)
    
    Special case: rto_code "ro-chn" matches all TN RTOs except explicit Chennai RTOs
    Returns empty list if declined or no matching rates found
    """
    try:
        from datetime import date
        
        # Check if any requested parameter has been marked declined/unavailable
        param_map = {
            'rto_code': rto_code,
            'vehicle_category': vehicle_category,
            'vehicle_type': vehicle_type,
            'fuel_type': fuel_type,
            'vehicle_age': vehicle_age,
            'policy_type': policy_type,
            'business_type': business_type,
            'cc_slab': cc_slab,
            'gvw_slab': gvw_slab,
            'watt_slab': watt_slab,
            'seating_capacity': seating_capacity,
            'ncb_slab': ncb_slab,
            'cpa_cover': cpa_cover,
            'zero_depreciation': zero_depreciation,
            'trailer': trailer,
            'make': make,
            'model': model
        }

        for fname, fval in param_map.items():
            if fval:
                if is_param_declined(fname, fval, state, rto_code):
                    # If any parameter is declined for this state/RTO, return no results
                    return []
        
                conn = get_connection()
                cursor = conn.cursor(dictionary=True)

                today = date.today()

                # Build query with all parameters
                query = """
                SELECT 
                        c.company_name,
                        p.payout_percentage,
                        p.conditions
                FROM payout_rules p
                JOIN companies c ON p.company_id = c.company_id
                WHERE p.state = %s
                    AND (p.vehicle_type = %s OR p.vehicle_type = 'All')
                    AND (p.fuel_type = %s OR p.fuel_type = 'All')
                    AND (p.policy_type = %s OR p.policy_type = 'All')
                    AND (p.effective_from IS NULL OR DATE(p.effective_from) <= %s)
                    AND (p.effective_to IS NULL OR DATE(p.effective_to) >= %s)
                """

                params = [state, vehicle_type, fuel_type, policy_type, today, today]
        
        # Handle RTO code - special case for "ro-chn"
        if rto_code and rto_code.lower() == 'ro-chn':
            # Match all TN RTOs except explicit Chennai RTOs
            ro_chn_list = ','.join([f"'{rto}'" for rto in REST_OF_CHENNAI])
            query += f" AND p.rto_code IN ({ro_chn_list})"
        else:
            query += " AND p.rto_code = %s"
            params.insert(1, rto_code)
        
        # Add all optional parameters if provided
        if business_type:
            query += " AND (p.business_type = %s OR p.business_type = 'All')"
            params.append(business_type)
        
        if vehicle_age:
            query += " AND (p.vehicle_age = %s OR p.vehicle_age = 'All')"
            params.append(vehicle_age)
        
        if vehicle_category:
            query += " AND (p.vehicle_category = %s OR p.vehicle_category = 'All')"
            params.append(vehicle_category)
        
        if cc_slab:
            query += " AND (p.cc_slab = %s OR p.cc_slab = 'All' OR p.cc_slab IS NULL)"
            params.append(cc_slab)
        
        if gvw_slab:
            query += " AND (p.gvw_slab = %s OR p.gvw_slab = 'All' OR p.gvw_slab IS NULL)"
            params.append(gvw_slab)
        
        if watt_slab:
            query += " AND (p.watt_slab = %s OR p.watt_slab = 'All' OR p.watt_slab IS NULL)"
            params.append(watt_slab)
        
        if seating_capacity:
            query += " AND (p.seating_capacity = %s OR p.seating_capacity = 'All' OR p.seating_capacity IS NULL)"
            params.append(seating_capacity)
        
        if ncb_slab:
            query += " AND (p.ncb_slab = %s OR p.ncb_slab = 'All' OR p.ncb_slab IS NULL)"
            params.append(ncb_slab)
        
        if cpa_cover:
            query += " AND (p.cpa_cover = %s OR p.cpa_cover = 'All' OR p.cpa_cover IS NULL)"
            params.append(cpa_cover)
        
        if zero_depreciation:
            query += " AND (p.zero_depreciation = %s OR p.zero_depreciation = 'All' OR p.zero_depreciation IS NULL)"
            params.append(zero_depreciation)
        
        if trailer:
            query += " AND (p.trailer = %s OR p.trailer = 'All' OR p.trailer IS NULL)"
            params.append(trailer)
        
        if make:
            query += " AND (p.make_model = %s OR p.make_model = 'All' OR p.make_model IS NULL)"
            params.append(make)
        
        if model:
            query += " AND (p.make_model = %s OR p.make_model = 'All' OR p.make_model IS NULL)"
            params.append(model)
        
        # Order by highest payout percentage and limit to top 5
        query += """
        ORDER BY p.payout_percentage DESC
        LIMIT 5
        """
        
        cursor.execute(query, params)
        results = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        # Format results with rank
        formatted_results = []
        for idx, row in enumerate(results, 1):
            formatted_results.append({
                'rank': idx,
                'company_name': row['company_name'],
                'payout_percentage': float(row['payout_percentage']),
                'payout_amount': f"{float(row['payout_percentage']) * 100:.1f}%",
                'conditions': row['conditions'] if row['conditions'] else None
            })
        
        print(f"[DATABASE] Found {len(formatted_results)} payouts")
        return formatted_results
        
    except Error as e:
        print(f"[DATABASE ERROR] Query failed: {e}")
        return []

def get_all_companies() -> List[Dict]:
    """Get list of all companies"""
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)
        
        cursor.execute("SELECT company_id, company_name FROM companies ORDER BY company_name")
        results = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return results
    except Error as e:
        print(f"[DATABASE ERROR] Failed to fetch companies: {e}")
        return []

def insert_payout_record(payout_data: Dict) -> bool:
    """Insert a single payout record"""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        # Get or create company
        company_name = payout_data.get('company_name')
        cursor.execute("SELECT company_id FROM companies WHERE company_name = %s", (company_name,))
        result = cursor.fetchone()
        
        if result:
            company_id = result[0]
        else:
            cursor.execute("INSERT INTO companies (company_name) VALUES (%s)", (company_name,))
            company_id = cursor.lastrowid
        
        # Insert payout record
        columns = ['company_id'] + [k for k in payout_data.keys() if k != 'company_name']
        values = [company_id] + [payout_data[k] for k in columns[1:]]
        
        placeholders = ', '.join(['%s'] * len(columns))
        query = f"INSERT INTO payout_rules ({', '.join(columns)}) VALUES ({placeholders})"
        
        cursor.execute(query, values)
        conn.commit()
        
        cursor.close()
        conn.close()
        
        return True
    except Error as e:
        print(f"[DATABASE ERROR] Failed to insert payout: {e}")
        return False

def get_record_count() -> int:
    """Get total number of payout records"""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        cursor.execute("SELECT COUNT(*) FROM payout_rules")
        count = cursor.fetchone()[0]
        
        cursor.close()
        conn.close()
        
        return count
    except Error as e:
        print(f"[DATABASE ERROR] Failed to get count: {e}")
        return 0

def log_query(state: str, rto_code: str, vehicle_type: str, fuel_type: str, 
              policy_type: str, results_count: int) -> bool:
    """Log a payout query for analytics"""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        query = """
        INSERT INTO query_logs (state, rto_code, vehicle_type, fuel_type, policy_type, results_count)
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        
        cursor.execute(query, (state, rto_code, vehicle_type, fuel_type, policy_type, results_count))
        conn.commit()
        
        cursor.close()
        conn.close()
        
        return True
    except Error as e:
        print(f"[DATABASE ERROR] Failed to log query: {e}")
        return False

# ==================== DROPDOWN VALUE FETCHERS ====================
# These functions return distinct values from database for UI dropdowns

def get_distinct_states() -> List[str]:
    """Get all distinct states from database"""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT state FROM payout_rules WHERE state IS NOT NULL ORDER BY state")
        results = cursor.fetchall()
        cursor.close()
        conn.close()
        return sorted([row[0] for row in results])
    except Error as e:
        print(f"[DATABASE ERROR] Failed to get states: {e}")
        return []

def get_distinct_rtos(state: str) -> List[str]:
    """Get all distinct RTO codes for a state"""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT rto_code FROM payout_rules WHERE state = %s AND rto_code IS NOT NULL ORDER BY rto_code", (state,))
        results = cursor.fetchall()
        cursor.close()
        conn.close()
        return sorted([row[0] for row in results])
    except Error as e:
        print(f"[DATABASE ERROR] Failed to get RTOs: {e}")
        return []

def get_distinct_vehicle_categories() -> List[str]:
    """Get all distinct vehicle categories"""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT vehicle_category FROM payout_rules WHERE vehicle_category IS NOT NULL ORDER BY vehicle_category")
        results = cursor.fetchall()
        cursor.close()
        conn.close()
        return sorted([row[0] for row in results])
    except Error as e:
        print(f"[DATABASE ERROR] Failed to get vehicle categories: {e}")
        return []

def get_distinct_vehicle_types(vehicle_category: str = None) -> List[str]:
    """Get all distinct vehicle types (optionally filtered by category)"""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        if vehicle_category:
            cursor.execute("SELECT DISTINCT vehicle_type FROM payout_rules WHERE vehicle_category = %s AND vehicle_type IS NOT NULL ORDER BY vehicle_type", (vehicle_category,))
        else:
            cursor.execute("SELECT DISTINCT vehicle_type FROM payout_rules WHERE vehicle_type IS NOT NULL ORDER BY vehicle_type")
        results = cursor.fetchall()
        cursor.close()
        conn.close()
        return sorted([row[0] for row in results])
    except Error as e:
        print(f"[DATABASE ERROR] Failed to get vehicle types: {e}")
        return []

def get_distinct_fuel_types(vehicle_type: str = None) -> List[str]:
    """Get all distinct fuel types (optionally filtered by vehicle type, excluding 'All' since it means 'applicable to all fuels')"""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        if vehicle_type:
            cursor.execute("SELECT DISTINCT fuel_type FROM payout_rules WHERE vehicle_type = %s AND fuel_type IS NOT NULL AND fuel_type != 'All' ORDER BY fuel_type", (vehicle_type,))
        else:
            cursor.execute("SELECT DISTINCT fuel_type FROM payout_rules WHERE fuel_type IS NOT NULL AND fuel_type != 'All' ORDER BY fuel_type")
        results = cursor.fetchall()
        cursor.close()
        conn.close()
        return sorted([row[0] for row in results])
    except Error as e:
        print(f"[DATABASE ERROR] Failed to get fuel types: {e}")
        return []

def get_distinct_policy_types() -> List[str]:
    """Get all distinct policy types"""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT policy_type FROM payout_rules WHERE policy_type IS NOT NULL ORDER BY policy_type")
        results = cursor.fetchall()
        cursor.close()
        conn.close()
        return sorted([row[0] for row in results])
    except Error as e:
        print(f"[DATABASE ERROR] Failed to get policy types: {e}")
        return []

def get_distinct_business_types() -> List[str]:
    """Get all distinct business types (excluding 'All' since it's universal)"""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT business_type FROM payout_rules WHERE business_type IS NOT NULL AND business_type != 'All' ORDER BY business_type")
        results = cursor.fetchall()
        cursor.close()
        conn.close()
        return sorted([row[0] for row in results])
    except Error as e:
        print(f"[DATABASE ERROR] Failed to get business types: {e}")
        return []

def get_distinct_vehicle_ages() -> List[str]:
    """Get all distinct vehicle ages (excluding 'All' since it's universal)"""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT vehicle_age FROM payout_rules WHERE vehicle_age IS NOT NULL AND vehicle_age != 'All' ORDER BY vehicle_age")
        results = cursor.fetchall()
        cursor.close()
        conn.close()
        return sorted([row[0] for row in results])
    except Error as e:
        print(f"[DATABASE ERROR] Failed to get vehicle ages: {e}")
        return []

def get_distinct_cc_slabs() -> List[str]:
    """Get all distinct CC slabs (excluding 'No' which means not applicable)"""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT cc_slab FROM payout_rules WHERE cc_slab IS NOT NULL AND cc_slab != 'No' ORDER BY cc_slab")
        results = cursor.fetchall()
        cursor.close()
        conn.close()
        return sorted([row[0] for row in results if row[0]])
    except Error as e:
        print(f"[DATABASE ERROR] Failed to get CC slabs: {e}")
        return []

def get_distinct_gvw_slabs() -> List[str]:
    """Get all distinct GVW slabs (excluding 'No' which means not applicable)"""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT gvw_slab FROM payout_rules WHERE gvw_slab IS NOT NULL AND gvw_slab != 'No' ORDER BY gvw_slab")
        results = cursor.fetchall()
        cursor.close()
        conn.close()
        return sorted([row[0] for row in results if row[0]])
    except Error as e:
        print(f"[DATABASE ERROR] Failed to get GVW slabs: {e}")
        return []

def get_distinct_watt_slabs() -> List[str]:
    """Get all distinct Watt slabs (excluding 'No' which means not applicable)"""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT watt_slab FROM payout_rules WHERE watt_slab IS NOT NULL AND watt_slab != 'No' ORDER BY watt_slab")
        results = cursor.fetchall()
        cursor.close()
        conn.close()
        return sorted([row[0] for row in results if row[0]])
    except Error as e:
        print(f"[DATABASE ERROR] Failed to get Watt slabs: {e}")
        return []

def get_distinct_seating_capacities() -> List[str]:
    """Get all distinct seating capacities (excluding 'No' which means not applicable)"""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT seating_capacity FROM payout_rules WHERE seating_capacity IS NOT NULL AND seating_capacity != 'No' ORDER BY seating_capacity")
        results = cursor.fetchall()
        cursor.close()
        conn.close()
        return sorted([row[0] for row in results if row[0]])
    except Error as e:
        print(f"[DATABASE ERROR] Failed to get seating capacities: {e}")
        return []

def get_distinct_ncb_slabs() -> List[str]:
    """Get all distinct NCB slabs"""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT ncb_slab FROM payout_rules WHERE ncb_slab IS NOT NULL ORDER BY ncb_slab")
        results = cursor.fetchall()
        cursor.close()
        conn.close()
        return sorted([row[0] for row in results if row[0]])
    except Error as e:
        print(f"[DATABASE ERROR] Failed to get NCB slabs: {e}")
        return []

def get_distinct_cpa_covers() -> List[str]:
    """Get all distinct CPA covers"""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT cpa_cover FROM payout_rules WHERE cpa_cover IS NOT NULL ORDER BY cpa_cover")
        results = cursor.fetchall()
        cursor.close()
        conn.close()
        return sorted([row[0] for row in results if row[0]])
    except Error as e:
        print(f"[DATABASE ERROR] Failed to get CPA covers: {e}")
        return []

def get_distinct_zero_depreciation() -> List[str]:
    """Get all distinct zero depreciation options"""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT zero_depreciation FROM payout_rules WHERE zero_depreciation IS NOT NULL ORDER BY zero_depreciation")
        results = cursor.fetchall()
        cursor.close()
        conn.close()
        return sorted([row[0] for row in results if row[0]])
    except Error as e:
        print(f"[DATABASE ERROR] Failed to get zero depreciation: {e}")
        return []

def get_distinct_trailers() -> List[str]:
    """Get all distinct trailer options"""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT trailer FROM payout_rules WHERE trailer IS NOT NULL ORDER BY trailer")
        results = cursor.fetchall()
        cursor.close()
        conn.close()
        return sorted([row[0] for row in results if row[0]])
    except Error as e:
        print(f"[DATABASE ERROR] Failed to get trailers: {e}")
        return []

def get_distinct_makes(vehicle_type: str = None) -> List[str]:
    """Get all distinct makes (optionally filtered by vehicle type)"""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        if vehicle_type:
            cursor.execute("SELECT DISTINCT make_model FROM payout_rules WHERE vehicle_type = %s AND make_model IS NOT NULL ORDER BY make_model", (vehicle_type,))
        else:
            cursor.execute("SELECT DISTINCT make_model FROM payout_rules WHERE make_model IS NOT NULL ORDER BY make_model")
        results = cursor.fetchall()
        cursor.close()
        conn.close()
        return sorted([row[0] for row in results if row[0]])
    except Error as e:
        print(f"[DATABASE ERROR] Failed to get makes: {e}")
        return []

