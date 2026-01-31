"""
Configuration file for POSP Payout Checker
Centralized settings for database and application
"""

import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# ==================== DATABASE CONFIG ====================
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", 3306))
DB_USER = os.getenv("DB_USER", "root")
DB_PASSWORD = os.getenv("DB_PASSWORD", "root")  # Change this to your MySQL password
DB_NAME = os.getenv("DB_NAME", "posp_payout_db")

# Connection pool settings
DB_POOL_SIZE = 5
DB_POOL_RESET_SESSION = True

# ==================== API CONFIG ====================
API_HOST = "127.0.0.1"
API_PORT = 8000
DEBUG_MODE = True

# ==================== APPLICATION CONSTANTS ====================
MAX_PAYLOAD_SIZE = 1024 * 1024  # 1MB
REQUEST_TIMEOUT = 30  # seconds
LOG_FILE = "logs/app.log"

# ==================== RTO MAPPING ====================
# Explicit Chennai RTOs - all others in TN are considered "Rest of Chennai" (RO-CHN)
CHENNAI_RTOS = {
    '01', '02', '03', '04', '05', '06', '07', '09', '10',
    '11', '12', '13', '14', '18', '22', '85'
}

# All Tamil Nadu RTO codes (01-99)
ALL_TN_RTOS = {str(i).zfill(2) for i in range(1, 100)}

# Rest of Chennai = All TN RTOs except explicit Chennai RTOs
REST_OF_CHENNAI = ALL_TN_RTOS - CHENNAI_RTOS

# ==================== DATABASE TABLES ====================
TABLES = {
    'companies': '''
    CREATE TABLE IF NOT EXISTS companies (
        company_id INT AUTO_INCREMENT PRIMARY KEY,
        company_name VARCHAR(100) NOT NULL UNIQUE,
        created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        INDEX idx_name (company_name)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    ''',
    
    'payout_rules': '''
    CREATE TABLE IF NOT EXISTS payout_rules (
        payout_id INT AUTO_INCREMENT PRIMARY KEY,
        company_id INT NOT NULL,
        state VARCHAR(5) NOT NULL,
        rto_code VARCHAR(5) NOT NULL,
        vehicle_category VARCHAR(30) NOT NULL,
        vehicle_type VARCHAR(50) NOT NULL,
        fuel_type VARCHAR(20) NOT NULL,
        vehicle_age VARCHAR(50) NOT NULL,
        policy_type VARCHAR(30) NOT NULL,
        business_type VARCHAR(20) NOT NULL,
        cc_slab VARCHAR(50),
        gvw_slab VARCHAR(50),
        watt_slab VARCHAR(50),
        seating_capacity VARCHAR(50),
        ncb_slab VARCHAR(10) NOT NULL,
        cpa_cover VARCHAR(10) NOT NULL,
        zero_depreciation VARCHAR(10) NOT NULL,
        trailer VARCHAR(10),
        payout_percentage DECIMAL(5,2) NOT NULL,
        conditions VARCHAR(500),
        make_model VARCHAR(500),
        effective_from DATE,
        effective_to DATE,
        updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        
        FOREIGN KEY (company_id) REFERENCES companies(company_id) ON DELETE CASCADE,
        
        INDEX idx_search (state, rto_code, vehicle_category, vehicle_type, fuel_type, vehicle_age, policy_type, business_type),
        INDEX idx_company (company_id),
        INDEX idx_vehicle (vehicle_category, vehicle_type),
        INDEX idx_fuel (fuel_type),
        INDEX idx_dates (effective_from, effective_to),
        INDEX idx_make (make_model(50))
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    ''',
    
    'query_logs': '''
    CREATE TABLE IF NOT EXISTS query_logs (
        log_id INT AUTO_INCREMENT PRIMARY KEY,
        state VARCHAR(50),
        rto_code VARCHAR(20),
        vehicle_type VARCHAR(50),
        fuel_type VARCHAR(30),
        policy_type VARCHAR(50),
        results_count INT,
        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        
        INDEX idx_timestamp (timestamp)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    ''',
    
    'declined_rtos': '''
    CREATE TABLE IF NOT EXISTS declined_rtos (
        declined_id INT AUTO_INCREMENT PRIMARY KEY,
        state VARCHAR(5) NOT NULL,
        rto_code VARCHAR(5) NOT NULL,
        reason VARCHAR(200),
        created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        
        UNIQUE KEY uk_state_rto (state, rto_code),
        INDEX idx_state_rto (state, rto_code)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    '''

    , 'declined_values': '''
    CREATE TABLE IF NOT EXISTS declined_values (
        declined_id INT AUTO_INCREMENT PRIMARY KEY,
        field_name VARCHAR(100) NOT NULL,
        field_value VARCHAR(200) NOT NULL,
        state VARCHAR(5),
        rto_code VARCHAR(10),
        reason VARCHAR(200),
        created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

        UNIQUE KEY uk_field_value_state_rto (field_name, field_value, state, rto_code),
        INDEX idx_field_value (field_name, field_value),
        INDEX idx_state (state)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    '''
}

# ==================== FILTER PARAMETERS ====================
# ALL parameters are used to determine the exact payout percentage
# These MUST match the UI parameters exactly
COMMISSION_AFFECTING_PARAMS = [
    'state',
    'rto_code',
    'vehicle_category',
    'vehicle_type',
    'fuel_type',
    'vehicle_age',
    'policy_type',
    'business_type',
    'cc_slab',
    'gvw_slab',
    'watt_slab',
    'seating_capacity',
    'ncb_slab',
    'cpa_cover',
    'zero_depreciation',
    'trailer',
    'make',
    'model'
]

# Parameters that have conditional/optional values
CONDITIONAL_PARAMS = {
    'cc_slab': ['Two-Wheeler', 'Private-Car'],  # Only for these categories
    'gvw_slab': ['GCV'],  # Only for GCV
    'watt_slab': ['Two-Wheeler'],  # Only for Two-Wheeler + Electric fuel
    'seating_capacity': ['PCV'],  # Only for PCV (non-Auto)
    'trailer': ['Misc'],  # Only for Misc (Tractor)
    'make': ['GCV', 'PCV', 'Two-Wheeler'],  # Optional for these categories
    'model': ['GCV', 'PCV', 'Two-Wheeler']  # Optional for these categories
}

print("[CONFIG] Configuration loaded successfully")
print(f"[CONFIG] Database: {DB_HOST}:{DB_PORT}/{DB_NAME}")
