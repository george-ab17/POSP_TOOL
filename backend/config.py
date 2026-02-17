"""Config and mappings used by the web app."""

# State display name to code mapping (for API requests)
STATE_CODE_MAP = {
    'Tamil Nadu': 'TN',
    'Kerala': 'KL',
    'Karnataka': 'KA',
    'Puducherry': 'PY',
    'Pondicherry': 'PY',
    'Telangana': 'TS',
    'Andhra Pradesh': 'AP',
    'Maharashtra': 'MH',
    'Madhya Pradesh': 'MP',
    'Assam': 'AS',
    'Haryana': 'HR',
    'Rajasthan': 'RJ',
    'Uttar Pradesh': 'UP',
}

# State code to display name mapping (for API responses)
STATE_DISPLAY_MAP = {
    'TN': 'Tamil Nadu',
    'KL': 'Kerala',
    'KA': 'Karnataka',
    'PY': 'Puducherry',
    'TS': 'Telangana',
    'AP': 'Andhra Pradesh',
    'MH': 'Maharashtra',
    'MP': 'Madhya Pradesh',
    'AS': 'Assam',
    'HR': 'Haryana',
    'RJ': 'Rajasthan',
    'UP': 'Uttar Pradesh',
}

STATE_DISPLAY_NAMES = [
    'Tamil Nadu',
    'Kerala',
    'Karnataka',
    'Telangana',
    'Andhra Pradesh',
    'Puducherry',
    'Maharashtra',
    'Madhya Pradesh',
    'Assam',
    'Haryana',
    'Rajasthan',
    'Uttar Pradesh',
]

# Vehicle category mappings (display name or variant -> code used in DB/Excel)
VEHICLE_CATEGORY_MAP = {
    'Goods Carrying Vehicle': 'GCV',
    'GCV': 'GCV',
    'gcv': 'GCV',
    'Passenger Carrying Vehicle': 'PCV',
    'PCV': 'PCV',
    'pcv': 'PCV',
    'Two-Wheeler': 'Two Wheeler',
    'two-wheeler': 'Two Wheeler',
    'Two Wheeler': 'Two Wheeler',
    'Private Car': 'Private Car',
    'Private-Car': 'Private Car',
    'Miscellaneous': 'Misc',
    'Misc': 'Misc',
    'misc': 'Misc',
}

VEHICLE_CATEGORY_DISPLAY = {
    'GCV': 'Goods Carrying Vehicle',
    'PCV': 'Passenger Carrying Vehicle',
    'Two Wheeler': 'Two Wheeler',
    'Private Car': 'Private Car',
    'Misc': 'Miscellaneous',
}

API_HOST = '0.0.0.0'
API_PORT = 8000
