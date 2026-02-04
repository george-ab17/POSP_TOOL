"""Config and mappings used by the web app."""

# State display name to code mapping (for API requests)
STATE_CODE_MAP = {
    'Tamil Nadu': 'TN',
    'Puducherry': 'PY',
    'Pondicherry': 'PY',
    'Kerala': 'KL',
    'Tamil Nadu - Chennai': 'TN-CHN',
    'Tamil Nadu - Pondicherry': 'TN-PY',
}

# State code to display name mapping (for API responses)
STATE_DISPLAY_MAP = {
    'TN': 'Tamil Nadu',
    'PY': 'Puducherry',
    'KL': 'Kerala',
    'TN-CHN': 'Tamil Nadu - Chennai',
    'TN-PY': 'Tamil Nadu - Pondicherry',
}

STATE_DISPLAY_NAMES = []

# Vehicle category mappings (display name or variant -> code used in DB/Excel)
VEHICLE_CATEGORY_MAP = {
    'Goods Carrying Vehicle': 'GCV',
    'GCV': 'GCV',
    'gcv': 'GCV',
    'Passenger Carrying Vehicle': 'PCV',
    'PCV': 'PCV',
    'pcv': 'PCV',
    'Two-Wheeler': 'Two-Wheeler',
    'two-wheeler': 'Two-Wheeler',
    'Private Car': 'Private-Car',
    'Private-Car': 'Private-Car',
    'Miscellaneous': 'Misc',
    'Misc': 'Misc',
    'misc': 'Misc',
}

VEHICLE_CATEGORY_DISPLAY = {
    'GCV': 'Goods Carrying Vehicle',
    'PCV': 'Passenger Carrying Vehicle',
    'Two-Wheeler': 'Two-Wheeler',
    'Private-Car': 'Private Car',
    'Misc': 'Miscellaneous',
}

API_HOST = '0.0.0.0'
API_PORT = 8000
