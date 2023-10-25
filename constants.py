JSON_TYPES = ['sites', 'chargingstations', 'availabilities']

JSON_KEYS = {
    'sites' : ['SiteId'],
    'chargingstations' : ['ChargingStationId'],
    'availabilities' : ['ChargingStationId', 'AvailabilityStatus', 'AvailabilityTime']
}

# These will be removed in converting columns to PascalCase:
CHARACTERS_TO_REPLACE = [' ', '.', '-', '(', ')', '/']

JSON_FILE_PATH_PREFIX = 'EVRoamJSON'

JSON_FILE_PATH = {
    'container': 'incoming-data-staging',
    'path': f'{JSON_FILE_PATH_PREFIX}/{{blob_prefix}}.json'
}

MAX_JSON_INGEST_BATCH = 1000

CSV_FILE_PATH_PREFIX = 'file-drop/EVRoam'

CSV_FILE_PATH = {
    'container': 'sharepoint',
    'path': {
        'sites': 'file-drop/EVRoam_01_Sites/Template_EVRoam_Sites.csv',
        'chargingstations': 'file-drop/EVRoam_02_ChargingStations/Template_EVRoam_ChargingStations.csv',
        'availabilities': 'file-drop/EVRoam_03_Availabilities/Template_EVRoam_Availabilities.csv'
    }
}

"""
JSON_TYPES Processing order is important, since some providers put availabilities information in the chargingstations file. 
For these providers we pull AVAILABILITIES_COLUMNS from chargingstations and drop CHARGINGSTATIONS_DROP_COLUMNS from chargingstations.
"""
assert JSON_TYPES.index('availabilities') > JSON_TYPES.index('chargingstations'), \
       "'availabilities' should come after 'chargingstations' in JSON_TYPES"
AVAILABILITIES_COLUMNS = ['Operator', 'ChargingStationId', 'AvailabilityStatus', 'KwAvailable', 'AvailabilityTime']
CHARGINGSTATIONS_DROP_COLUMNS = ['AvailabilityStatus', 'KwAvailable', 'AvailabilityTime']