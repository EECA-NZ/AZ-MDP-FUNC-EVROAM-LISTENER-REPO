import os
import sys
import json
import argparse
import http.client
import urllib.parse
import pandas as pd
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient
from inflection import camelize
from pathlib import Path

sys.path.append(str(Path('..').resolve()))
from constants import *

# Parse command line arguments
parser = argparse.ArgumentParser(description='Get environment (dev or prd)')
parser.add_argument('env', type=str, help='Environment (dev or prd)')
args = parser.parse_args()

# Load environment variables from the parent directory
env_path = Path('..') / '.env'
load_dotenv(dotenv_path=env_path)

# Get environment variables based on provided environment
CONTAINER_NAME = CSV_FILE_PATH['container']
CHARGINGSTATIONS_BLOB_NAME = CSV_FILE_PATH['path']['chargingstations']
AVAILABILITIES_BLOB_NAME = CSV_FILE_PATH['path']['availabilities']
SUBSCRIPTION_KEY = os.getenv("EVROAM_SUBSCRIPTION_KEY")
if args.env == 'dev':
    CONNECTION_STRING = os.getenv("DEV_CONNECTION_STRING")
elif args.env == 'prd':
    CONNECTION_STRING = os.getenv("PRD_CONNECTION_STRING")
else:
    raise ValueError('Invalid environment provided. Choose "dev" or "prd"')

headers = {
    # Request headers
    'Ocp-Apim-Subscription-Key': SUBSCRIPTION_KEY,
}

all_data = []
result_page = 1

while True:
    params = urllib.parse.urlencode({
        # Request parameters
        'resultPage': result_page,
    })

    try:
        conn = http.client.HTTPSConnection('evroam.azure-api.net')
        conn.request("GET", "/consumer/api/ChargingStation?%s" % params, "{body}", headers)
        response = conn.getresponse()
        data = json.loads(response.read())
        all_data.extend(data['chargingStations'])
        conn.close()
        if not data["hasMoreResults"]:
            break
        result_page += 1
    except Exception as e:
        print(e)
        break


df = pd.json_normalize(all_data)
# Replace undesired characters and PascalCase-ify
for c in CHARACTERS_TO_REPLACE:
    df.columns = [
        col.replace(c, ' ')
        for col in df.columns
    ]
df.columns = [
    camelize(col, True).replace(' ', '')
    for col in df.columns
]

availabilities = df[AVAILABILITIES_COLUMNS]
availabilities = availabilities.drop_duplicates(
    subset=JSON_KEYS['availabilities']
)
availabilities.dropna(
    inplace=True,
    subset=JSON_KEYS['availabilities'],
    how='any'
)

chargingstations = df.drop(columns=CHARGINGSTATIONS_DROP_COLUMNS)
chargingstations = chargingstations.drop_duplicates(
    subset=JSON_KEYS['chargingstations']
)
chargingstations.dropna(
    inplace=True,
    subset=JSON_KEYS['chargingstations'],
    how='any'
)

#availabilities.to_csv(os.path.basename(AVAILABILITIES_BLOB_NAME), index=False)
blob_service_client = BlobServiceClient.from_connection_string(CONNECTION_STRING)
availabilities_csv_str = availabilities.to_csv(index=False)
blob_client = blob_service_client.get_blob_client(CONTAINER_NAME, AVAILABILITIES_BLOB_NAME)
blob_client.upload_blob(availabilities_csv_str, overwrite=True)

#chargingstations.to_csv(os.path.basename(CHARGINGSTATIONS_BLOB_NAME), index=False)
blob_service_client = BlobServiceClient.from_connection_string(CONNECTION_STRING)
chargingstations_csv_str = chargingstations.to_csv(index=False)
blob_client = blob_service_client.get_blob_client(CONTAINER_NAME, CHARGINGSTATIONS_BLOB_NAME)
blob_client.upload_blob(chargingstations_csv_str, overwrite=True)