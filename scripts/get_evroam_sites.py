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
BLOB_NAME = CSV_FILE_PATH['path']['sites']
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
        conn.request("GET", "/consumer/api/Site?%s" % params, "{body}", headers)
        response = conn.getresponse()
        data = json.loads(response.read())
        all_data.extend(data["sites"])
        conn.close()
        if not data["hasMoreResults"]:
            break
        result_page += 1
    except Exception as e:
        print(e)
        break

# Create dataframe and pascal case columns
df = pd.DataFrame(all_data)
df.columns = [camelize(col, True) for col in df.columns]

# Remove duplicates
df.drop_duplicates(inplace=True, subset=JSON_KEYS['sites'])

# Write dataframe to csv
csv_str = df.to_csv(index=False)

# Upload CSV to Azure Blob Storage
blob_service_client = BlobServiceClient.from_connection_string(CONNECTION_STRING)
blob_client = blob_service_client.get_blob_client(CONTAINER_NAME, BLOB_NAME)
blob_client.upload_blob(csv_str, overwrite=True)

# df.to_csv(os.path.basename(BLOB_NAME), index=False)