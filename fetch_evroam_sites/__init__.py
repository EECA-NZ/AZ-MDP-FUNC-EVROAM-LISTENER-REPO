import azure.functions as func
import os
import json
import logging
import datetime
import http.client
import urllib.parse
import pandas as pd
from azure.storage.blob import BlobServiceClient
from inflection import camelize


#### Constants based on provided environment

from constants import *
CONNECT_STR = os.getenv('StorageAccountConnectionString')
CONTAINER_NAME = CSV_FILE_PATH['container']
BLOB_NAME = CSV_FILE_PATH['path']['sites']
SUBSCRIPTION_KEY = os.getenv('EvroamSubscriptionKey')

if not CONNECT_STR:
    raise ValueError("StorageAccountConnectionString is not set or is empty!")
if not SUBSCRIPTION_KEY:
    raise ValueError("EvroamSubscriptionKey is not set or is empty!")


#### Functions

def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()
    if mytimer.past_due:
        logging.info('The timer is past due!')
    all_data = fetch_evroam_sites_data()
    if all_data:
        df = process_data_to_dataframe(all_data)
        logging.info(f"Collected site data: {len(df)} rows")
        upload_csv_to_blob(df)
        logging.info(f"Delivered site data to {BLOB_NAME}")
    logging.info('Python timer trigger function ran at %s', utc_timestamp)


def fetch_evroam_sites_data():
    headers = {
        'Ocp-Apim-Subscription-Key': SUBSCRIPTION_KEY,
    }
    all_data = []
    result_page = 1
    while True:
        params = urllib.parse.urlencode({
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
    return all_data


def process_data_to_dataframe(all_data):
    df = pd.DataFrame(all_data)
    df.columns = [camelize(col, True) for col in df.columns]
    df.drop_duplicates(inplace=True, subset=JSON_KEYS['sites'])
    return df


def upload_csv_to_blob(df):
    csv_str = df.to_csv(index=False)
    blob_service_client = BlobServiceClient.from_connection_string(CONNECT_STR)
    blob_client = blob_service_client.get_blob_client(CONTAINER_NAME, BLOB_NAME)
    blob_client.upload_blob(csv_str, overwrite=True)