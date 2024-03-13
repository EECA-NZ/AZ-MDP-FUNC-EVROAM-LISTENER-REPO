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
from sharedCode import database_utils
CONNECT_STR = os.getenv('StorageAccountConnectionString')
CONTAINER_NAME = CSV_FILE_PATH['container']
CHARGINGSTATIONS_BLOB_NAME = CSV_FILE_PATH['path']['chargingstations']
AVAILABILITIES_BLOB_NAME = CSV_FILE_PATH['path']['availabilities']
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
    all_data = fetch_evroam_chargingstations_data()
    if all_data:
        availabilities, chargingstations = process_data_to_dataframes(all_data)
        database_utils.write_availabilities_to_db(availabilities)
        database_utils.write_chargingstations_to_db(chargingstations)
        logging.info("Charging Station and Availability data written to SQL Database")
        upload_csv_to_blob(availabilities, AVAILABILITIES_BLOB_NAME)
        upload_csv_to_blob(chargingstations, CHARGINGSTATIONS_BLOB_NAME)
    logging.info('Python timer trigger function ran at %s', utc_timestamp)


def fetch_evroam_chargingstations_data():
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
    return all_data


def process_data_to_dataframes(all_data):
    df = pd.json_normalize(all_data)
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
    return availabilities, chargingstations


def upload_csv_to_blob(df, blob_name):
    csv_str = df.to_csv(index=False)
    blob_service_client = BlobServiceClient.from_connection_string(CONNECT_STR)
    blob_client = blob_service_client.get_blob_client(CONTAINER_NAME, blob_name)
    blob_client.upload_blob(csv_str, overwrite=True)