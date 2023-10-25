"""
This function runs on a timer and aggregates the data from the EVRoam API
"""

import os
import json
import logging
import datetime
import pandas as pd
from inflection import camelize
from azure.storage.blob import BlobServiceClient
import azure.functions as func

from constants import *
CONNECT_STR = os.getenv('StorageAccountConnectionString')
blob_service_client = BlobServiceClient.from_connection_string(CONNECT_STR)

def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)

    container_client = blob_service_client.get_container_client(
        JSON_FILE_PATH['container']
    )

    data = {json_type: [] for json_type in JSON_TYPES}
    blobs_to_delete = []

    # loop over the files in the blob storage
    for blob in container_client.list_blobs(name_starts_with=JSON_FILE_PATH_PREFIX):
        if len(blobs_to_delete) >= MAX_JSON_INGEST_BATCH:
            break
        if blob.name.endswith('.json'):
            logging.info('Processing %s', blob.name)
            blob_client = blob_service_client.get_blob_client(
                JSON_FILE_PATH['container'],
                blob.name
            )

            # read blob content into pandas dataframe
            blob_data = blob_client.download_blob().readall().decode('utf8')
            try:
                json_data = json.loads(blob_data)
                dataframe = pd.json_normalize(json_data)
                # add or transform data to match the schema
                for json_type in JSON_TYPES:
                    if json_type in blob.name.lower():
                        data[json_type].append(dataframe)
                blobs_to_delete.append(blob_client)
            except json.JSONDecodeError:  # catch JSONDecodeError specifically
                logging.error('Could not parse %s as JSON. Blob content: %s', blob.name, blob_data)

    # concatenate dataframes and write to the database
    for blob_prefix in JSON_TYPES:
        logging.info('blob_prefix %s', blob_prefix)
        if data[blob_prefix]:
            dataframe = pd.concat(data[blob_prefix])
            logging.info('Replace undesired characters and PascalCase-ify')
            for c in CHARACTERS_TO_REPLACE:
                dataframe.columns = [
                    col.replace(c, ' ')
                    for col in dataframe.columns
                ]
            dataframe.columns = [
                camelize(col, True).replace(' ', '')
                for col in dataframe.columns
            ]
            dataframe = dataframe.drop_duplicates(
                subset=JSON_KEYS[blob_prefix]
            )
            dataframe = dataframe.dropna(
                subset=JSON_KEYS[blob_prefix],
                how='any'
            )
            if blob_prefix == "chargingstations":
                data['availabilities'].append(dataframe[AVAILABILITIES_COLUMNS].copy())
                dataframe.drop(columns=CHARGINGSTATIONS_DROP_COLUMNS, inplace=True)

            csv_str = dataframe.to_csv(index=False)
            blob_file_path = CSV_FILE_PATH['path'][blob_prefix]
            container = CSV_FILE_PATH['container']
            try:
                blob_client = blob_service_client.get_blob_client(
                    container=container,
                    blob=blob_file_path
                )
            except Exception as e:
                logging.error("Error obtaining blob client: %s", str(e))
            blob_client.upload_blob(csv_str.encode('utf-8'), overwrite=True)
            logging.info('Uploaded %s to %s', blob_file_path, container)

    # delete all blobs that were successfully ingested
    for blob_client in blobs_to_delete:
        blob_client.delete_blob()