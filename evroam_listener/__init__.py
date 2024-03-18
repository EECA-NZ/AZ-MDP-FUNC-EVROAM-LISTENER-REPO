"""
EVRoam Event Grid Listener

This function handles the EVRoam Event Grid trigger.
- For SubscriptionValidationEvent, it returns the validation code.
- For all other events, it downloads the data from the URL in the
event body and uploads it to the SQL database.
"""

import os
import json
import logging
from urllib.parse import urlparse

import pandas as pd
from inflection import camelize
import requests
import azure.functions as func
from azure.storage.blob import BlobServiceClient
from constants import *
from sharedCode import database_utils

# Get environment variables
CONNECT_STR = os.getenv("StorageAccountConnectionString")
blob_service_client = BlobServiceClient.from_connection_string(CONNECT_STR)

# Constant variables
SUBSCRIBE = "Microsoft.EventGrid.SubscriptionValidationEvent"
TIMEOUT = 5
WRITE_TO_DB = {
    "chargingstations": database_utils.write_chargingstations_to_db,
    "sites": database_utils.write_sites_to_db,
    "availabilities": database_utils.write_availabilities_to_db,
}


def process_json_data(data_url, json_data):
    """
    JSON data manipulation and insertion into the SQL database

    Args:
        data_url (str): The data URL
        json_data (dict): The JSON data to process

    Returns:
        None: The function does not return anything
    """
    # Normalize JSON data to DataFrame and transform to match schema
    dataframe = pd.json_normalize(json_data)
    # Correct approach to replace characters and camelCase the columns
    corrected_columns = []
    for col in dataframe.columns:
        for character in CHARACTERS_TO_REPLACE:
            col = col.replace(character, " ")
        col = camelize(col.strip(), uppercase_first_letter=True).replace(" ", "")
        corrected_columns.append(col)
    dataframe.columns = corrected_columns
    # Drop duplicates and rows with missing keys
    json_type = [json_type for json_type in JSON_TYPES if json_type in data_url.lower()]
    if json_type:
        if len(json_type) > 1:
            logging.warning("Multiple json_type matches found: %s", json_type)
        json_type = json_type[0]
        logging.info("JSON Type: %s", json_type)
        if set(JSON_KEYS[json_type]) != set(
            dataframe.columns.intersection(JSON_KEYS[json_type])
        ):
            return
        dataframe = dataframe.drop_duplicates(subset=JSON_KEYS[json_type])
        dataframe = dataframe.dropna(subset=JSON_KEYS[json_type], how="any")
        # Handle charging stations with availability information
        if (
            json_type == "chargingstations"
            and "AvailabilityStatus" in dataframe.columns
        ):
            availabilities_df = dataframe[AVAILABILITIES_COLUMNS].copy()
            database_utils.write_availabilities_to_db(availabilities_df)
            logging.info("Availability data written successfully to SQL Database")
            dataframe.drop(columns=CHARGINGSTATIONS_DROP_COLUMNS, inplace=True)
        # Write processed data to database
        try:
            WRITE_TO_DB[json_type](dataframe)
            logging.info("%s data written successfully to SQL Database", json_type)
        except Exception as error:  # pylint: disable=broad-except
            logging.error("Error during database insertion: %s", str(error))


def handle_request_error(error: Exception, message: str) -> func.HttpResponse:
    """
    Function to handle request errors

    Args:
        error (Exception): The error that occurred
        message (str): The error message

    Returns:
        func.HttpResponse: The HTTP response
    """
    logging.error("%s. %s", message, str(error), exc_info=True)
    return func.HttpResponse(message, status_code=400)


def main(req: func.HttpRequest) -> func.HttpResponse:
    """
    This function handles the EVRoam Event Grid trigger.
    For SubscriptionValidationEvent, it returns the validation code.
    For all other events, it downloads the data from the URL in the event body,
    extracts it and enters it into the SQL database.
    """
    logging.info("Python HTTP trigger function processed a request.")

    try:
        req_body = req.get_json()
    except ValueError as error:
        return handle_request_error(error, "Failed to parse the request body.")

    for event in req_body:
        try:
            if event["eventType"] == SUBSCRIBE:
                validation_code = event["data"]["validationCode"]
                validation_response = {"validationResponse": validation_code}
                return func.HttpResponse(
                    body=json.dumps(validation_response),
                    status_code=200,
                    mimetype="application/json",
                )

            event_type = event["eventType"]
            data_url = event["data"]["url"]

            logging.info("Event Type: %s", event_type)
            logging.info("Data URL: %s", data_url)

            if not data_url:
                logging.info(
                    "This HTTP triggered function executed successfully, "
                    "but data_url is not defined."
                )
                continue

            try:
                response = requests.get(data_url, timeout=TIMEOUT)
                response.raise_for_status()
                json_data = response.json()
                if json_data:
                    process_json_data(data_url, json_data)
                else:
                    logging.warning("No data found in the event.")
            except requests.exceptions.RequestException as error:
                logging.error(
                    "Failed to download data from %s. Error: %s", data_url, error
                )

        except Exception as error:  # pylint: disable=broad-except
            logging.error("Error processing event: %s", str(error))
            continue

    return func.HttpResponse(
        "This HTTP triggered function executed successfully.", status_code=200
    )
