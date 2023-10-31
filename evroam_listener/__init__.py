import os
import json
import logging
from urllib.parse import urlparse
import requests
import azure.functions as func
from azure.storage.blob import BlobServiceClient
from constants import *

# Get environment variables
CONNECT_STR = os.getenv('StorageAccountConnectionString')
blob_service_client = BlobServiceClient.from_connection_string(CONNECT_STR)

# Constant variables
SUBSCRIBE = "Microsoft.EventGrid.SubscriptionValidationEvent"
TIMEOUT = 5

def handle_request_error(error: Exception, message: str) -> func.HttpResponse:
    logging.error(message, exc_info=True)
    return func.HttpResponse(message, status_code=400)

def main(req: func.HttpRequest) -> func.HttpResponse:
    """
    This function handles the EVRoam Event Grid trigger.
    For SubscriptionValidationEvent, it returns the validation code.
    For all other events, it downloads the data from the URL in the event body,
    converts it to a CSV file and uploads it to the file-drop container.
    """
    logging.info('Python HTTP trigger function processed a request.')

    try:
        req_body = req.get_json()
    except ValueError as error:
        return handle_request_error(error, "Failed to parse the request body.")

    for event in req_body:
        if event["eventType"] == SUBSCRIBE:
            validation_code = event["data"]["validationCode"]
            validation_response = {"validationResponse": validation_code}
            return func.HttpResponse(
                body=json.dumps(validation_response),
                status_code=200,
                mimetype="application/json"
            )

        event_type = event['eventType']
        data_url = event['data']['url']
        blob_prefix = os.path.splitext(os.path.basename(urlparse(event['subject']).path))[0]
        logging.info("Event Type: %s", event_type)
        logging.info("Data URL: %s", data_url)

        if not data_url:
            return func.HttpResponse(
                "This HTTP triggered function executed successfully, " + \
                "but data_url is not defined.",
                status_code=200
            )

        try:
            response = requests.get(data_url, timeout=TIMEOUT)
            response.raise_for_status()
        except requests.exceptions.RequestException as error:
            return handle_request_error(
                error,
                f"Failed to download data from {data_url}. Error: {str(error)}"
            )

        data = response.json()
        if not data:
            return func.HttpResponse(
                f"No data received from {data_url}",
                status_code=200
            )

        try:
            blob_client = blob_service_client.get_blob_client(
                container=JSON_FILE_PATH['container'],
                blob=JSON_FILE_PATH['path'].format(blob_prefix=blob_prefix)
            )
            blob_client.upload_blob(json.dumps(data).encode('utf-8'), overwrite=True)
        except Exception as error:
            return handle_request_error(
                error,
                f"Failed to process data. Error: {str(error)}"
            )

        return func.HttpResponse(
            "This HTTP triggered function executed successfully. " + \
            f"The data is expected at {data_url}.",
            status_code=200
        )
