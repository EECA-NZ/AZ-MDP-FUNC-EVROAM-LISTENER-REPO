"""
This is a timer-triggered function. 

It fetches the latest EV charger site data from the EVRoam
API and enters it into the SQL database.

It acts as a fallback for the EVRoam webhook listener, in case
the listener fails to receive the data.
"""

import os
import json
import logging
import datetime
import http.client
import urllib.parse
import pandas as pd
from inflection import camelize
import azure.functions as func
from constants import *
from sharedCode import database_utils

# Ensure the subscription key is available
SUBSCRIPTION_KEY = os.getenv("EvroamSubscriptionKey")
if not SUBSCRIPTION_KEY:
    logging.error("EvroamSubscriptionKey is not set or is empty!")
    raise ValueError("EvroamSubscriptionKey is not set or is empty!")


def main(mytimer: func.TimerRequest) -> None:
    """
    Main function for the Azure timer trigger that
    fetches EVRoam sites data and writes it to the database.
    """
    utc_timestamp = (
        datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc).isoformat()
    )
    if mytimer.past_due:
        logging.warning("The timer is past due!")

    try:
        all_data = fetch_evroam_sites_data()
        if all_data:
            data_frame = process_data_to_dataframe(all_data)
            logging.info("Collected site data: %s rows", len(data_frame))
            database_utils.write_sites_to_db(data_frame)
            logging.info("Site data successfully written to SQL Database")
        else:
            logging.warning("No data was fetched from EVRoam.")
    except Exception as exc:  # pylint: disable=broad-except
        logging.error("Failed to fetch and process EVRoam sites data: %s", exc)

    logging.info("Python timer trigger function ran at %s", utc_timestamp)


def fetch_evroam_sites_data():
    """
    Fetches EVRoam sites data from the API.
    """
    headers = {"Ocp-Apim-Subscription-Key": SUBSCRIPTION_KEY}
    all_data = []
    result_page = 1
    while True:
        params = urllib.parse.urlencode({"resultPage": result_page})
        try:
            conn = http.client.HTTPSConnection("evroam.azure-api.net")
            conn.request("GET", f"/consumer/api/Site?{params}", "{body}", headers)
            response = conn.getresponse()
            if response.status in (200, 202):
                data = json.loads(response.read())
                all_data.extend(data["sites"])
                logging.info(
                    "Successfully fetched page %s: %s sites",
                    result_page,
                    len(data["sites"]),
                )
                if not data["hasMoreResults"]:
                    break
                result_page += 1
            else:
                logging.error(
                    "Failed to fetch data: HTTP %s - %s",
                    response.status,
                    response.reason,
                )
                break
        except http.client.HTTPException as http_exc:
            logging.error("Error fetching data from EVRoam: %s", http_exc)
            break
        finally:
            conn.close()
    return all_data


def process_data_to_dataframe(all_data):
    """
    Processes raw EVRoam sites data into a pandas DataFrame.
    """
    data_frame = pd.DataFrame(all_data)
    corrected_columns = []
    for col in data_frame.columns:
        for character in CHARACTERS_TO_REPLACE:
            col = col.replace(character, " ")
        col = camelize(col.strip(), uppercase_first_letter=True).replace(" ", "")
        corrected_columns.append(col)
    data_frame.columns = corrected_columns
    data_frame.drop_duplicates(inplace=True, subset=JSON_KEYS["sites"])
    logging.info("DataFrame prepared with %s unique sites.", len(data_frame))
    return data_frame
