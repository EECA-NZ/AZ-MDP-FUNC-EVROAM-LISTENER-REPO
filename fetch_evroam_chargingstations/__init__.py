"""
This is a timer-triggered function.

It fetches the latest EV charger charging stations and availability data from the EVRoam
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
    fetches EVRoam charging stations data and writes it to the database.
    """
    utc_timestamp = (
        datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc).isoformat()
    )
    if mytimer.past_due:
        logging.warning("The timer is past due!")

    try:
        all_data = fetch_evroam_chargingstations_data()
        if all_data:
            availabilities, chargingstations = process_data_to_dataframes(all_data)
            logging.info("Charging Station and Availability data processed")
            database_utils.write_availabilities_to_db(availabilities)
            database_utils.write_chargingstations_to_db(chargingstations)
            logging.info(
                "Charging Station and Availability data written to SQL Database"
            )
        else:
            logging.warning("No data was fetched from EVRoam.")
    except Exception as exc: # pylint: disable=broad-except
        logging.error(
            "Failed to fetch and process EVRoam charging stations data: %s", exc
        )

    logging.info("Python timer trigger function ran at %s", utc_timestamp)


def fetch_evroam_chargingstations_data():
    """
    Fetches EVRoam charging stations data from the API.
    """
    headers = {"Ocp-Apim-Subscription-Key": SUBSCRIPTION_KEY}
    all_data = []
    result_page = 1
    while True:
        params = urllib.parse.urlencode({"resultPage": result_page})
        try:
            conn = http.client.HTTPSConnection("evroam.azure-api.net")
            conn.request(
                "GET", f"/consumer/api/ChargingStation?{params}", "{body}", headers
            )
            response = conn.getresponse()
            if response.status in (200, 202):
                data = json.loads(response.read())
                all_data.extend(data["chargingStations"])
                logging.info(
                    "Successfully fetched page %s: %s charging stations",
                    result_page,
                    len(data["chargingStations"]),
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


def process_data_to_dataframes(all_data):
    """
    Processes raw EVRoam charging stations data into pandas DataFrames for
    charging stations and availabilities.
    """
    dataframe = pd.json_normalize(all_data)
    corrected_columns = []
    for col in dataframe.columns:
        for character in CHARACTERS_TO_REPLACE:
            col = col.replace(character, " ")
        col = camelize(col.strip(), uppercase_first_letter=True).replace(" ", "")
        corrected_columns.append(col)
    dataframe.columns = corrected_columns

    availabilities_df = dataframe[AVAILABILITIES_COLUMNS].copy()
    availabilities_df.drop_duplicates(inplace=True, subset=JSON_KEYS["availabilities"])
    availabilities_df.dropna(
        inplace=True, subset=JSON_KEYS["availabilities"], how="any"
    )

    chargingstations_df = dataframe.drop(columns=CHARGINGSTATIONS_DROP_COLUMNS).copy()
    chargingstations_df.drop_duplicates(
        inplace=True, subset=JSON_KEYS["chargingstations"]
    )
    chargingstations_df.dropna(
        inplace=True, subset=JSON_KEYS["chargingstations"], how="any"
    )

    return availabilities_df, chargingstations_df
