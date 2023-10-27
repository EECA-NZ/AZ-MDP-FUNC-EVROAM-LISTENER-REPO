"""
Script to subscribe to EVRoam's webhook listener.

To configure the script, first set the environment variables as follows:

EVROAM_SUBSCRIPTION_KEY: API key for EVRoam
- Log into the EVRoam developer portal (https://evroam.portal.azure-api.net/). 
- Navigate to your [developer profile](https://evroam.portal.azure-api.net/developer) and click 'Show API subscription key'.
- Copy the key and set it as the value for the SUBSCRIPTION_KEY environment variable.

EVROAM_WEBHOOK_ENDPOINT: URL of evroam_listener function
- Ensure the evroam_listener function is active in your deployed function app.
- Click "Get Function Url" in the Azure portal to get the URL for the function. 
  Select default (host key).
- Copy the URL and set it as the value for the WEBHOOK_ENDPOINT environment variable.

After setting up the environment variables, the script can be run to subscribe to the EVRoam webhook listener. 
Upon successful execution, the script will return a response like this:

b'"Subscription created with end point https://<function-app-name-endpoint-url>"'
"""

import os
import json
import http.client
import urllib.parse
import argparse
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Parse command line arguments for environment
parser = argparse.ArgumentParser(description='Set up notifications for evroam based on the environment.')
parser.add_argument('env', choices=['dev', 'prd'], help='Environment (dev or prd)')
args = parser.parse_args()

# Depending on environment, set webhook endpoint variable
if args.env == 'dev':
    WEBHOOK_ENDPOINT_VAR = "DEV_EVROAM_WEBHOOK_ENDPOINT"
elif args.env == 'prd':
    WEBHOOK_ENDPOINT_VAR = "PRD_EVROAM_WEBHOOK_ENDPOINT"

# Get environment variables
SUBSCRIPTION_KEY = os.getenv("EVROAM_SUBSCRIPTION_KEY")
WEBHOOK_ENDPOINT = os.getenv(WEBHOOK_ENDPOINT_VAR)

if not WEBHOOK_ENDPOINT:
    print(f"Webhook endpoint for {args.env} not found.")
    exit()

# Set request headers and body
headers = {
    'Content-Type': 'application/json',
    'Ocp-Apim-Subscription-Key': SUBSCRIPTION_KEY,
}

body = json.dumps({
    "webHookEndPoint": WEBHOOK_ENDPOINT,
    "inlcudeSiteChanges": True,
    "includeChargingStationChanges": True,
    "includeChargingStationAvailabilityChanges": True
})

# Empty parameters for the POST request
params = urllib.parse.urlencode({})

try:
    # Establish connection and make request
    conn = http.client.HTTPSConnection('evroam.azure-api.net')
    conn.request("POST", "/consumer/api/Notification?%s" % params, body, headers)

    # Get response and print
    response = conn.getresponse()
    data = response.read()
    print(data)

    # Close connection
    conn.close()

except OSError as e:
    # Handle exception and print error message
    print(f"[Errno {e.errno}] {e.strerror}")