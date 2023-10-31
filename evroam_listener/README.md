# EVRoam Webhook Listener Subscription Guide

This guide will help you set up a subscription to the EVRoam change notification feed. The steps involve deploying an Azure function app, setting the required environment variables, and executing the provided Python script. After setup, you can monitor the function's activity in the Azure portal.

## Prerequisites

Ensure you have the following:

- Access to the Azure portal
- Python installed on your machine
- The provided Python script: `../scripts/subscribe_evroam_listener.py`

## Steps

Follow these steps to activate the subscription:

### 1. Deploy the Function App

First, deploy this function app in the Azure portal. Make sure that the `evroam_listener` function is active.

### 2. Set Environment Variables

The `subscribe_evroam_listener.py` script requires specific environment variables to run correctly. To set these, you will need to populate the `.env` file. You can find detailed instructions in the docstring of the `subscribe_evroam_listener.py` script.

The variables you need to set are:

- `SUBSCRIPTION_KEY`: Your API key for EVRoam.
- `WEBHOOK_ENDPOINT`: The URL of the `evroam_listener` function.

### 3. Run the Script

Navigate to the directory containing `subscribe_evroam_listener.py` and run the script. In most systems, you can do this by opening a terminal window, navigating to the correct directory, and entering the command `python subscribe_evroam_listener.py`.

### 4. Monitor the Function Activity

After setting up the subscription, you can monitor the activity of the `evroam_listener` function in the Azure portal. The function typically gets invoked every 5 minutes.

## Troubleshooting

If you encounter any issues while setting up or monitoring the function, check the error messages provided in the Azure portal or in the script's output. Most issues can be resolved by ensuring you've followed all steps correctly and by verifying the values of your environment variables.
