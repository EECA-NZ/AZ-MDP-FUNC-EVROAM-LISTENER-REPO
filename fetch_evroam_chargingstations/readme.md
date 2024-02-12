# Fetch EVRoam Charging Stations - Azure Function

This Azure Function is designed to automatically retrieve the latest information on EVRoam charging stations and their availabilities, and upload it to Azure Blob Storage. It is triggered by a `TimerTrigger`, executing the process every week. This function complements the EVRoam Listener and Aggregator functions (also part of this function app), which receive push notifications from EVRoam. The purpose of this function is to ensure that the EVRoam data is always up-to-date, even if the push notifications fail for any reason.

## How it works

The function uses a `TimerTrigger` with a cron expression to schedule its executions. The cron expression `0 0 0 * * 0` is set to trigger the function weekly, precisely at midnight on Sunday.

## Functionality

- **Fetch EVRoam Charging Stations Data:** Connects to the EVRoam API to pull the latest data on charging stations and their availabilities.
- **Data Processing:** Processes the retrieved data, normalizing and converting it into a CSV format suitable for storage and further ingestion processes.
- **Upload to Blob Storage:** Uploads the processed CSV files to designated containers in Azure Blob Storage, ensuring data is ready for analysis and integration.

## Configuration

The function requires the following environment variables to be set:

- `StorageAccountConnectionString`: The connection string for the Azure Storage account where the CSV files will be stored. This connection string varies depending on whether the function is running in the "dev" or "prd" environment.
- `EvroamSubscriptionKey`: The subscription key necessary for authenticating requests to the EVRoam API.

These variables need to be set in the Function's Application Settings. For the function operating in the "dev" environment, use the connection string from [the development storage account](https://portal.azure.com/#@eeca.govt.nz/asset/Microsoft_Azure_Storage/StorageAccount/subscriptions/5c1e4ea3-4b2c-40dd-abaf-0d7ad15fd545/resourceGroups/eeca-rg-DWBI-dev-aue/providers/Microsoft.Storage/storageAccounts/eecadlsdevaue). For the function in the "prd" environment, the connection string should be sourced from [the production storage account](https://portal.azure.com/#@eeca.govt.nz/asset/Microsoft_Azure_Storage/StorageAccount/subscriptions/7d9b57bb-9c10-49e7-94d7-8cf71f638c24/resourceGroups/eeca-rg-DWBI-prd-aue/providers/Microsoft.Storage/storageAccounts/eecadlsprdaue).