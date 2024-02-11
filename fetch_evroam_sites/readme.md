# Fetch EVRoam Sites - Azure Function

This Azure Function is designed to automatically fetch the latest EVRoam sites data and upload it to Azure Blob Storage. It is triggered by a `TimerTrigger`, executing the process every week. This is a supplementary function to the EVRoam Listener and Aggregator functions (also part of this function app), which receive push notifications from EVRoam. The purpose of this function is to ensure that the EVRoam data is always up-to-date, even if the push notifications fail for any reason.

## How it works

The function uses a `TimerTrigger` with a cron expression to schedule executions. The cron expression `0 0 0 * * 0` configures the function to run weekly at midnight on Sunday.

## Functionality

- **Fetch EVRoam Data:** Retrieves current data on EVRoam sites using the EVRoam API.
- **Data Processing:** Converts the fetched data into a CSV format, ensuring it's ready for storage and ingest via Synapse pipelines.
- **Upload to Blob Storage:** The processed CSV file is uploaded to the file drop container in Azure Blob Storage.

## Configuration

The function requires the following environment variables to be set:

- `StorageAccountConnectionString`: The connection string to the Azure Storage account where the CSV will be stored.
- `EvroamSubscriptionKey`: The subscription key for accessing the EVRoam API.

Ensure these are configured in your Azure Function's Application Settings. For the function in the "dev" account, StorageAccountConnectionString should be the connection string for [the development storage account](https://portal.azure.com/#@eeca.govt.nz/asset/Microsoft_Azure_Storage/StorageAccount/subscriptions/5c1e4ea3-4b2c-40dd-abaf-0d7ad15fd545/resourceGroups/eeca-rg-DWBI-dev-aue/providers/Microsoft.Storage/storageAccounts/eecadlsdevaue). For the function in the "prd" account, StorageAccountConnectionString should be the connection string for [the production storage account](https://portal.azure.com/#@eeca.govt.nz/asset/Microsoft_Azure_Storage/StorageAccount/subscriptions/7d9b57bb-9c10-49e7-94d7-8cf71f638c24/resourceGroups/eeca-rg-DWBI-prd-aue/providers/Microsoft.Storage/storageAccounts/eecadlsprdaue).
