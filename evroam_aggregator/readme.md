# Azure Function: EVRoam Data Aggregator (TimerTrigger - Python)

The `EVRoam Data Aggregator` is a serverless Azure function which periodically aggregates data from the EVRoam API and stores it in Azure Blob Storage. This function is executed on a schedule as specified by a cron expression. 

## Overview

This function fetches and processes data from JSON blobs stored in an Azure Blob Storage, in the "incoming-data-staging" container. The blobs are processed in a predefined order, transformed into pandas DataFrames, and concatenated based on their types. 

The aggregated data is then saved back to Blob Storage as CSV files in the file-drop area for ingest into our SQL database, with each type of data stored in its own CSV file. After successful processing and storage, the original JSON blobs are deleted.

## How it works

The function uses a `TimerTrigger` to run at a set schedule. This is defined by a [cron expression](https://en.wikipedia.org/wiki/Cron#CRON_expression) that specifies the timing for function execution. 

For example, a cron expression like `0 0 5 * * *` means: "Run this function at 5 AM every day". 

In this particular case, the cron expression for the function is set to execute once every day at 5 AM.

## Set Up and Execution

1. Make sure you have the necessary environment variables set up in your Azure Function. These include your Blob Storage connection strings, database connection details, and other important configuration values.

2. Deploy your Azure Function.

3. Once deployed, the function will run automatically at the specified schedule. You can also trigger it manually from the Azure portal if needed.
