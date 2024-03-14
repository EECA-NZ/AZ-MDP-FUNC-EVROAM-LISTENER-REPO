[![Pylint](https://github.com/EECA-NZ/AZ-MDP-FUNC-EVROAM-LISTENER-REPO/actions/workflows/pylint.yml/badge.svg)](https://github.com/EECA-NZ/AZ-MDP-FUNC-EVROAM-LISTENER-REPO/actions/workflows/pylint.yml) [![Deploy](https://github.com/EECA-NZ/AZ-MDP-FUNC-EVROAM-LISTENER-REPO/actions/workflows/deploy-to-dev.yml/badge.svg)](https://github.com/EECA-NZ/AZ-MDP-FUNC-EVROAM-LISTENER-REPO/actions/workflows/deploy-to-dev.yml) [![Unit Test](https://github.com/EECA-NZ/AZ-MDP-FUNC-EVROAM-LISTENER-REPO/actions/workflows/python-tests.yml/badge.svg)](https://github.com/EECA-NZ/AZ-MDP-FUNC-EVROAM-LISTENER-REPO/actions/workflows/python-tests.yml)

# AZ-MDP-FUNC-EVROAM-LISTENER-REPO

This repository is for the function app deployed at `eeca-func-DWBI-evroam-listener-[dev/prd]-aue` in the resource group `eeca-rg-DWBI-[dev/prd]-aue`.

## Functions Overview

* `evroam_listener` - Receives push notifications from EVRoam, fetching JSON files with dataset updates.
* `fetch_evroam_sites` - Fetches EVRoam site information periodically, ensuring data remains up-to-date.
* `fetch_evroam_chargingstations` - Fetches charging station and availability information periodically as a backup to push notifications.

Once the function is deployed into the `dev`/`prd` environment, follow the instructions in the `scripts/subscribe_evroam_listener.py` to activate a subscription to push notifications from evroam. Note that only one subscription can be active (for a given EVRoam API key) at a time.

## Managed Identity Configuration

To enable Managed Identity for your Azure Function App and grant it access to an Azure SQL Database, follow these steps:

### Enable Managed Identity

1. Navigate to your Function App in the Azure Portal.
2. Under **Platform features**, find the **Identity** section.
3. Enable **System Assigned Managed Identity**.

### Grant Access to SQL Server

1. Ensure SQL Server allows Azure services to access it.
2. Grant the Function App's managed identity appropriate roles (e.g., **SQL DB Contributor**).

### SQL Permissions

Ensure that the following SQL commands are present in the `02-AddUsers.sql` postdeployment script of the SQL database:
```sql
CREATE USER [eeca-func-DWBI-evroam-listener-[dev/prd]-aue] FOR EXTERNAL PROVIDER;
CREATE SCHEMA EECAEVROAM
GRANT CONTROL ON SCHEMA::[EECAEVROAM] TO [your-function-app-name];
```

### Schema Management

The app uses SQLAlchemy for ORM. Tables are auto-created on first run but not altered. To update the schema during development:

```sql
DROP TABLE EECAEVROAM.ChargingStations;
DROP TABLE EECAEVROAM.Sites;
```

## Development and Deployment

We use Visual Studio Code with the Azure Functions extension for development. The `dev` environment is used for development and testing before deployment to `prd`.

To begin work on the function app, clone the repository from GitHub. The function app is developed in a branch off of the `dev` branch. Merging changes into the `dev` branch triggers the GitHub Actions workflow to deploy the function app to the `dev` environment. Once the function app is ready for deployment, it is merged into the `main` branch. This triggers the GitHub Actions workflow to deploy the function app to the `prd` environment.

## To clone the repository from GitHub:

*	Open a terminal window
*   Change to the directory where you want to clone the repository
*   Clone the `dev` branch of the repository from GitHub
*   Create a new branch for your work
```bash
git clone --recurse-submodules git@github.com:EECA-NZ/AZ-MDP-FUNC-EVROAM-LISTENER-REPO.git -b dev
git checkout -b dev-[task-description-from-teams-task-planner-board]
```
Then when you are ready to merge your changes into the `dev` branch, create a pull request in GitHub.

## To see the function in Azure Portal:

*	Open Azure Portal
*	Click on the icon Resource groups
*	Select the resource group eeca-rg-DWBI-dev-aue
*	Select the function app eeca-func-DWBI-evroam-listener-dev-aue
*	On the side bar select Functions
*	Select the function

## To sync down app settings from Azure:
```bash
func azure functionapp fetch-app-settings eeca-func-DWBI-dev-aue
```

## To run the function locally:

This can be done in VSCode if the root directory of this project has been opened via `Open Folder`, and a virtual environment has been set up by accepting the offer from VSCode to do so. Run the function app locally via `Run -> Start Debugging (F5)`.

Alternatively, the function app can be run via command line as follows:
* The following assume that the appropriate virtual environment has been created via `python3 -m venv .venv`.
* If not, create it via `python3 -m venv .venv` and activate it via `.venv/Scripts/Activate.ps1` (`.venv/bin/activate` on Linux/WSL).
* In the above commands, replace 'python3' with the path to python version 3.x on your system.
* The currently deployed function apps use python 3.10.0, so it is recommended to use this version.

```bash
.venv/Scripts/Activate.ps1
pip install -r requirements.txt
func start
```

## Manual deployment the function app in the `dev` environment.

Manual deployment to the `dev` environment can be used for testing the app while it is in development. Note that merging to the `dev` branch also will trigger a CICD deployment to the `dev` environment.

It is assumed that this project is active in VSCode (the project has been opened via `Open Folder`), and that the user has logged into Azure with VSCode.
* Navigate in `Azure Resources` to the `eeca-func-DWBI-evroam-listener-dev-aue` function app.
* Right-click on `eeca-func-DWBI-evroam-listener-dev-aue`, and select "Deploy to Function App".
* Agree to overwrite the existing deployment.

Deployment to the `prd` environment follows an equivalent process.

Note that the deployment requires an appropriate `StorageAccountConnectionString` parameter to be set manually in the Azure Portal.