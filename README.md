# AZ-MDP-FUNC-REPO

We use the Azure Functions extension in Visual Studio Code to write Python code that can be integrated into the pipeline.

The python code is tested locally before deploying it to the environment of Azure Functions.


## To clone the repository from GitHub:

*	Open a terminal window
*   Change to the directory where you want to clone the repositor
*  Run the following command:
```bash
git clone --recurse-submodules git@github.com:EECA-NZ/AZ-MDP-FUNC-REPO.git
```
Note that the `--recurse-submodules` option is required to clone the `py-eecadata` submodule as well.

## To update the `py-eecadata` submodule (when the `py-eecadata` library has been updated):
```bash
git submodule update --remote --merge
```

## To see the function in Azure Portal:

*	Open Azure Portal
*	Click on the icon Resource groups
*	Select the resource group eeca-rg-DWBI-dev-aue
*	Select the function app eeca-func-DWBI-dev-aue
*	On the side bar select Functions
*	Select the function

## To test the function we use VS Code and Postman using the following steps:

1.	Install the Azure Functions extension in Visual Studio Code
2.	Clone (or pull) the folder AZ-MDP-FUNC-REPO from GitHub
3.	Open the folder AZ-MDP-FUNC-REPO in Visual Studio Code


## To sync down app settings from Azure:
```bash
func azure functionapp fetch-app-settings eeca-func-DWBI-dev-aue
```

## To run the function locally in the VS Code terminal:

* The following assume that the appropriate virtual environment has been created via `python3 -m venv .venv`.
* If not, create it via `python3 -m venv .venv` and activate it via `.venv/Scripts/Activate.ps1` (`.venv/bin/activate` on Linux/WSL).
* In the above commands, replace 'python3' with the path to python version 3.x on your system, and make the path work on your system.
* The currently deployed function apps use python 3.10.0, so it is recommended to use this version.

```bash
.venv/Scripts/Activate.ps1
pip install -r requirements.txt
func start
```
