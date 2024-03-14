import sys
import os

sys.path.append(os.path.abspath('../sharedCode'))
import database_utils
from datetime import datetime

def get_engine():
    server = 'eeca-sql-dev-aue.database.windows.net'
    database = 'eeca-sqldb-dev-aue-01'
    driver = 'ODBC Driver 17 for SQL Server'
    connection_string = f'mssql+pyodbc://@{server}/{database}?driver={driver}&Authentication=ActiveDirectoryInteractive&TrustServerCertificate=no&Encrypt=yes'
    engine = create_engine(connection_string, echo=True)
    return engine

def test_add_or_update_site():
    site_id = "TestSite001"
    name = "Test Site"
    address = "123 Test Address, Test City"
    other_fields = {
        "Is24Hours": True,
        "MaxTimeLimit": "None",
        "CarParkCount": 10,
        "HasCarparkCost": False,
        "WaterMark": datetime.utcnow()
    }
    result = database_utils.add_or_update_evroam_site(site_id, name, address, **other_fields)
    print(f"Added/Updated Site ID: {result}")

def test_add_or_update_charging_station():
    charging_station_id = "CS001"
    site_id = "TestSite001"
    owner = "Test Owner"
    installation_status = "Operational"
    other_fields = {
        "WaterMark": datetime.utcnow()
    }
    result = database_utils.add_or_update_charging_station(charging_station_id, site_id, owner, installation_status, **other_fields)
    print(f"Added/Updated Charging Station ID: {result}")

# Add similar functions for test_add_or_update_availability and test_add_or_update_connector

if __name__ == "__main__":
    database_utils.create_tables(database_utils.get_engine())  # Ensure tables exist
    test_add_or_update_site()
    test_add_or_update_charging_station()
    # Call other test functions here
