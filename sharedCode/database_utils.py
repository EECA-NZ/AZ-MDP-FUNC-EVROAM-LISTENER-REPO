"""
This module provides utilities for interacting with the database in the EVRoam project. It includes
definitions for the database models (EVRoamSites, EVRoamChargingStations, EVRoamAvailabilities) and
functions to add or update these entities using SCD Type 2 logic.
"""

import os
import urllib
import logging
import hashlib
from datetime import datetime
from contextlib import contextmanager
import pyodbc
import sqlalchemy
import pandas as pd
from sqlalchemy import create_engine, CHAR
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import String, Float, Boolean, Integer, DateTime
from sqlalchemy import Column, VARBINARY


env = os.getenv("env", "dev")

SCHEMA = "EECAEVRoam"

Base = declarative_base()

# pylint: disable=too-few-public-methods
# pylint: disable=broad-exception-caught


class EVRoamSites(Base):
    """
    Represents an EV charging site within the EVRoam ecosystem. This model captures
    comprehensive details about each site, including its location, operational status,
    and various attributes that describe the site's features and offerings.
    """

    __tablename__ = "dboEVRoamSites"
    __table_args__ = {"schema": SCHEMA}
    SiteId = Column(
        String(255),
        index=True,
        info={
            "description": "Vendor provided UNIQUE ID to link to the ChargingStation worksheet."
        },
    )
    AccessLocations = Column(
        String(4096), info={"description": "JSON string of access locations."}
    )
    Address = Column(
        String(2048), info={"description": "Physical address of the site."}
    )
    CarParkCount = Column(
        Integer, info={"description": "Number of car parks available at the site."}
    )
    HasCarparkCost = Column(
        Boolean,
        info={
            "description": "Indicates if there is a cost associated with the carpark."
        },
    )
    HasTouristAttraction = Column(
        Boolean,
        info={"description": "Indicates if there is a tourist attraction nearby."},
    )
    Is24Hours = Column(
        Boolean, info={"description": "States if available 24 hrs or not."}
    )
    MaxTimeLimit = Column(
        String(255), info={"description": "Maximum time limit for parking."}
    )
    Name = Column(String(255), info={"description": "Name of Site."})
    Operator = Column(String(255), info={"description": "Operator of the site."})
    ProviderDeleted = Column(
        Boolean,
        info={
            "description": "Flag to indicate if the provider has marked the site as deleted."
        },
    )
    WaterMark = Column(
        DateTime,
        default=datetime.utcnow,
        info={"description": "Timestamp for the last update."},
    )
    ODSdboEVRoamSitesSKID = Column(Integer, primary_key=True, autoincrement=True)
    ODSBatchID = Column(Integer, info={"description": "Batch ID for the operation."})
    ODSEffectiveFrom = Column(
        DateTime, info={"description": "Effective from date for SCD."}
    )
    ODSEffectiveTo = Column(
        DateTime, info={"description": "Effective to date for SCD, null if current."}
    )
    ODSIsCurrent = Column(
        Boolean, info={"description": "Indicates if the record is the current record."}
    )
    ODSDMLType = Column(CHAR(1), info={"description": "Type of DML operation."})
    ODSHashKey = Column(
        VARBINARY(8000), info={"description": "Hash key for detecting changes."}
    )
    ODSExternalID = Column(
        String(50), info={"description": "External ID for reference."}
    )


class EVRoamChargingStations(Base):
    """
    Represents a charging station at an EVRoam site. This model stores information
    specific to individual charging stations, such as the type of connectors available,
    the station's operational status, and its physical characteristics.
    """

    __tablename__ = "dboEVRoamChargingStations"
    __table_args__ = {"schema": SCHEMA}
    ChargingStationId = Column(
        String(255),
        index=True,
        info={"description": "Vendor provided UNIQUE ID."},
    )
    SiteId = Column(
        String(36),
        info={
            "description": (
                "SiteId from the previous Site worksheet. IDs in this column do "
                "not need to be unique as you can have many charging stations to one site."
            )
        },
    )
    AssetId = Column(
        String(255),
        info={
            "description": (
                "Vendors Asset ID. This defaults to the "
                "chargingStationId but can be overwritten with another ID."
            )
        },
    )
    Connectors = Column(
        String(4096),
        info={
            "description": (
                "JSON string representing connector "
                "information for this Charging Station."
            )
        },
    )
    Current = Column(
        String(255), info={"description": 'Acceptable values are "AC" or "DC".'}
    )
    DateFirstOperational = Column(
        DateTime,
        info={
            "description": "Date it was first able to be used. Date without time is acceptable."
        },
    )
    FloorLevel = Column(
        String(255),
        info={
            "description": (
                "A human-readable description of the floor/level "
                'the charging station is on, e.g., "1", "2B", or "UG".'
            )
        },
    )
    HasChargingCost = Column(
        Boolean,
        info={
            "description": (
                "Indicates if there is a cost associated with using the charging station."
            )
        },
    )
    Images = Column(
        String(4096),
        info={
            "description": "Public image URLs for the charging station, up to 3, comma separated."
        },
    )
    InstallationStatus = Column(
        String(255),
        info={
            "description": (
                'Current installation status of the charging station, e.g., "Proposed", '
                '"Planned", "Under Construction", "Commissioned", "Decommissioned", "Abandoned".'
            )
        },
    )
    KwRated = Column(
        Integer,
        info={
            "description": "The rated power output of the charging station in kilowatts."
        },
    )
    Locationlat = Column(
        Float, info={"description": "Latitude of the charging station location."}
    )
    Locationlon = Column(
        Float, info={"description": "Longitude of the charging station location."}
    )
    Manufacturer = Column(
        String(2048), info={"description": "Manufacturer of the charging station."}
    )
    Model = Column(String(2048), info={"description": "Model of the charging station."})
    NextPlannedOutage = Column(
        DateTime,
        info={
            "description": (
                "Datetime of any planned outages, in the format; "
                "startDateTime,endDateTime."
            )
        },
    )
    Operator = Column(
        String(255),
        info={"description": "Name of the operator of the charging station."},
    )
    Owner = Column(
        String(255), info={"description": "Name of the owner of the charging station."}
    )
    ProviderDeleted = Column(
        Boolean,
        info={
            "description": (
                "Flag to indicate if the provider has marked the charging station "
                "as deleted."
            )
        },
    )
    WaterMark = Column(
        DateTime,
        default=datetime.utcnow,
        info={"description": "Timestamp for the last update."},
    )
    ODSdboEVRoamChargingStationsSKID = Column(
        Integer, primary_key=True, autoincrement=True
    )
    ODSBatchID = Column(Integer, info={"description": "Batch ID for the operation."})
    ODSEffectiveFrom = Column(
        DateTime, info={"description": "Effective from date for SCD."}
    )
    ODSEffectiveTo = Column(
        DateTime, info={"description": "Effective to date for SCD, null if current."}
    )
    ODSIsCurrent = Column(
        Boolean, info={"description": "Indicates if the record is the current record."}
    )
    ODSDMLType = Column(CHAR(1), info={"description": "Type of DML operation."})
    ODSHashKey = Column(
        VARBINARY(8000), info={"description": "Hash key for detecting changes."}
    )
    ODSExternalID = Column(
        String(50), info={"description": "External ID for reference."}
    )


class EVRoamAvailabilities(Base):
    """
    Tracks the availability status of EVRoam charging stations. This model is used to
    log changes in availability, such as when a station becomes occupied or available,
    and includes timestamps for these status updates.
    """

    __tablename__ = "dboEVRoamAvailabilities"
    __table_args__ = {"schema": SCHEMA}
    ChargingStationId = Column(
        String(255),
        info={
            "description": (
                "The charging station this availability record pertains to."
            )
        },
    )
    AvailabilityTime = Column(
        DateTime,
        info={"description": "Timestamp when the availability status was recorded."},
    )
    AvailabilityStatus = Column(
        String(255),
        info={
            "description": (
                "Current availability status of the charging "
                "station (e.g., Available, Occupied)."
            )
        },
    )
    KwAvailable = Column(
        Float,
        info={
            "description": (
                "The amount of power (in kW) available at the charging "
                "station at the time of this availability record."
            )
        },
    )
    Operator = Column(
        String(255), info={"description": "Operator of the charging station."}
    )
    WaterMark = Column(
        DateTime,
        default=datetime.utcnow,
        info={"description": "Timestamp for the last update of this record."},
    )
    ODSdboEVRoamAvailabilitiesSKID = Column(
        Integer,
        primary_key=True,
        autoincrement=True,
        info={"description": "Auto-incremented primary key."},
    )
    ODSBatchID = Column(
        Integer,
        info={
            "description": (
                "Batch ID associated with the operation "
                "that generated or modified this record."
            )
        },
    )
    ODSEffectiveFrom = Column(
        DateTime,
        info={
            "description": (
                "The datetime from which this record is "
                "considered effective (start of validity)."
            )
        },
    )
    ODSEffectiveTo = Column(
        DateTime,
        info={
            "description": (
                "The datetime until which this record is considered effective "
                "(end of validity). Null if currently effective."
            )
        },
    )
    ODSIsCurrent = Column(
        Boolean,
        info={
            "description": "Flag indicating whether this record is the current, effective record."
        },
    )
    ODSDMLType = Column(
        CHAR(1),
        info={
            "description": (
                "Indicates the type of DML operation that generated "
                "this record (e.g., Insert, Update, Delete)."
            )
        },
    )
    ODSHashKey = Column(
        VARBINARY(8000),
        info={
            "description": "A hash key generated from the record's contents to detect changes."
        },
    )
    ODSExternalID = Column(
        String(50),
        info={
            "description": (
                "An external identifier that can be used to link "
                "this record to external systems or datasets."
            )
        },
    )


def get_engine(verbose=False):
    """
    Creates and returns a SQLAlchemy engine instance configured
    for Azure SQL Database using Active Directory MSI authentication.

    The function constructs the connection URL dynamically using
    environment variables to determine the environment ('dev' by
    default) and constructs the connection string accordingly.

    Returns:
        sqlalchemy.engine.Engine: An instance of SQLAlchemy engine
        connected to the specified Azure SQL Database.
    """
    server = f"eeca-sql-{env}-aue.database.windows.net"
    database = f"eeca-sqldb-{env}-aue-01"
    if os.getenv("WEBSITE_HOSTNAME"):
        auth_method = "Authentication=ActiveDirectoryMsi"
    else:
        auth_method = "Authentication=ActiveDirectoryInteractive"
    drivers_to_try = [
        "ODBC Driver 18 for SQL Server",
        "ODBC Driver 17 for SQL Server",
        "ODBC Driver 13 for SQL Server",
    ]
    for driver in drivers_to_try:
        try:
            params = urllib.parse.quote_plus(
                f"Driver={{{driver}}};"
                f"Server=tcp:{server},1433;"
                f"Database={database};"
                f"{auth_method};"
                "Encrypt=yes;"
                "TrustServerCertificate=no;"
                "Connection Timeout=30;"
            )
            connection_url = f"mssql+pyodbc:///?odbc_connect={params}"
            engine = create_engine(connection_url, echo=verbose)
            # Test the connection
            with engine.connect() as _:
                logging.info("Successfully connected using %s", driver)
                return engine
        except sqlalchemy.exc.DBAPIError as error:
            logging.info("Failed to connect using %s: %s", driver, error)
            logging.info("Trying next driver...")
        except pyodbc.Error as error:  # pylint: disable=c-extension-no-member
            logging.info("Failed to connect using %s: %s", driver, error)
            logging.info("Trying next driver...")
    # pylint: disable=broad-exception-raised
    raise Exception("Failed to connect to SQL using any of the drivers tried.")


def create_tables(engine):
    """
    Creates all tables in the database based on the SQLAlchemy Base metadata.

    This function should be called after all model classes have been defined
    and associated with the Base metadata object. It uses the engine to connect
    to the database and creates all tables that don't already exist.

    Args:
        engine (sqlalchemy.engine.Engine): The SQLAlchemy engine instance used
        for database connection.

    """
    Base.metadata.create_all(engine)


def get_session():
    """
    Creates a new SQLAlchemy session factory bound to the engine and returns
    a new session instance.

    This function is a factory that produces new session objects when called,
    using the engine configuration defined in `get_engine`.

    Returns:
        sqlalchemy.orm.session.Session: A new SQLAlchemy session object for
        database operations.
    """
    engine = get_engine()
    create_tables(engine)
    session = sessionmaker(bind=engine)
    return session()


@contextmanager
def session_scope():
    """Provide a transactional scope around a series of operations."""
    session = get_session()
    try:
        yield session
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()


def generate_hash_key(*args):
    """
    Generates a SHA-256 hash key from the provided arguments.

    This function concatenates all arguments into a single string and
    generates a SHA-256 hash of this string. It's used to create a unique
    hash key for database records to facilitate change detection.

    Args:
        *args: Variable length argument list used to generate the hash key.

    Returns:
        bytes: The generated SHA-256 hash key.
    """
    hash_key = hashlib.sha256()
    for arg in args:
        hash_key.update(str(arg).encode("utf-8"))
    return hash_key.digest()


def validate_required_fields(model_class, provided_fields):
    """
    Validates that all required fields are present in provided_fields.

    :param model_class: The SQLAlchemy model class to validate against.
    :param provided_fields: Dictionary of fields provided to the function.
    :raises ValueError: If a required field is missing.
    """
    # Inspect the model class for column attributes
    for column in model_class.__table__.columns:
        # Check if the column is nullable or has a default value
        is_nullable = column.nullable
        has_default = column.default is not None or column.server_default is not None

        # If the field is required (not nullable and no default), check it's provided
        if not is_nullable and not has_default:
            if column.name not in provided_fields:
                raise ValueError(f"Missing required field: {column.name}")


def add_or_update_record(model, unique_keys, hash_keys, session=None, **fields):
    """
    Adds a new record or updates an existing one using SCD Type 2 logic.

    This function checks if the provided record already exists based on its unique keys.
    If it does and changes are detected (via hash comparison), it marks the existing record
    as historical and inserts a new record with the current data. If the record does not
    exist, it simply inserts a new record.

    Args:
        model (Base): The SQLAlchemy model class for the table.
        unique_keys (dict): Dictionary of unique key-value pairs identifying the record.
        hash_keys (list): List of keys used to generate the hash for change detection.
        session (sqlalchemy.orm.session.Session, optional): The SQLAlchemy session to use. Optional.
        **fields: Additional fields of the record, passed as keyword arguments.

    Returns:
        The primary key of the newly added or updated record.
    """
    own_session = False
    if session is None:
        session = get_session()
        own_session = True

    try:
        # Generate hash key for incoming data
        hash_values = [fields[key] for key in hash_keys]
        incoming_hash = generate_hash_key(*hash_values)

        # Check if the record already exists and if it's the current version
        query = session.query(model).filter_by(**unique_keys, ODSIsCurrent=True)
        existing_record = query.first()

        if existing_record and existing_record.ODSHashKey == incoming_hash:
            logging.debug(
                "No material change detected for %s with ID %s.",
                model.__name__,
                unique_keys,
            )
            # Assuming a single PK field, dynamically get the PK name and value
            pk_name = model.__table__.primary_key.columns.keys()[0]
            return getattr(existing_record, pk_name)
        logging.debug(
            "Material change detected or new record for %s: %s.", model.__name__, fields
        )

        if existing_record:
            existing_record.ODSEffectiveTo = datetime.now()
            existing_record.ODSIsCurrent = False

        # Insert new or updated record as current
        new_record = model(
            **unique_keys,
            **fields,
            ODSEffectiveFrom=datetime.now(),
            ODSEffectiveTo=None,
            ODSIsCurrent=True,
            ODSHashKey=incoming_hash,
        )
        session.add(new_record)
        if own_session:
            session.commit()

        # Dynamically get the PK name and value for the new record
        pk_name = model.__table__.primary_key.columns.keys()[0]
        return getattr(new_record, pk_name)
    except Exception as exception:
        if own_session:
            session.rollback()
        logging.warning("Error: %s", exception)
        raise
    finally:
        if own_session:
            session.close()


def get_dynamic_hash_keys(model, exclude=None):
    """
    Generate a list of hash keys for a given SQLAlchemy model,
    excluding any fields that match certain criteria.

    Args:
        model (Base): The SQLAlchemy model class for the table.
        exclude (list): List of strings to exclude columns that contain any of these.

    Returns:
        list: A list of strings representing the hash keys.
    """
    if exclude is None:
        exclude = []

    hash_keys = []
    for column in model.__table__.columns:
        column_name = column.name
        if not any(excluded in column_name for excluded in exclude):
            hash_keys.append(column_name)
    return hash_keys


def add_or_update_evroam_site(site_id, name, address, session=None, **other_fields):
    """
    Adds a new EVRoam site or updates an existing one using SCD
    Type 2 logic.

    This function checks if the provided site already exists based
    on its `site_id`. If it does and changes are detected,
    it marks the existing record as historical (sets `ODSEffectiveTo`
    to now) and inserts a new record with the current data.
    If the site does not exist, it simply inserts a new record.

    Args:
        site_id (str): The unique identifier for the site.
        name (str): The name of the site.
        address (str): The address of the site.
        **other_fields: Additional fields of the site, passed as keyword
        arguments. These fields should match the column names of the
        `EVRoamSites` table.

    Returns:
        int: The primary key (`ODSdboEVRoamSitesSKID`) of the newly
        added or updated site record.

    Raises:
        Exception: If any database operation fails.
    """
    unique_keys = {"SiteId": site_id}
    hash_keys = get_dynamic_hash_keys(
        EVRoamSites, exclude=["WaterMark", "ODS", "SiteId"]
    )

    return add_or_update_record(
        EVRoamSites,
        unique_keys,
        hash_keys,
        Name=name,
        Address=address,
        session=session,
        **other_fields,
    )


def add_or_update_charging_station(
    charging_station_id,
    site_id,
    owner,
    installation_status,
    session=None,
    **other_fields,
):
    """
    Adds a new charging station or updates an existing one using SCD
    Type 2 logic.

    This function checks if the provided charging station already exists
    based on its `charging_station_id`. If it does and changes are detected,
    it marks the existing record as historical (sets `ODSEffectiveTo` to now)
    and inserts a new record with the current data. If the charging station
    does not exist, it simply inserts a new record.

    Args:
        charging_station_id (str): The unique identifier for the charging station.
        site_id (str): The unique identifier of the site to which the charging station belongs.
        owner (str): The owner of the charging station.
        installation_status (str): The installation status of the charging station.
        **other_fields: Additional fields of the charging station, passed as keyword arguments.
        These fields should match the column names of the `EVRoamChargingStations` table.

    Returns:
        int: The primary key (`ODSdboEVRoamChargingStationsSKID`) of the newly added or updated
        charging station record.

    Raises:
        Exception: If any database operation fails.
    """
    unique_keys = {"ChargingStationId": charging_station_id}
    hash_keys = get_dynamic_hash_keys(
        EVRoamChargingStations, exclude=["WaterMark", "ODS", "ChargingStationId"]
    )

    fields = {
        "SiteId": site_id,
        "Owner": owner,
        "InstallationStatus": installation_status,
        **other_fields,
    }
    return add_or_update_record(
        EVRoamChargingStations, unique_keys, hash_keys, session=session, **fields
    )


def add_or_update_availability(
    charging_station_id,
    availability_status,
    availability_time,
    session=None,
    **other_fields,
):
    """
    Adds a new availability record or updates an existing one for a charging
    station using SCD Type 2 logic.

    This function checks if an availability record for the provided `charging_station_id`
    already exists and if changes are detected,
    it marks the existing record as historical (sets `ODSEffectiveTo` to now) and inserts
    a new record with the current data.
    If no existing record is found, it inserts a new record.

    Args:
        charging_station_id (str): The unique identifier of the charging
        station for which the availability is being reported.
        availability_status (str): The current availability status of the charging station.
        availability_time (datetime): The time at which the availability status is reported.
        **other_fields: Additional fields related to the availability, passed as keyword arguments.
        These fields should match the column names of the `EVRoamAvailabilities` table.

    Returns:
        int: The primary key (`ODSdboEVRoamAvailabilitiesSKID`) of the newly added
        or updated availability record.

    Raises:
        Exception: If any database operation fails.
    """
    unique_keys = {"ChargingStationId": charging_station_id}
    hash_keys = get_dynamic_hash_keys(
        EVRoamAvailabilities, exclude=["WaterMark", "ODS", "ChargingStationId"]
    )

    fields = {
        "AvailabilityStatus": availability_status,
        "AvailabilityTime": availability_time,
        **other_fields,
    }
    return add_or_update_record(
        EVRoamAvailabilities, unique_keys, hash_keys, session=session, **fields
    )


def write_sites_to_db(dataframe):
    """
    Writes charging station site data to the database.

    Args:
        dataframe (pandas.DataFrame): A DataFrame containing charging station site data.

    Returns:
        None
    """
    # Replace `nan` values with `None` for proper SQL NULL handling
    dataframe = dataframe.where(pd.notnull(dataframe), None)

    with session_scope() as session:
        for _, row in dataframe.iterrows():
            site_data = row.to_dict()
            try:
                add_or_update_evroam_site(
                    site_data.pop("SiteId"),
                    site_data.pop("Name"),
                    site_data.pop("Address"),
                    session=session,
                    **site_data,
                )
            except Exception as exception:
                logging.error("Error adding or updating site: %s", exception)


def write_chargingstations_to_db(dataframe):
    """
    Writes charging station data to the database.

    Args:
        dataframe (pandas.DataFrame): A DataFrame containing charging station data.

    Returns:
        None
    """
    # Replace `nan` values with `None` for proper SQL NULL handling
    dataframe = dataframe.where(pd.notnull(dataframe), None)

    with session_scope() as session:
        for _, row in dataframe.iterrows():
            charging_station_data = row.to_dict()
            try:
                add_or_update_charging_station(
                    charging_station_data.pop("ChargingStationId"),
                    charging_station_data.pop("SiteId"),
                    charging_station_data.pop("Owner"),
                    charging_station_data.pop("InstallationStatus"),
                    session=session,
                    **charging_station_data,
                )
            except Exception as exception:
                logging.error(
                    "Error adding or updating charging station: %s", exception
                )


def write_availabilities_to_db(dataframe):
    """
    Writes availability data to the database.

    Args:
        dataframe (pandas.DataFrame): A DataFrame containing availability data.

    Returns:
        None
    """
    # Replace `nan` values with `None` for proper SQL NULL handling
    dataframe = dataframe.where(pd.notnull(dataframe), None)

    with session_scope() as session:
        for _, row in dataframe.iterrows():
            availability_data = row.to_dict()
            try:
                add_or_update_availability(
                    availability_data.pop("ChargingStationId"),
                    availability_data.pop("AvailabilityStatus"),
                    availability_data.pop("AvailabilityTime"),
                    session=session,
                    **availability_data,
                )
            except Exception as exception:
                logging.error("Error adding or updating availability: %s", exception)
