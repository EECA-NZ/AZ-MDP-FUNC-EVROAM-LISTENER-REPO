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

from sqlalchemy import create_engine, CHAR
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import String, Float, Boolean, Integer, DateTime
from sqlalchemy import Column, VARBINARY, ForeignKey
from sqlalchemy.orm import relationship

env = os.getenv("env", "dev")

Base = declarative_base()

# pylint: disable=too-few-public-methods


class EVRoamSites(Base):
    """
    Represents an EV charging site within the EVRoam ecosystem. This model captures
    comprehensive details about each site, including its location, operational status,
    and various attributes that describe the site's features and offerings.
    """

    __tablename__ = "dboEVRoamSites"
    __table_args__ = {"schema": "EECAEVRoam"}
    ODSdboEVRoamSitesSKID = Column(Integer, primary_key=True, autoincrement=True)
    AccessLocations = Column(String(4096), info={"description": "JSON string of access locations."})
    Address = Column(String(2048), info={"description": "Physical address of the site."})
    CarParkCount = Column(
        Integer, info={"description": "Number of car parks available at the site."}
    )
    DataStewardEmail = Column(
        String(255),
        info={"description": "Email of the person responsible for the data stewardship."},
    )
    Deleted = Column(
        Boolean, info={"description": "Flag to indicate if the site has been deleted."}
    )
    HasCarparkCost = Column(
        Boolean,
        info={"description": "Indicates if there is a cost associated with the carpark."},
    )
    HasTouristAttraction = Column(
        Boolean,
        info={"description": "Indicates if there is a tourist attraction nearby."},
    )
    Is24Hours = Column(Boolean, info={"description": "States if available 24 hrs or not."})
    MaxTimeLimit = Column(String(255), info={"description": "Maximum time limit for parking."})
    Name = Column(String(255), info={"description": "Name of Site."})
    Operator = Column(String(255), info={"description": "Operator of the site."})
    ProviderDeleted = Column(
        Boolean,
        info={"description": "Flag to indicate if the provider has marked the site as deleted."},
    )
    SiteId = Column(
        String(255),
        index=True,
        info={"description": "Vendor provided UNIQUE ID to link to the ChargingStation worksheet."},
    )
    WaterMark = Column(
        DateTime,
        default=datetime.utcnow,
        info={"description": "Timestamp for the last update."},
    )
    ODSBatchID = Column(Integer, info={"description": "Batch ID for the operation."})
    ODSEffectiveFrom = Column(DateTime, info={"description": "Effective from date for SCD."})
    ODSEffectiveTo = Column(
        DateTime, info={"description": "Effective to date for SCD, null if current."}
    )
    ODSIsCurrent = Column(
        Boolean, info={"description": "Indicates if the record is the current record."}
    )
    ODSDMLType = Column(CHAR(1), info={"description": "Type of DML operation."})
    ODSHashKey = Column(VARBINARY(8000), info={"description": "Hash key for detecting changes."})
    ODSExternalID = Column(String(50), info={"description": "External ID for reference."})
    ChargingStations = relationship("EVRoamChargingStations", back_populates="Site")


class EVRoamChargingStations(Base):
    """
    Represents a charging station at an EVRoam site. This model stores information
    specific to individual charging stations, such as the type of connectors available,
    the station's operational status, and its physical characteristics.
    """

    __tablename__ = "dboEVRoamChargingStations"
    __table_args__ = {"schema": "EECAEVRoam"}
    ODSdboEVRoamChargingStationsSKID = Column(Integer, primary_key=True, autoincrement=True)
    AssetId = Column(
        String(255),
        info={
            "description": (
                "Vendors Asset ID. This defaults to the "
                "chargingStationId but can be overwritten with another ID."
            )
        },
    )
    ChargingStationId = Column(
        String(255),
        index=True,
        info={"description": "Vendor provided UNIQUE ID to link to the Connector worksheet."},
    )
    Connectors = Column(
        String(4096),
        info={
            "description": "Maximum of 3 public image URLs per charging station comma separated."
        },
    )
    Current = Column(String(255), info={"description": 'Acceptable values are "AC" or "DC".'})
    DataStewardEmail = Column(
        String(255),
        info={"description": "Email of the person responsible for the data stewardship."},
    )
    DateFirstOperational = Column(
        DateTime,
        info={"description": "Date it was first able to be used. Date without time is acceptable."},
    )
    Deleted = Column(
        Boolean,
        info={"description": "Flag to indicate if the charging station has been deleted."},
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
                "Indicates if there is a cost associated " "with using the charging station."
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
        info={"description": "The rated power output of the charging station in kilowatts."},
    )
    Locationlat = Column(Float, info={"description": "Latitude of the charging station location."})
    Locationlon = Column(Float, info={"description": "Longitude of the charging station location."})
    Manufacturer = Column(
        String(2048), info={"description": "Manufacturer of the charging station."}
    )
    Model = Column(String(2048), info={"description": "Model of the charging station."})
    NextPlannedOutage = Column(
        DateTime,
        info={
            "description": (
                "Datetime of any planned outages, in the format; " "startDateTime,endDateTime."
            )
        },
    )
    Operator = Column(
        String(255),
        info={"description": "Name of the operator of the charging station."},
    )
    Owner = Column(String(255), info={"description": "Name of the owner of the charging station."})
    ProviderDeleted = Column(
        Boolean,
        info={
            "description": (
                "Flag to indicate if the provider has marked the " "charging station as deleted."
            )
        },
    )
    SiteId = Column(
        String(36),
        ForeignKey("EVRoam.dboEVRoamSites.SiteId"),
        info={
            "description": (
                "SiteId from the previous Site worksheet. IDs in this column do "
                "not need to be unique as you can have many charging stations to one site."
            )
        },
    )
    WaterMark = Column(
        DateTime,
        default=datetime.utcnow,
        info={"description": "Timestamp for the last update."},
    )
    ODSBatchID = Column(Integer, info={"description": "Batch ID for the operation."})
    ODSEffectiveFrom = Column(DateTime, info={"description": "Effective from date for SCD."})
    ODSEffectiveTo = Column(
        DateTime, info={"description": "Effective to date for SCD, null if current."}
    )
    ODSIsCurrent = Column(
        Boolean, info={"description": "Indicates if the record is the current record."}
    )
    ODSDMLType = Column(CHAR(1), info={"description": "Type of DML operation."})
    ODSHashKey = Column(VARBINARY(8000), info={"description": "Hash key for detecting changes."})
    ODSExternalID = Column(String(50), info={"description": "External ID for reference."})
    Site = relationship("EVRoamSites", back_populates="ChargingStations")


class EVRoamAvailabilities(Base):
    """
    Tracks the availability status of EVRoam charging stations. This model is used to
    log changes in availability, such as when a station becomes occupied or available,
    and includes timestamps for these status updates.
    """

    __tablename__ = "dboEVRoamAvailabilities"
    __table_args__ = {"schema": "EECAEVRoam"}
    ODSdboEVRoamAvailabilitiesSKID = Column(
        Integer,
        primary_key=True,
        autoincrement=True,
        info={"description": "Auto-incremented primary key."},
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
    AvailabilityTime = Column(
        DateTime,
        info={"description": "Timestamp when the availability status was recorded."},
    )
    ChargingStationId = Column(
        String(255),
        ForeignKey("EVRoam.dboEVRoamChargingStations.ChargingStationId"),
        info={
            "description": (
                "Foreign key linking to the charging "
                "station this availability record pertains to."
            )
        },
    )
    DataStewardEmail = Column(
        String(255),
        info={
            "description": (
                "Email of the person responsible for the " "data stewardship of this record."
            )
        },
    )
    Deleted = Column(
        Boolean,
        info={
            "description": (
                "Flag indicating whether this availability " "record has been logically deleted."
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
    Operator = Column(String(255), info={"description": "Operator of the charging station."})
    WaterMark = Column(
        DateTime,
        default=datetime.utcnow,
        info={"description": "Timestamp for the last update of this record."},
    )
    ODSBatchID = Column(
        Integer,
        info={
            "description": (
                "Batch ID associated with the operation " "that generated or modified this record."
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
        info={"description": "A hash key generated from the record's contents to detect changes."},
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
    ChargingStation = relationship(
        "EVRoamChargingStations",
        backref="availabilities",
        info={"description": "Relationship back to the associated charging station."},
    )


class Connectors(Base):
    """
    Represents a connector for an EV charging station within the EVRoam ecosystem.
    This model stores information about individual connectors, including their type
    and operational status.
    """

    __tablename__ = "connectors"
    __table_args__ = {"schema": "EECAEVRoam"}
    chargingStationId = Column(
        String(255),
        ForeignKey("EECAEVRoam.dboEVRoamChargingStations.ChargingStationId"),
        primary_key=True,
        info={
            "description": (
                "chargingStationId from the previous ChargingStation worksheet. "
                "IDs in this column do not need to be unique as you can have many "
                "connectors to one chargingStation."
            )
        },
    )
    connectorId = Column(
        String(255),
        primary_key=True,
        info={"description": "Unique ID for the connector."},
    )
    connectorType = Column(
        String(255),
        info={
            "description": (
                'Acceptable values are: "Type 1 Tethered", '
                '"Type 2 Tethered", "Type 2 Socketed", "CHAdeMO", '
                '"Type 1 CCS", "Type 2 CCS".'
            )
        },
    )
    operationStatus = Column(
        String(255),
        info={"description": 'Acceptable values are: "Operative", "Inoperative", "Unknown".'},
    )

    charging_station = relationship("EVRoamChargingStations", back_populates="connectors")


def get_engine():
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
    params = urllib.parse.quote_plus(
        f"Driver={{ODBC Driver 17 for SQL Server}};"
        f"Server=tcp:{server},1433;"
        f"Database={database};"
        "Authentication=ActiveDirectoryMsi;"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )
    connection_url = f"mssql+pyodbc:///?odbc_connect={params}"
    engine = create_engine(connection_url, echo=True)
    return engine


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
    session = sessionmaker(bind=engine)
    return session()


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


def add_or_update_record(model, unique_keys, hash_keys, **fields):
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
        **fields: Additional fields of the record, passed as keyword arguments.

    Returns:
        The primary key of the newly added or updated record.
    """
    session = get_session()
    try:
        # Generate hash key for incoming data
        hash_values = [fields[key] for key in hash_keys]
        incoming_hash = generate_hash_key(*hash_values)

        # Check if the record already exists and if it's the current version
        query = session.query(model).filter_by(**unique_keys, ODSIsCurrent=True)
        existing_record = query.first()

        if existing_record and existing_record.ODSHashKey == incoming_hash:
            return getattr(existing_record, model.__primary_key__)  # Assuming a single PK field

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
        session.commit()
        return getattr(new_record, model.__primary_key__)
    except Exception as exception:
        session.rollback()
        logging.warning("Error: %s", exception)
        raise
    finally:
        session.close()


def add_or_update_evroam_site(site_id, name, address, **other_fields):
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
    hash_keys = ["name", "address"] + list(other_fields.keys())
    return add_or_update_record(
        EVRoamSites, unique_keys, hash_keys, name=name, address=address, **other_fields
    )


def add_or_update_charging_station(
    charging_station_id, site_id, owner, installation_status, **other_fields
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
    hash_keys = ["site_id", "owner", "installation_status"] + list(other_fields.keys())
    fields = {
        "SiteId": site_id,
        "Owner": owner,
        "InstallationStatus": installation_status,
        **other_fields,
    }
    return add_or_update_record(EVRoamChargingStations, unique_keys, hash_keys, **fields)


def add_or_update_availability(
    charging_station_id, availability_status, availability_time, **other_fields
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
    hash_keys = ["availability_status", "availability_time"] + list(other_fields.keys())
    fields = {
        "AvailabilityStatus": availability_status,
        "AvailabilityTime": availability_time,
        **other_fields,
    }
    return add_or_update_record(EVRoamAvailabilities, unique_keys, hash_keys, **fields)


def add_or_update_connector(
    charging_station_id, connector_id, connector_type, operation_status, **other_fields
):
    """
    Adds a new connector record or updates an existing one
    for a charging station using SCD Type 2 logic.

    This function checks if a connector record for the provided `charging_station_id`
    and `connector_id` already exists and if changes are detected,
    it marks the existing record as historical (sets `ODSEffectiveTo` to now) and inserts
    a new record with the current data.
    If no existing record is found, it inserts a new record.

    Args:
        charging_station_id (str): The unique identifier of
        the charging station to which the connector belongs.
        connector_id (str): The unique identifier of the connector.
        connector_type (str): The type of the connector (e.g., "Type 2 Tethered", "CHAdeMO").
        operation_status (str): The current operational status
        of the connector (e.g., "Operative", "Inoperative").
        **other_fields: Additional fields related to the connector, passed as keyword arguments.
        These fields should match the column names of the `Connectors` table.

    Returns:
        str: The `connector_id` of the newly added or updated connector record.

    Raises:
        Exception: If any database operation fails.
    """
    unique_keys = {"ChargingStationId": charging_station_id, "ConnectorId": connector_id}
    hash_keys = ["connector_type", "operation_status"] + list(other_fields.keys())
    fields = {
        "ConnectorType": connector_type,
        "OperationStatus": operation_status,
        **other_fields,
    }
    return add_or_update_record(Connectors, unique_keys, hash_keys, **fields)
