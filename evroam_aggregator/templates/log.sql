

---------- STEP 0

-- Delete records from Config and ETL tables
DELETE FROM [Config].[EDWTables] WHERE TableCatalog = 'EVRoam' AND TableName = 'EVRoamChargingStations'
DELETE FROM [Config].[Metadata] WHERE TableCatalog = 'EVRoam' AND TableName = 'EVRoamChargingStations'
DELETE FROM [ETL].[ExcelFileConfig] WHERE FileType = 'EVRoam_02_ChargingStations' AND SheetName = 'ChargingStations'
DELETE FROM [ETL].[Control] WHERE TargetTableSchemaName = 'EVRoam' AND SourceTableName = 'dboEVRoamChargingStations'
DELETE FROM [ETL].[LogFileOperation] WHERE FileName LIKE '%EVRoam_02_ChargingStations.xlsx' OR FileName LIKE '%EVRoam_02_ChargingStations.csv'

-- Drop tables, views and procedures that may have been created
DROP VIEW IF EXISTS Config.Metadata_EVRoam_EVRoamChargingStations
DROP VIEW IF EXISTS EVRoam.ChargingStations
DROP TABLE IF EXISTS EVRoam.dboEVRoamChargingStations
DROP TABLE IF EXISTS LANDING.dboEVRoamChargingStations
DROP PROCEDURE IF EXISTS EVRoam.UpdatedboEVRoamChargingStations
DROP PROCEDURE IF EXISTS LANDING.ValidateEVRoamChargingStations

GO



TRUNCATE TABLE ETL.ExcelFileConfig

INSERT INTO ETL.ExcelFileConfig(FileType, SheetName, TableName)
SELECT * FROM [Config].[InputExcelFileDetails]



---------- STEP 2

CREATE view [Config].[Metadata_EVRoam_EVRoamChargingStations] 
AS
Select 1 AS [ID], 'EVRoam' AS [TableCatalog], 'dbo' AS [TableSchema], 'EVRoamChargingStations' AS [TableName], 'ChargingStationId' AS [ColumnName], 1 AS [OrdinalPosition], 'varchar' AS [DataType], 255 AS [MaxLength], NULL AS [Precision], NULL AS [Scale], 0 AS [IsNullable], 1 AS [IsPrimary], NULL AS [DefaultValue] UNION ALL 
Select 1 AS [ID], 'EVRoam' AS [TableCatalog], 'dbo' AS [TableSchema], 'EVRoamChargingStations' AS [TableName], 'SiteId' AS [ColumnName], 2 AS [OrdinalPosition], 'varchar' AS [DataType], 255 AS [MaxLength], NULL AS [Precision], NULL AS [Scale], 0 AS [IsNullable], NULL AS [IsPrimary], NULL AS [DefaultValue] UNION ALL 
Select 1 AS [ID], 'EVRoam' AS [TableCatalog], 'dbo' AS [TableSchema], 'EVRoamChargingStations' AS [TableName], 'Operator' AS [ColumnName], 3 AS [OrdinalPosition], 'varchar' AS [DataType], 255 AS [MaxLength], NULL AS [Precision], NULL AS [Scale], 1 AS [IsNullable], NULL AS [IsPrimary], NULL AS [DefaultValue] UNION ALL 
Select 1 AS [ID], 'EVRoam' AS [TableCatalog], 'dbo' AS [TableSchema], 'EVRoamChargingStations' AS [TableName], 'Owner' AS [ColumnName], 4 AS [OrdinalPosition], 'varchar' AS [DataType], 255 AS [MaxLength], NULL AS [Precision], NULL AS [Scale], 1 AS [IsNullable], NULL AS [IsPrimary], NULL AS [DefaultValue] UNION ALL 
Select 1 AS [ID], 'EVRoam' AS [TableCatalog], 'dbo' AS [TableSchema], 'EVRoamChargingStations' AS [TableName], 'AssetId' AS [ColumnName], 5 AS [OrdinalPosition], 'varchar' AS [DataType], 255 AS [MaxLength], NULL AS [Precision], NULL AS [Scale], 1 AS [IsNullable], NULL AS [IsPrimary], NULL AS [DefaultValue] UNION ALL 
Select 1 AS [ID], 'EVRoam' AS [TableCatalog], 'dbo' AS [TableSchema], 'EVRoamChargingStations' AS [TableName], 'Current' AS [ColumnName], 6 AS [OrdinalPosition], 'varchar' AS [DataType], 255 AS [MaxLength], NULL AS [Precision], NULL AS [Scale], 1 AS [IsNullable], NULL AS [IsPrimary], NULL AS [DefaultValue] UNION ALL 
Select 1 AS [ID], 'EVRoam' AS [TableCatalog], 'dbo' AS [TableSchema], 'EVRoamChargingStations' AS [TableName], 'HasChargingCost' AS [ColumnName], 7 AS [OrdinalPosition], 'bit' AS [DataType], NULL AS [MaxLength], NULL AS [Precision], NULL AS [Scale], 1 AS [IsNullable], NULL AS [IsPrimary], NULL AS [DefaultValue] UNION ALL 
Select 1 AS [ID], 'EVRoam' AS [TableCatalog], 'dbo' AS [TableSchema], 'EVRoamChargingStations' AS [TableName], 'KwRated' AS [ColumnName], 8 AS [OrdinalPosition], 'int' AS [DataType], NULL AS [MaxLength], NULL AS [Precision], NULL AS [Scale], 1 AS [IsNullable], NULL AS [IsPrimary], NULL AS [DefaultValue] UNION ALL 
Select 1 AS [ID], 'EVRoam' AS [TableCatalog], 'dbo' AS [TableSchema], 'EVRoamChargingStations' AS [TableName], 'DateFirstOperational' AS [ColumnName], 9 AS [OrdinalPosition], 'datetime' AS [DataType], NULL AS [MaxLength], NULL AS [Precision], NULL AS [Scale], 1 AS [IsNullable], NULL AS [IsPrimary], NULL AS [DefaultValue] UNION ALL 
Select 1 AS [ID], 'EVRoam' AS [TableCatalog], 'dbo' AS [TableSchema], 'EVRoamChargingStations' AS [TableName], 'NextPlannedOutage' AS [ColumnName], 10 AS [OrdinalPosition], 'datetime' AS [DataType], NULL AS [MaxLength], NULL AS [Precision], NULL AS [Scale], 1 AS [IsNullable], NULL AS [IsPrimary], NULL AS [DefaultValue] UNION ALL 
Select 1 AS [ID], 'EVRoam' AS [TableCatalog], 'dbo' AS [TableSchema], 'EVRoamChargingStations' AS [TableName], 'InstallationStatus' AS [ColumnName], 11 AS [OrdinalPosition], 'varchar' AS [DataType], 255 AS [MaxLength], NULL AS [Precision], NULL AS [Scale], 1 AS [IsNullable], NULL AS [IsPrimary], NULL AS [DefaultValue] UNION ALL 
Select 1 AS [ID], 'EVRoam' AS [TableCatalog], 'dbo' AS [TableSchema], 'EVRoamChargingStations' AS [TableName], 'FloorLevel' AS [ColumnName], 12 AS [OrdinalPosition], 'varchar' AS [DataType], 255 AS [MaxLength], NULL AS [Precision], NULL AS [Scale], 1 AS [IsNullable], NULL AS [IsPrimary], NULL AS [DefaultValue] UNION ALL 
Select 1 AS [ID], 'EVRoam' AS [TableCatalog], 'dbo' AS [TableSchema], 'EVRoamChargingStations' AS [TableName], 'Manufacturer' AS [ColumnName], 13 AS [OrdinalPosition], 'varchar' AS [DataType], 2048 AS [MaxLength], NULL AS [Precision], NULL AS [Scale], 1 AS [IsNullable], NULL AS [IsPrimary], NULL AS [DefaultValue] UNION ALL 
Select 1 AS [ID], 'EVRoam' AS [TableCatalog], 'dbo' AS [TableSchema], 'EVRoamChargingStations' AS [TableName], 'Model' AS [ColumnName], 14 AS [OrdinalPosition], 'varchar' AS [DataType], 2048 AS [MaxLength], NULL AS [Precision], NULL AS [Scale], 1 AS [IsNullable], NULL AS [IsPrimary], NULL AS [DefaultValue] UNION ALL 
Select 1 AS [ID], 'EVRoam' AS [TableCatalog], 'dbo' AS [TableSchema], 'EVRoamChargingStations' AS [TableName], 'Images' AS [ColumnName], 15 AS [OrdinalPosition], 'varchar' AS [DataType], 4096 AS [MaxLength], NULL AS [Precision], NULL AS [Scale], 1 AS [IsNullable], NULL AS [IsPrimary], NULL AS [DefaultValue] UNION ALL 
Select 1 AS [ID], 'EVRoam' AS [TableCatalog], 'dbo' AS [TableSchema], 'EVRoamChargingStations' AS [TableName], 'Connectors' AS [ColumnName], 16 AS [OrdinalPosition], 'varchar' AS [DataType], 4096 AS [MaxLength], NULL AS [Precision], NULL AS [Scale], 1 AS [IsNullable], NULL AS [IsPrimary], NULL AS [DefaultValue] UNION ALL 
Select 1 AS [ID], 'EVRoam' AS [TableCatalog], 'dbo' AS [TableSchema], 'EVRoamChargingStations' AS [TableName], 'ProviderDeleted' AS [ColumnName], 17 AS [OrdinalPosition], 'bit' AS [DataType], NULL AS [MaxLength], NULL AS [Precision], NULL AS [Scale], 1 AS [IsNullable], NULL AS [IsPrimary], NULL AS [DefaultValue] UNION ALL 
Select 1 AS [ID], 'EVRoam' AS [TableCatalog], 'dbo' AS [TableSchema], 'EVRoamChargingStations' AS [TableName], 'Locationlat' AS [ColumnName], 18 AS [OrdinalPosition], 'float' AS [DataType], NULL AS [MaxLength], NULL AS [Precision], 2 AS [Scale], 1 AS [IsNullable], NULL AS [IsPrimary], NULL AS [DefaultValue] UNION ALL 
Select 1 AS [ID], 'EVRoam' AS [TableCatalog], 'dbo' AS [TableSchema], 'EVRoamChargingStations' AS [TableName], 'Locationlon' AS [ColumnName], 19 AS [OrdinalPosition], 'float' AS [DataType], NULL AS [MaxLength], NULL AS [Precision], 2 AS [Scale], 1 AS [IsNullable], NULL AS [IsPrimary], NULL AS [DefaultValue] UNION ALL 
Select 1 AS [ID], 'EVRoam' AS [TableCatalog], 'dbo' AS [TableSchema], 'EVRoamChargingStations' AS [TableName], 'DataStewardEmail' AS [ColumnName], 20 AS [OrdinalPosition], 'varchar' AS [DataType], 255 AS [MaxLength], NULL AS [Precision], NULL AS [Scale], NULL AS [IsNullable], NULL AS [IsPrimary], NULL AS [DefaultValue] UNION ALL 
Select 1 AS [ID], 'EVRoam' AS [TableCatalog], 'dbo' AS [TableSchema], 'EVRoamChargingStations' AS [TableName], 'Deleted' AS [ColumnName], 21 AS [OrdinalPosition], 'bit' AS [DataType], NULL AS [MaxLength], NULL AS [Precision], NULL AS [Scale], NULL AS [IsNullable], NULL AS [IsPrimary], NULL AS [DefaultValue] UNION ALL 
Select 1 As [ID], 'EVRoam' As [TableCatalog], 'dbo' As [TableSchema], 'EVRoamChargingStations' As [TableName], 'WaterMark' As [ColumnName], 22 As [OrdinalPosition], 'datetime' As [DataType], NULL As [MaxLength], NULL As [Precision], NULL As [Scale], 0 As [IsNullable], 0 As [IsPrimary], 'DEFAULT getdate() at time zone ''New Zealand Standard Time''' as [DefaultValue]
GO



---------- STEP 2.1: Paste block into 04-ConfigMetadata.sql PostDeploymentScript

INSERT INTO Config.Metadata
(TableCatalog, TableSchema, TableName, ColumnName, OrdinalPosition, DataType, MaxLength, Precision, Scale, IsNullable, IsPrimary, DefaultValue)
SELECT [TableCatalog],
       [TableSchema],
       [TableName],
       [ColumnName],
       [OrdinalPosition],
       [DataType],
       [MaxLength],
       [Precision],
       [Scale],
       [IsNullable],
       [IsPrimary],
       [DefaultValue]
FROM [Config].[Metadata_EVRoam_EVRoamChargingStations]

GO




---------- STEP 3

CREATE OR ALTER VIEW [Config].[InputExcelFileDetails] AS
SELECT 'CSVTest' AS [FileType], 'CSV' AS [SheetName], 'dboCSVTest' AS [TableName] UNION ALL
SELECT 'EEUD' AS [FileType], 'Data' AS [SheetName], 'dboEEUD' AS [TableName] UNION ALL
SELECT 'EfficientAndLowEmissionsTransportMotFleetStats' AS [FileType], 'MotFleetCompositionStats' AS [SheetName], 'dboMotFleetCompositionStats' AS [TableName] UNION ALL
SELECT 'EfficientAndLowEmissionsTransportMotFleetStats' AS [FileType], 'MotNewLightVehicleEmissionStats' AS [SheetName], 'dboMotNewLightVehicleEmissionStats' AS [TableName] UNION ALL
SELECT 'EfficientAndLowEmissionsTransportMotMonthlyEvStats' AS [FileType], 'MotMonthlyEvStats' AS [SheetName], 'dboMotMonthlyEvStats' AS [TableName] UNION ALL
SELECT 'EnergyEfficientHomesTMY' AS [FileType], 'ALL DATA' AS [SheetName], 'dboTypicalMeteorologicalYear' AS [TableName] UNION ALL
SELECT 'ERSPilot' AS [FileType], 'Auditors' AS [SheetName], 'dboERSAuditors' AS [TableName] UNION ALL
SELECT 'ERSPilot' AS [FileType], 'Client' AS [SheetName], 'dboERSClient' AS [TableName] UNION ALL
SELECT 'ERSPilot' AS [FileType], 'Equipment' AS [SheetName], 'dboERSEquipment' AS [TableName] UNION ALL
SELECT 'ERSPilot' AS [FileType], 'Files' AS [SheetName], 'dboERSFiles' AS [TableName] UNION ALL
SELECT 'ERSPilot' AS [FileType], 'MDUAudit' AS [SheetName], 'dboERSMDUAudit' AS [TableName] UNION ALL
SELECT 'ERSPilot' AS [FileType], 'Site' AS [SheetName], 'dboERSSite' AS [TableName] UNION ALL
SELECT 'ETAEnergyAndEconomics' AS [FileType], 'ProjectOptions' AS [SheetName], 'dboETAProjectOptions' AS [TableName] UNION ALL
SELECT 'ETAProject' AS [FileType], 'Client' AS [SheetName], 'dboETAClient' AS [TableName] UNION ALL
SELECT 'ETAProject' AS [FileType], 'Projects' AS [SheetName], 'dboETAProjects' AS [TableName] UNION ALL
SELECT 'ETAProject' AS [FileType], 'SiteFuels' AS [SheetName], 'dboETASiteFuels' AS [TableName] UNION ALL
SELECT 'ETAProject' AS [FileType], 'Sites' AS [SheetName], 'dboETASites' AS [TableName] UNION ALL
SELECT 'EVPublicChargers' AS [FileType], 'Data' AS [SheetName], 'dboEVPublicChargers' AS [TableName] UNION ALL
SELECT 'EVRoadmap' AS [FileType], 'Destinations' AS [SheetName], 'dboEVDestinations' AS [TableName] UNION ALL
SELECT 'EVRoadmap' AS [FileType], 'ProposedEVChargers' AS [SheetName], 'dboEVProposedChargers' AS [TableName] UNION ALL
SELECT 'EVRoamAvailabilities' AS [FileType], 'CSV' AS [SheetName], 'dboEVRoamAvailabilities' AS [TableName] UNION ALL
SELECT 'EVRoamChargingStations' AS [FileType], 'CSV' AS [SheetName], 'dboEVRoamChargingStations' AS [TableName] UNION ALL
SELECT 'EVRoamSites' AS [FileType], 'CSV' AS [SheetName], 'dboEVRoamSites' AS [TableName] UNION ALL
SELECT 'GIDIAMModerationMinutes' AS [FileType], 'AMModerationMinutes' AS [SheetName], 'dboGIDIAMModerationMinutes' AS [TableName] UNION ALL
SELECT 'GIDIApplication' AS [FileType], 'Applications' AS [SheetName], 'dboGIDIApplications' AS [TableName] UNION ALL
SELECT 'GIDIFinancialAssessment' AS [FileType], 'CostsScenarios' AS [SheetName], 'dboGIDICostsScenarios' AS [TableName] UNION ALL
SELECT 'GIDIFinancialAssessment' AS [FileType], 'FuelsScenarios' AS [SheetName], 'dboGIDIFuelsScenarios' AS [TableName] UNION ALL
SELECT 'GIDIMaster' AS [FileType], 'ForecastInputs' AS [SheetName], 'dboGIDIForecastInputs' AS [TableName] UNION ALL
SELECT 'GIDIMaster' AS [FileType], 'MilestoneEnergyAndCarbon' AS [SheetName], 'dboGIDIMilestoneEnergyAndCarbon' AS [TableName] UNION ALL
SELECT 'GIDIMaster' AS [FileType], 'OriginallyContractedTimeline' AS [SheetName], 'dboGIDIOriginallyContractedTimeline' AS [TableName] UNION ALL
SELECT 'GIDIMaster' AS [FileType], 'ProjectDetails' AS [SheetName], 'dboGIDIProjectDetails' AS [TableName] UNION ALL
SELECT 'GIDIMilestoneAbatementAndRisk' AS [FileType], 'MilestoneAbatement' AS [SheetName], 'dboGIDIMilestoneAbatement' AS [TableName] UNION ALL
SELECT 'GIDIMilestoneAbatementAndRisk' AS [FileType], 'MilestoneRisk' AS [SheetName], 'dboGIDIMilestoneRisk' AS [TableName] UNION ALL
SELECT 'GIDIMilestoneAbatementAndRiskA' AS [FileType], 'MilestoneAbatementAndRiskA' AS [SheetName], 'dboGIDIMilestoneAbatementAndRiskA' AS [TableName] UNION ALL
SELECT 'GIDIMilestoneAbatementAndRiskB' AS [FileType], 'MilestoneAbatementAndRiskB' AS [SheetName], 'dboGIDIMilestoneAbatementAndRiskB' AS [TableName] UNION ALL
SELECT 'GIDIMilestoneAbatementAndRiskC' AS [FileType], 'MilestoneAbatementC' AS [SheetName], 'dboGIDIMilestoneAbatementC' AS [TableName] UNION ALL
SELECT 'GIDIMilestoneAbatementAndRiskC' AS [FileType], 'MilestoneRiskC' AS [SheetName], 'dboGIDIMilestoneRiskC' AS [TableName] UNION ALL
SELECT 'GIDIMilestoneStatus' AS [FileType], 'MilestoneStatus' AS [SheetName], 'dboGIDIMilestoneStatus' AS [TableName] UNION ALL
SELECT 'GIDIMonitoringMilestone' AS [FileType], 'FuelConsumption' AS [SheetName], 'dboGIDIFuelConsumption' AS [TableName] UNION ALL
SELECT 'GIDIMonitoringMilestone' AS [FileType], 'OtherImpacts' AS [SheetName], 'dboGIDIOtherImpacts' AS [TableName] UNION ALL
SELECT 'GIDIProjectOverview' AS [FileType], 'ProjectOverview' AS [SheetName], 'dboGIDIProjectOverview' AS [TableName] UNION ALL
SELECT 'PipelineProspects' AS [FileType], 'Organisation' AS [SheetName], 'dboPipelineOrganisation' AS [TableName] UNION ALL
SELECT 'PipelineProspects' AS [FileType], 'Project' AS [SheetName], 'dboPipelineProject' AS [TableName] UNION ALL
SELECT 'PipelineProspects' AS [FileType], 'ProjectFuel' AS [SheetName], 'dboPipelineProjectFuel' AS [TableName] UNION ALL
SELECT 'PipelineProspects' AS [FileType], 'Site' AS [SheetName], 'dboPipelineSite' AS [TableName] UNION ALL
SELECT 'PipelineProspects' AS [FileType], 'SiteBoilerFuel' AS [SheetName], 'dboPipelineSiteBoilerFuel' AS [TableName] UNION ALL
SELECT 'PipelineProspects' AS [FileType], 'SiteBoilers' AS [SheetName], 'dboPipelineSiteBoilers' AS [TableName] UNION ALL
SELECT 'PipelineProspects' AS [FileType], 'SiteEmissions' AS [SheetName], 'dboPipelineSiteEmissions' AS [TableName] UNION ALL
SELECT 'PipelineProspects' AS [FileType], 'SiteFuel' AS [SheetName], 'dboPipelineSiteFuel' AS [TableName] UNION ALL
SELECT 'ProspectPipeline' AS [FileType], 'Organisation' AS [SheetName], 'dboProspectsOrganisation' AS [TableName] UNION ALL
SELECT 'ProspectPipeline' AS [FileType], 'Project' AS [SheetName], 'dboProspectsProject' AS [TableName] UNION ALL
SELECT 'ProspectPipeline' AS [FileType], 'ProjectFuel' AS [SheetName], 'dboProspectsProjectFuel' AS [TableName] UNION ALL
SELECT 'ProspectPipeline' AS [FileType], 'Site' AS [SheetName], 'dboProspectsSite' AS [TableName] UNION ALL
SELECT 'ProspectPipeline' AS [FileType], 'SiteBoilerFuel' AS [SheetName], 'dboProspectsSiteBoilerFuel' AS [TableName] UNION ALL
SELECT 'ProspectPipeline' AS [FileType], 'SiteBoilers' AS [SheetName], 'dboProspectsSiteBoilers' AS [TableName] UNION ALL
SELECT 'ProspectPipeline' AS [FileType], 'SiteEmissions' AS [SheetName], 'dboProspectsSiteEmissions' AS [TableName] UNION ALL
SELECT 'ProspectPipeline' AS [FileType], 'SiteFuel' AS [SheetName], 'dboProspectsSiteFuel' AS [TableName] UNION ALL
SELECT 'ProspectsPipeline' AS [FileType], 'Organisation' AS [SheetName], 'dboProspectsOrganisation' AS [TableName] UNION ALL
SELECT 'ProspectsPipeline' AS [FileType], 'Project' AS [SheetName], 'dboProspectsProject' AS [TableName] UNION ALL
SELECT 'ProspectsPipeline' AS [FileType], 'ProjectFuel' AS [SheetName], 'dboProspectsProjectFuel' AS [TableName] UNION ALL
SELECT 'ProspectsPipeline' AS [FileType], 'Site' AS [SheetName], 'dboProspectsSite' AS [TableName] UNION ALL
SELECT 'ProspectsPipeline' AS [FileType], 'SiteBoilerFuel' AS [SheetName], 'dboProspectsSiteBoilerFuel' AS [TableName] UNION ALL
SELECT 'ProspectsPipeline' AS [FileType], 'SiteBoilers' AS [SheetName], 'dboProspectsSiteBoilers' AS [TableName] UNION ALL
SELECT 'ProspectsPipeline' AS [FileType], 'SiteEmissions' AS [SheetName], 'dboProspectsSiteEmissions' AS [TableName] UNION ALL
SELECT 'ProspectsPipeline' AS [FileType], 'SiteFuel' AS [SheetName], 'dboProspectsSiteFuel' AS [TableName] UNION ALL
SELECT 'RHDDAllTables' AS [FileType], 'BoilerCapacity' AS [SheetName], 'dboRHDDBoilerCapacity' AS [TableName] UNION ALL
SELECT 'RHDDAllTables' AS [FileType], 'Boilers' AS [SheetName], 'dboRHDDBoilers' AS [TableName] UNION ALL
SELECT 'RHDDAllTables' AS [FileType], 'Sites' AS [SheetName], 'dboRHDDSites' AS [TableName] UNION ALL
SELECT 'RHDDAllTables' AS [FileType], 'SitesConsumption' AS [SheetName], 'dboRHDDSitesConsumption' AS [TableName] UNION ALL
SELECT 'SandRRegulatedAppliancesEnergySavings' AS [FileType], 'Categories' AS [SheetName], 'dboSandRRegulatedAppliancesCategories' AS [TableName] UNION ALL
SELECT 'SandRRegulatedAppliancesEnergySavings' AS [FileType], 'EnergySavings' AS [SheetName], 'dboSandRRegulatedAppliancesEnergySavings' AS [TableName] UNION ALL
SELECT 'SandRRegulatedAppliancesEnergySavings' AS [FileType], 'Targets' AS [SheetName], 'dboSandRRegulatedAppliancesTargets' AS [TableName] UNION ALL
SELECT 'SOISPEWarmerKiwiHomes' AS [FileType], 'DATA' AS [SheetName], 'dboSOISPEWarmerKiwiHomes' AS [TableName] UNION ALL
SELECT 'TimesNzOutputs' AS [FileType], 'TimesNzKea' AS [SheetName], 'dboTimesNzKea' AS [TableName] UNION ALL
SELECT 'TimesNzOutputs' AS [FileType], 'TimesNzTui' AS [SheetName], 'dboTimesNzTui' AS [TableName] UNION ALL
SELECT 'WKHProgrammeData' AS [FileType], 'ProgrammeData' AS [SheetName], 'dboWKHProgrammeData' AS [TableName] UNION ALL
SELECT 'EVRoamChargingStations' AS [FileType], 'ChargingStations' AS [SheetName], 'dboEVRoamChargingStations' AS [TableName]

GO

TRUNCATE TABLE ETL.ExcelFileConfig

INSERT INTO ETL.ExcelFileConfig(FileType, SheetName, TableName)
SELECT * FROM [Config].[InputExcelFileDetails]



---------- STEP 4


UPDATE [Config].[Metadata] 
SET DefaultValue = ' DEFAULT getdate() at time zone ''New Zealand Standard Time''' 
WHERE ColumnName = 'WaterMark'


---------- STEP 4.1: Paste block into 05-GenerateTablesAndMetadata.sql PostDeploymentScript


EXEC [Config].[GenerateMetaDataFromSource_V2] 
@OutputTableCreateScript            = 1
,@OutputPopulateControlCreateScript = 1
,@PopulateControlTable              = 1
,@pSourceTableSchema                = NULL
,@pSourceTableName                  = 'EVRoamChargingStations'
,@pSourceIncrementalColName         = 'Watermark'
,@pTargetODSSchema                  = 'EVRoam'
,@pODSSystemColPrefix               = 'ODS'
,@pSourceSystem                     = 'EVRoamChargingStations'
,@pSourceSystemType                 = 'Excel'
,@pTargetSystemType                 = 'ASQL'
,@pTargetLandingSchema              = 'Landing'
,@pGenerateODSControlTable          = 1
,@pGenerateODSTableScript           = 1
,@pIsScdType2                       = 1
,@pStageFromDataLake                = 1
,@CreateTablesAndSchemas            = 1

GO




---------- STEP 5: Paste block into 06-GenerateStoredProcedures.sql

EXEC Config.GenerateStoredProcedureSCDType2
@ODSSystemColPrefix  = 'ODS',
@TableCatalog        = 'EVRoam',
@TargetTableSchema   = 'dbo',
@TargetTableName     = 'EVRoamChargingStations',
@pIsSCDType2         = 1,
@printOutPut         = 1

GO




---------- STEP 6

CREATE OR ALTER procedure [Landing].[ValidateEVRoamChargingStations]
as

DECLARE @LandingTable VARCHAR(MAX)='[Landing].[dboEVRoamChargingStations]'
DECLARE @MetadataTable VARCHAR(MAX)='[Config].[Metadata_EVRoam_EVRoamChargingStations]'

-- First tidy up any rows containing all NULL values
DECLARE	@clearnullrows_return_value int
EXEC	@clearnullrows_return_value = [Landing].[ClearNullRows]
		@LandingTable = @LandingTable,
		@MetadataTable = @MetadataTable

-- Check for any key rows with NULL values in the dataset, which may indicate inappropriate columns identified as primary.
DECLARE	@nullkey_return_value int
EXEC	@nullkey_return_value = [Landing].[CheckForNULLKeyRows]
		@LandingTable = @LandingTable,
		@MetadataTable = @MetadataTable

-- Check for any rows with NULL values for non-nullable colunns.
DECLARE	@nullnonnullable_return_value int
EXEC	@nullnonnullable_return_value = [Landing].[CheckForNullRowsThatAreNotIsNullable]
		@LandingTable = @LandingTable,
		@MetadataTable = @MetadataTable

-- Check for any duplicated key rows in the dataset, which typically would indicate too few columns identified as primary.
DECLARE	@dupkey_return_value int
EXEC	@dupkey_return_value = [Landing].[CheckForDuplicateKeyRows]
		@LandingTable = @LandingTable,
		@MetadataTable = @MetadataTable

GO




---------- STEP 7

CREATE VIEW EVRoam.ChargingStations AS

SELECT "ChargingStationId", "SiteId", "Operator", "Owner", "AssetId", "Current", "HasChargingCost", "KwRated", "DateFirstOperational", "NextPlannedOutage", "InstallationStatus", "FloorLevel", "Manufacturer", "Model", "Images", "Connectors", "ProviderDeleted", "Locationlat", "Locationlon"
FROM EVRoam.dboEVRoamChargingStations
WHERE ODSIsCurrent = 1
    AND ISNULL(Deleted, 0) != 1

GO


