/*
* Data Warehouse Initialization & Schema Definition
* Script Type: DDL (Data Definition Language) - Infrastructure Setup
*
* Function: Establishes the core database and the three-layer Medallion Architecture schemas.
* Action: Drops and recreates the 'DataWarehouse' database if it already exists, ensuring a
* clean starting point for development or deployment.
*
* Defined Schemas:
* 1. bronze: The raw, immutable data ingestion layer.
* 2. silver: The cleansed, standardized, and normalized data layer.
* 3. gold: The business-ready, highly-modeled (Star Schema) data layer for analytics.
*
* WARNING: Execution will result in the **PERMANENT LOSS** of all existing data
* within the 'DataWarehouse' database.
*/

USE master;
GO

-- Drop and recreate the 'DataWarehouse' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
    ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataWarehouse;
END;
GO

-- Create the 'DataWarehouse' database
CREATE DATABASE DataWarehouse;
GO

USE DataWarehouse;
GO

-- Create Schemas
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO
