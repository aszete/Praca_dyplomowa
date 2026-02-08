/*
=============================================================
Create Database and Schemas
=============================================================
*/

USE master;
GO
-- DROP database

IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DW_Ecom')
BEGIN
    ALTER DATABASE DW_Ecom SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DW_Ecom;
END;
GO

--CREATE database
CREATE DATABASE DW_Ecom;
GO

ALTER DATABASE DW_Ecom SET RECOVERY SIMPLE;
GO

USE DW_Ecom;
GO

--CREATE schemas
# Why SIMPLE works for data warehouses:

==================================================================================

# Data warehouses are loaded from source systems through ETL/ELT processes
# If something goes wrong, you can reload data from your source systems
# You don't need point-in-time recovery since it's not handling live transactions
# SIMPLE recovery prevents transaction logs from growing excessively during large data loads
# It's more efficient for the bulk insert operations common in data warehouses

# When you might use FULL for a data warehouse:

# If your warehouse contains calculated/aggregated data that would be very expensive to recreate
# If you have strict audit requirements for every change
# If some data in the warehouse can't be recreated from sources

# For a school project demonstrating a data warehouse, SIMPLE is definitely the standard choice.
