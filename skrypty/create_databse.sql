/*
===========================================================================
Kod T-SQL tworzący wydzieloną bazę danych i schematy architektury medalion
===========================================================================
*/

USE master;
GO
-- DROP database

IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DW_Ecom')
BEGIN
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

CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO
