/*
====================================================================================================
Create Database and Schemas
=====================================================================================================
Script Purpose:
	This script creates a  new database named 'DataWarehouse after checking if it already exists.
	If the database exixts, it is dropped and recreated. Additionally , the script sets up three schemas within
	the database: 'bronze','silver', 'gold'.

WARNING: 
		Running this script will drop the entire 'DataWarehouse database if it exists.
		All data in the database will be permanently deleted. Proceed with caution 
		and ensure you have a proper backups before running the scripts.
*/







use master;
go
--Drop and recreate the 'DataWarehouse database

if Exists (select 1 from sys.databases where name='DataWarehouse')
begin
	alter database DataWarehouse set single_user with rollback immediate;
	drop database DataWarehouse;
end;
go
-- Create the DataWarehouse' database
create database DataWarehouse;
go 
use DataWarehouse;
go
-- Creating different Schemas
create schema bronze;
go
create schema silver;
go 
create schema gold;
go
