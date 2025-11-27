/* 
=================================================================================================
stored procedure: Load Bronze Layer  (Source -> Bronze)
==================================================================================================
Script Purpose:
  This stored procedure loads data inot the 'bronze' schema from external CSV files.
  It perfrorms the following actions:
  -Truncates the bronze tables before loading data
  -uses the 'BULK INSERT' command to load data from the csv from csv files to bronze tables.

Parameters:
None.
This sorted procedure does not accept any parameter or return any values.
Usage example:
EXEC bronze.load_bronze;
============================================================================================
*/


create or alter procedure bronze.load_bronze as
begin
	declare @start_time datetime, @end_time datetime, @batch_start datetime, @batch_end datetime;
	Begin try

		set @batch_start=GETDATE();

		print '==============================================================';
		print ' Loading Bronze Layer';
		print '==============================================================';
		print' --------------------------------------------------------------';
		print' Loading CRM Tables';
		print' --------------------------------------------------------------';

		set @start_time=getdate();

		print' >>Truncating table bronze.crm_cust_info'


		truncate table bronze.crm_cust_info;

		print' Inserting Data Into : bronze.crm_cust_info'

		bulk insert bronze.crm_cust_info
		from 'C:\Users\dsuti\Downloads\data warehouse\f78e076e5b83435d84c6b6af75d8a679\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		with (
		firstrow=2,
		fieldterminator =',',
		tablock
		);
		set @end_time=getdate();

		print '>> load duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) +'seconds'; 
		print' --------------------------------------------------------------';

		set @start_time=getdate();
		
		print' >>Truncating table bronze.crm_prd_info'
		truncate table bronze.crm_prd_info;

		print' Inserting Data Into : bronze.crm_prd_info'
		bulk insert bronze.crm_prd_info
		from 'C:\Users\dsuti\Downloads\data warehouse\f78e076e5b83435d84c6b6af75d8a679\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		with (
		firstrow=2,
		fieldterminator =',',
		tablock
		);

		set @end_time=getdate();

		print '>> load duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) +'seconds'; 
		print' --------------------------------------------------------------';


		set @start_time=getdate();

		print' >>Truncating table bronze.crm_sales_details'

		truncate table bronze.crm_sales_details;

		print' Inserting Data Into : bronze.crm_sales_details'
		bulk insert bronze.crm_sales_details
		from 'C:\Users\dsuti\Downloads\data warehouse\f78e076e5b83435d84c6b6af75d8a679\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		with (
		firstrow=2,
		fieldterminator =',',
		tablock
		);

		set @end_time=getdate();

		print '>> load duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) +'seconds'; 
		print' --------------------------------------------------------------';


		set @start_time=getdate();

		print' >>Truncating table bronze.erp_cust_az12'
		truncate table bronze.erp_cust_az12;


		bulk insert bronze.erp_cust_az12
		from 'C:\Users\dsuti\Downloads\data warehouse\f78e076e5b83435d84c6b6af75d8a679\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		with (
		firstrow=2,
		fieldterminator =',',
		tablock
		);

		set @end_time=getdate();

		print '>> load duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) +'seconds'; 
		print' --------------------------------------------------------------';


		set @start_time=getdate();


		print 'truncating bronze.erp_loc_a101'

		truncate table bronze.erp_loc_a101;

		print' inserting Data Into : bronze.erp_loc_a101'

		bulk insert bronze.erp_loc_a101
		from 'C:\Users\dsuti\Downloads\data warehouse\f78e076e5b83435d84c6b6af75d8a679\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		with (
		firstrow=2,
		fieldterminator =',',
		tablock
		);

		set @end_time=getdate();

		print '>> load duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) +'seconds'; 
		print' --------------------------------------------------------------';



		set @start_time=getdate();


		print' truncating bronze.erp_px_cat_g1v2'

		truncate table bronze.erp_px_cat_g1v2;

		print' inseting Data Into: bronze.erp_px_cat_g1v2'

		bulk insert bronze.erp_px_cat_g1v2
		from 'C:\Users\dsuti\Downloads\data warehouse\f78e076e5b83435d84c6b6af75d8a679\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		with (
		firstrow=2,
		fieldterminator =',',
		tablock
		);

		set @end_time=getdate();

		print '>> load duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) +'seconds'; 
		print' --------------------------------------------------------------';

		set @batch_end=getdate();
		print' ================================================================';
		print' Loading Bronze layer is completed' 

		print '>> total load duration: ' + cast(datediff(second, @batch_start, @batch_end) as nvarchar) +'seconds'; 
		print' ================================================================';
	end try
	begin catch
		print ' =============================='
		print ' ERROR OCCURED DURING LOADING BRONZE LAYER'
		print ' Error Message '+ ERROR_MESSAGE();  
		print ' Error Message '+ cast(ERROR_number() as nvarchar);
		print ' Error Message '+ cast(ERROR_state() as nvarchar);
		print ' =============================='
	end catch

	

end
