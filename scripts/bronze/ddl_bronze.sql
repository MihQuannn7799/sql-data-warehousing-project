/*
==========================================
Stored proccedure : load bronze layer 
=========================================
purpose: this stored procedure load data form external CSV files;
-truncates the bronze tables before loading
-use BULK INSERT to load data from CSV files to bronze tables
*/
CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME,@batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
	SET @batch_start_time = GETDATE();
		PRINT '====================================';
		PRINT 'Loading bronze layer';
		PRINT '====================================';

		PRINT '------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> truncating table crm_cust_info ';
		TRUNCATE TABLE bronze.crm_cust_info
		PRINT '>> inserting table crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM "C:\SQL Data warehousing project\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv"
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT'>> load DURATION ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' second';
		PRINT'-------------------'


		SET @start_time = GETDATE();
		PRINT '>> truncating table crm_prd_info ';
		TRUNCATE TABLE bronze.crm_prd_info
		PRINT '>> inserting table crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM "C:\SQL Data warehousing project\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv"
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT'>> load DURATION ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' second';
		PRINT'-------------------'


		SET @start_time = GETDATE();
		PRINT '>> truncating table crm_sales_details ';
		TRUNCATE TABLE bronze.crm_sales_details
		PRINT '>> inserting table crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM "C:\SQL Data warehousing project\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv"
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT'>> load DURATION ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' second';
		PRINT'-------------------'


		PRINT '------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '------------------------------------';


		SET @start_time = GETDATE();
		PRINT '>> truncating table erp_cust_a101 ';
		TRUNCATE TABLE bronze.erp_cust_a101
		PRINT '>> inserting table erp_cust_a101';
		BULK INSERT bronze.erp_cust_a101
		FROM "C:\SQL Data warehousing project\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv"
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT'>> load DURATION ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' second';
		PRINT'-------------------'


		SET @start_time = GETDATE();
		PRINT '>> truncating table erp_loc_a101 ';
		TRUNCATE TABLE bronze.erp_loc_a101
		PRINT '>> inserting table erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM "C:\SQL Data warehousing project\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv"
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT'>> load DURATION ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' second';
		PRINT'-------------------'
		

		SET @start_time = GETDATE();
		PRINT '>> truncating table erp_px_cat_g1v2 ';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2
		PRINT '>> inserting table erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM "C:\SQL Data warehousing project\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv"
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT'>> load DURATION ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' second';
		PRINT'-------------------'
		SET @batch_end_time = GETDATE();
		PRINT'=========================='
		PRINT'>> Total load DURATION ' + CAST(DATEDIFF(second,@batch_start_time,@batch_end_time) AS NVARCHAR) + ' second';
		PRINT'=========================='
	END TRY 
	BEGIN CATCH
		PRINT '====================================';
		PRINT 'error occured during loading bronze layer';
		PRINT 'error message' + ERROR_MESSAGE();
		PRINT 'error message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'error message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '====================================';
	END CATCH
END
