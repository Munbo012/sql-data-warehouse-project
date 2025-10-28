/*
* Data Warehouse Layer: BRONZE
* Script Type: Stored Procedure - ETL/ELT Pipeline Component
*
* Function: Executes the primary data extraction and loading (EL) step for the Bronze Layer.
*
* Process Details:
* 1. **Preparation:** Truncates the target tables in the 'bronze' schema to ensure idempotency.
* 2. **Loading:** Utilizes the `BULK INSERT` command to load raw data directly from external source CSV files.
*
* Naming Convention: bronze.load_bronze
* Usage: EXEC bronze.load_bronze;
*
* The Bronze layer ensures a one-to-one, unmodified staging of all source data.
*/

-- Example of Usage:

-- EXEC bronze.load_bronze;

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
  
BEGIN
DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
	SET @batch_start_time = GETDATE();
	PRINT '============================================================================';
	PRINT 'Loading Bronze Layer';
	PRINT '============================================================================';

	PRINT '----------------------------------------------------------------------------';
	PRINT 'Loading CRM Tables';
	PRINT '----------------------------------------------------------------------------';

	SET @start_time = GETDATE();
	PRINT '>> Truncating Table : bronze.crm_prd_info';
	TRUNCATE TABLE bronze.crm_prd_info;

	PRINT '>> Inserting Data into : bronze.crm_prd_info';
	BULK INSERT bronze.crm_prd_info
			FROM 'C:\Users\idris\Desktop\DataSets\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
			WITH (
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
	);
	SET @end_time = GETDATE();
	PRINT '>> Load Duration : ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'

	PRINT '----------------------------------------------------------------------------';

	SET @start_time = GETDATE()
	PRINT '>> Truncating Table : bronze.crm_cust_info';
	TRUNCATE TABLE bronze.crm_cust_info;

	PRINT '>> Inserting Data into : bronze.crm_cust_info';
	BULK INSERT bronze.crm_cust_info
			FROM 'C:\Users\idris\Desktop\DataSets\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
			WITH (
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
	);
	SET @end_time = GETDATE();
	PRINT '>> Load Duration : ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'

	PRINT '----------------------------------------------------------------------------';

	SET @start_time = GETDATE()
	PRINT '>> Truncating Table : bronze.crm_sales_details';
	TRUNCATE TABLE bronze.crm_sales_details;

	PRINT '>> Inserting Data into : bronze.crm_sales_details';
	BULK INSERT bronze.crm_sales_details
			FROM 'C:\Users\idris\Desktop\DataSets\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
			WITH (
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
	);
	SET @end_time = GETDATE();
	PRINT '>> Load Duration : ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'

	PRINT '----------------------------------------------------------------------------';
	PRINT 'Loading ERP Tables';
	PRINT '----------------------------------------------------------------------------';

	SET @start_time = GETDATE()
	PRINT '>> Truncating Table : bronze.erp_cust_az12';
	TRUNCATE TABLE bronze.erp_cust_az12;

	PRINT '>> Inserting Data into : bronze.erp_cust_az12';
	BULK INSERT bronze.erp_cust_az12
			FROM 'C:\Users\idris\Desktop\DataSets\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
			WITH (
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
	);
	SET @end_time = GETDATE();
	PRINT '>> Load Duration : ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'

	PRINT '----------------------------------------------------------------------------';

	SET @start_time = GETDATE()
	PRINT '>> Truncating Table : bronze.erp_loc_a101';
	TRUNCATE TABLE bronze.erp_loc_a101;

	PRINT '>> Inserting Data into : bronze.erp_loc_a101';
	BULK INSERT bronze.erp_loc_a101
			FROM 'C:\Users\idris\Desktop\DataSets\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
			WITH (
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
	);
	SET @end_time = GETDATE();
	PRINT '>> Load Duration : ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'

	PRINT '----------------------------------------------------------------------------';

	SET @start_time = GETDATE()

	PRINT '>> Truncating Table : bronze.erp_px_cat_g1v2';
	TRUNCATE TABLE bronze.erp_px_cat_g1v2;

	PRINT '>> Inserting Data into : bronze.erp_px_cat_g1v2';
	BULK INSERT bronze.erp_px_cat_g1v2
			FROM 'C:\Users\idris\Desktop\DataSets\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
			WITH (
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
	);
	SET @end_time = GETDATE();
	PRINT '>> Load Duration : ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'
	PRINT '----------------------------------------------------------------------------';
	SET @batch_end_time = GETDATE();
	PRINT '=========================================================';
	PRINT 'Loading Bronze Layer is Completed Succesfully';
	PRINT '      - Total Load Duration: ' + CAST(DATEDIFF(second,@batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds -';
	PRINT '=========================================================';
	END TRY
	BEGIN CATCH
		PRINT '=========================================================';
		PRINT 'Error Occured During loading Bronze Layer';
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=========================================================';
	END CATCH

END
