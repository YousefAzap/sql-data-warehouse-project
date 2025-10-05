------------------------CREATING CRM TABLES---------------------------------------
IF OBJECT_ID ('bronze.crm_cust_info', 'U' ) IS NOT NULL
	DROP TABLE bronze.crm_cust_info;
CREATE TABLE bronze.crm_cust_info(
cst_id INT,
cst_key NVARCHAR(50),
cst_firstname NVARCHAR(50),
cst_lastname NVARCHAR(50),
cst_materian_status NVARCHAR(50),
cst_gndr NVARCHAR(50),
cst_creat_date Date
);
-------------------------
IF OBJECT_ID ('bronze.crm_prd_info', 'U' ) IS NOT NULL
	DROP TABLE bronze.crm_prd_info;
CREATE TABLE bronze.crm_prd_info(
prd_id INT,
prd_key NVARCHAR(50),
prd_nm NVARCHAR(50),
prd_cost INT,
prd_line NVARCHAR(50),
prd_start_dt DATETIME,
prd_end_dt DATETIME
);
-------------------------
IF OBJECT_ID ('bronze.crm_sales_details', 'U' ) IS NOT NULL
	DROP TABLE bronze.crm_sales_details;
CREATE TABLE bronze.crm_sales_details(
sls_ord_num NVARCHAR(50),
sls_prd_key NVARCHAR(50),
sls_cust_id INT,
sls_order_dt INT,
sls_ship_dt INT,
sls_due_dt INT,
sls_sales INT,
sls_quantity INT,
sls_price INT,
);
------------------------CREATING ERP TABLES---------------------------------------

IF OBJECT_ID ('bronze.erp_loc_a101', 'U' ) IS NOT NULL
	DROP TABLE bronze.erp_loc_a101;
CREATE TABLE bronze.erp_loc_a101(
cid NVARCHAR(50),
cntry NVARCHAR(50)
);
-------------------------
IF OBJECT_ID ('bronze.erp_cust_az12', 'U' ) IS NOT NULL
	DROP TABLE bronze.erp_cust_az12;
CREATE TABLE bronze.erp_cust_az12(
cid NVARCHAR(50),
bdate DATE,
gen NVARCHAR(50)
);
-------------------------
IF OBJECT_ID ('bronze.erp_px_cat_g1v2', 'U' ) IS NOT NULL
	DROP TABLE bronze.erp_px_cat_g1v2;
CREATE TABLE bronze.erp_px_cat_g1v2(
id NVARCHAR(50),
cat NVARCHAR(50),
subcat NVARCHAR(50),
maintenance NVARCHAR(50)
);
------------------------INSERTING THE DATA INTO CRM TABLES---------------------------------------
CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME , @batch_start_time DATETIME , @batch_end_time DATETIME;
	BEGIN TRY
	SET @batch_start_time = GETDATE();
		PRINT '========================================';
		PRINT 'LOADING THE BRONZE LAYER';
		PRINT '========================================';

		PRINT '----------------------------------------';
		PRINT 'LOADING CRM TABLES';
		PRINT '----------------------------------------';

		PRINT '>> TRUNCATING TABLE: bronze.crm_cust_info';
		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.crm_cust_info;

		PRINT '>> INSERTING DATA INTO bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\win 10-11\Desktop\DwhProject\datasets\source_crm\cust_info.csv'
		WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
		);
		SET @end_time = GETDATE();
		print '>> LOAD DURATION: ' + CAST (DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds';
		PRINT '--------------------------------------';
		-------------------------
		PRINT '>> TRUNCATING TABLE: bronze.crm_prd_info';
		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.crm_prd_info;

		PRINT '>> INSERTING DATA INTO bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\win 10-11\Desktop\DwhProject\datasets\source_crm\prd_info.csv'
		WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
		);
		SET @end_time = GETDATE();
		print '>> LOAD DURATION: ' + CAST (DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds';
		PRINT '--------------------------------------';
		-------------------------
		PRINT '>> TRUNCATING TABLE: bronze.crm_sales_details';
		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.crm_sales_details;

		PRINT '>> INSERTING DATA INTO bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\win 10-11\Desktop\DwhProject\datasets\source_crm\sales_details.csv'
		WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
		);
		SET @end_time = GETDATE();
		print '>> LOAD DURATION: ' + CAST (DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds';
		PRINT '--------------------------------------';
		------------------------INSERTING THE DATA INTO ERP TABLES---------------------------------------
		PRINT '----------------------------------------';
		PRINT 'LOADING ERP TABLES';
		PRINT '----------------------------------------';

		PRINT '>> TRUNCATING TABLE: bronze.erp_loc_a101';
		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.erp_loc_a101;

		PRINT '>> INSERTING DATA INTO bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\win 10-11\Desktop\DwhProject\datasets\source_erp\LOC_A101.csv'
		WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
		);
		SET @end_time = GETDATE();
		print '>> LOAD DURATION: ' + CAST (DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds';
		PRINT '--------------------------------------';
		-------------------------
		PRINT '>> TRUNCATING TABLE: bronze.erp_cust_az12';
		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.erp_cust_az12;

		PRINT '>> INSERTING DATA INTO bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\win 10-11\Desktop\DwhProject\datasets\source_erp\CUST_AZ12.csv'
		WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
		);
		SET @end_time = GETDATE();
		print '>> LOAD DURATION: ' + CAST (DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds';
		PRINT '--------------------------------------';
		-------------------------
		PRINT '>> TRUNCATING TABLE: bronze.erp_px_cat_g1v2';
		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;

		PRINT '>> INSERTING DATA INTO bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\win 10-11\Desktop\DwhProject\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
		);
		SET @end_time = GETDATE();
		print '>> LOAD DURATION: ' + CAST (DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds';
		PRINT '--------------------------------------';
		SET @batch_end_time = GETDATE();
		print '>> LOADING BRONZE LAYER IS COMPLETED !';
		PRINT '----------------------------------------';
		print '- TOTAL LOAD DURATION FOR ALL THE BATCH: ' + CAST (DATEDIFF(second,@batch_start_time,@batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '----------------------------------------';
	END TRY
	BEGIN CATCH
	PRINT '========================================';
	PRINT 'ERROR OCCURED DURING LAODING THE BRONZE LAYER';
	PRINT 'ERROR MESSAGE : ' + ERROR_MESSAGE();
	PRINT 'ERROR MESSAGE : ' + CAST(ERROR_NUMBER() AS NVARCHAR);
	PRINT 'ERROR MESSAGE : ' + CAST(ERROR_STATE() AS NVARCHAR);
	PRINT '========================================';
	END CATCH
END
