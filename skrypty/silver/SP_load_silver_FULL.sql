CREATE OR ALTER PROCEDURE silver.load_full
    @silver_batch_id VARCHAR(50) = NULL,
    @source_batch_id VARCHAR(50) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    
    -- 1. Initialize Batch IDs
    -- Ensures we never pass a NULL to the worker procedures
    SET @silver_batch_id = ISNULL(@silver_batch_id, 'SILVER_' + FORMAT(SYSDATETIME(), 'yyyyMMdd_HHmmss'));
    
    IF @source_batch_id IS NULL
    BEGIN
        SELECT TOP 1 @source_batch_id = batch_id 
        FROM bronze.metadata 
        WHERE status = 'Success' 
        ORDER BY load_end_time DESC;
        
        -- Fallback if bronze.metadata is empty or has no 'Success' records
        SET @source_batch_id = ISNULL(@source_batch_id, 'INITIAL_LOAD');
    END

    PRINT '================================================';
    PRINT 'STARTING SILVER LOAD BATCH: ' + @silver_batch_id;
    PRINT 'SOURCE BATCH IDENTIFIED:    ' + @source_batch_id;
    PRINT '================================================';

    -- 2. DIMENSIONS (Order matters if you have FK constraints)
    PRINT '>> Loading Dimensions...';
    BEGIN TRY EXEC silver.load_addresses @silver_batch_id, @source_batch_id; PRINT '   [OK] Addresses'; END TRY 
    BEGIN CATCH PRINT 'FAILED: Addresses'; END CATCH

    BEGIN TRY EXEC silver.load_brands @silver_batch_id, @source_batch_id; PRINT '   [OK] Brands'; END TRY 
    BEGIN CATCH PRINT 'FAILED: Brands'; END CATCH

    BEGIN TRY EXEC silver.load_categories @silver_batch_id, @source_batch_id; PRINT '   [OK] Categories'; END TRY 
    BEGIN CATCH PRINT 'FAILED: Categories'; END CATCH

    BEGIN TRY EXEC silver.load_customers @silver_batch_id, @source_batch_id; PRINT '   [OK] Customers'; END TRY 
    BEGIN CATCH PRINT 'FAILED: Customers'; END CATCH

    BEGIN TRY EXEC silver.load_payment_methods @silver_batch_id, @source_batch_id; PRINT '   [OK] Payment Methods'; END TRY 
    BEGIN CATCH PRINT 'FAILED: Payment Methods'; END CATCH

    BEGIN TRY EXEC silver.load_products @silver_batch_id, @source_batch_id; PRINT '   [OK] Products'; END TRY 
    BEGIN CATCH PRINT 'FAILED: Products'; END CATCH

    -- 3. FACTS (Load these after Dimensions)
    PRINT '>> Loading Facts...';
    BEGIN TRY EXEC silver.load_website_sessions @silver_batch_id, @source_batch_id; PRINT '   [OK] Sessions'; END TRY 
    BEGIN CATCH PRINT 'FAILED: Sessions'; END CATCH

    BEGIN TRY EXEC silver.load_pageviews @silver_batch_id, @source_batch_id; PRINT '   [OK] Pageviews'; END TRY 
    BEGIN CATCH PRINT 'FAILED: Pageviews'; END CATCH

    BEGIN TRY EXEC silver.load_orders @silver_batch_id, @source_batch_id; PRINT '   [OK] Orders'; END TRY 
    BEGIN CATCH PRINT 'FAILED: Orders'; END CATCH

    BEGIN TRY EXEC silver.load_order_items @silver_batch_id, @source_batch_id; PRINT '   [OK] Order Items'; END TRY 
    BEGIN CATCH PRINT 'FAILED: Order Items'; END CATCH

    BEGIN TRY EXEC silver.load_order_item_returns @silver_batch_id, @source_batch_id; PRINT '   [OK] Returns'; END TRY 
    BEGIN CATCH PRINT 'FAILED: Returns'; END CATCH

    PRINT '================================================';
    PRINT 'SILVER LOAD COMPLETE';
    PRINT '================================================';

    -- 4. Final Summary Report
    SELECT 
        table_name, 
        status, 
        rows_inserted, 
        rows_updated, 
        DATEDIFF(SECOND, load_start_time, load_end_time) AS duration_sec,
        error_message
    FROM silver.metadata 
    WHERE silver_batch_id = @silver_batch_id
    ORDER BY load_start_time;
END;
