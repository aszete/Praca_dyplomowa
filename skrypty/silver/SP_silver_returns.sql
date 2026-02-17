CREATE OR ALTER PROCEDURE silver.load_order_item_returns
    @silver_batch_id VARCHAR(50),
    @source_batch_id VARCHAR(50)
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @table_name VARCHAR(100) = 'order_item_returns';
    DECLARE @start_time DATETIME2 = SYSDATETIME();
    DECLARE @rows_inserted INT = 0;

    BEGIN TRY
        PRINT '>> Loading: silver.' + @table_name;

        -- 1. Fact tables in this design are Truncate & Load
        TRUNCATE TABLE silver.order_item_returns;

        -- 2. Transformation & Insertion
        INSERT INTO silver.order_item_returns (
			return_id, order_item_id, return_date, quantity_returned, refund_amount, reason,
            source_created_at, source_updated_at, dwh_load_date, dwh_batch_id
		)
        SELECT return_id, order_item_id,
            CASE WHEN return_date > GETDATE() THEN NULL ELSE return_date END,
            ISNULL(ABS(quantity_returned), 0), ISNULL(ABS(refund_amount), 0),
            CASE WHEN reason IS NULL OR TRIM(reason) IN ('', '-') THEN 'N/A' 
            ELSE UPPER(LEFT(TRIM(reason), 1)) + LOWER(SUBSTRING(TRIM(reason), 2, LEN(reason))) END,
            created_at, updated_at, SYSDATETIME(), @silver_batch_id
        FROM bronze.order_item_returns;


        SET @rows_inserted = @@ROWCOUNT;

        -- 3. Log Success (SCD Type is 'N/A' for Facts)
        EXEC silver.log_metadata 
            @table_name, @start_time, @silver_batch_id, @source_batch_id, 
            @ins = @rows_inserted, @scd = 'Fact', @status = 'Success';

    END TRY
    BEGIN CATCH
        DECLARE @err_msg NVARCHAR(MAX) = ERROR_MESSAGE();
        EXEC silver.log_metadata 
            @table_name, @start_time, @silver_batch_id, @source_batch_id, 
            @status = 'Failed', @error = @err_msg;
            
        PRINT 'ERROR in ' + @table_name + ': ' + @err_msg;
    END CATCH
END;
