CREATE OR ALTER PROCEDURE silver.load_order_items
    @silver_batch_id VARCHAR(50),
    @source_batch_id VARCHAR(50)
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @table_name VARCHAR(100) = 'order_items';
    DECLARE @start_time DATETIME2 = SYSDATETIME();
    DECLARE @rows_inserted INT = 0;

    BEGIN TRY
        PRINT '>> Loading: silver.' + @table_name;

        -- 1. Fact tables in this design are Truncate & Load
        TRUNCATE TABLE silver.order_items;

        -- 2. Transformation & Insertion
        INSERT INTO silver.order_items (
			order_item_id, order_id, product_id, quantity, unit_price, 
            discount_amount, tax_amount, line_total, source_created_at,
			source_updated_at, dwh_load_date, dwh_batch_id
		)
        SELECT 
			order_item_id,
			order_id,
			product_id,
			quantity,
			ISNULL(ABS(unit_price), 0),
            ISNULL(ABS(discount_amount), 0), 
			ISNULL(ABS(tax_amount), 0),
            ((quantity * ISNULL(ABS(unit_price), 0)) + ISNULL(ABS(tax_amount), 0)) - ISNULL(ABS(discount_amount), 0),
            created_at, 
			updated_at, 
			SYSDATETIME(), 
			@silver_batch_id
        FROM bronze.order_items;

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
