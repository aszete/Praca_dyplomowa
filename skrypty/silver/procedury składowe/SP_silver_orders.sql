CREATE OR ALTER PROCEDURE silver.load_orders
    @silver_batch_id VARCHAR(50),
    @source_batch_id VARCHAR(50)
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @table_name VARCHAR(100) = 'orders';
    DECLARE @start_time DATETIME2 = SYSDATETIME();
    DECLARE @rows_inserted INT = 0;

    BEGIN TRY
        PRINT '>> Loading: silver.' + @table_name;

        -- 1. Fact tables in this design are Truncate & Load
        TRUNCATE TABLE silver.orders;

        -- 2. Transformation & Insertion
        INSERT INTO silver.orders (
            order_id, customer_id, order_date, payment_method_id, 
            session_id, order_status, subtotal_amount, discount_amount, 
            tax_amount, shipping_amount, total_amount,
            source_created_at, source_updated_at, dwh_load_date, dwh_batch_id
        )
        SELECT 
            order_id, 
            customer_id, 
            order_date, 
            payment_method_id, 
            session_id, 
            UPPER(TRIM(order_status)),
            ISNULL(ABS(subtotal_amount), 0), 
            ISNULL(ABS(discount_amount), 0), 
            ISNULL(ABS(tax_amount), 0),
            ISNULL(ABS(shipping_amount), 0), 
            ISNULL(ABS(total_amount), 0),
            created_at, 
            updated_at, 
            SYSDATETIME(), 
            @silver_batch_id
        FROM bronze.orders;

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
