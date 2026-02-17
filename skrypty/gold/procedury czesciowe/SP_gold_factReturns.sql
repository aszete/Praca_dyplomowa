CREATE OR ALTER PROCEDURE gold.load_fact_returns
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @start_time DATETIME2 = SYSDATETIME();

    BEGIN TRY
        -- 1. Ensure Table Exists
        IF OBJECT_ID('gold.fact_returns') IS NULL
        BEGIN
            CREATE TABLE gold.fact_returns (
                return_key INT IDENTITY(1,1) PRIMARY KEY NONCLUSTERED,
                return_id INT,
                order_item_id INT,
                order_id INT,
                customer_key INT,
                product_key INT,
                return_date_key INT,
                return_time_key INT,
                return_reason NVARCHAR(255),
                quantity_returned INT,
                refund_amount DECIMAL(18, 2),
                dwh_load_date DATETIME2 DEFAULT SYSDATETIME()
            );
        END

        -- 2. Clear Table
        TRUNCATE TABLE gold.fact_returns;

        -- 3. Transform and Insert
        INSERT INTO gold.fact_returns (
            return_id, order_item_id, order_id, customer_key, product_key, 
            return_date_key, return_time_key, return_reason, 
            quantity_returned, refund_amount
        )
        SELECT 
            sr.return_id,
            sr.order_item_id,
            oi.order_id,                  -- Found via silver.order_items
            ISNULL(dc.customer_key, -1), -- Found via silver.orders
            ISNULL(dp.product_key, -1), -- Found via silver.order_items -> gold.dim_products
            CAST(FORMAT(sr.return_date, 'yyyyMMdd') AS INT),
            CAST(FORMAT(sr.return_date, 'HHmm') AS INT),
            sr.reason,                    -- Your column name is 'reason'
            sr.quantity_returned,
            sr.refund_amount
        FROM silver.order_item_returns sr
        -- First hop: Get order_id and product_id from order_items
        LEFT JOIN silver.order_items oi 
            ON sr.order_item_id = oi.order_item_id
        -- Second hop: Get customer_id from orders
        LEFT JOIN silver.orders so 
            ON oi.order_id = so.order_id
        -- Third hop: Get Surrogate Keys from Gold Dimensions
        LEFT JOIN gold.dim_customers dc 
            ON so.customer_id = dc.customer_id
        LEFT JOIN gold.dim_products dp 
            ON oi.product_id = dp.product_id;

        -- 4. Log Success
        EXEC gold.log_metadata 'fact_returns', @start_time, @@ROWCOUNT, 'Success';
        PRINT 'Gold fact_returns loaded successfully.';

    END TRY
    BEGIN CATCH
        DECLARE @err_msg NVARCHAR(MAX) = ERROR_MESSAGE();
        EXEC gold.log_metadata 'fact_returns', @start_time, 0, 'Error', @err_msg;
        THROW;
    END CATCH
END;
