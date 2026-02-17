CREATE OR ALTER PROCEDURE gold.load_fact_sales
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @start_time DATETIME2 = SYSDATETIME();
    
    BEGIN TRY
        -- 1. Ensure Table Exists
        IF OBJECT_ID('gold.fact_sales') IS NULL
        BEGIN
            CREATE TABLE gold.fact_sales (
                sales_key INT IDENTITY(1,1) PRIMARY KEY NONCLUSTERED,
                order_id INT,
                session_key INT,
                customer_key INT,
                product_key INT,
                date_key INT,
                time_key INT,
                payment_method NVARCHAR(50),
                order_status NVARCHAR(50),
                quantity INT,
                unit_price DECIMAL(18, 2),
                discount_amount DECIMAL(18, 2),  -- ← ADD THIS
                tax_amount DECIMAL(18, 2),
                total_amount DECIMAL(18, 2),
                dwh_load_date DATETIME2 DEFAULT SYSDATETIME()
            );
        END
        ELSE
        BEGIN
            -- Add discount_amount column if it doesn't exist (for existing tables)
            IF NOT EXISTS (
                SELECT 1 FROM sys.columns 
                WHERE object_id = OBJECT_ID('gold.fact_sales') 
                AND name = 'discount_amount'
            )
            BEGIN
                ALTER TABLE gold.fact_sales 
                ADD discount_amount DECIMAL(18, 2);
            END
        END
        
        -- 2. Clear Table
        TRUNCATE TABLE gold.fact_sales;
        
        -- 3. Insert Logic
        INSERT INTO gold.fact_sales (
            order_id, session_key, customer_key, product_key, date_key, time_key,
            payment_method, order_status, quantity, unit_price, discount_amount, 
            tax_amount, total_amount
        )
        SELECT 
            o.order_id,
            o.session_id,
            ISNULL(dc.customer_key, -1),
            ISNULL(dp.product_key, -1),
            CAST(FORMAT(o.order_date, 'yyyyMMdd') AS INT),
            CAST(FORMAT(o.order_date, 'HHmm') AS INT),
            ISNULL(pm.payment_method_name, 'Unknown'),
            o.order_status,
            oi.quantity,
            oi.unit_price,
            ISNULL(oi.discount_amount, 0),  -- ← ADD THIS
            oi.tax_amount,
            (oi.quantity * oi.unit_price) + oi.tax_amount - ISNULL(oi.discount_amount, 0)  -- ← ADJUST CALCULATION
        FROM silver.orders o
        JOIN silver.order_items oi ON o.order_id = oi.order_id
        LEFT JOIN gold.dim_customers dc ON o.customer_id = dc.customer_id
        LEFT JOIN gold.dim_products dp ON oi.product_id = dp.product_id
        LEFT JOIN silver.payment_methods pm ON o.payment_method_id = pm.payment_method_id;
        
        -- 4. Logging
        EXEC gold.log_metadata 'fact_sales', @start_time, @@ROWCOUNT, 'Success';
        PRINT 'Gold fact_sales loaded successfully.';
        
    END TRY
    BEGIN CATCH
        DECLARE @err NVARCHAR(MAX) = ERROR_MESSAGE();
        EXEC gold.log_metadata 'fact_sales', @start_time, 0, 'Error', @err;
        THROW;
    END CATCH
END;
