CREATE OR ALTER PROCEDURE silver.load_silver
    @silver_batch_id VARCHAR(50) = NULL,
    @source_batch_id VARCHAR(50) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @start_time DATETIME2;
    DECLARE @table_name VARCHAR(100);
    DECLARE @rows_inserted INT;
    DECLARE @rows_updated INT;
    DECLARE @rows_deleted INT;
    DECLARE @scd_type VARCHAR(20);
    
    IF @silver_batch_id IS NULL
        SET @silver_batch_id = 'SILVER_' + FORMAT(SYSDATETIME(), 'yyyyMMdd_HHmmss');
    
    IF @source_batch_id IS NULL
    BEGIN
        SELECT TOP 1 @source_batch_id = batch_id 
        FROM bronze.metadata 
        WHERE status = 'Success' 
        ORDER BY load_end_time DESC;
    END
    
    PRINT '========================================';
    PRINT 'Starting Silver Layer Load';
    PRINT 'Silver Batch ID: ' + @silver_batch_id;
    PRINT 'Source Batch ID: ' + ISNULL(@source_batch_id, 'NULL');
    PRINT '========================================';
    
    -- ================================================================
    -- ADDRESSES (SCD Type 1)
    -- ================================================================
    BEGIN TRY
        SET @table_name = 'addresses';
        SET @scd_type = 'Type 1';
        SET @start_time = SYSDATETIME();
        
        PRINT '>> Loading: silver.' + @table_name;
        
        MERGE silver.addresses AS target
        USING (
            SELECT  
                address_id,
                TRIM(country) AS country,
                UPPER(LEFT(TRIM(city), 1)) + LOWER(SUBSTRING(TRIM(city), 2, LEN(TRIM(city)))) AS city,
                CASE 
                    WHEN street IS NULL OR TRIM(street) = '' THEN 'UNKNOWN'
                    ELSE TRIM('ul. ' + UPPER(LEFT(LTRIM(REPLACE(TRIM(street), 'ul.', '')), 1)) + 
                         LOWER(SUBSTRING(LTRIM(REPLACE(TRIM(street), 'ul.', '')), 2, LEN(street))))
                END AS street,
                REPLACE(TRIM(postal_code), ' ', '') AS postal_code,
                created_at,
                updated_at
            FROM bronze.addresses
        ) AS source
        ON target.address_id = source.address_id
        WHEN MATCHED AND (
            ISNULL(target.country, '') <> ISNULL(source.country, '') OR
            ISNULL(target.city, '') <> ISNULL(source.city, '') OR
            ISNULL(target.street, '') <> ISNULL(source.street, '') OR
            ISNULL(target.postal_code, '') <> ISNULL(source.postal_code, '')
        ) THEN
            UPDATE SET
                country = source.country,
                city = source.city,
                street = source.street,
                postal_code = source.postal_code,
                source_created_at = source.created_at,
                source_updated_at = source.updated_at,
                dwh_load_date = SYSDATETIME(),
                dwh_batch_id = @silver_batch_id
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (address_id, country, city, street, postal_code, 
                    source_created_at, source_updated_at, dwh_load_date, dwh_batch_id)
            VALUES (source.address_id, source.country, source.city, source.street, 
                    source.postal_code, source.created_at, source.updated_at, 
                    SYSDATETIME(), @silver_batch_id);
        
        SET @rows_inserted = @@ROWCOUNT;
        
        INSERT INTO silver.metadata (table_name, source_layer, load_start_time, load_end_time, 
            silver_batch_id, source_batch_id, rows_inserted, rows_updated, rows_deleted, scd_type, status)
        VALUES (@table_name, 'bronze', @start_time, SYSDATETIME(), 
            @silver_batch_id, @source_batch_id, @rows_inserted, 0, 0, @scd_type, 'Success');
        
        PRINT '    Rows Affected: ' + CAST(@rows_inserted AS VARCHAR);
    END TRY
    BEGIN CATCH
        INSERT INTO silver.metadata (table_name, source_layer, load_start_time, load_end_time, 
            silver_batch_id, source_batch_id, scd_type, status, error_message)
        VALUES (@table_name, 'bronze', @start_time, SYSDATETIME(), 
            @silver_batch_id, @source_batch_id, @scd_type, 'Failed', ERROR_MESSAGE());
        PRINT 'ERROR: ' + ERROR_MESSAGE();
    END CATCH
    
    -- ================================================================
    -- BRANDS (SCD Type 1)
    -- ================================================================
    BEGIN TRY
        SET @table_name = 'brands';
        SET @scd_type = 'Type 1';
        SET @start_time = SYSDATETIME();
        
        PRINT '>> Loading: silver.' + @table_name;
        
        MERGE silver.brands AS target
        USING (
            SELECT brand_id, ISNULL(TRIM(name), 'N/A') AS brand_name, created_at, updated_at
            FROM bronze.brands
        ) AS source
        ON target.brand_id = source.brand_id
        WHEN MATCHED AND ISNULL(target.brand_name, '') <> ISNULL(source.brand_name, '') THEN
            UPDATE SET brand_name = source.brand_name, source_created_at = source.created_at,
                source_updated_at = source.updated_at, dwh_load_date = SYSDATETIME(), dwh_batch_id = @silver_batch_id
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (brand_id, brand_name, source_created_at, source_updated_at, dwh_load_date, dwh_batch_id)
            VALUES (source.brand_id, source.brand_name, source.created_at, source.updated_at, SYSDATETIME(), @silver_batch_id);
        
        SET @rows_inserted = @@ROWCOUNT;
        
        INSERT INTO silver.metadata (table_name, source_layer, load_start_time, load_end_time, 
            silver_batch_id, source_batch_id, rows_inserted, rows_updated, rows_deleted, scd_type, status)
        VALUES (@table_name, 'bronze', @start_time, SYSDATETIME(), 
            @silver_batch_id, @source_batch_id, @rows_inserted, 0, 0, @scd_type, 'Success');
        
        PRINT '    Rows Affected: ' + CAST(@rows_inserted AS VARCHAR);
    END TRY
    BEGIN CATCH
        INSERT INTO silver.metadata (table_name, source_layer, load_start_time, load_end_time, 
            silver_batch_id, source_batch_id, scd_type, status, error_message)
        VALUES (@table_name, 'bronze', @start_time, SYSDATETIME(), 
            @silver_batch_id, @source_batch_id, @scd_type, 'Failed', ERROR_MESSAGE());
        PRINT 'ERROR: ' + ERROR_MESSAGE();
    END CATCH
    
    -- ================================================================
    -- CATEGORIES (SCD Type 1)
    -- ================================================================
    BEGIN TRY
        SET @table_name = 'categories';
        SET @scd_type = 'Type 1';
        SET @start_time = SYSDATETIME();
        
        PRINT '>> Loading: silver.' + @table_name;
        
        MERGE silver.categories AS target
        USING (
            SELECT category_id, ISNULL(TRIM(name), 'N/A') AS category_name, 
                   parent_category_id, created_at, updated_at
            FROM bronze.categories
        ) AS source
        ON target.category_id = source.category_id
        WHEN MATCHED AND (
            ISNULL(target.category_name, '') <> ISNULL(source.category_name, '') OR
            ISNULL(target.parent_category_id, -1) <> ISNULL(source.parent_category_id, -1)
        ) THEN
            UPDATE SET category_name = source.category_name, parent_category_id = source.parent_category_id,
                source_created_at = source.created_at, source_updated_at = source.updated_at,
                dwh_load_date = SYSDATETIME(), dwh_batch_id = @silver_batch_id
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (category_id, category_name, parent_category_id, source_created_at, source_updated_at, dwh_load_date, dwh_batch_id)
            VALUES (source.category_id, source.category_name, source.parent_category_id, source.created_at, source.updated_at, SYSDATETIME(), @silver_batch_id);
        
        SET @rows_inserted = @@ROWCOUNT;
        
        INSERT INTO silver.metadata (table_name, source_layer, load_start_time, load_end_time, 
            silver_batch_id, source_batch_id, rows_inserted, rows_updated, rows_deleted, scd_type, status)
        VALUES (@table_name, 'bronze', @start_time, SYSDATETIME(), 
            @silver_batch_id, @source_batch_id, @rows_inserted, 0, 0, @scd_type, 'Success');
        
        PRINT '    Rows Affected: ' + CAST(@rows_inserted AS VARCHAR);
    END TRY
    BEGIN CATCH
        INSERT INTO silver.metadata (table_name, source_layer, load_start_time, load_end_time, 
            silver_batch_id, source_batch_id, scd_type, status, error_message)
        VALUES (@table_name, 'bronze', @start_time, SYSDATETIME(), 
            @silver_batch_id, @source_batch_id, @scd_type, 'Failed', ERROR_MESSAGE());
        PRINT 'ERROR: ' + ERROR_MESSAGE();
    END CATCH
    
    -- ================================================================
    -- PAYMENT_METHODS (SCD Type 1)
    -- ================================================================
    BEGIN TRY
        SET @table_name = 'payment_methods';
        SET @scd_type = 'Type 1';
        SET @start_time = SYSDATETIME();
        
        PRINT '>> Loading: silver.' + @table_name;
        
        MERGE silver.payment_methods AS target
        USING (
            SELECT payment_method_id, TRIM(payment_method_name) AS payment_method_name, 
                   is_active, created_at, updated_at
            FROM bronze.payment_methods
        ) AS source
        ON target.payment_method_id = source.payment_method_id
        WHEN MATCHED AND (
            ISNULL(target.payment_method_name, '') <> ISNULL(source.payment_method_name, '') OR
            ISNULL(target.is_active, 0) <> ISNULL(source.is_active, 0)
        ) THEN
            UPDATE SET payment_method_name = source.payment_method_name, is_active = source.is_active,
                source_created_at = source.created_at, source_updated_at = source.updated_at,
                dwh_load_date = SYSDATETIME(), dwh_batch_id = @silver_batch_id
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (payment_method_id, payment_method_name, is_active, source_created_at, source_updated_at, dwh_load_date, dwh_batch_id)
            VALUES (source.payment_method_id, source.payment_method_name, source.is_active, source.created_at, source.updated_at, SYSDATETIME(), @silver_batch_id);
        
        SET @rows_inserted = @@ROWCOUNT;
        
        INSERT INTO silver.metadata (table_name, source_layer, load_start_time, load_end_time, 
            silver_batch_id, source_batch_id, rows_inserted, rows_updated, rows_deleted, scd_type, status)
        VALUES (@table_name, 'bronze', @start_time, SYSDATETIME(), 
            @silver_batch_id, @source_batch_id, @rows_inserted, 0, 0, @scd_type, 'Success');
        
        PRINT '    Rows Affected: ' + CAST(@rows_inserted AS VARCHAR);
    END TRY
    BEGIN CATCH
        INSERT INTO silver.metadata (table_name, source_layer, load_start_time, load_end_time, 
            silver_batch_id, source_batch_id, scd_type, status, error_message)
        VALUES (@table_name, 'bronze', @start_time, SYSDATETIME(), 
            @silver_batch_id, @source_batch_id, @scd_type, 'Failed', ERROR_MESSAGE());
        PRINT 'ERROR: ' + ERROR_MESSAGE();
    END CATCH
    
    -- ================================================================
    -- CUSTOMERS (SCD Type 2)
    -- ================================================================
    BEGIN TRY
        SET @table_name = 'customers';
        SET @scd_type = 'Type 2';
        SET @start_time = SYSDATETIME();
        
        PRINT '>> Loading: silver.' + @table_name;
        
        UPDATE target
        SET valid_to = SYSDATETIME(), is_current = 0
        FROM silver.customers target
        INNER JOIN (
            SELECT customer_id,
                CASE WHEN first_name IS NULL OR TRIM(first_name) = '' THEN 'N/A' 
                    ELSE UPPER(LEFT(TRIM(first_name), 1)) + LOWER(SUBSTRING(TRIM(first_name), 2, LEN(TRIM(first_name)))) END AS first_name,
                CASE WHEN last_name IS NULL OR TRIM(last_name) = '' THEN 'N/A' 
                    ELSE UPPER(LEFT(TRIM(last_name), 1)) + LOWER(SUBSTRING(TRIM(last_name), 2, LEN(TRIM(last_name)))) END AS last_name,
                TRIM(LOWER(email)) AS email,
                CASE WHEN date_of_birth > GETDATE() THEN NULL ELSE date_of_birth END AS date_of_birth,
                CASE WHEN gender LIKE '%obieta' OR UPPER(gender) = 'K' THEN 'Kobieta'
                    WHEN gender LIKE '%ezczyzna' OR UPPER(gender) = 'M' THEN 'Mezczyzna' END AS gender,
                join_date, address_id
            FROM bronze.customers
        ) AS source ON target.customer_id = source.customer_id
        WHERE target.is_current = 1
        AND (
            ISNULL(target.first_name, '') <> ISNULL(source.first_name, '') OR
            ISNULL(target.last_name, '') <> ISNULL(source.last_name, '') OR
            ISNULL(target.email, '') <> ISNULL(source.email, '') OR
            ISNULL(target.date_of_birth, '1900-01-01') <> ISNULL(source.date_of_birth, '1900-01-01') OR
            ISNULL(target.gender, '') <> ISNULL(source.gender, '') OR
            ISNULL(target.address_id, -1) <> ISNULL(source.address_id, -1)
        );
        
        SET @rows_updated = @@ROWCOUNT;
        
        INSERT INTO silver.customers (customer_id, first_name, last_name, email, date_of_birth, gender, 
            join_date, address_id, valid_from, valid_to, is_current, source_created_at, source_updated_at, dwh_load_date, dwh_batch_id)
        SELECT source.customer_id, source.first_name, source.last_name, source.email, source.date_of_birth, source.gender,
            source.join_date, source.address_id, SYSDATETIME(), NULL, 1, source.created_at, source.updated_at, SYSDATETIME(), @silver_batch_id
        FROM (
            SELECT customer_id,
                CASE WHEN first_name IS NULL OR TRIM(first_name) = '' THEN 'N/A' 
                    ELSE UPPER(LEFT(TRIM(first_name), 1)) + LOWER(SUBSTRING(TRIM(first_name), 2, LEN(TRIM(first_name)))) END AS first_name,
                CASE WHEN last_name IS NULL OR TRIM(last_name) = '' THEN 'N/A' 
                    ELSE UPPER(LEFT(TRIM(last_name), 1)) + LOWER(SUBSTRING(TRIM(last_name), 2, LEN(TRIM(last_name)))) END AS last_name,
                TRIM(LOWER(email)) AS email,
                CASE WHEN date_of_birth > GETDATE() THEN NULL ELSE date_of_birth END AS date_of_birth,
                CASE WHEN gender LIKE '%obieta' OR UPPER(gender) = 'K' THEN 'Kobieta'
                    WHEN gender LIKE '%ezczyzna' OR UPPER(gender) = 'M' THEN 'Mezczyzna' END AS gender,
                join_date, address_id, created_at, updated_at
            FROM bronze.customers
        ) source
        LEFT JOIN silver.customers target ON source.customer_id = target.customer_id AND target.is_current = 1
        WHERE target.customer_id IS NULL;
        
        SET @rows_inserted = @@ROWCOUNT;
        
        INSERT INTO silver.metadata (table_name, source_layer, load_start_time, load_end_time, 
            silver_batch_id, source_batch_id, rows_inserted, rows_updated, rows_deleted, scd_type, status)
        VALUES (@table_name, 'bronze', @start_time, SYSDATETIME(), 
            @silver_batch_id, @source_batch_id, @rows_inserted, @rows_updated, 0, @scd_type, 'Success');
        
        PRINT '    Rows Inserted: ' + CAST(@rows_inserted AS VARCHAR) + ', Expired: ' + CAST(@rows_updated AS VARCHAR);
    END TRY
    BEGIN CATCH
        INSERT INTO silver.metadata (table_name, source_layer, load_start_time, load_end_time, 
            silver_batch_id, source_batch_id, scd_type, status, error_message)
        VALUES (@table_name, 'bronze', @start_time, SYSDATETIME(), 
            @silver_batch_id, @source_batch_id, @scd_type, 'Failed', ERROR_MESSAGE());
        PRINT 'ERROR: ' + ERROR_MESSAGE();
    END CATCH
    
    -- ================================================================
    -- PRODUCTS (SCD Type 2)
    -- ================================================================
    BEGIN TRY
        SET @table_name = 'products';
        SET @scd_type = 'Type 2';
        SET @start_time = SYSDATETIME();
        
        PRINT '>> Loading: silver.' + @table_name;
        
        UPDATE target
        SET valid_to = SYSDATETIME(), is_current = 0
        FROM silver.products target
        INNER JOIN (
            SELECT product_id, ISNULL(TRIM(name), 'N/A') AS product_name,
                ISNULL(TRIM(REPLACE(description, '%', ' ')), 'N/A') AS description,
                category_id, brand_id, ABS(list_price) AS list_price
            FROM bronze.products
        ) AS source ON target.product_id = source.product_id
        WHERE target.is_current = 1
        AND (
            ISNULL(target.product_name, '') <> ISNULL(source.product_name, '') OR
            ISNULL(target.description, '') <> ISNULL(source.description, '') OR
            ISNULL(target.category_id, -1) <> ISNULL(source.category_id, -1) OR
            ISNULL(target.brand_id, -1) <> ISNULL(source.brand_id, -1) OR
            ISNULL(target.list_price, 0) <> ISNULL(source.list_price, 0)
        );
        
        SET @rows_updated = @@ROWCOUNT;
        
        INSERT INTO silver.products (product_id, product_name, description, category_id, brand_id, list_price,
            valid_from, valid_to, is_current, source_created_at, source_updated_at, dwh_load_date, dwh_batch_id)
        SELECT source.product_id, source.product_name, source.description, source.category_id, source.brand_id, source.list_price,
            SYSDATETIME(), NULL, 1, source.created_at, source.updated_at, SYSDATETIME(), @silver_batch_id
        FROM (
            SELECT product_id, ISNULL(TRIM(name), 'N/A') AS product_name,
                ISNULL(TRIM(REPLACE(description, '%', ' ')), 'N/A') AS description,
                category_id, brand_id, ABS(list_price) AS list_price, created_at, updated_at
            FROM bronze.products
        ) source
        LEFT JOIN silver.products target ON source.product_id = target.product_id AND target.is_current = 1
        WHERE target.product_id IS NULL;
        
        SET @rows_inserted = @@ROWCOUNT;
        
        INSERT INTO silver.metadata (table_name, source_layer, load_start_time, load_end_time, 
            silver_batch_id, source_batch_id, rows_inserted, rows_updated, rows_deleted, scd_type, status)
        VALUES (@table_name, 'bronze', @start_time, SYSDATETIME(), 
            @silver_batch_id, @source_batch_id, @rows_inserted, @rows_updated, 0, @scd_type, 'Success');
        
        PRINT '    Rows Inserted: ' + CAST(@rows_inserted AS VARCHAR) + ', Expired: ' + CAST(@rows_updated AS VARCHAR);
    END TRY
    BEGIN CATCH
        INSERT INTO silver.metadata (table_name, source_layer, load_start_time, load_end_time, 
            silver_batch_id, source_batch_id, scd_type, status, error_message)
        VALUES (@table_name, 'bronze', @start_time, SYSDATETIME(), 
            @silver_batch_id, @source_batch_id, @scd_type, 'Failed', ERROR_MESSAGE());
        PRINT 'ERROR: ' + ERROR_MESSAGE();
    END CATCH
    
    -- ================================================================
    -- ORDERS (Fact Table)
    -- ================================================================
    BEGIN TRY
        SET @table_name = 'orders';
        SET @start_time = SYSDATETIME();
        
        PRINT '>> Loading: silver.' + @table_name;
        
        TRUNCATE TABLE silver.orders;
        
        INSERT INTO silver.orders (order_id, customer_id, order_date, payment_method_id, session_id, order_status,
            subtotal_amount, discount_amount, tax_amount, shipping_amount, total_amount,
            source_created_at, source_updated_at, dwh_load_date, dwh_batch_id)
        SELECT order_id, customer_id, order_date, payment_method_id, session_id, UPPER(TRIM(order_status)),
            ISNULL(ABS(subtotal_amount), 0), ISNULL(ABS(discount_amount), 0), ISNULL(ABS(tax_amount), 0),
            ISNULL(ABS(shipping_amount), 0), ISNULL(ABS(total_amount), 0),
            created_at, updated_at, SYSDATETIME(), @silver_batch_id
        FROM bronze.orders;
        
        SET @rows_inserted = @@ROWCOUNT;
        
        INSERT INTO silver.metadata (table_name, source_layer, load_start_time, load_end_time, 
            silver_batch_id, source_batch_id, rows_inserted, rows_updated, rows_deleted, scd_type, status)
        VALUES (@table_name, 'bronze', @start_time, SYSDATETIME(), 
            @silver_batch_id, @source_batch_id, @rows_inserted, 0, 0, 'N/A', 'Success');
        
        PRINT '    Rows Inserted: ' + CAST(@rows_inserted AS VARCHAR);
    END TRY
    BEGIN CATCH
        INSERT INTO silver.metadata (table_name, source_layer, load_start_time, load_end_time, 
            silver_batch_id, source_batch_id, scd_type, status, error_message)
        VALUES (@table_name, 'bronze', @start_time, SYSDATETIME(), 
            @silver_batch_id, @source_batch_id, 'N/A', 'Failed', ERROR_MESSAGE());
        PRINT 'ERROR: ' + ERROR_MESSAGE();
    END CATCH
    
    -- ================================================================
    -- ORDER_ITEMS (Fact Table)
    -- ================================================================
    BEGIN TRY
        SET @table_name = 'order_items';
        SET @start_time = SYSDATETIME();
        
        PRINT '>> Loading: silver.' + @table_name;
        
        TRUNCATE TABLE silver.order_items;
        
        INSERT INTO silver.order_items (order_item_id, order_id, product_id, quantity, unit_price, 
            discount_amount, tax_amount, line_total, source_created_at, source_updated_at, dwh_load_date, dwh_batch_id)
        SELECT order_item_id, order_id, product_id, quantity, ISNULL(ABS(unit_price), 0),
            ISNULL(ABS(discount_amount), 0), ISNULL(ABS(tax_amount), 0),
            ((quantity * ISNULL(ABS(unit_price), 0)) + ISNULL(ABS(tax_amount), 0)) - ISNULL(ABS(discount_amount), 0),
            created_at, updated_at, SYSDATETIME(), @silver_batch_id
        FROM bronze.order_items;
        
        SET @rows_inserted = @@ROWCOUNT;
        
        INSERT INTO silver.metadata (table_name, source_layer, load_start_time, load_end_time, 
            silver_batch_id, source_batch_id, rows_inserted, rows_updated, rows_deleted, scd_type, status)
        VALUES (@table_name, 'bronze', @start_time, SYSDATETIME(), 
            @silver_batch_id, @source_batch_id, @rows_inserted, 0, 0, 'N/A', 'Success');
        
        PRINT '    Rows Inserted: ' + CAST(@rows_inserted AS VARCHAR);
    END TRY
    BEGIN CATCH
        INSERT INTO silver.metadata (table_name, source_layer, load_start_time, load_end_time, 
            silver_batch_id, source_batch_id, scd_type, status, error_message)
        VALUES (@table_name, 'bronze', @start_time, SYSDATETIME(), 
            @silver_batch_id, @source_batch_id, 'N/A', 'Failed', ERROR_MESSAGE());
        PRINT 'ERROR: ' + ERROR_MESSAGE();
    END CATCH
    
    -- ================================================================
    -- ORDER_ITEM_RETURNS (Fact Table)
    -- ================================================================
    BEGIN TRY
        SET @table_name = 'order_item_returns';
        SET @start_time = SYSDATETIME();
        
        PRINT '>> Loading: silver.' + @table_name;
        
        TRUNCATE TABLE silver.order_item_returns;
        
        INSERT INTO silver.order_item_returns (return_id, order_item_id, return_date, quantity_returned, refund_amount, reason,
            source_created_at, source_updated_at, dwh_load_date, dwh_batch_id)
        SELECT return_id, order_item_id,
            CASE WHEN return_date > GETDATE() THEN NULL ELSE return_date END,
            ISNULL(ABS(quantity_returned), 0), ISNULL(ABS(refund_amount), 0),
            CASE WHEN reason IS NULL OR TRIM(reason) IN ('', '-') THEN 'N/A' 
                ELSE UPPER(LEFT(TRIM(reason), 1)) + LOWER(SUBSTRING(TRIM(reason), 2, LEN(reason))) END,
            created_at, updated_at, SYSDATETIME(), @silver_batch_id
        FROM bronze.order_item_returns;
        
        SET @rows_inserted = @@ROWCOUNT;
        
        INSERT INTO silver.metadata (table_name, source_layer, load_start_time, load_end_time, 
            silver_batch_id, source_batch_id, rows_inserted, rows_updated, rows_deleted, scd_type, status)
        VALUES (@table_name, 'bronze', @start_time, SYSDATETIME(), 
            @silver_batch_id, @source_batch_id, @rows_inserted, 0, 0, 'N/A', 'Success');
        
        PRINT '    Rows Inserted: ' + CAST(@rows_inserted AS VARCHAR);
    END TRY
    BEGIN CATCH
        INSERT INTO silver.metadata (table_name, source_layer, load_start_time, load_end_time, 
            silver_batch_id, source_batch_id, scd_type, status, error_message)
        VALUES (@table_name, 'bronze', @start_time, SYSDATETIME(), 
            @silver_batch_id, @source_batch_id, 'N/A', 'Failed', ERROR_MESSAGE());
        PRINT 'ERROR: ' + ERROR_MESSAGE();
    END CATCH
    
    -- ================================================================
    -- WEBSITE_SESSIONS (Fact Table)
    -- ================================================================
    BEGIN TRY
        SET @table_name = 'website_sessions';
        SET @start_time = SYSDATETIME();
        
        PRINT '>> Loading: silver.' + @table_name;
        
        TRUNCATE TABLE silver.website_sessions;
        
        INSERT INTO silver.website_sessions (website_session_id, session_start, user_id, is_repeat_session, 
            utm_source, utm_campaign, utm_content, device_type, http_referer,
            source_created_at, source_updated_at, dwh_load_date, dwh_batch_id)
        SELECT website_session_id, created_at, user_id, is_repeat_session,
            ISNULL(LOWER(TRIM(utm_source)), 'direct'), ISNULL(TRIM(LOWER(utm_campaign)), 'organic traffic'),
            ISNULL(TRIM(utm_content), 'organic'), ISNULL(TRIM(LOWER(device_type)), 'N/A'), TRIM(http_referer),
            created_at, NULL, SYSDATETIME(), @silver_batch_id
        FROM bronze.website_sessions;
        
        SET @rows_inserted = @@ROWCOUNT;
        
        INSERT INTO silver.metadata (table_name, source_layer, load_start_time, load_end_time, 
            silver_batch_id, source_batch_id, rows_inserted, rows_updated, rows_deleted, scd_type, status)
        VALUES (@table_name, 'bronze', @start_time, SYSDATETIME(), 
            @silver_batch_id, @source_batch_id, @rows_inserted, 0, 0, 'N/A', 'Success');
        
        PRINT '    Rows Inserted: ' + CAST(@rows_inserted AS VARCHAR);
    END TRY
    BEGIN CATCH
        INSERT INTO silver.metadata (table_name, source_layer, load_start_time, load_end_time, 
            silver_batch_id, source_batch_id, scd_type, status, error_message)
        VALUES (@table_name, 'bronze', @start_time, SYSDATETIME(), 
            @silver_batch_id, @source_batch_id, 'N/A', 'Failed', ERROR_MESSAGE());
        PRINT 'ERROR: ' + ERROR_MESSAGE();
    END CATCH
    
    -- ================================================================
    -- PAGEVIEWS (Fact Table)
    -- ================================================================
    BEGIN TRY
        SET @table_name = 'pageviews';
        SET @start_time = SYSDATETIME();
        
        PRINT '>> Loading: silver.' + @table_name;
        
        TRUNCATE TABLE silver.pageviews;
        
        INSERT INTO silver.pageviews (website_pageview_id, pageview_time, website_session_id, pageview_url,
            source_created_at, source_updated_at, dwh_load_date, dwh_batch_id)
        SELECT website_pageview_id, created_at, website_session_id, LOWER(TRIM(pageview_url)),
            created_at, NULL, SYSDATETIME(), @silver_batch_id
        FROM bronze.pageviews;
        
        SET @rows_inserted = @@ROWCOUNT;
        
        INSERT INTO silver.metadata (table_name, source_layer, load_start_time, load_end_time, 
            silver_batch_id, source_batch_id, rows_inserted, rows_updated, rows_deleted, scd_type, status)
        VALUES (@table_name, 'bronze', @start_time, SYSDATETIME(), 
            @silver_batch_id, @source_batch_id, @rows_inserted, 0, 0, 'N/A', 'Success');
        
        PRINT '    Rows Inserted: ' + CAST(@rows_inserted AS VARCHAR);
    END TRY
    BEGIN CATCH
        INSERT INTO silver.metadata (table_name, source_layer, load_start_time, load_end_time, 
            silver_batch_id, source_batch_id, scd_type, status, error_message)
        VALUES (@table_name, 'bronze', @start_time, SYSDATETIME(), 
            @silver_batch_id, @source_batch_id, 'N/A', 'Failed', ERROR_MESSAGE());
        PRINT 'ERROR: ' + ERROR_MESSAGE();
    END CATCH
    
    -- ================================================================
    -- FINAL SUMMARY
    -- ================================================================
    PRINT '========================================';
    PRINT 'Silver Layer Load Complete';
    PRINT 'Silver Batch ID: ' + @silver_batch_id;
    PRINT '========================================';
    
    SELECT 
        table_name,
        scd_type,
        rows_inserted,
        rows_updated,
        status,
        DATEDIFF(SECOND, load_start_time, load_end_time) AS duration_seconds
    FROM silver.metadata
    WHERE silver_batch_id = @silver_batch_id
    ORDER BY load_start_time;
    
END;
GO
