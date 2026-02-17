CREATE OR ALTER PROCEDURE silver.load_customers
    @silver_batch_id VARCHAR(50) = NULL,
    @source_batch_id VARCHAR(50) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Auto-generate batch IDs if not provided
    SET @silver_batch_id = ISNULL(@silver_batch_id, 'AUTO_' + FORMAT(SYSDATETIME(), 'yyyyMMddHHmmss'));
    SET @source_batch_id = ISNULL(@source_batch_id, @silver_batch_id);
    
    DECLARE @table_name VARCHAR(100) = 'customers';
    DECLARE @start_time DATETIME2 = SYSDATETIME();
    DECLARE @current_time DATETIME2 = SYSDATETIME();
    
    DECLARE @rows_inserted INT = 0;
    DECLARE @rows_updated INT = 0;
    
    BEGIN TRY
        PRINT '>> Processing SCD Type 2: silver.' + @table_name;
        
        -- ============================================================
        -- STEP 1: Transform and clean source data into temp table
        -- ============================================================
        IF OBJECT_ID('tempdb..#CleanedCustomers') IS NOT NULL DROP TABLE #CleanedCustomers;
        
        SELECT 
            customer_id,
            CASE WHEN first_name IS NULL OR TRIM(first_name) = '' THEN 'N/A' 
                 ELSE UPPER(LEFT(TRIM(first_name), 1)) + LOWER(SUBSTRING(TRIM(first_name), 2, LEN(TRIM(first_name)))) 
            END AS first_name,
            CASE WHEN last_name IS NULL OR TRIM(last_name) = '' THEN 'N/A' 
                 ELSE UPPER(LEFT(TRIM(last_name), 1)) + LOWER(SUBSTRING(TRIM(last_name), 2, LEN(TRIM(last_name)))) 
            END AS last_name,
            TRIM(LOWER(email)) AS email,
            CASE WHEN date_of_birth > GETDATE() THEN NULL ELSE date_of_birth END AS date_of_birth,
            CASE WHEN gender LIKE '%obieta' OR UPPER(gender) = 'K' THEN 'Kobieta'
                 WHEN gender LIKE '%ezczyzna' OR UPPER(gender) = 'M' THEN 'Mezczyzna' 
                 ELSE 'Inna' 
            END AS gender,
            join_date,
            address_id,
            created_at,
            updated_at
        INTO #CleanedCustomers
        FROM bronze.customers;
        
        -- ============================================================
        -- STEP 2: Identify change types into temp table
        -- ============================================================
        IF OBJECT_ID('tempdb..#ChangedRecords') IS NOT NULL DROP TABLE #ChangedRecords;
        
        SELECT 
            cc.*,
            sc.customer_skey,
            sc.address_id AS old_address_id,
            CASE 
                -- New customer (doesn't exist in silver)
                WHEN sc.customer_skey IS NULL THEN 'INSERT'
                
                -- Address changed (SCD Type 2 attribute) - THIS IS THE KEY ONE!
                WHEN ISNULL(cc.address_id, -1) <> ISNULL(sc.address_id, -1) THEN 'SCD2_UPDATE'
                
                -- Other attributes changed (Type 1 - update in place)
                WHEN cc.first_name <> sc.first_name 
                  OR cc.last_name <> sc.last_name
                  OR cc.email <> sc.email
                  OR ISNULL(cc.date_of_birth, '1900-01-01') <> ISNULL(sc.date_of_birth, '1900-01-01')
                  OR cc.gender <> sc.gender
                THEN 'TYPE1_UPDATE'
                
                -- No changes
                ELSE 'NO_CHANGE'
            END AS change_type
        INTO #ChangedRecords
        FROM #CleanedCustomers cc
        LEFT JOIN silver.customers sc 
            ON cc.customer_id = sc.customer_id 
            AND sc.is_current = 1;  -- Only compare with current records
        
        -- ============================================================
        -- STEP 3: Close out old records (expire them) - Address Changes
        -- ============================================================
        UPDATE sc
        SET 
            valid_to = @current_time,
            is_current = 0
        FROM silver.customers sc
        INNER JOIN #ChangedRecords cr 
            ON sc.customer_id = cr.customer_id
            AND sc.is_current = 1
        WHERE cr.change_type = 'SCD2_UPDATE';
        
        SET @rows_updated = @@ROWCOUNT;
        
        IF @rows_updated > 0
            PRINT '   [SCD2] Expired ' + CAST(@rows_updated AS VARCHAR) + ' customer record(s) due to address change';
        
        -- ============================================================
        -- STEP 4: Insert new records (new customers + address changes)
        -- ============================================================
        INSERT INTO silver.customers (
            customer_id, 
            first_name, 
            last_name, 
            email, 
            date_of_birth, 
            gender,
            join_date,
            address_id,
            valid_from, 
            valid_to, 
            is_current, 
            source_created_at, 
            source_updated_at, 
            dwh_load_date, 
            dwh_batch_id
        )
        SELECT 
            customer_id,
            first_name,
            last_name,
            email,
            date_of_birth,
            gender,
            join_date,
            address_id,
            @current_time AS valid_from,
            NULL AS valid_to,
            1 AS is_current,
            created_at,
            updated_at,
            @current_time AS dwh_load_date,
            @silver_batch_id
        FROM #ChangedRecords
        WHERE change_type IN ('INSERT', 'SCD2_UPDATE');  -- New customers and address changes
        
        SET @rows_inserted = @@ROWCOUNT;
        
        IF @rows_inserted > 0
            PRINT '   [SCD2] Inserted ' + CAST(@rows_inserted AS VARCHAR) + ' new customer version(s)';
        
        -- ============================================================
        -- STEP 5: Handle Type 1 updates (overwrite in place)
        -- Updates non-address attributes without creating new version
        -- ============================================================
        UPDATE sc
        SET 
            first_name = cr.first_name,
            last_name = cr.last_name,
            email = cr.email,
            date_of_birth = cr.date_of_birth,
            gender = cr.gender,
            source_updated_at = cr.updated_at,
            dwh_load_date = @current_time,
            dwh_batch_id = @silver_batch_id
        FROM silver.customers sc
        INNER JOIN #ChangedRecords cr 
            ON sc.customer_id = cr.customer_id
            AND sc.is_current = 1
        WHERE cr.change_type = 'TYPE1_UPDATE';
        
        DECLARE @type1_updates INT = @@ROWCOUNT;
        
        IF @type1_updates > 0
            PRINT '   [Type1] Updated ' + CAST(@type1_updates AS VARCHAR) + ' customer record(s) in place';
        
        -- ============================================================
        -- STEP 6: Handle deletions (customers removed from source)
        -- ============================================================
        UPDATE sc
        SET 
            valid_to = @current_time,
            is_current = 0
        FROM silver.customers sc
        WHERE sc.is_current = 1
          AND NOT EXISTS (
              SELECT 1 
              FROM #CleanedCustomers cc 
              WHERE cc.customer_id = sc.customer_id
          );
        
        DECLARE @rows_deleted INT = @@ROWCOUNT;
        
        IF @rows_deleted > 0
            PRINT '   [Deleted] Closed out ' + CAST(@rows_deleted AS VARCHAR) + ' inactive customer(s)';
        
        -- ============================================================
        -- Cleanup temp tables
        -- ============================================================
        DROP TABLE IF EXISTS #CleanedCustomers;
        DROP TABLE IF EXISTS #ChangedRecords;
        
        -- ============================================================
        -- STEP 7: Log metadata
        -- ============================================================
        EXEC silver.log_metadata 
            @table_name, 
            @start_time, 
            @silver_batch_id, 
            @source_batch_id, 
            @ins = @rows_inserted, 
            @upd = @rows_updated, 
            @scd = 'Type 2', 
            @status = 'Success';
        
        PRINT 'SCD Type 2 processing complete';
        
    END TRY
    BEGIN CATCH
        DECLARE @err_msg NVARCHAR(MAX) = ERROR_MESSAGE();
        PRINT '!! ERROR: ' + @err_msg;
        
        EXEC silver.log_metadata 
            @table_name, 
            @start_time, 
            @silver_batch_id, 
            @source_batch_id, 
            @status = 'Failed', 
            @error = @err_msg;
        
        -- Optionally re-throw the error
        THROW;
    END CATCH
END;
