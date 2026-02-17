CREATE OR ALTER PROCEDURE silver.load_brands
    @silver_batch_id VARCHAR(50),
    @source_batch_id VARCHAR(50)
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @table_name VARCHAR(100) = 'brands';
    DECLARE @scd_type VARCHAR(20) = 'Type 1';
    DECLARE @start_time DATETIME2 = SYSDATETIME();
    DECLARE @rows_affected INT = 0;

    BEGIN TRY
        PRINT '>> Loading: silver.' + @table_name;

        -- 1. Transformation Logic (CTE)
        WITH CleanedBrands AS (
			SELECT brand_id, ISNULL(TRIM(name), 'N/A') AS brand_name, created_at, updated_at
            FROM bronze.brands
        )
        -- 2. Upsert Logic
        MERGE silver.brands AS target
        USING CleanedBrands AS source
        ON target.brand_id = source.brand_id
        WHEN MATCHED AND (
            ISNULL(target.brand_name, '') <> ISNULL(source.brand_name, '')
        ) THEN
            UPDATE SET
                brand_name = source.brand_name,
                source_created_at = source.created_at,
                source_updated_at = source.updated_at,
                dwh_load_date = SYSDATETIME(),
                dwh_batch_id = @silver_batch_id
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (brand_id, brand_name, source_created_at, source_updated_at, dwh_load_date, dwh_batch_id)
            VALUES (source.brand_id, source.brand_name, source.created_at, source.updated_at, SYSDATETIME(), @silver_batch_id);

        SET @rows_affected = @@ROWCOUNT;

        -- 3. Log Success
        EXEC silver.log_metadata 
            @table_name, @start_time, @silver_batch_id, @source_batch_id, 
            @ins = @rows_affected, @scd = @scd_type, @status = 'Success';

    END TRY
    BEGIN CATCH
        -- 4. Log Failure
        DECLARE @err_msg NVARCHAR(MAX) = ERROR_MESSAGE();
        EXEC silver.log_metadata 
            @table_name, @start_time, @silver_batch_id, @source_batch_id, 
            @status = 'Failed', @error = @err_msg;
            
        PRINT 'ERROR in ' + @table_name + ': ' + @err_msg;
    END CATCH
END;
