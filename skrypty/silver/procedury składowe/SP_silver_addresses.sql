CREATE OR ALTER PROCEDURE silver.load_addresses
    @silver_batch_id VARCHAR(50),
    @source_batch_id VARCHAR(50)
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @table_name VARCHAR(100) = 'addresses';
    DECLARE @scd_type VARCHAR(20) = 'Type 1';
    DECLARE @start_time DATETIME2 = SYSDATETIME();
    DECLARE @rows_affected INT = 0;

    BEGIN TRY
        PRINT '>> Loading: silver.' + @table_name;

        -- 1. Transformation Logic (CTE)
        WITH CleanedAddresses AS (
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
        )
        -- 2. Upsert Logic
        MERGE silver.addresses AS target
        USING CleanedAddresses AS source
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
