CREATE OR ALTER PROCEDURE silver.load_pageviews
    @silver_batch_id VARCHAR(50),
    @source_batch_id VARCHAR(50)
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @table_name VARCHAR(100) = 'pageviews';
    DECLARE @start_time DATETIME2 = SYSDATETIME();
    DECLARE @rows_inserted INT = 0;

    BEGIN TRY
        PRINT '>> Loading: silver.' + @table_name;

        -- 1. Fact tables in this design are Truncate & Load
        TRUNCATE TABLE silver.pageviews;

        -- 2. Transformation & Insertion
        INSERT INTO silver.pageviews (
			website_pageview_id, pageview_time, website_session_id, pageview_url,
            source_created_at, source_updated_at, dwh_load_date, dwh_batch_id
		)
        SELECT website_pageview_id, 
			created_at, 
			website_session_id, 
			LOWER(TRIM(pageview_url)),
            created_at, 
			NULL, 
			SYSDATETIME(), 
			@silver_batch_id
        FROM bronze.pageviews;

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
