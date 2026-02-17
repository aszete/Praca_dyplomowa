CREATE OR ALTER PROCEDURE gold.load_fact_sessions
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @start_time DATETIME2 = SYSDATETIME();
    
    BEGIN TRY
        -- 1. Ensure Table Exists
        IF OBJECT_ID('gold.fact_sessions') IS NULL
        BEGIN
            CREATE TABLE gold.fact_sessions (
                session_key INT PRIMARY KEY NONCLUSTERED,
                customer_key INT,
                start_date_key INT,
                start_time_key INT,
                total_pageviews INT,
                duration_seconds INT,
                resulted_in_sale BIT,
                dwh_load_date DATETIME2 DEFAULT SYSDATETIME()
            );
        END
        
        -- 2. Clear Table
        TRUNCATE TABLE gold.fact_sessions;
        
        -- 3. Insert Logic with Calculated Metrics
        INSERT INTO gold.fact_sessions (
            session_key, customer_key, start_date_key, start_time_key,
            total_pageviews, duration_seconds, resulted_in_sale
        )
        SELECT 
            s.website_session_id,
            ISNULL(dc.customer_key, -1),
            CAST(FORMAT(s.session_start, 'yyyyMMdd') AS INT) AS start_date_key,
            CAST(FORMAT(s.session_start, 'HHmm') AS INT) AS start_time_key,
            
            -- Calculate total pageviews from pageviews table
            ISNULL(pv_stats.total_pageviews, 0) AS total_pageviews,
            
            -- Calculate session duration (last pageview - first pageview in seconds)
            ISNULL(pv_stats.duration_seconds, 0) AS duration_seconds,
            
            -- Check if session resulted in a sale
            CASE WHEN EXISTS (
                SELECT 1 
                FROM silver.orders o 
                WHERE o.session_id = s.website_session_id
            ) THEN 1 ELSE 0 END AS resulted_in_sale
        FROM silver.website_sessions s
        LEFT JOIN gold.dim_customers dc ON s.user_id = dc.customer_id
        
        -- Subquery to calculate pageview statistics per session
        LEFT JOIN (
            SELECT 
                website_session_id,
                COUNT(*) AS total_pageviews,
                DATEDIFF(SECOND, MIN(pageview_time), MAX(pageview_time)) AS duration_seconds
            FROM silver.pageviews
            GROUP BY website_session_id
        ) pv_stats ON s.website_session_id = pv_stats.website_session_id;
        
        -- 4. Logging
        EXEC gold.log_metadata 'fact_sessions', @start_time, @@ROWCOUNT, 'Success';
        PRINT 'Gold fact_sessions loaded successfully.';
        
    END TRY
    BEGIN CATCH
        DECLARE @err NVARCHAR(MAX) = ERROR_MESSAGE();
        EXEC gold.log_metadata 'fact_sessions', @start_time, 0, 'Error', @err;
        THROW;
    END CATCH
END;
