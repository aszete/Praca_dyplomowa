CREATE OR ALTER PROCEDURE gold.load_fact_web_analytics
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @start_time DATETIME2 = SYSDATETIME();
    
    BEGIN TRY
        -- 1. Ensure Table Exists
        IF OBJECT_ID('gold.fact_web_analytics') IS NULL
        BEGIN
            CREATE TABLE gold.fact_web_analytics (
                web_analytics_key INT IDENTITY(1,1) PRIMARY KEY NONCLUSTERED,
                pageview_id INT,
                session_key INT,  -- Changed from session_key
                customer_key INT,
                date_key INT,
                time_key INT,
                page_name NVARCHAR(255),
                utm_source NVARCHAR(100),
                utm_campaign NVARCHAR(100),
                utm_content NVARCHAR(100),
                device_type NVARCHAR(50),
                dwh_load_date DATETIME2 DEFAULT SYSDATETIME()
            );
            
        END
        
        -- 2. Clear Table
        TRUNCATE TABLE gold.fact_web_analytics;
        
        -- 3. Insert Logic (adjust based on your silver schema)
        INSERT INTO gold.fact_web_analytics (
            pageview_id, session_key, customer_key, date_key, time_key,
            page_name, utm_source, utm_campaign, utm_content, device_type
        )
        SELECT 
            pv.website_pageview_id,
            pv.website_session_id,  -- Direct from silver, no lookup
            ISNULL(dc.customer_key, -1),
            CAST(FORMAT(pv.pageview_time, 'yyyyMMdd') AS INT),
            CAST(FORMAT(pv.pageview_time, 'HHmm') AS INT),
            pv.pageview_url,
            s.utm_source,
            s.utm_campaign,
            s.utm_content,
            s.device_type
        FROM silver.pageviews pv
        LEFT JOIN silver.website_sessions s ON pv.website_session_id = s.website_session_id
        LEFT JOIN gold.dim_customers dc ON s.user_id = dc.customer_id;
        
        -- 4. Logging
        EXEC gold.log_metadata 'fact_web_analytics', @start_time, @@ROWCOUNT, 'Success';
        PRINT 'Gold fact_web_analytics loaded successfully.';
        
    END TRY
    BEGIN CATCH
        DECLARE @err NVARCHAR(MAX) = ERROR_MESSAGE();
        EXEC gold.log_metadata 'fact_web_analytics', @start_time, 0, 'Error', @err;
        THROW;
    END CATCH
END;
