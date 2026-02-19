CREATE OR ALTER PROCEDURE gold.load_full
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @batch_start_time DATETIME2 = SYSDATETIME();
    DECLARE @TotalDuration INT;
    
    PRINT '================================================';
    PRINT '>>>          STARTING GOLD LAYER             <<<';
    PRINT '================================================';

    BEGIN TRY
        -- Faza 1: Ładowanie tabeli wymiarów
        PRINT 'Phase 1: Loading Dimension Tables...';
        EXEC gold.load_dim_date;
        EXEC gold.load_dim_time;
        EXEC gold.load_dim_customers;
        EXEC gold.load_dim_products;

        -- Faza 2: Ładowanie tabeli faktów
        PRINT 'Phase 4: Loading Fact Tables...';
		EXEC gold.load_fact_sessions;
        EXEC gold.load_fact_sales;
        EXEC gold.load_fact_web_analytics;
        EXEC gold.load_fact_returns;

        SET @TotalDuration = DATEDIFF(SECOND, @batch_start_time, SYSDATETIME());

        PRINT '================================================';
        PRINT '>>>         LOADING GOLD SUCCESSFUL          <<<';
        PRINT 'Duration: ' + CAST(@TotalDuration AS VARCHAR(10)) + ' seconds';
        PRINT '================================================';

    END TRY
    BEGIN CATCH
        -- Wyłapanie komunikatu o błędzie
        DECLARE @ErrorMessage NVARCHAR(MAX) = ERROR_MESSAGE();
        
        PRINT '!!! ERROR !!!';
        PRINT @ErrorMessage;
        
        -- Zapisanie błędu w tabeli metadata
        EXEC gold.log_metadata 
            @table_name = 'ORCHESTRATOR_FAILURE', 
            @start_time = @batch_start_time, 
            @ins = 0, 
            @status = 'Error', 
            @error = @ErrorMessage;

        -- Rethrow to notify external tools
        THROW 50000, @ErrorMessage, 1; 
    END CATCH
END;
