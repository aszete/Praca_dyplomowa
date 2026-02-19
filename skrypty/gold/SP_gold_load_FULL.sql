CREATE OR ALTER PROCEDURE gold.load_full
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @batch_start_time DATETIME2 = SYSDATETIME();
    DECLARE @TotalDuration INT;
    
    PRINT '>>>    START ORKIESTRATORA WARSTWY GOLD   	<<<';

    BEGIN TRY
        -- PHASE 1: Independent Dimensions
        PRINT 'Phase 1: Ladowanie tabeli wymiarow...';
        EXEC gold.load_dim_date;
        EXEC gold.load_dim_time;
        EXEC gold.load_dim_customers;
        EXEC gold.load_dim_products;

        -- PHASE 4: Fact Tables
        PRINT 'Phase 4: Ladowanie tabel faktów...';
		EXEC gold.load_fact_sessions;
        EXEC gold.load_fact_sales;
        EXEC gold.load_fact_web_analytics;
        EXEC gold.load_fact_returns;

        SET @TotalDuration = DATEDIFF(SECOND, @batch_start_time, SYSDATETIME());

        PRINT '>>>     SUKCES ŁADOWANIA WARSTWY GOLD        <<<';
        PRINT 'Czas ladowania: ' + CAST(@TotalDuration AS VARCHAR(10)) + ' sekund';

    END TRY
    BEGIN CATCH

        DECLARE @ErrorMessage NVARCHAR(MAX) = ERROR_MESSAGE();
        
        PRINT 'ERROR';
        PRINT @ErrorMessage;
        
        -- Ładowanie błędów do tabeli metadata
        EXEC gold.log_metadata 
            @table_name = 'ORCHESTRATOR FAILURE', 
            @start_time = @batch_start_time, 
            @ins = 0, 
            @status = 'Error', 
            @error = @ErrorMessage;

        -- Rethrow to notify external tools
        THROW 50000, @ErrorMessage, 1; 
    END CATCH
END;
