/*
===============================================================================
Procedura składowana (stored procedure): Ładowanie warstwy Gold
===============================================================================
Cel:
Procedura pełni rolę orkiestratora warstwy Gold i odpowiada za budowę
docelowego modelu analitycznego (model gwiazdy) na podstawie danych
przygotowanych w warstwie Silver.

Warstwa Gold zawiera:
- Tabele wymiarów (Dimension Tables)
- Tabele faktów (Fact Tables)
Gotowe do raportowania w narzędziach BI (np. Power BI).

Procedura implementuje ostatni etap architektury Medallion:
Źródło OLTP → Bronze (surowy) → Silver (oczyszczony) → Gold (wymiarowy)

Działanie:
1. Rejestruje czas rozpoczęcia ładowania.
2. Uruchamia procedury ładujące:
   - Niezależne wymiary (Date, Time, Customers, Products)
   - Tabele faktów (Sessions, Sales, Web Analytics, Returns)
3. Mierzy całkowity czas wykonania procesu.
4. W przypadku błędu:
   - Przechwytuje wyjątek (TRY/CATCH),
   - Loguje informację do tabeli gold.metadata,
   - Rzuca wyjątek ponownie (THROW), aby powiadomić narzędzia
     orkiestrujące (np. SQL Agent, Azure Data Factory).

Sposób użycia:

-- Pełne ładowanie warstwy Gold:

EXEC gold.load_full;
===============================================================================
*/


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
