/*
===============================================================================
Procedura składowana: Ładowanie warstwy Silver - procedura nadrzędna
===============================================================================
Cel:
Procedura pełni rolę orkiestratora warstwy Silver:automatyzuje pełne ładowanie warstwy 
Silver na podstawie danych z warstwy Bronze poprzez wywołanie procedur dla poszczególnych tabel. 
Tabele podrzędne odpowiadają za transformację, czyszczenie, deduplikację oraz standaryzację 
danych przed ich dalszym wykorzystaniem analitycznym.

Procedura realizuje drugi etap architektury Medallion:
Źródło OLTP → Bronze (surowy) → Silver (oczyszczony, ustandaryzowany) → Gold (model analityczny)

Działanie:
1. Generuje identyfikator partii Silver (@silver_batch_id), jeśli nie został przekazany.
2. Automatycznie wybiera ostatni poprawnie zakończony batch z warstwy Bronze,
   jeśli nie podano @source_batch_id.
3. Uruchamia sekwencyjnie procedury ładujące:
   - Tabele wymiarów (Dimensions)
   - Tabele faktów (Facts)
4. Obsługuje błędy per tabela (TRY/CATCH), dzięki czemu pojedyncza awaria
   nie przerywa całego procesu.
5. Rejestruje metryki ładowania w tabeli silver.metadata.
6. Zwraca podsumowanie procesu (status, liczba wierszy, czas trwania, błędy).

Parametry:
@silver_batch_id VARCHAR(50) — opcjonalny identyfikator partii Silver.
                                Jeśli NULL, generowany automatycznie
                                w formacie: SILVER_yyyyMMdd_HHmmss

@source_batch_id VARCHAR(50) — opcjonalny identyfikator partii Bronze,
                                która ma zostać przetworzona.
                                Jeśli NULL, wybierany jest ostatni batch
                                ze statusem 'Success'.
                                Jeśli brak danych — używany 'INITIAL_LOAD'.

-- Pełne ładowanie Silver na podstawie ostatniego poprawnego batcha Bronze:
EXEC silver.load_full;
===============================================================================
*/


CREATE OR ALTER PROCEDURE silver.load_full
    @silver_batch_id VARCHAR(50) = NULL,
    @source_batch_id VARCHAR(50) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    
    SET @silver_batch_id = ISNULL(@silver_batch_id, 'SILVER_' + FORMAT(SYSDATETIME(), 'yyyyMMdd_HHmmss'));
    
    IF @source_batch_id IS NULL
    BEGIN
        SELECT TOP 1 @source_batch_id = batch_id 
        FROM bronze.metadata 
        WHERE status = 'Success' 
        ORDER BY load_end_time DESC;
        
        -- Fallback
        SET @source_batch_id = ISNULL(@source_batch_id, 'INITIAL_LOAD');
    END

    PRINT '================================================';
    PRINT 'START LADOWANIA SILVER BATCH: ' + @silver_batch_id;
    PRINT 'ZRODLO BATCH:    ' + @source_batch_id;
    PRINT '================================================';

    -- 2. WYMIARY
    PRINT '>> Ladowanie tabeli wymiarów...';
    BEGIN TRY EXEC silver.load_addresses @silver_batch_id, @source_batch_id; PRINT 'OK Addresses'; END TRY 
    BEGIN CATCH PRINT 'NIEUDANE: Addresses'; END CATCH

    BEGIN TRY EXEC silver.load_brands @silver_batch_id, @source_batch_id; PRINT 'OK Brands'; END TRY 
    BEGIN CATCH PRINT 'NIEUDANE: Brands'; END CATCH

    BEGIN TRY EXEC silver.load_categories @silver_batch_id, @source_batch_id; PRINT 'OK Categories'; END TRY 
    BEGIN CATCH PRINT 'NIEUDANE: Categories'; END CATCH

    BEGIN TRY EXEC silver.load_customers @silver_batch_id, @source_batch_id; PRINT 'OK Customers'; END TRY 
    BEGIN CATCH PRINT 'NIEUDANE: Customers'; END CATCH

    BEGIN TRY EXEC silver.load_payment_methods @silver_batch_id, @source_batch_id; PRINT 'OK Payment Methods'; END TRY 
    BEGIN CATCH PRINT 'NIEUDANE: Payment Methods'; END CATCH

    BEGIN TRY EXEC silver.load_products @silver_batch_id, @source_batch_id; PRINT 'OK Products'; END TRY 
    BEGIN CATCH PRINT 'NIEUDANE: Products'; END CATCH

    -- 3. FAKTY
    PRINT '>> Ladowanie tabeli faktow...';
    BEGIN TRY EXEC silver.load_website_sessions @silver_batch_id, @source_batch_id; PRINT 'OK Sessions'; END TRY 
    BEGIN CATCH PRINT 'NIEUDANE: Sessions'; END CATCH

    BEGIN TRY EXEC silver.load_pageviews @silver_batch_id, @source_batch_id; PRINT 'OK Pageviews'; END TRY 
    BEGIN CATCH PRINT 'NIEUDANE: Pageviews'; END CATCH

    BEGIN TRY EXEC silver.load_orders @silver_batch_id, @source_batch_id; PRINT 'OK Orders'; END TRY 
    BEGIN CATCH PRINT 'NIEUDANE: Orders'; END CATCH

    BEGIN TRY EXEC silver.load_order_items @silver_batch_id, @source_batch_id; PRINT 'OK Order Items'; END TRY 
    BEGIN CATCH PRINT 'NIEUDANE: Order Items'; END CATCH

    BEGIN TRY EXEC silver.load_order_item_returns @silver_batch_id, @source_batch_id; PRINT 'OK Returns'; END TRY 
    BEGIN CATCH PRINT 'NIEUDANE: Returns'; END CATCH

    PRINT '================================================';
    PRINT 'SILVER LOAD COMPLETE';
    PRINT '================================================';

    -- 4. Podsumowanie
    SELECT 
        table_name, 
        status, 
        rows_inserted, 
        rows_updated, 
        DATEDIFF(SECOND, load_start_time, load_end_time) AS duration_sec,
        error_message
    FROM silver.metadata 
    WHERE silver_batch_id = @silver_batch_id
    ORDER BY load_start_time;
END;
