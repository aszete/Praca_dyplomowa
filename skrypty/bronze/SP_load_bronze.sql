/*
===============================================================================
Procedura składowana (stored procedure): Ładowanie warstwy Bronze
===============================================================================
Cel:
Procedura automatyzuje ładowanie surowych danych z plików źródłowych OLTP 
(pliki CSV) do warstwy Bronze. Dodatkowo rejestruje metryki łądowania w
tabeli metadata.

Procedura implementuje początkowy etap pobierania Medallion Architektura:
Źródło OLTP → Bronze (surowy) → Silver (oczyszczony) → Gold (wymiarowy)

Parametry:
@batch_id VARCHAR(50) — opcjonalny identyfikator partii.
                        Jeśli wartość jest równa NULL, generowany jest
                        unikalny identyfikator na podstawie bieżącego
                        znacznika czasu systemu.

Sposób użycia:
-- Ładowanie wszystkich tabel z automatycznie wygenerowanym identyfikatorem:

EXEC bronze.load_bronze;

-- Ładowanie wybranych tabele z określonym identyfikatorem:

EXEC bronze.load_bronze @batch_id = 'BATCH_2024_001';
===============================================================================
*/


CREATE OR ALTER PROCEDURE bronze.load_bronze
    @batch_id VARCHAR(50) = NULL
AS
BEGIN
    SET NOCOUNT ON;

    -- 1. Zmienne
    DECLARE @start_time DATETIME2;
    DECLARE @row_count INT;
    DECLARE @table_name VARCHAR(100);
    DECLARE @file_path NVARCHAR(500);
    DECLARE @sql_command NVARCHAR(MAX);

    IF @batch_id IS NULL
        SET @batch_id = 'BATCH_' + FORMAT(SYSDATETIME(), 'yyyyMMdd_HHmmss');

    -- 2. Zdefiniowanie tabel do załadowania
    DECLARE @tables TABLE (name VARCHAR(100));
    INSERT INTO @tables (name)
    VALUES ('customers'), ('addresses'), ('brands'), ('categories'), 
           ('products'), ('order_item_returns'), ('order_items'), 
           ('orders'), ('pageviews'), ('payment_methods'), ('website_sessions');

    -- 3. Iteracja przez każdą tabelę
    DECLARE table_cursor CURSOR FOR SELECT name FROM @tables;
    OPEN table_cursor;
    FETCH NEXT FROM table_cursor INTO @table_name;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        SET @start_time = SYSDATETIME();
        SET @file_path = 'C:\Projekt dyplomowy\bronze\' + @table_name + '.csv';

        BEGIN TRY
            PRINT '>> Loading: bronze.' + @table_name;

            -- Dynamiczny TRUNCATE
            SET @sql_command = 'TRUNCATE TABLE bronze.' + QUOTENAME(@table_name);
            EXEC sp_executesql @sql_command;

            -- Dynamiczny BULK INSERT
            SET @sql_command = '
                BULK INSERT bronze.' + QUOTENAME(@table_name) + '
                FROM ''' + @file_path + '''
                WITH (
                    FIRSTROW = 2,
                    FIELDTERMINATOR = '','',
                    ROWTERMINATOR = ''0x0a'',
                    KEEPNULLS,
                    TABLOCK
                );';
            EXEC sp_executesql @sql_command;

            -- Dynamiczna aktualizacja kolumn audytu
            SET @sql_command = '
                UPDATE bronze.' + QUOTENAME(@table_name) + '
                SET batch_id = @b, load_date = GETDATE()
                WHERE batch_id IS NULL;';
            EXEC sp_executesql @sql_command, N'@b VARCHAR(50)', @b = @batch_id;

            -- Ilość wierszy
            SET @sql_command = 'SELECT @rc = COUNT(*) FROM bronze.' + QUOTENAME(@table_name);
            EXEC sp_executesql @sql_command, N'@rc INT OUTPUT', @rc = @row_count OUTPUT;

            -- Zapisanie udanego ładowania w tabeli metadata
            INSERT INTO bronze.metadata (table_name, load_start_time, load_end_time, last_load_date, batch_id, row_count, status)
            VALUES (@table_name, @start_time, SYSDATETIME(), SYSDATETIME(), @batch_id, @row_count, 'Success');

            PRINT '    Rows Loaded: ' + CAST(@row_count AS VARCHAR);
        END TRY
        BEGIN CATCH
            -- Zapisanie nieudanego ładowania w tabeli metadata
            INSERT INTO bronze.metadata (table_name, load_start_time, load_end_time, last_load_date, batch_id, status, error_message)
            VALUES (@table_name, @start_time, SYSDATETIME(), SYSDATETIME(), @batch_id, 'Failed', ERROR_MESSAGE());
            
            PRINT 'ERROR loading ' + @table_name + ': ' + ERROR_MESSAGE();
        END CATCH

        FETCH NEXT FROM table_cursor INTO @table_name;
    END

    CLOSE table_cursor;
    DEALLOCATE table_cursor;
END;
