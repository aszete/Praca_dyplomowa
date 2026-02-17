CREATE OR ALTER PROCEDURE gold.load_dim_date
AS
BEGIN
    SET NOCOUNT ON;

    -- 1. Ensure Table Exists
    IF OBJECT_ID('gold.dim_date') IS NULL
    BEGIN
        CREATE TABLE gold.dim_date (
            date_key INT PRIMARY KEY,
            full_date DATE,
            year INT,
            quarter INT,
            month INT,
            month_name VARCHAR(15),
            week_of_year INT,
            day_of_week INT,
            day_name VARCHAR(15)
        );
    END

    -- 2. Clear existing data
    TRUNCATE TABLE gold.dim_date;

    -- 3. High-performance insert using a CTE (2023 to 2030)
    DECLARE @StartDate DATE = '2023-01-01';
    DECLARE @TotalDays INT = DATEDIFF(DAY, @StartDate, '2030-12-31');

    WITH DateSeries AS (
        SELECT 0 AS n
        UNION ALL
        SELECT n + 1 FROM DateSeries WHERE n < @TotalDays
    ),
    DateList AS (
        SELECT DATEADD(DAY, n, @StartDate) AS d
        FROM DateSeries
    )
    INSERT INTO gold.dim_date (
        date_key, full_date, year, quarter, month, 
        month_name, week_of_year, day_of_week, day_name
    )
    SELECT 
        CAST(FORMAT(d, 'yyyyMMdd') AS INT),
        d,
        YEAR(d),
        DATEPART(QUARTER, d),
        MONTH(d),
        DATENAME(MONTH, d),
        DATEPART(WEEK, d),
        DATEPART(WEEKDAY, d),
        DATENAME(WEEKDAY, d)
    FROM DateList
    OPTION (MAXRECURSION 0); -- Allows the loop to exceed 100 days

    PRINT 'Gold dim_date loaded successfully.';
END;
