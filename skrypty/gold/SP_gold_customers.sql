CREATE OR ALTER PROCEDURE gold.load_dim_customers
AS
BEGIN
    SET NOCOUNT ON;

    -- 1. Ensure Table Exists
    IF OBJECT_ID('gold.dim_customers') IS NULL
    BEGIN
        CREATE TABLE gold.dim_customers (
            customer_key INT IDENTITY(1,1) PRIMARY KEY, -- Gold Surrogate Key
            customer_id INT,                            -- Original Business ID
            full_name NVARCHAR(255),
            email NVARCHAR(255),
            gender NVARCHAR(50),
            age INT,
            age_group NVARCHAR(50),
            city NVARCHAR(100),
            country NVARCHAR(100),
            join_date DATE,
            dwh_load_date DATETIME2 DEFAULT SYSDATETIME()
        );
    END

    -- 2. Truncate and Reload (Standard for Gold Dimensions)
    TRUNCATE TABLE gold.dim_customers;

-- 3. Re-insert the Ghost Record (Inferred Member)
    SET IDENTITY_INSERT gold.dim_customers ON;
    INSERT INTO gold.dim_customers (customer_key, customer_id, full_name, city, country)
    VALUES (-1, -1, 'Nieznany', 'Nieznane', 'Nieznany');
    SET IDENTITY_INSERT gold.dim_customers OFF;

    -- 4. Transform and Insert
    INSERT INTO gold.dim_customers (
        customer_id, full_name, email, gender, age, 
        age_group, city, country, join_date
    )
    SELECT 
        c.customer_id,
        c.first_name + ' ' + c.last_name AS full_name,
        c.email,
        c.gender,
        -- Calculate Age based on DOB
        DATEDIFF(YEAR, c.date_of_birth, GETDATE()) - 
            CASE WHEN (MONTH(c.date_of_birth) > MONTH(GETDATE())) OR 
                      (MONTH(c.date_of_birth) = MONTH(GETDATE()) AND DAY(c.date_of_birth) > DAY(GETDATE())) 
                 THEN 1 ELSE 0 END AS age,
        -- Marketing Age Groups
        CASE 
            WHEN DATEDIFF(YEAR, c.date_of_birth, GETDATE()) < 18 THEN 'Under 18'
            WHEN DATEDIFF(YEAR, c.date_of_birth, GETDATE()) BETWEEN 18 AND 24 THEN '18-24'
            WHEN DATEDIFF(YEAR, c.date_of_birth, GETDATE()) BETWEEN 25 AND 34 THEN '25-34'
            WHEN DATEDIFF(YEAR, c.date_of_birth, GETDATE()) BETWEEN 35 AND 44 THEN '35-44'
            WHEN DATEDIFF(YEAR, c.date_of_birth, GETDATE()) BETWEEN 45 AND 54 THEN '45-54'
            ELSE '55+' 
        END AS age_group,
        a.city,
        a.country,
        c.join_date
    FROM silver.customers c
    LEFT JOIN silver.addresses a ON c.address_id = a.address_id
    WHERE c.is_current = 1; -- Only take the latest version of the customer

    PRINT 'Gold dim_customers loaded successfully.';
END;
