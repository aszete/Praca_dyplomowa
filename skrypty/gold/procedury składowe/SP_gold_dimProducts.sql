CREATE OR ALTER PROCEDURE gold.load_dim_products
AS
BEGIN
    SET NOCOUNT ON;

    -- 1. Ensure Table Exists
    IF OBJECT_ID('gold.dim_products') IS NULL
    BEGIN
        CREATE TABLE gold.dim_products (
            product_key INT IDENTITY(1,1) PRIMARY KEY,
            product_id INT,
            product_name NVARCHAR(255),
            description NVARCHAR(MAX),
            brand_name NVARCHAR(100),
            category NVARCHAR(100),      -- Was Parent Category
            subcategory NVARCHAR(100),   -- Was Category
            list_price DECIMAL(18, 2),
            dwh_load_date DATETIME2 DEFAULT SYSDATETIME()
        );
    END

	-- 2. Clear Table
	TRUNCATE TABLE gold.dim_products;

	-- 3. Re-insert the Ghost Record (Inferred Member)
    -- We use IDENTITY_INSERT so we can force the Key to be -1
    SET IDENTITY_INSERT gold.dim_products ON;
    INSERT INTO gold.dim_products (product_key, product_id, product_name, category, subcategory, brand_name, list_price)
    VALUES (-1, -1, 'Brak produktu', 'Inne', 'Inne', 'Nieznana Marka', 0);
    SET IDENTITY_INSERT gold.dim_products OFF;

    -- 2. Transform and Insert
    INSERT INTO gold.dim_products (
        product_id, product_name, description, brand_name, 
        category, subcategory, list_price
    )
    SELECT 
        p.product_id,
        p.product_name,
        p.description,
        ISNULL(b.brand_name, 'Nieznana Marka'),
        -- We join categories to themselves to get Parent -> Sub relationship
        ISNULL(parent.category_name, 'Inne') AS category,
        ISNULL(child.category_name, 'Inne') AS subcategory,
        p.list_price
    FROM silver.products p
    LEFT JOIN silver.brands b ON p.brand_id = b.brand_id
    LEFT JOIN silver.categories child ON p.category_id = child.category_id
    LEFT JOIN silver.categories parent ON child.parent_category_id = parent.category_id
    WHERE p.is_current = 1;

    PRINT 'Gold dim_products loaded successfully.';
END;
