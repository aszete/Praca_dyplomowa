/*
===============================================================================
Kod T-SQL tworzący tabele warstwy Silver
===============================================================================
*/

-- Silver Addresses (SCD Type 1)
IF OBJECT_ID('silver.addresses', 'U') IS NOT NULL DROP TABLE silver.addresses;
GO
CREATE TABLE silver.addresses (
    address_skey INT IDENTITY(1,1) PRIMARY KEY,
    address_id INT NOT NULL,
    country NVARCHAR(100),
    city NVARCHAR(100),
    street NVARCHAR(255),
    postal_code NVARCHAR(20),
    source_created_at DATETIME2, 
    source_updated_at DATETIME2,
    dwh_load_date DATETIME2 DEFAULT SYSDATETIME(),
    dwh_batch_id VARCHAR(50)
);
GO

-- Silver Customers (SCD Type 2)
IF OBJECT_ID('silver.customers', 'U') IS NOT NULL DROP TABLE silver.customers;
GO
CREATE TABLE silver.customers (
    customer_skey INT IDENTITY(1,1) PRIMARY KEY,
    customer_id INT NOT NULL,
    first_name NVARCHAR(100),
    last_name NVARCHAR(100),
    email NVARCHAR(255),
    date_of_birth DATE,
    gender VARCHAR(50),
    join_date DATE,
    address_id INT,
    valid_from DATETIME2 NOT NULL,
    valid_to DATETIME2 NULL,
    is_current BIT NOT NULL DEFAULT 1,
    source_created_at DATETIME2, 
    source_updated_at DATETIME2,
    dwh_load_date DATETIME2 DEFAULT SYSDATETIME(),
    dwh_batch_id VARCHAR(50)
);

-- Silver Brands (SCD Type 1)
IF OBJECT_ID('silver.brands', 'U') IS NOT NULL DROP TABLE silver.brands;
GO
CREATE TABLE silver.brands (
    brand_skey INT IDENTITY(1,1) PRIMARY KEY,
    brand_id INT NOT NULL,
    brand_name NVARCHAR(100),
    source_created_at DATETIME2, 
    source_updated_at DATETIME2,
    dwh_load_date DATETIME2 DEFAULT SYSDATETIME(),
    dwh_batch_id VARCHAR(50)
);
GO

-- Silver Categories (SCD Type 1)
IF OBJECT_ID('silver.categories', 'U') IS NOT NULL DROP TABLE silver.categories;
GO
CREATE TABLE silver.categories (
    category_skey INT IDENTITY(1,1) PRIMARY KEY,
    category_id INT NOT NULL,
    category_name NVARCHAR(255),
    parent_category_id INT NULL,
    source_created_at DATETIME2, 
    source_updated_at DATETIME2,
    dwh_load_date DATETIME2 DEFAULT SYSDATETIME(),
    dwh_batch_id VARCHAR(50)
);
GO

-- Silver Payment Methods (SCD Type 1)
IF OBJECT_ID('silver.payment_methods', 'U') IS NOT NULL DROP TABLE silver.payment_methods;
GO
CREATE TABLE silver.payment_methods (
    payment_method_skey INT IDENTITY(1,1) PRIMARY KEY,
    payment_method_id INT NOT NULL,
    payment_method_name NVARCHAR(100),
    is_active BIT,
    source_created_at DATETIME2, 
    source_updated_at DATETIME2,
    dwh_load_date DATETIME2 DEFAULT SYSDATETIME(),
    dwh_batch_id VARCHAR(50)
);
GO

-- Silver Products (SCD Type 2)
IF OBJECT_ID('silver.products', 'U') IS NOT NULL DROP TABLE silver.products;
GO
CREATE TABLE silver.products (
    product_skey INT IDENTITY(1,1) PRIMARY KEY,
    product_id INT NOT NULL,
    product_name NVARCHAR(255),
    description NVARCHAR(MAX),
    category_id INT,
    brand_id INT,
    list_price DECIMAL(18,2),
    valid_from DATETIME2 NOT NULL,
    valid_to DATETIME2 NULL,
    is_current BIT NOT NULL DEFAULT 1,
    source_created_at DATETIME2, 
    source_updated_at DATETIME2,
    dwh_load_date DATETIME2 DEFAULT SYSDATETIME(),
    dwh_batch_id VARCHAR(50)
);
GO

-- ==========================================================
-- 2. Fact Tables
-- ==========================================================

-- Silver Orders
IF OBJECT_ID('silver.orders', 'U') IS NOT NULL DROP TABLE silver.orders;
GO
CREATE TABLE silver.orders (
    order_skey INT IDENTITY(1,1) PRIMARY KEY,
    order_id INT NOT NULL,
    customer_id INT,
    order_date DATETIME2,
    payment_method_id INT,
    session_id INT,
    order_status NVARCHAR(50),
    subtotal_amount DECIMAL(18,2),
    discount_amount DECIMAL(18,2),
    tax_amount DECIMAL(18,2),
    shipping_amount DECIMAL(18,2),
    total_amount DECIMAL(18,2),
    source_created_at DATETIME2, 
    source_updated_at DATETIME2,
    dwh_load_date DATETIME2 DEFAULT SYSDATETIME(),
    dwh_batch_id VARCHAR(50)
);
GO

-- Silver Order Items
IF OBJECT_ID('silver.order_items', 'U') IS NOT NULL DROP TABLE silver.order_items;
GO
CREATE TABLE silver.order_items (
    order_item_skey INT IDENTITY(1,1) PRIMARY KEY,
    order_item_id INT NOT NULL,
    order_id INT,
    product_id INT,
    quantity INT,
    unit_price DECIMAL(18,2),
    discount_amount DECIMAL(18,2),
    tax_amount DECIMAL(18,2),
    line_total DECIMAL(18,2),
    source_created_at DATETIME2, 
    source_updated_at DATETIME2,
    dwh_load_date DATETIME2 DEFAULT SYSDATETIME(),
    dwh_batch_id VARCHAR(50)
);
GO

-- Silver Order Item Returns
IF OBJECT_ID('silver.order_item_returns', 'U') IS NOT NULL DROP TABLE silver.order_item_returns;
GO
CREATE TABLE silver.order_item_returns (
    return_skey INT IDENTITY(1,1) PRIMARY KEY,
    return_id INT NOT NULL,
    order_item_id INT,
    return_date DATETIME2,
    quantity_returned INT,
    refund_amount DECIMAL(18,2),
    reason NVARCHAR(MAX),
    source_created_at DATETIME2, 
    source_updated_at DATETIME2,
    dwh_load_date DATETIME2 DEFAULT SYSDATETIME(),
    dwh_batch_id VARCHAR(50)
);
GO

-- Silver Website Sessions
IF OBJECT_ID('silver.website_sessions', 'U') IS NOT NULL DROP TABLE silver.website_sessions;
GO
CREATE TABLE silver.website_sessions (
    website_session_skey INT IDENTITY(1,1) PRIMARY KEY,
    website_session_id INT NOT NULL,
    session_start DATETIME2,
    user_id INT,
    is_repeat_session BIT,
    utm_source NVARCHAR(255),
    utm_campaign NVARCHAR(255),
    utm_content NVARCHAR(255),
    device_type NVARCHAR(50),
    http_referer NVARCHAR(255),
    source_created_at DATETIME2, 
    source_updated_at DATETIME2,
    dwh_load_date DATETIME2 DEFAULT SYSDATETIME(),
    dwh_batch_id VARCHAR(50)
);
GO

-- Silver Pageviews
IF OBJECT_ID('silver.pageviews', 'U') IS NOT NULL DROP TABLE silver.pageviews;
GO
CREATE TABLE silver.pageviews (
    pageview_skey INT IDENTITY(1,1) PRIMARY KEY,
    website_pageview_id INT NOT NULL,
    pageview_time DATETIME2,
    website_session_id INT,
    pageview_url NVARCHAR(255),
    source_created_at DATETIME2, 
    source_updated_at DATETIME2,
    dwh_load_date DATETIME2 DEFAULT SYSDATETIME(),
    dwh_batch_id VARCHAR(50)
);
GO

-- Silver metadata
IF OBJECT_ID('silver.metadata', 'U') IS NOT NULL DROP TABLE silver.metadata;
GO
CREATE TABLE silver.metadata (
    metadata_id INT IDENTITY(1,1) PRIMARY KEY,
    table_name VARCHAR(100) NOT NULL,
    source_layer VARCHAR(20) NOT NULL DEFAULT 'bronze',
    load_start_time DATETIME2 NULL,
    load_end_time DATETIME2 NULL,
    silver_batch_id VARCHAR(50) NOT NULL,
    source_batch_id VARCHAR(50),
    rows_inserted INT NULL,
    rows_updated INT NULL,
    rows_deleted INT NULL,
    scd_type VARCHAR(20) NULL,
    status VARCHAR(50) NOT NULL,
    error_message VARCHAR(MAX) NULL,
    created_at DATETIME2 NOT NULL DEFAULT GETDATE()
);
GO
