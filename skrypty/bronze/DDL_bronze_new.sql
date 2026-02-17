--customers
IF OBJECT_ID('bronze.customers', 'U') IS NOT NULL
    DROP TABLE bronze.customers;

GO
    
CREATE TABLE bronze.customers (
    customer_id INT NOT NULL PRIMARY KEY NONCLUSTERED,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    email VARCHAR(255) NOT NULL,
    date_of_birth DATE NULL,
    gender VARCHAR(20) NULL,
    join_date DATE NOT NULL,
    address_id INT NULL,
    -- Source timestamps
    created_at DATETIME2 NULL,
    updated_at DATETIME2 NULL,
    -- ETL audit (Made NULLable for bulk loading)
    load_date DATETIME2 NULL,
    batch_id VARCHAR(50) NULL,
    PRIMARY KEY NONCLUSTERED (customer_id)
);

--addresses
IF OBJECT_ID('bronze.addresses', 'U') IS NOT NULL
    DROP TABLE bronze.addresses;

GO

CREATE TABLE bronze.addresses (
    address_id INT NOT NULL PRIMARY KEY NONCLUSTERED,
    country VARCHAR(100) NOT NULL,
    city VARCHAR(100) NOT NULL,
    street VARCHAR(255) NOT NULL,
    postal_code VARCHAR(20) NOT NULL,
    created_at DATETIME2 NULL,
    updated_at DATETIME2 NULL,
    load_date DATETIME2 NULL,
    batch_id VARCHAR(50) NULL
);

-- brands
IF OBJECT_ID('bronze.brands', 'U') IS NOT NULL
    DROP TABLE bronze.brands;

GO

CREATE TABLE bronze.brands (
    brand_id INT NOT NULL PRIMARY KEY NONCLUSTERED,
    name VARCHAR(255) NOT NULL,
    created_at DATETIME2 NULL,
    updated_at DATETIME2 NULL,
    load_date DATETIME2 NULL,
    batch_id VARCHAR(50) NULL
);

-- categories
IF OBJECT_ID('bronze.categories', 'U') IS NOT NULL
    DROP TABLE bronze.categories;

GO
    
CREATE TABLE bronze.categories (
    category_id INT NOT NULL PRIMARY KEY NONCLUSTERED,
    name VARCHAR(255) NOT NULL,
    parent_category_id INT NULL,
    created_at DATETIME2 NULL,
    updated_at DATETIME2 NULL,
    load_date DATETIME2 NULL,
    batch_id VARCHAR(50) NULL
);

-- products
IF OBJECT_ID('bronze.products', 'U') IS NOT NULL
    DROP TABLE bronze.products;

GO
    
CREATE TABLE bronze.products (
    product_id INT NOT NULL PRIMARY KEY NONCLUSTERED,
    name VARCHAR(255) NOT NULL,
    description VARCHAR(1000) NULL,
    category_id INT NULL,
    brand_id INT NULL,
    list_price DECIMAL(18,2) NULL,
    created_at DATETIME2 NULL,
    updated_at DATETIME2 NULL,
    load_date DATETIME2 NULL,
    batch_id VARCHAR(50) NULL
);

-- payment methods
IF OBJECT_ID('bronze.payment_methods', 'U') IS NOT NULL
    DROP TABLE bronze.payment_methods;

GO

CREATE TABLE bronze.payment_methods (
    payment_method_id INT NOT NULL PRIMARY KEY NONCLUSTERED,
    payment_method_name VARCHAR(100) NOT NULL,
    is_active BIT NOT NULL DEFAULT 1,
    created_at DATETIME2 NULL,
    updated_at DATETIME2 NULL,
    load_date DATETIME2 NULL,
    batch_id VARCHAR(50) NULL
);

-- Orders
IF OBJECT_ID('bronze.orders', 'U') IS NOT NULL
    DROP TABLE bronze.orders;

GO

CREATE TABLE bronze.orders (
    order_id INT NOT NULL PRIMARY KEY NONCLUSTERED,
    customer_id INT NOT NULL,
    order_date DATETIME2 NOT NULL,
    payment_method_id INT NULL,
    session_id INT NULL,
    order_status VARCHAR(50) NOT NULL,
    subtotal_amount DECIMAL(18,2) NOT NULL,       -- amount without discounts, taxes
    discount_amount DECIMAL(18,2) NULL DEFAULT 0,
    tax_amount DECIMAL(18,2) NULL DEFAULT 0,
    shipping_amount DECIMAL(18,2) NULL DEFAULT 0,
    total_amount DECIMAL(18,2) NOT NULL,
    created_at DATETIME2 NULL,
    updated_at DATETIME2 NULL,
    load_date DATETIME2 NULL,
    batch_id VARCHAR(50) NULL
);

-- Order Items
IF OBJECT_ID('bronze.order_items', 'U') IS NOT NULL
    DROP TABLE bronze.order_items;

GO

CREATE TABLE bronze.order_items (
    order_item_id INT NOT NULL PRIMARY KEY NONCLUSTERED,
    order_id INT NOT NULL,
    product_id INT NOT NULL,
    quantity INT NOT NULL,
    unit_price DECIMAL(18,2) NOT NULL,
    discount_amount DECIMAL(18,2) NULL DEFAULT 0,
    tax_amount DECIMAL(18,2) NULL DEFAULT 0,
    created_at DATETIME2 NULL,
    updated_at DATETIME2 NULL,
    load_date DATETIME2 NULL,
    batch_id VARCHAR(50) NULL
);

-- Order Item Returns
IF OBJECT_ID('bronze.order_item_returns', 'U') IS NOT NULL
    DROP TABLE bronze.order_item_returns;

GO

CREATE TABLE bronze.order_item_returns (
    return_id INT NOT NULL PRIMARY KEY NONCLUSTERED,
    order_item_id INT NOT NULL,
    return_date DATETIME2 NOT NULL,
    quantity_returned INT NOT NULL,
    refund_amount DECIMAL(18,2) NULL,
    reason VARCHAR(255) NULL,
    created_at DATETIME2 NULL,
    updated_at DATETIME2 NULL,
    load_date DATETIME2 NULL,
    batch_id VARCHAR(50) NULL
);

-- Website Sessions
IF OBJECT_ID('bronze.website_sessions', 'U') IS NOT NULL
    DROP TABLE bronze.website_sessions;

GO

CREATE TABLE bronze.website_sessions (
    website_session_id INT NOT NULL PRIMARY KEY NONCLUSTERED,
    created_at DATETIME2 NOT NULL,
    user_id INT NULL,
    is_repeat_session BIT NULL,
    utm_source VARCHAR(100) NULL,
    utm_campaign VARCHAR(100) NULL,
    utm_content VARCHAR(100) NULL,
    device_type VARCHAR(50) NULL,
    http_referer VARCHAR(255) NULL,
    load_date DATETIME2 NULL,
    batch_id VARCHAR(50) NULL
);

-- Pageviews
IF OBJECT_ID('bronze.pageviews', 'U') IS NOT NULL
    DROP TABLE bronze.pageviews;

GO

CREATE TABLE bronze.pageviews (
    website_pageview_id INT NOT NULL PRIMARY KEY NONCLUSTERED,
    created_at DATETIME2 NOT NULL,
    website_session_id INT NOT NULL,
    pageview_url VARCHAR(255) NULL,
    load_date DATETIME2 NULL,
    batch_id VARCHAR(50) NULL
);

-- Metadata table tracking ETL loads for all tables
CREATE TABLE bronze.metadata (
    metadata_id INT IDENTITY(1,1) PRIMARY KEY,
    table_name VARCHAR(100) NOT NULL,
    source_system VARCHAR(100) NULL,
    load_start_time DATETIME2 NULL,
    load_end_time DATETIME2 NULL,
    last_load_date DATETIME2 NOT NULL,
    batch_id VARCHAR(50) NOT NULL,
    row_count INT NULL,
    status VARCHAR(50) NOT NULL, -- 'Success', 'Failed', 'Running', 'Partial'
    error_message VARCHAR(MAX) NULL,
    comments VARCHAR(255) NULL,
    created_at DATETIME2 NOT NULL DEFAULT GETDATE()
);


---------------------------------------------------------------------    NOTES    -------------------------------------------------------------------------------

-- IDENTITY(1,1)

-- **What it does:** Auto-incrementing number generator, used for surrogate keys, audit/log tables

-- - **First 1**: Start counting from 1
-- - **Second 1**: Increment by 1 each time

-- **Example:**
-- ```
-- Row 1: metadata_id = 1 (automatic)
-- Row 2: metadata_id = 2 (automatic)
-- Row 3: metadata_id = 3 (automatic)



