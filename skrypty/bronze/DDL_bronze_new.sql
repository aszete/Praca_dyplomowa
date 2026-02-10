--customers
CREATE TABLE bronze.customers (
    customer_id INT NOT NULL,
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
    -- ETL audit
    source_system VARCHAR(50) NOT NULL DEFAULT 'OLTP_DB',
    load_date DATETIME2 NOT NULL DEFAULT GETDATE(),
    batch_id VARCHAR(50) NULL,
    PRIMARY KEY NONCLUSTERED (customer_id)
);

--addresses
CREATE TABLE bronze.addresses (
    address_id INT NOT NULL,
    country VARCHAR(100) NOT NULL,
    city VARCHAR(100) NOT NULL,
    street VARCHAR(255) NOT NULL,
    postal_code VARCHAR(20) NOT NULL,
    created_at DATETIME2 NULL,
    updated_at DATETIME2 NULL,
    source_system VARCHAR(50) NOT NULL DEFAULT 'OLTP_DB',
    load_date DATETIME2 NOT NULL DEFAULT GETDATE(),
    batch_id VARCHAR(50) NULL,
    PRIMARY KEY NONCLUSTERED (address_id)
);

-- brands
CREATE TABLE bronze.brands (
    brand_id INT NOT NULL PRIMARY KEY NONCLUSTERED,
    name VARCHAR(255) NOT NULL,
    created_at DATETIME2 NULL,
    updated_at DATETIME2 NULL,
    source_system VARCHAR(50) NOT NULL DEFAULT 'OLTP_DB',
    load_date DATETIME2 NOT NULL DEFAULT GETDATE(),
    batch_id VARCHAR(50) NULL
);

-- categories
CREATE TABLE bronze.categories (
    category_id INT NOT NULL PRIMARY KEY NONCLUSTERED,
    name VARCHAR(255) NOT NULL,
    parent_category_id INT NULL,
    created_at DATETIME2 NULL,
    updated_at DATETIME2 NULL,
    source_system VARCHAR(50) NOT NULL DEFAULT 'OLTP_DB',
    load_date DATETIME2 NOT NULL DEFAULT GETDATE(),
    batch_id VARCHAR(50) NULL
);

-- products
CREATE TABLE bronze.products (
    product_id INT NOT NULL PRIMARY KEY NONCLUSTERED,
    name VARCHAR(255) NOT NULL,
    description VARCHAR(1000) NULL,
    category_id INT NULL,
    brand_id INT NULL,
    list_price DECIMAL(18,2) NULL,
    created_at DATETIME2 NULL,
    updated_at DATETIME2 NULL,
    source_system VARCHAR(50) NOT NULL DEFAULT 'OLTP_DB',
    load_date DATETIME2 NOT NULL DEFAULT GETDATE(),
    batch_id VARCHAR(50) NULL
);

-- payment methods
CREATE TABLE bronze.payment_methods (
    payment_method_id INT NOT NULL PRIMARY KEY NONCLUSTERED,
    payment_method_name VARCHAR(100) NOT NULL,
    is_active BIT NOT NULL DEFAULT 1,
    created_at DATETIME2 NULL,
    updated_at DATETIME2 NULL,
    source_system VARCHAR(50) NOT NULL DEFAULT 'OLTP_DB',
    load_date DATETIME2 NOT NULL DEFAULT GETDATE(),
    batch_id VARCHAR(50) NULL
);

-- Orders
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
    source_system VARCHAR(50) NOT NULL DEFAULT 'OLTP_DB',
    load_date DATETIME2 NOT NULL DEFAULT GETDATE(),
    batch_id VARCHAR(50) NULL
);

-- Order Items
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
    source_system VARCHAR(50) NOT NULL DEFAULT 'OLTP_DB',
    load_date DATETIME2 NOT NULL DEFAULT GETDATE(),
    batch_id VARCHAR(50) NULL
);

-- Order Item Returns
CREATE TABLE bronze.order_item_returns (
    return_id INT NOT NULL PRIMARY KEY NONCLUSTERED,
    order_item_id INT NOT NULL,
    return_date DATETIME2 NOT NULL,
    quantity_returned INT NOT NULL,
    refund_amount DECIMAL(18,2) NULL,
    reason VARCHAR(255) NULL,
    created_at DATETIME2 NULL,
    updated_at DATETIME2 NULL,
    source_system VARCHAR(50) NOT NULL DEFAULT 'OLTP_DB',
    load_date DATETIME2 NOT NULL DEFAULT GETDATE(),
    batch_id VARCHAR(50) NULL
);

-- Website Sessions
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
    source_system VARCHAR(50) NOT NULL DEFAULT 'OLTP_DB',
    load_date DATETIME2 NOT NULL DEFAULT GETDATE(),
    batch_id VARCHAR(50) NULL
);

-- Pageviews
CREATE TABLE bronze.pageviews (
    website_pageview_id INT NOT NULL PRIMARY KEY NONCLUSTERED,
    created_at DATETIME2 NOT NULL,
    website_session_id INT NOT NULL,
    pageview_url VARCHAR(255) NULL,
    source_system VARCHAR(50) NOT NULL DEFAULT 'OLTP_DB',
    load_date DATETIME2 NOT NULL DEFAULT GETDATE(),
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

-- Index for querying metadata by table
CREATE INDEX IX_bronze_metadata_table_status 
ON bronze.metadata(table_name, status, load_start_time DESC);

CREATE INDEX IX_bronze_metadata_batch 
ON bronze.metadata(batch_id);



---------------------------------------------------------------------    NOTES    -------------------------------------------------------------------------------

-- But use NONCLUSTERED PKs to avoid the overhead of maintaining a clustered index. ????????
-----------------------------------------------------
-- Bronze should be:
-- - Fast to load (no index maintenance overhead)
-- - Append-only or full refresh
-- - Minimal transformations
-- - Just a staging area
---------------------------------------------------
-- Reasons to skip indexes in bronze:

-- ✅ Faster bulk inserts - No index maintenance during loads
-- ✅ Simpler design - Raw data, minimal optimization
-- ✅ Lower storage - Indexes take up space
-- ✅ Bronze is temporary - Data moves to silver quickly

------------------------------------------------------

-- And YES - add indexes to metadata tables because:

-- They're queried frequently for monitoring
-- They're small (won't slow down loads)
-- They demonstrate understanding of when indexes are appropriate

----------------------------------------------------------------

-- IDENTITY(1,1)

-- **What it does:** Auto-incrementing number generator, used for surrogate keys, audit/log tables

-- - **First 1**: Start counting from 1
-- - **Second 1**: Increment by 1 each time

-- **Example:**
-- ```
-- Row 1: metadata_id = 1 (automatic)
-- Row 2: metadata_id = 2 (automatic)
-- Row 3: metadata_id = 3 (automatic)

-------------------------------------------------------------------
-- NONCLUSTERED
-- What it does: Tells SQL Server how to physically store/organize data:
    
-- Data is NOT sorted by this column (in CLUSTERED it is sorted byt this column)
-- Creates a separate lookup index (like a book's index)
-- Can have many per table
-- Faster for bulk inserts (no sorting needed) (CLUSTERED is slower for inserts (must maintain sort order)
-- Slightly slower for queries (extra lookup step)


