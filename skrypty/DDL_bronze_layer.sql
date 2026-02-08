
CREATE TABLE bronze.customers (
    customer_id INT NOT NULL,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    email VARCHAR(255) NOT NULL,
    age INT NULL,
    gender CHAR(1) NULL,
    join_date DATE NOT NULL,
);


CREATE TABLE bronze.addresses (
    address_id INT NOT NULL,
    customer_id INT NOT NULL,
    country VARCHAR(100) NOT NULL,
    city VARCHAR(100) NOT NULL,
    street VARCHAR(255) NOT NULL,
    postal_code VARCHAR(20) NOT NULL
);

CREATE TABLE bronze.brands (
    brand_id INT,
    name VARCHAR(255)
);

CREATE TABLE bronze.categories (
    category_id INT,
    name VARCHAR(255),
    parent_category_id INT
);

CREATE TABLE bronze.products (
    product_id INT,
    name VARCHAR(255),
    category_id INT,
    brand_id INT,
    list_price DECIMAL(18,2),
    created_at DATETIME2,
    updated_at DATETIME2
);

CREATE TABLE bronze.payment_methods (
    payment_method_id INT,
    payment_method_name VARCHAR(100),
    is_active BIT
);

CREATE TABLE bronze.orders (
    order_id INT,
    customer_id INT,
    order_date DATETIME2,
    payment_method_id INT,
    session_id INT,
    order_status VARCHAR(50),
    subtotal_amount DECIMAL(18,2),
    discount_amount DECIMAL(18,2),
    tax_amount DECIMAL(18,2),
    shipping_amount DECIMAL(18,2),
    total_amount DECIMAL(18,2),
    created_at DATETIME2,
    updated_at DATETIME2,
    load_date DATETIME2,
    batch_id VARCHAR(50)
);

CREATE TABLE bronze.order_items (
    order_item_id INT,
    order_id INT,
    product_id INT,
    quantity INT,
    unit_price DECIMAL(18,2),
    discount_amount DECIMAL(18,2),
    tax_amount DECIMAL(18,2),
    created_at DATETIME2,
    updated_at DATETIME2,
    load_date DATETIME2,
    batch_id VARCHAR(50)
);

CREATE TABLE bronze.order_item_returns (
    return_id INT,
    order_item_id INT,
    return_date DATETIME2,
    quantity_returned INT,
    refund_amount DECIMAL(18,2),
    reason VARCHAR(255),
    created_at DATETIME2,
    updated_at DATETIME2,
    load_date DATETIME2,
    batch_id VARCHAR(50)
);

CREATE TABLE bronze.website_sessions (
    website_session_id INT,
    created_at DATETIME2,
    user_id INT,
    is_repeat_session BIT,
    utm_source VARCHAR(100),
    utm_campaign VARCHAR(100),
    utm_content VARCHAR(100),
    device_type VARCHAR(50),
    http_referer VARCHAR(255),
    load_date DATETIME2,
    batch_id VARCHAR(50)
);

CREATE TABLE bronze.pageviews (
    website_pageview_id INT,
    created_at DATETIME2,
    website_session_id INT,
    pageview_url VARCHAR(255),
    load_date DATETIME2,
    batch_id VARCHAR(50)
);

# Metadata table tracking ETL loads for all tables

CREATE TABLE bronze.metadata (
    table_name VARCHAR(100),
    last_load_date DATETIME2,
    batch_id VARCHAR(50),
    row_count INT,
    status VARCHAR(50),
    comments VARCHAR(255)
);

