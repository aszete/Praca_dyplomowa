-- Widok podsumowania sprzedaży według miesięcy

CREATE OR ALTER VIEW gold.vw_sales_summary AS
SELECT 
    dd.year,
    dd.month,
    dd.month_name,
    COUNT(DISTINCT fs.order_id) AS num_orders,
    COUNT(*) AS num_line_items,
    COUNT(DISTINCT fs.customer_key) AS unique_customers,
    SUM(fs.total_amount) AS total_revenue,
    SUM(fs.discount_amount) AS total_discounts,
    SUM(fs.tax_amount) AS total_tax,
    SUM(fs.quantity) AS total_units_sold,
    AVG(fs.total_amount) AS avg_order_line_value
FROM gold.fact_sales fs
JOIN gold.dim_date dd ON fs.date_key = dd.date_key
JOIN gold.dim_customers dc ON fs.customer_key = dc.customer_key
GROUP BY dd.year, dd.month, dd.month_name;
