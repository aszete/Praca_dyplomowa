-- widok podsumowujący dane, aktywność i wartość klientów
CREATE OR ALTER VIEW vw_customer_summary AS
WITH customer_sessions AS (
    SELECT 
        fs.customer_key,
        fs.session_key,
        fs.start_date_key,
        fs.resulted_in_sale,
        w.utm_campaign,
        w.device_type,
        ROW_NUMBER() OVER (PARTITION BY fs.customer_key ORDER BY fs.start_date_key, fs.start_time_key) AS session_number,
        COUNT(*) OVER (PARTITION BY fs.customer_key) AS total_sessions
    FROM gold.fact_sessions fs
    LEFT JOIN (
        SELECT DISTINCT session_key, utm_campaign, device_type
        FROM gold.fact_web_analytics
    ) w ON fs.session_key = w.session_key
    WHERE fs.customer_key <> -1
)
SELECT 
    cs.customer_key,
    dc.full_name,
    dc.email,
    dc.country,
    dc.age_group,
    
    -- Journey
    cs.total_sessions,
    MAX(CASE WHEN cs.session_number = 1 THEN cs.utm_campaign END) AS first_campaign,
    MAX(CASE WHEN cs.session_number = 1 THEN cs.device_type END) AS first_device,
    MAX(CASE WHEN cs.resulted_in_sale = 1 THEN cs.utm_campaign END) AS converting_campaign,
    
    -- Konwersja
    SUM(CASE WHEN cs.resulted_in_sale = 1 THEN 1 ELSE 0 END) AS sessions_with_purchase,
    MIN(CASE WHEN cs.resulted_in_sale = 1 THEN cs.session_number END) AS first_purchase_session,
    
    -- LTV
    SUM(s.total_amount) AS lifetime_value,
    COUNT(DISTINCT s.order_id) AS total_orders
FROM customer_sessions cs
JOIN gold.dim_customers dc ON cs.customer_key = dc.customer_key
LEFT JOIN gold.fact_sales s ON cs.session_key = s.session_key
GROUP BY 
    cs.customer_key, cs.total_sessions,
    dc.full_name, dc.email, dc.country, dc.age_group;
