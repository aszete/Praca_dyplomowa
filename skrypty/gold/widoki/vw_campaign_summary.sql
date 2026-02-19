-- Widok z podsumowaniem kampanii reklamowych według miesięcy

CREATE OR ALTER VIEW gold.vw_campaign_summary AS
SELECT 
    w.utm_campaign,
    dd.year,
    dd.month,
    COUNT(DISTINCT s.order_id) AS num_orders,
    SUM(s.total_amount) AS revenue,
    COUNT(DISTINCT fs.session_key) AS total_sessions,
    ROUND(CAST(COUNT(DISTINCT s.order_id) AS FLOAT) / 
        NULLIF(COUNT(DISTINCT fs.session_key), 0),2)AS conversion_rate
FROM gold.fact_sessions fs
JOIN gold.dim_date dd ON fs.start_date_key = dd.date_key
JOIN (
    -- Get distinct session + campaign to avoid M:M duplication
    SELECT DISTINCT session_key, utm_campaign
    FROM gold.fact_web_analytics
    WHERE utm_campaign IS NOT NULL
) w ON fs.session_key = w.session_key
LEFT JOIN gold.fact_sales s ON fs.session_key = s.session_key
GROUP BY w.utm_campaign, dd.year, dd.month;
