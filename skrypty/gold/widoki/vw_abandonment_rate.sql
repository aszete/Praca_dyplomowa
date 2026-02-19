-- Widok przedstawiający wskaźnik porzucenia dla poszczególnych stron skepu internetowego

CREATE OR ALTER VIEW vw_abandonment_rate AS
SELECT 
    wa.page_name,
    COUNT(DISTINCT wa.session_key) AS sessions_visited,
    COUNT(DISTINCT CASE WHEN fs.resulted_in_sale = 1 THEN wa.session_key END) AS sessions_converted,
    COUNT(DISTINCT CASE WHEN fs.resulted_in_sale = 0 THEN wa.session_key END) AS sessions_abandoned,
    ROUND(CAST(COUNT(DISTINCT CASE WHEN fs.resulted_in_sale = 0 THEN wa.session_key END) AS FLOAT) / 
        NULLIF(COUNT(DISTINCT wa.session_key), 0) * 100, 2) AS abandonment_rate
FROM gold.fact_web_analytics wa
JOIN gold.fact_sessions fs ON wa.session_key = fs.session_key
GROUP BY wa.page_name;
