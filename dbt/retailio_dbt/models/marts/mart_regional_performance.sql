-- Purpose: mart_regional_performance shows sales performance broken down by region and category over time
-- Solves: Data silos by replacing manual branch reports with unified regional view
-- Source: int_enriched_sales
-- Grain: One row per month, region and category combination

SELECT
    DATE_TRUNC('month', order_date) as month,
    STRFTIME(order_date, '%B %Y') as month_name,
    region,
    category,
    COUNT(DISTINCT order_id) as total_order,
    SUM(sales_amount) as total_revenue,
    sum(profit) as total_profit,
    ROUND((SUM(profit)/SUM(sales_amount))*100,2) as profit_margin_perct
FROM
    {{ ref('int_enriched_sales') }}
GROUP BY 1,2,3,4,
ORDER BY 1,4












