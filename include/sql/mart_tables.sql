CREATE OR REPLACE TABLE mart_daily_store_kpis AS
SELECT 
    CAST(o.order_ts AS DATE) as order_date,
    s.zone, s.store_id, s.store_name,
    SUM(CASE WHEN o.order_status = 'DELIVERED' THEN o.quantity * o.unit_price * (1 - o.discount_pct / 100.0) ELSE 0 END) as net_revenue,
    COUNT(DISTINCT o.order_id) as total_orders
FROM stg_orders o
JOIN stg_stores s ON o.store_id = s.store_id
WHERE o.dq_status = 'PASSED'
GROUP BY 1, 2, 3, 4;

CREATE OR REPLACE TABLE mart_category_margin AS
SELECT 
    p.category,
    SUM(CASE WHEN o.order_status = 'DELIVERED' THEN o.quantity * o.unit_price * (1 - o.discount_pct / 100.0) ELSE 0 END) as net_revenue,
    SUM(CASE WHEN o.order_status = 'DELIVERED' THEN (o.quantity * o.unit_price * (1 - o.discount_pct / 100.0)) - (o.quantity * p.cost_price) ELSE 0 END) as gross_margin
FROM stg_orders o
JOIN stg_products p ON o.product_id = p.product_id
WHERE o.dq_status = 'PASSED'
GROUP BY 1;

CREATE OR REPLACE TABLE mart_delivery_sla AS
SELECT 
    s.store_name,
    AVG(o.delivery_minutes) as avg_delivery_minutes,
    ROUND(100.0 * COUNT(CASE WHEN o.delivery_minutes <= 15 THEN 1 END) / COUNT(*), 2) as pct_within_15_min
FROM stg_orders o
JOIN stg_stores s ON o.store_id = s.store_id
WHERE o.dq_status = 'PASSED' AND o.order_status = 'DELIVERED'
GROUP BY 1;

CREATE OR REPLACE TABLE mart_top_customers AS
SELECT 
    c.customer_id, c.full_name, c.email, c.phone, c.loyalty_tier,
    SUM(o.quantity * o.unit_price * (1 - o.discount_pct / 100.0)) as total_spend
FROM stg_orders o
JOIN stg_customers c ON o.customer_id = c.customer_id
WHERE o.dq_status = 'PASSED' AND o.order_status = 'DELIVERED'
GROUP BY 1, 2, 3, 4, 5
ORDER BY total_spend DESC
LIMIT 10;
