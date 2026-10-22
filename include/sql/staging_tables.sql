CREATE TABLE IF NOT EXISTS stg_orders AS
SELECT 
    order_id,
    TRY_CAST(order_ts AS TIMESTAMP) as order_ts,
    customer_id, store_id, product_id,
    TRY_CAST(quantity AS INT) as quantity,
    TRY_CAST(unit_price AS DOUBLE) as unit_price,
    TRY_CAST(discount_pct AS DOUBLE) as discount_pct,
    TRY_CAST(order_amount AS DOUBLE) as order_amount,
    UPPER(TRIM(payment_mode)) as payment_mode,
    UPPER(TRIM(order_status)) as order_status,
    channel,
    TRY_CAST(delivery_minutes AS INT) as delivery_minutes,
    CASE 
        WHEN customer_id IS NULL THEN 'REJECT: Null customer'
        WHEN UPPER(TRIM(order_status)) NOT IN ('DELIVERED', 'CANCELLED', 'RETURNED') THEN 'REJECT: Invalid status'
        WHEN TRY_CAST(quantity AS INT) <= 0 THEN 'REJECT: Non-positive quantity'
        ELSE 'PASSED'
    END as dq_status,
    _loaded_at, _run_id
FROM raw_orders;

CREATE TABLE IF NOT EXISTS stg_customers AS
SELECT 
    customer_id, full_name,
    CASE 
        WHEN email LIKE '%@%' THEN SUBSTR(email, 1, 1) || '***@' || SPLIT_PART(email, '@', 2)
        ELSE 'invalid@masked.com'
    END as email,
    CASE 
        WHEN LENGTH(TRIM(phone)) >= 10 THEN SUBSTR(TRIM(phone), 1, 2) || '******' || SUBSTR(TRIM(phone), -2)
        ELSE '**********'
    END as phone,
    city, area, pincode, signup_date,
    UPPER(SUBSTR(TRIM(loyalty_tier), 1, 1)) || LOWER(SUBSTR(TRIM(loyalty_tier), 2)) as loyalty_tier,
    is_active
FROM raw_customers;

CREATE TABLE IF NOT EXISTS stg_products AS
SELECT 
    product_id, product_name,
    COALESCE(category, 'Uncategorized') as category,
    sub_category, brand,
    TRY_CAST(mrp AS DOUBLE) as mrp,
    TRY_CAST(cost_price AS DOUBLE) as cost_price,
    unit, is_perishable, launch_date
FROM raw_products;

CREATE TABLE IF NOT EXISTS stg_stores AS
SELECT 
    store_id, store_name,
    COALESCE(zone, 'Unknown') as zone,
    area, pincode, open_date, manager_name,
    TRY_CAST(capacity_orders_per_day AS INT) as capacity_orders_per_day
FROM raw_stores;
