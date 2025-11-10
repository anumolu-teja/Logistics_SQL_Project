
 
USE logistics_ops;
SELECT order_id, event_type, event_ts_utc, ingest_ts_ist
FROM raw_orders
ORDER BY raw_order_id DESC
LIMIT 20;

USE logistics_ops;
SELECT order_id, delivery_id, status, base_cost, fuel_surcharge, other_cost
FROM raw_deliveries
ORDER BY raw_delivery_id DESC
LIMIT 30;

USE logistics_ops;

-- Staging table for orders
CREATE TABLE IF NOT EXISTS stg_orders AS
SELECT
  order_id,
  MAX(event_ts_utc) AS latest_event_ts,
  ANY_VALUE(customer_id) AS customer_id,
  ANY_VALUE(store_id) AS store_id,
  ANY_VALUE(order_value) AS order_value,
  ANY_VALUE(payment_method) AS payment_method,
  ANY_VALUE(promised_eta_min) AS promised_eta_min
FROM raw_orders
WHERE is_valid = 1
GROUP BY order_id;

-- Staging table for deliveries
CREATE TABLE IF NOT EXISTS stg_deliveries AS
SELECT
  delivery_id,
  order_id,
  courier_id,
  hub_id,
  status,
  promised_drop_utc,
  actual_drop_utc,
  distance_km,
  base_cost,
  fuel_surcharge,
  other_cost,
  currency
FROM raw_deliveries
WHERE is_valid = 1;

SHOW TABLES LIKE 'stg_%';
SELECT COUNT(*) FROM stg_orders;
SELECT COUNT(*) FROM stg_deliveries; 

