CREATE DATABASE IF NOT EXISTS logistics_ops
DEFAULT CHARACTER SET utf8mb4
COLLATE utf8mb4_0900_ai_ci;  

USE logistics_ops;

SHOW DATABASES LIKE 'logistics_ops';
SELECT DATABASE() AS current_db; 

-- Use your project database
USE logistics_ops;

-- Create a small settings table if it doesn't already exist
CREATE TABLE IF NOT EXISTS project_settings (
  setting_key   VARCHAR(64) PRIMARY KEY,   -- acts like a name for the setting
  setting_value VARCHAR(256) NOT NULL      -- actual value stored as text
);

-- Insert our first setting: the timezone we’ll use across ETL & reports.
-- The ON DUPLICATE KEY UPDATE clause means:
-- "If the row already exists (same setting_key), just update its value."
INSERT INTO project_settings (setting_key, setting_value)
VALUES ('timezone', 'Asia/Kolkata')
ON DUPLICATE KEY UPDATE setting_value = VALUES(setting_value);  

SELECT *
FROM project_settings;

-- One row = one "order event" as it arrived (create/update/cancel, etc.)
-- Append-only design: no in-place updates. We keep full history.

CREATE TABLE IF NOT EXISTS raw_orders (
  -- Surrogate key for storage and ingestion order
  raw_order_id      BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,

  -- Where did this event come from? (useful when you have multiple feeds)
  src_system        VARCHAR(64)     NOT NULL,     -- e.g., 'simulator', 'partner_x', 'form'

  -- What kind of event is this?
  event_type        VARCHAR(32)     NOT NULL,     -- 'create','update','cancel', etc.

  -- Timestamps:
  -- event_ts_utc  = the time the source says the event happened (in UTC)
  -- ingest_ts_ist = when *we* ingested it (converted to IST for easy reading)
  event_ts_utc      DATETIME(3)     NOT NULL,
  ingest_ts_ist     DATETIME(3)     NOT NULL,

  -- Business keys (lightweight parsed fields so you can filter/join fast)
  order_id          VARCHAR(64)     NOT NULL,     -- primary business identifier
  customer_id       VARCHAR(64)     NULL,
  store_id          VARCHAR(64)     NULL,

  -- Useful operational attributes (optional if the source didn’t send them)
  order_value       DECIMAL(12,2)   NULL,
  payment_method    VARCHAR(32)     NULL,         -- 'cod','card','upi', etc.
  promised_eta_min  INT             NULL,         -- e.g., "we promise delivery in 35 minutes"

  -- The **raw** event as-is (never modified) for full fidelity/audits
  payload_json      JSON            NOT NULL,

  -- Basic data quality flags you (or an ingestion script) can set
  is_valid          TINYINT(1)      NOT NULL DEFAULT 1,
  validation_notes  VARCHAR(255)    NULL,

  -- Indexes to make common queries fast
  KEY idx_order_id (order_id),
  KEY idx_event_ts_utc (event_ts_utc),
  KEY idx_ingest_ts_ist (ingest_ts_ist),
  KEY idx_src_event (src_system, event_type)
) ENGINE=InnoDB;  

SHOW TABLES LIKE 'raw_orders';

-- See column definitions
DESCRIBE raw_orders;

-- See the indexes
SHOW INDEX FROM raw_orders; 

INSERT INTO raw_orders (
  src_system, event_type, event_ts_utc, ingest_ts_ist,
  order_id, customer_id, store_id, order_value, payment_method, promised_eta_min, payload_json
)
VALUES (
  'simulator', 'create',
  UTC_TIMESTAMP(3),
  CONVERT_TZ(UTC_TIMESTAMP(3), '+00:00', '+05:30'),
  'ORD_1001', 'CUST_42', 'STORE_7', 899.00, 'upi', 35,
  JSON_OBJECT(
    'order_id','ORD_1001',
    'items', JSON_ARRAY(JSON_OBJECT('sku','SKU123','qty',1,'price',899.00)),
    'address','Bengaluru',
    'note','test insert'
  )
);

SELECT raw_order_id, order_id, event_type, event_ts_utc, ingest_ts_ist
FROM raw_orders
ORDER BY raw_order_id DESC
LIMIT 1;

USE logistics_ops;

-- One row per "delivery event"
CREATE TABLE IF NOT EXISTS raw_deliveries (
  raw_delivery_id    BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,

  src_system         VARCHAR(64)     NOT NULL,      -- source system
  event_type         VARCHAR(32)     NOT NULL,      -- 'assign','pickup','drop','cost_posted', etc.

  event_ts_utc       DATETIME(3)     NOT NULL,      -- event time from source
  ingest_ts_ist      DATETIME(3)     NOT NULL,      -- ingestion time converted to IST

  order_id           VARCHAR(64)     NOT NULL,      -- link to raw_orders.order_id
  delivery_id        VARCHAR(64)     NOT NULL,      -- unique per delivery attempt
  courier_id         VARCHAR(64)     NULL,
  hub_id             VARCHAR(64)     NULL,

  status             VARCHAR(32)     NULL,          -- 'out_for_delivery','delivered','failed','returned'
  promised_drop_utc  DATETIME(3)     NULL,          -- planned delivery time
  actual_drop_utc    DATETIME(3)     NULL,          -- actual delivery time
  distance_km        DECIMAL(8,3)    NULL,

  base_cost          DECIMAL(12,2)   NULL,
  fuel_surcharge     DECIMAL(12,2)   NULL,
  other_cost         DECIMAL(12,2)   NULL,
  currency           CHAR(3)         NULL,          -- e.g., 'INR'

  payload_json       JSON            NOT NULL,      -- full raw message
  is_valid           TINYINT(1)      NOT NULL DEFAULT 1,
  validation_notes   VARCHAR(255)    NULL,

  UNIQUE KEY uk_delivery_event (delivery_id, event_ts_utc, event_type),
  KEY idx_order (order_id),
  KEY idx_status (status),
  KEY idx_event_ts (event_ts_utc),
  KEY idx_drop_times (promised_drop_utc, actual_drop_utc)
) ENGINE=InnoDB; 

SHOW TABLES LIKE 'raw_deliveries';
DESCRIBE raw_deliveries; 

INSERT INTO raw_deliveries (
  src_system, event_type, event_ts_utc, ingest_ts_ist,
  order_id, delivery_id, courier_id, hub_id,
  status, promised_drop_utc, actual_drop_utc, distance_km,
  base_cost, fuel_surcharge, other_cost, currency, payload_json
)
VALUES (
  'simulator', 'drop',
  UTC_TIMESTAMP(3),
  CONVERT_TZ(UTC_TIMESTAMP(3), '+00:00', '+05:30'),
  'ORD_1001', 'DEL_1001', 'COURIER_5', 'HUB_BLR',
  'delivered',
  UTC_TIMESTAMP(3), UTC_TIMESTAMP(3), 8.200,
  70.00, 10.00, 5.00, 'INR',
  JSON_OBJECT('delivery_id','DEL_1001','note','test delivery insert')
);

SELECT raw_delivery_id, delivery_id, status, base_cost, event_ts_utc, ingest_ts_ist
FROM raw_deliveries
ORDER BY raw_delivery_id DESC
LIMIT 1;  

-- A union of recent raw rows from both tables
CREATE OR REPLACE VIEW vw_recent_raw AS
SELECT
  'orders' AS table_name,
  raw_order_id AS raw_id,
  order_id,
  event_type,
  event_ts_utc,
  ingest_ts_ist
FROM raw_orders
UNION ALL
SELECT
  'deliveries' AS table_name,
  raw_delivery_id AS raw_id,
  order_id,
  event_type,
  event_ts_utc,
  ingest_ts_ist
FROM raw_deliveries
ORDER BY ingest_ts_ist DESC;  

-- Confirm the view exists
SHOW FULL TABLES WHERE Table_type = 'VIEW';

-- See most recent arrivals
SELECT * FROM vw_recent_raw
ORDER BY ingest_ts_ist DESC
LIMIT 10;  

