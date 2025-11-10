USE logistics_ops;

-- Create or replace a transformed view for delivery KPIs
CREATE OR REPLACE VIEW vw_delivery_metrics AS
SELECT
    d.delivery_id,
    d.order_id,
    d.courier_id,
    d.hub_id,
    d.status,
    d.promised_drop_utc,
    d.actual_drop_utc,
    TIMESTAMPDIFF(MINUTE, d.promised_drop_utc, d.actual_drop_utc) AS delay_minutes,

    CASE
        WHEN d.actual_drop_utc <= d.promised_drop_utc THEN 1
        ELSE 0
    END AS is_on_time,

    ROUND(
        COALESCE(d.base_cost,0) + COALESCE(d.fuel_surcharge,0) + COALESCE(d.other_cost,0),
        2
    ) AS total_cost,

    d.currency
FROM stg_deliveries d; 

SELECT * FROM vw_delivery_metrics LIMIT 10; 

-- Overal Delivery KPIs
SELECT
    COUNT(*) AS total_deliveries,
    SUM(is_on_time) AS on_time_deliveries,
    ROUND(100 * SUM(is_on_time) / COUNT(*), 2) AS on_time_rate_pct,
    ROUND(AVG(delay_minutes), 2) AS avg_delay_minutes,
    ROUND(AVG(total_cost), 2) AS avg_delivery_cost
FROM vw_delivery_metrics;  

-- KPIs by Hub 
SELECT
    hub_id,
    COUNT(*) AS total_deliveries,
    ROUND(100 * SUM(is_on_time) / COUNT(*), 2) AS on_time_rate_pct,
    ROUND(AVG(total_cost), 2) AS avg_cost,
    ROUND(AVG(delay_minutes), 2) AS avg_delay
FROM vw_delivery_metrics
GROUP BY hub_id
ORDER BY on_time_rate_pct DESC;   

-- KPIs by Courier 
SELECT
    courier_id,
    COUNT(*) AS total_jobs,
    ROUND(100 * SUM(is_on_time) / COUNT(*), 2) AS on_time_rate_pct,
    ROUND(AVG(total_cost), 2) AS avg_cost,
    ROUND(AVG(delay_minutes), 2) AS avg_delay
FROM vw_delivery_metrics
GROUP BY courier_id
ORDER BY on_time_rate_pct DESC
LIMIT 10;  

-- Dimension table for hubs (locations / depots)
CREATE TABLE IF NOT EXISTS dim_hub (
  hub_key   INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,  -- surrogate key for joins
  hub_id    VARCHAR(64)  NOT NULL UNIQUE,                      -- natural/business key (e.g., HUB_BLR)
  hub_name  VARCHAR(128) NULL,                                 -- optional: friendly name (can fill later)
  is_active TINYINT(1)   NOT NULL DEFAULT 1,                   -- soft-activate/deactivate hubs
  created_at TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB;  

INSERT INTO dim_hub (hub_id)
SELECT DISTINCT d.hub_id
FROM stg_deliveries d
LEFT JOIN dim_hub h ON h.hub_id = d.hub_id
WHERE d.hub_id IS NOT NULL
  AND h.hub_id IS NULL;  -- only insert new hubs  
  
SELECT * FROM dim_hub ORDER BY hub_key;
SELECT COUNT(*) AS hub_count FROM dim_hub; 

-- One row per courier (surrogate-keyed)
CREATE TABLE IF NOT EXISTS dim_courier (
  courier_key  INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,  -- surrogate key
  courier_id   VARCHAR(64)  NOT NULL UNIQUE,                      -- natural/business key (e.g., COURIER_7)
  courier_name VARCHAR(128) NULL,                                 -- optional: add later if you have it
  is_active    TINYINT(1)   NOT NULL DEFAULT 1,                   -- soft delete / deactivate
  created_at   TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB;  

INSERT INTO dim_courier (courier_id)
SELECT DISTINCT d.courier_id
FROM stg_deliveries d
LEFT JOIN dim_courier c ON c.courier_id = d.courier_id
WHERE d.courier_id IS NOT NULL
  AND c.courier_id IS NULL;   -- only insert new couriers
  
SELECT * FROM dim_courier ORDER BY courier_key;
SELECT COUNT(*) AS courier_count FROM dim_courier; 

CREATE TABLE IF NOT EXISTS dim_date (
  date_key      INT         NOT NULL PRIMARY KEY,  -- yyyymmdd (e.g., 20251109)
  full_date     DATE        NOT NULL,
  iso_year      INT         NOT NULL,
  iso_week      INT         NOT NULL,              -- ISO week number (1..53)
  year_num      INT         NOT NULL,
  quarter_num   TINYINT     NOT NULL,              -- 1..4
  month_num     TINYINT     NOT NULL,              -- 1..12
  month_name    VARCHAR(10) NOT NULL,
  day_of_month  TINYINT     NOT NULL,              -- 1..31
  day_of_week   TINYINT     NOT NULL,              -- 1=Mon .. 7=Sun (ISO)
  day_name      VARCHAR(10) NOT NULL,
  is_weekend    TINYINT(1)  NOT NULL
) ENGINE=InnoDB; 

WITH RECURSIVE
date_bounds AS (
  SELECT
    DATE(MIN(promised_drop_utc)) AS start_dt,
    DATE(MAX(COALESCE(actual_drop_utc, promised_drop_utc))) AS end_dt
  FROM stg_deliveries
),
dates AS (
  SELECT start_dt AS d FROM date_bounds
  UNION ALL
  SELECT DATE_ADD(d, INTERVAL 1 DAY)
  FROM dates
  JOIN date_bounds ON d < end_dt
)

SELECT * FROM dates;
INSERT INTO dim_date (
  date_key, full_date, iso_year, iso_week, year_num, quarter_num,
  month_num, month_name, day_of_month, day_of_week, day_name, is_weekend
)
SELECT
  DATE_FORMAT(d, '%Y%m%d') + 0                 AS date_key,
  d                                            AS full_date,
  YEARWEEK(d, 3) DIV 100                       AS iso_year,
  YEARWEEK(d, 3) % 100                         AS iso_week,
  YEAR(d)                                      AS year_num,
  QUARTER(d)                                   AS quarter_num,
  MONTH(d)                                     AS month_num,
  DATE_FORMAT(d, '%b')                         AS month_name,
  DAY(d)                                       AS day_of_month,
  WEEKDAY(d) + 1                               AS day_of_week,
  DATE_FORMAT(d, '%a')                         AS day_name,
  CASE WHEN WEEKDAY(d) IN (5,6) THEN 1 ELSE 0 END AS is_weekend
FROM (
  WITH RECURSIVE
  date_bounds AS (
    SELECT
      DATE(MIN(promised_drop_utc)) AS start_dt,
      DATE(MAX(COALESCE(actual_drop_utc, promised_drop_utc))) AS end_dt
    FROM stg_deliveries
  ),
  dates AS (
    SELECT start_dt AS d FROM date_bounds
    UNION ALL
    SELECT DATE_ADD(d, INTERVAL 1 DAY)
    FROM dates
    JOIN date_bounds ON d < end_dt
  )
  SELECT d FROM dates
) AS src
ON DUPLICATE KEY UPDATE
  full_date = VALUES(full_date);
  
SELECT COUNT(*) AS date_rows FROM dim_date;
SELECT * FROM dim_date ORDER BY full_date LIMIT 5;
SELECT * FROM dim_date ORDER BY full_date DESC LIMIT 5; 


CREATE TABLE IF NOT EXISTS fact_delivery (
  fact_id           BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  delivery_id       VARCHAR(64) NOT NULL,
  order_id          VARCHAR(64) NOT NULL,

  hub_key           INT UNSIGNED,
  courier_key       INT UNSIGNED,
  date_key          INT,

  status            VARCHAR(32),
  delay_minutes     INT,
  is_on_time        TINYINT(1),
  total_cost        DECIMAL(12,2),
  currency          CHAR(3),

  -- housekeeping
  created_at        TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

  -- indexes for joins
  KEY idx_hub (hub_key),
  KEY idx_courier (courier_key),
  KEY idx_date (date_key)
) ENGINE=InnoDB; 

INSERT INTO fact_delivery (
  delivery_id, order_id,
  hub_key, courier_key, date_key,
  status, delay_minutes, is_on_time, total_cost, currency
)
SELECT
  d.delivery_id,
  d.order_id,
  h.hub_key,
  c.courier_key,
  DATE_FORMAT(DATE(d.promised_drop_utc), '%Y%m%d') + 0 AS date_key,
  d.status,
  TIMESTAMPDIFF(MINUTE, d.promised_drop_utc, d.actual_drop_utc) AS delay_minutes,
  CASE WHEN d.actual_drop_utc <= d.promised_drop_utc THEN 1 ELSE 0 END AS is_on_time,
  ROUND(COALESCE(d.base_cost,0) + COALESCE(d.fuel_surcharge,0) + COALESCE(d.other_cost,0), 2),
  d.currency
FROM stg_deliveries d
LEFT JOIN dim_hub h ON h.hub_id = d.hub_id
LEFT JOIN dim_courier c ON c.courier_id = d.courier_id; 

SELECT COUNT(*) AS fact_rows FROM fact_delivery;

SELECT
  f.delivery_id,
  h.hub_id,
  c.courier_id,
  f.delay_minutes,
  f.is_on_time,
  f.total_cost
FROM fact_delivery f
JOIN dim_hub h     ON f.hub_key = h.hub_key
JOIN dim_courier c ON f.courier_key = c.courier_key
LIMIT 10; 

SELECT
  (SELECT COUNT(*) FROM stg_deliveries)  AS staging_rows,
  (SELECT COUNT(*) FROM fact_delivery)   AS fact_rows; 
  
CREATE OR REPLACE VIEW vw_kpi_daily AS
SELECT
  d.full_date,
  COUNT(*)                               AS total_deliveries,
  SUM(f.is_on_time)                      AS on_time_deliveries,
  ROUND(100 * SUM(f.is_on_time) / COUNT(*), 2) AS on_time_rate_pct,
  ROUND(AVG(f.delay_minutes), 2)         AS avg_delay_minutes,
  ROUND(AVG(f.total_cost), 2)            AS avg_delivery_cost
FROM fact_delivery f
JOIN dim_date d
  ON f.date_key = d.date_key
GROUP BY d.full_date
ORDER BY d.full_date; 

SELECT * FROM vw_kpi_daily ORDER BY full_date; 

CREATE OR REPLACE VIEW vw_kpi_daily_by_hub AS
SELECT
  d.full_date,
  h.hub_id,
  COUNT(*) AS total_deliveries,
  SUM(f.is_on_time) AS on_time_deliveries,
  ROUND(100 * SUM(f.is_on_time) / COUNT(*), 2) AS on_time_rate_pct,
  ROUND(AVG(f.delay_minutes), 2) AS avg_delay_minutes,
  ROUND(AVG(f.total_cost), 2) AS avg_delivery_cost
FROM fact_delivery f
JOIN dim_hub h ON f.hub_key = h.hub_key
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY d.full_date, h.hub_id
ORDER BY d.full_date, h.hub_id; 

SELECT * FROM vw_kpi_daily_by_hub ORDER BY full_date, hub_id; 

CREATE OR REPLACE VIEW vw_kpi_by_courier AS
SELECT
  c.courier_id,
  COUNT(*) AS total_deliveries,
  SUM(f.is_on_time) AS on_time_deliveries,
  ROUND(100 * SUM(f.is_on_time) / COUNT(*), 2) AS on_time_rate_pct,
  ROUND(AVG(f.delay_minutes), 2) AS avg_delay_minutes,
  ROUND(AVG(f.total_cost), 2) AS avg_delivery_cost
FROM fact_delivery f
JOIN dim_courier c ON f.courier_key = c.courier_key
GROUP BY c.courier_id
ORDER BY on_time_rate_pct DESC; 

SELECT * FROM vw_kpi_by_courier LIMIT 10; 

