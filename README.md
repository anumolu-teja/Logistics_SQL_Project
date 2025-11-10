# Retail Delivery Analytics — Logistics & Supply Chain (MySQL)

End-to-end SQL project:
- **Extract**: live simulated orders & deliveries → raw tables
- **Transform**: staging cleanup → metrics
- **Load/Analytics**: star-schema fact table + KPI views

## Tech
MySQL 8, Python (PyMySQL), Jupyter

## Database Objects
- **Schemas/Tables**: `raw_orders`, `raw_deliveries`, `stg_orders`, `stg_deliveries`, `dim_hub`, `dim_courier`, `dim_date`, `fact_delivery`
- **Views**: `vw_delivery_metrics`, `vw_kpi_daily`, `vw_kpi_daily_by_hub`, `vw_kpi_by_courier`

## How to Reproduce

### 1) Create DB & tables
Run `scripts/01_setup_extract.sql` in MySQL Workbench.

### 2) (Optional) Stream sample data
Open `notebooks/etl_streamer.ipynb`, set your MySQL credentials in the first cell (never commit secrets), and run the cells to insert sample data.

### 3) Build staging & analytics
Run `scripts/02_transform_load.sql`.

### 4) Export for Tableau (optional)
Query any view (e.g., `SELECT * FROM vw_kpi_daily;`) and export CSV.

## Security Notes
- **Do not commit secrets** (passwords, connection strings). Use a local `.env` file (ignored by git).
- SQL files contain no secrets; Python cells should read passwords from environment variables.
