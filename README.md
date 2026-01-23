# airflow-sales-pipeline

An end-to-end ETL data pipeline built with Apache Airflow and PostgreSQL.

This project demonstrates how to orchestrate a multi-layer data warehouse pipeline including:
- Raw ingestion
- Staging transformations
- Analytics-ready mart tables
- Automated data quality checks
- Incremental daily loads

---

## 🏗 Architecture

**Pipeline Flow:**

raw.sales → staging.sales_clean → mart.daily_sales_summary → data_quality_checks

![DAG Graph](screenshots/airflow_dag.png)

---

## 🧩 Technologies Used

- Apache Airflow
- PostgreSQL
- Docker & Docker Compose
- Python
- SQL

---

## 🔄 DAG Overview

This DAG runs daily and performs the following steps:

1. **load_raw_sales**  
   Loads 10 new sales rows per day into the raw layer (idempotent daily load)

2. **transform_sales_clean**  
   Cleans and transforms raw data into staging  
   - Normalizes product names  
   - Adds total_amount column  
   - Ensures re-runnable upserts  

3. **build_daily_sales_summary**  
   Aggregates daily metrics into a mart table  
   - Total orders  
   - Total quantity  
   - Total revenue  
   - Average order value  

4. **data_quality_checks**  
   Validates pipeline health  
   - Row counts  
   - Missing data  
   - Invalid values  

![Airflow Overview](screenshots/airflow_overview.png)

---

## 📊 Data Layers

### Raw Layer

![Raw Table](screenshots/raw_table.png)

---

### Staging Layer

![Staging Table](screenshots/staging_table.png)

---

### Mart Layer

![Mart Table](screenshots/mart_table.png)

---

## ✅ Data Quality Checks

![Quality Checks](screenshots/quality_checks.png)

---

## 🚀 How to Run Locally

```bash
docker compose up -d

Access Airflow UI at:

http://localhost:8080

Default login:

Username: airflow
Password: airflow
