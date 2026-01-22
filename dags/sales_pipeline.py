from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import random

CONN_ID = "warehouse_postgres"

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def load_fake_sales():
    """
    Simulated source load:
    - Inserts 10 new rows every run into raw.sales.
    - This mimics a real upstream source that keeps producing new records.
    """
    hook = PostgresHook(postgres_conn_id=CONN_ID)
    conn = hook.get_conn()
    cur = conn.cursor()

    # Ensure schemas exist
    cur.execute("CREATE SCHEMA IF NOT EXISTS raw;")
    cur.execute("CREATE SCHEMA IF NOT EXISTS staging;")
    cur.execute("CREATE SCHEMA IF NOT EXISTS mart;")

    # Raw table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS raw.sales (
            id SERIAL PRIMARY KEY,
            sale_date DATE NOT NULL,
            product TEXT NOT NULL,
            quantity INT NOT NULL,
            price NUMERIC NOT NULL
        );
    """)

    products = ["Widget", "Gadget", "Doohickey", "Thingamajig"]
    today = datetime.today().date()

    # Insert 10 new rows every run (simulated upstream feed)
    for _ in range(10):
        cur.execute(
            """
            INSERT INTO raw.sales (sale_date, product, quantity, price)
            VALUES (%s, %s, %s, %s);
            """,
            (
                today,
                random.choice(products),
                random.randint(1, 5),
                round(random.uniform(10.0, 50.0), 2),
            ),
        )

    conn.commit()
    cur.close()
    conn.close()


def transform_sales_clean():
    """
    Watermark incremental transform:
    - Tracks the last processed raw.sales id in raw.etl_state
    - Only transforms rows with id > last_processed_id
    - Updates watermark after success
    """
    hook = PostgresHook(postgres_conn_id=CONN_ID)
    conn = hook.get_conn()
    cur = conn.cursor()

    # Ensure schemas exist
    cur.execute("CREATE SCHEMA IF NOT EXISTS raw;")
    cur.execute("CREATE SCHEMA IF NOT EXISTS staging;")

    # State table (watermark storage)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS raw.etl_state (
            pipeline_name TEXT PRIMARY KEY,
            last_processed_id INT NOT NULL DEFAULT 0,
            updated_at TIMESTAMP NOT NULL DEFAULT NOW()
        );
    """)

    pipeline_name = "sales_clean"

    # Ensure state row exists
    cur.execute("""
        INSERT INTO raw.etl_state (pipeline_name, last_processed_id)
        VALUES (%s, 0)
        ON CONFLICT (pipeline_name) DO NOTHING;
    """, (pipeline_name,))

    # Read watermark
    cur.execute("SELECT last_processed_id FROM raw.etl_state WHERE pipeline_name = %s;", (pipeline_name,))
    last_processed_id = cur.fetchone()[0]

    # Staging table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS staging.sales_clean (
            id INT PRIMARY KEY,
            sale_date DATE NOT NULL,
            product TEXT NOT NULL,
            quantity INT NOT NULL,
            price NUMERIC NOT NULL,
            total_amount NUMERIC NOT NULL,
            loaded_at TIMESTAMP NOT NULL DEFAULT NOW()
        );
    """)

    # Transform only new rows since watermark
    cur.execute("""
        INSERT INTO staging.sales_clean (id, sale_date, product, quantity, price, total_amount)
        SELECT
            s.id,
            s.sale_date,
            INITCAP(TRIM(s.product)) AS product,
            s.quantity,
            s.price,
            (s.quantity * s.price) AS total_amount
        FROM raw.sales s
        WHERE s.id > %s
        ON CONFLICT (id) DO UPDATE SET
            sale_date = EXCLUDED.sale_date,
            product = EXCLUDED.product,
            quantity = EXCLUDED.quantity,
            price = EXCLUDED.price,
            total_amount = EXCLUDED.total_amount,
            loaded_at = NOW();
    """, (last_processed_id,))

    # Update watermark to current max id in raw.sales
    cur.execute("SELECT COALESCE(MAX(id), 0) FROM raw.sales;")
    new_max_id = cur.fetchone()[0]

    cur.execute("""
        UPDATE raw.etl_state
        SET last_processed_id = %s,
            updated_at = NOW()
        WHERE pipeline_name = %s;
    """, (new_max_id, pipeline_name))

    conn.commit()
    cur.close()
    conn.close()


def build_daily_sales_summary():
    hook = PostgresHook(postgres_conn_id=CONN_ID)
    conn = hook.get_conn()
    cur = conn.cursor()

    cur.execute("CREATE SCHEMA IF NOT EXISTS mart;")

    # Mart table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS mart.daily_sales_summary (
            sale_date DATE PRIMARY KEY,
            total_orders INT NOT NULL,
            total_quantity INT NOT NULL,
            total_revenue NUMERIC NOT NULL,
            avg_order_value NUMERIC NOT NULL,
            loaded_at TIMESTAMP NOT NULL DEFAULT NOW()
        );
    """)

    # Rebuild/upsert mart by date
    cur.execute("""
        INSERT INTO mart.daily_sales_summary (
            sale_date, total_orders, total_quantity, total_revenue, avg_order_value
        )
        SELECT
            sale_date,
            COUNT(*) AS total_orders,
            SUM(quantity) AS total_quantity,
            SUM(total_amount) AS total_revenue,
            CASE
                WHEN COUNT(*) = 0 THEN 0
                ELSE SUM(total_amount) / COUNT(*)
            END AS avg_order_value
        FROM staging.sales_clean
        GROUP BY sale_date
        ON CONFLICT (sale_date) DO UPDATE SET
            total_orders = EXCLUDED.total_orders,
            total_quantity = EXCLUDED.total_quantity,
            total_revenue = EXCLUDED.total_revenue,
            avg_order_value = EXCLUDED.avg_order_value,
            loaded_at = NOW();
    """)

    conn.commit()
    cur.close()
    conn.close()


def data_quality_checks():
    hook = PostgresHook(postgres_conn_id=CONN_ID)
    conn = hook.get_conn()
    cur = conn.cursor()

    today = datetime.today().date()

    # Row counts
    cur.execute("SELECT COUNT(*) FROM raw.sales;")
    raw_count = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM staging.sales_clean;")
    staging_count = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM mart.daily_sales_summary WHERE sale_date = %s;", (today,))
    mart_today_count = cur.fetchone()[0]

    # Validity checks
    cur.execute("SELECT COUNT(*) FROM raw.sales WHERE quantity <= 0 OR price <= 0;")
    bad_raw = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM staging.sales_clean WHERE quantity <= 0 OR price <= 0 OR total_amount <= 0;")
    bad_staging = cur.fetchone()[0]

    failures = []
    if raw_count <= 0:
        failures.append(f"raw.sales has 0 rows (raw_count={raw_count})")
    if staging_count <= 0:
        failures.append(f"staging.sales_clean has 0 rows (staging_count={staging_count})")
    if mart_today_count <= 0:
        failures.append(f"mart.daily_sales_summary missing row for today ({today})")
    if bad_raw > 0:
        failures.append(f"raw.sales has {bad_raw} bad rows (quantity<=0 or price<=0)")
    if bad_staging > 0:
        failures.append(f"staging.sales_clean has {bad_staging} bad rows (quantity<=0 or price<=0 or total_amount<=0)")

    cur.close()
    conn.close()

    if failures:
        raise ValueError("DATA QUALITY CHECKS FAILED: " + " | ".join(failures))


with DAG(
    dag_id="sales_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["tutorial", "warehouse"],
    default_args=default_args,
) as dag:

    load_raw_sales = PythonOperator(
        task_id="load_raw_sales",
        python_callable=load_fake_sales,
    )

    transform_sales_clean_task = PythonOperator(
        task_id="transform_sales_clean",
        python_callable=transform_sales_clean,
    )

    build_mart_daily_summary = PythonOperator(
        task_id="build_daily_sales_summary",
        python_callable=build_daily_sales_summary,
    )

    quality_checks = PythonOperator(
        task_id="data_quality_checks",
        python_callable=data_quality_checks,
    )

    load_raw_sales >> transform_sales_clean_task >> build_mart_daily_summary >> quality_checks
