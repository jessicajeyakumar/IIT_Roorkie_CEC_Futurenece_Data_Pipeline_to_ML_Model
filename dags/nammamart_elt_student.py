from airflow.decorators import dag, task
import pendulum
import pandas as pd
import duckdb
import os

DB_PATH = '/usr/local/airflow/include/warehouse_student.duckdb'
DATA_DIR = '/usr/local/airflow/include/data/nammamart'
SQL_DIR = '/usr/local/airflow/include/sql'

@dag(
    dag_id='nammamart_elt_student',
    schedule='0 9 * * 1-6',
    start_date=pendulum.datetime(2026, 9, 1, tz="Asia/Kolkata"),
    catchup=False,
    default_args={
        'owner': 'student',
        'retries': 2,
        'retry_delay': pendulum.duration(minutes=5),
        'execution_timeout': pendulum.duration(minutes=15)
    },
    tags=['elt', 'nammamart']
)
def nammamart_elt_pipeline():

    @task
    def load_raw_tables(**context):
        run_id = context['run_id']
        conn = duckdb.connect(DB_PATH)
        with open(f"{SQL_DIR}/raw_tables.sql", "r") as f:
            conn.execute(f.read())
            
        files = {
            'raw_orders': 'orders.csv', 'raw_payments': 'payments.csv',
            'raw_customers': 'customers.csv', 'raw_products': 'products.csv',
            'raw_stores': 'stores.csv'
        }
        for tbl, fname in files.items():
            df = pd.read_csv(f"{DATA_DIR}/{fname}", dtype=str)
            df['_loaded_at'] = pd.Timestamp.now()
            df['_source_file'] = fname
            df['_run_id'] = run_id
            conn.execute(f"INSERT INTO {tbl} SELECT * FROM df")
        conn.close()

    @task
    def transform_staging(**context):
        conn = duckdb.connect(DB_PATH)
        with open(f"{SQL_DIR}/staging_tables.sql", "r") as f:
            conn.execute(f.read())
        conn.close()

    @task
    def process_incremental_orders(**context):
        inc_path = f"{DATA_DIR}/orders_2026-10-01_increment.csv"
        if not os.path.exists(inc_path):
            return "No increment file found"
            
        run_id = context['run_id']
        conn = duckdb.connect(DB_PATH)
        df_inc = pd.read_csv(inc_path, dtype=str)
        df_inc['_loaded_at'] = pd.Timestamp.now()
        df_inc['_source_file'] = 'orders_2026-10-01_increment.csv'
        df_inc['_run_id'] = run_id
        
        conn.execute("INSERT INTO raw_orders SELECT * FROM df_inc")
        conn.execute("""
            MERGE INTO stg_orders AS target
            USING (
                SELECT 
                    order_id, TRY_CAST(order_ts AS TIMESTAMP) as order_ts, customer_id, store_id, product_id,
                    TRY_CAST(quantity AS INT) as quantity, TRY_CAST(unit_price AS DOUBLE) as unit_price,
                    TRY_CAST(discount_pct AS DOUBLE) as discount_pct, TRY_CAST(order_amount AS DOUBLE) as order_amount,
                    UPPER(TRIM(payment_mode)) as payment_mode, UPPER(TRIM(order_status)) as order_status,
                    channel, TRY_CAST(delivery_minutes AS INT) as delivery_minutes, 'PASSED' as dq_status,
                    _loaded_at, _run_id
                FROM df_inc
            ) AS source
            ON target.order_id = source.order_id
            WHEN MATCHED THEN UPDATE SET order_status = source.order_status, _loaded_at = source._loaded_at
            WHEN NOT MATCHED THEN INSERT VALUES (source.order_id, source.order_ts, source.customer_id, source.store_id, source.product_id, source.quantity, source.unit_price, source.discount_pct, source.order_amount, source.payment_mode, source.order_status, source.channel, source.delivery_minutes, source.dq_status, source._loaded_at, source._run_id);
        """)
        conn.close()

    @task
    def build_marts():
        conn = duckdb.connect(DB_PATH)
        with open(f"{SQL_DIR}/mart_tables.sql", "r") as f:
            conn.execute(f.read())
        conn.close()

    load_raw_tables() >> transform_staging() >> process_incremental_orders() >> build_marts()

nammamart_elt_pipeline()
