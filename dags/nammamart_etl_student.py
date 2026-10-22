from airflow.decorators import dag, task
import pendulum
import pandas as pd
import numpy as np
import duckdb
import os
import sys

sys.path.append('/usr/local/airflow/include')
from quality_checks import validate_and_clean_orders
from masking import mask_email, mask_phone

DB_PATH = '/usr/local/airflow/include/warehouse_student.duckdb'
DATA_DIR = '/usr/local/airflow/include/data/nammamart'
QUARANTINE_DIR = '/usr/local/airflow/include/quarantine/student'

@dag(
    dag_id='nammamart_etl_student',
    schedule='0 9 * * 1-6',
    start_date=pendulum.datetime(2026, 9, 1, tz="Asia/Kolkata"),
    catchup=False,
    default_args={
        'owner': 'student',
        'retries': 2,
        'retry_delay': pendulum.duration(minutes=5),
        'execution_timeout': pendulum.duration(minutes=15)
    },
    tags=['etl', 'nammamart']
)
def nammamart_etl_pipeline():

    @task
    def extract_orders():
        return pd.read_csv(f"{DATA_DIR}/orders.csv").to_dict(orient='records')

    @task
    def extract_payments():
        return pd.read_csv(f"{DATA_DIR}/payments.csv").to_dict(orient='records')

    @task
    def extract_customers():
        return pd.read_csv(f"{DATA_DIR}/customers.csv").to_dict(orient='records')

    @task
    def extract_products():
        return pd.read_csv(f"{DATA_DIR}/products.csv").to_dict(orient='records')

    @task
    def extract_stores():
        return pd.read_csv(f"{DATA_DIR}/stores.csv").to_dict(orient='records')

    @task
    def validate_and_quarantine(orders, customers, products, stores, **context):
        passed_df, failed_df = validate_and_clean_orders(
            pd.DataFrame(orders), pd.DataFrame(customers),
            pd.DataFrame(products), pd.DataFrame(stores)
        )
        os.makedirs(QUARANTINE_DIR, exist_ok=True)
        run_id = context['run_id'].replace(':', '_').replace('+', '_')
        if not failed_df.empty:
            failed_df.to_csv(f"{QUARANTINE_DIR}/orders_failed_{run_id}.csv", index=False)
            
        return {
            'passed': passed_df.to_dict(orient='records'),
            'failed_count': len(failed_df)
        }

    @task
    def transform_and_load(validated_data, payments_data, customers_data, products_data, stores_data, **context):
        run_id = context['run_id']
        passed_orders = pd.DataFrame(validated_data['passed'])
        df_pay = pd.DataFrame(payments_data)
        df_cust = pd.DataFrame(customers_data)
        df_prod = pd.DataFrame(products_data)
        df_store = pd.DataFrame(stores_data)
        
        df_cust['email'] = df_cust['email'].apply(mask_email)
        df_cust['phone'] = df_cust['phone'].apply(mask_phone)
        df_cust['loyalty_tier'] = df_cust['loyalty_tier'].astype(str).str.strip().str.title()
        df_cust = df_cust.drop_duplicates(subset=['customer_id'], keep='last')
        
        df_prod['category'] = df_prod['category'].fillna('Uncategorized')
        df_prod = df_prod.drop_duplicates(subset=['product_id'], keep='last')
        
        df_store['zone'] = df_store['zone'].fillna('Unknown')
        df_store = df_store.drop_duplicates(subset=['store_id'], keep='last')
        
        df_ord = passed_orders.copy()
        df_ord['order_status'] = df_ord['order_status'].astype(str).str.strip().str.upper()
        df_ord['payment_mode'] = df_ord['payment_mode'].astype(str).str.strip().str.upper().replace({'UPI ': 'UPI', 'COD': 'CASH ON DELIVERY'})
        df_ord['order_ts'] = pd.to_datetime(df_ord['order_ts'])
        df_ord['order_date'] = df_ord['order_ts'].dt.date
        df_ord['net_revenue'] = np.where(df_ord['order_status'] == 'DELIVERED', (df_ord['quantity'] * df_ord['unit_price'] * (1 - df_ord['discount_pct'] / 100.0)).round(2), 0.0)
        
        df_ord = df_ord.merge(df_prod[['product_id', 'cost_price']], on='product_id', how='left').fillna({'cost_price': 0})
        df_ord['gross_margin'] = np.where(df_ord['order_status'] == 'DELIVERED', (df_ord['net_revenue'] - (df_ord['quantity'] * df_ord['cost_price'])).round(2), 0.0)
        df_ord['within_15_min'] = (df_ord['order_status'] == 'DELIVERED') & (df_ord['delivery_minutes'] <= 15)
        
        conn = duckdb.connect(DB_PATH)
        conn.execute("CREATE OR REPLACE TABLE etl_dim_customer AS SELECT * FROM df_cust;")
        conn.execute("CREATE OR REPLACE TABLE etl_dim_product AS SELECT * FROM df_prod;")
        conn.execute("CREATE OR REPLACE TABLE etl_dim_store AS SELECT * FROM df_store;")
        conn.execute("CREATE OR REPLACE TABLE etl_fact_orders AS SELECT * FROM df_ord;")
        
        reconcile_df = df_ord.merge(df_pay[df_pay['payment_status'] == 'SUCCESS'], on='order_id', how='outer', suffixes=('_order', '_payment'))
        reconcile_df['mismatch_reason'] = np.select(
            [reconcile_df['payment_id'].isnull() & (reconcile_df['order_status'] == 'DELIVERED'), reconcile_df['order_id'].isnull(), abs(reconcile_df['order_amount'] - reconcile_df['amount_paid']) > 0.01],
            ['Delivered order missing payment record', 'Payment record missing matching order', 'Order amount does not match amount paid'],
            default='Reconciled'
        )
        mismatches = reconcile_df[reconcile_df['mismatch_reason'] != 'Reconciled'].copy()
        conn.execute("CREATE OR REPLACE TABLE etl_payment_reconciliation AS SELECT * FROM mismatches;")
        
        conn.execute("""
            CREATE TABLE IF NOT EXISTS pipeline_runs (
                run_id VARCHAR, dag_id VARCHAR, start_time TIMESTAMP, end_time TIMESTAMP,
                rows_extracted INT, rows_loaded INT, rows_quarantined INT, status VARCHAR
            );
        """)
        conn.execute(f"INSERT INTO pipeline_runs VALUES ('{run_id}', 'nammamart_etl_student', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, {len(passed_orders) + validated_data['failed_count']}, {len(df_ord)}, {validated_data['failed_count']}, 'SUCCESS');")
        conn.close()

    orders = extract_orders()
    payments = extract_payments()
    customers = extract_customers()
    products = extract_products()
    stores = extract_stores()
    
    validated = validate_and_quarantine(orders, customers, products, stores)
    transform_and_load(validated, payments, customers, products, stores)

nammamart_etl_pipeline()
