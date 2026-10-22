import pandas as pd
import numpy as np
import duckdb
import os
import sys

sys.path.append('include')
from quality_checks import validate_and_clean_orders
from masking import mask_email, mask_phone

DB_PATH = 'include/warehouse_student.duckdb'
DATA_DIR = 'include/data/nammamart'
QUARANTINE_DIR = 'include/quarantine/student'

print("--- Running ETL Pipeline ---")
orders = pd.read_csv(f"{DATA_DIR}/orders.csv")
payments = pd.read_csv(f"{DATA_DIR}/payments.csv")
customers = pd.read_csv(f"{DATA_DIR}/customers.csv")
products = pd.read_csv(f"{DATA_DIR}/products.csv")
stores = pd.read_csv(f"{DATA_DIR}/stores.csv")

passed_orders, failed_orders = validate_and_clean_orders(orders, customers, products, stores)
os.makedirs(QUARANTINE_DIR, exist_ok=True)

if not failed_orders.empty:
    failed_orders.to_csv(f"{QUARANTINE_DIR}/orders_failed_standalone.csv", index=False)
    print(f"Quarantined {len(failed_orders)} failed rows to {QUARANTINE_DIR}/orders_failed_standalone.csv")

# PII Masking
customers['email'] = customers['email'].apply(mask_email)
customers['phone'] = customers['phone'].apply(mask_phone)

# DuckDB Warehouse Loads
conn = duckdb.connect(DB_PATH)
conn.execute("CREATE OR REPLACE TABLE etl_dim_customer AS SELECT * FROM customers;")
conn.execute("CREATE OR REPLACE TABLE etl_dim_product AS SELECT * FROM products;")
conn.execute("CREATE OR REPLACE TABLE etl_dim_store AS SELECT * FROM stores;")
conn.execute("CREATE OR REPLACE TABLE etl_fact_orders AS SELECT * FROM passed_orders;")

print("ETL Pipeline Completed Successfully!")

print("\n--- Running ELT Pipeline ---")
with open("include/sql/raw_tables.sql", "r") as f:
    conn.execute(f.read())

files = {'raw_orders': 'orders.csv', 'raw_payments': 'payments.csv', 'raw_customers': 'customers.csv', 'raw_products': 'products.csv', 'raw_stores': 'stores.csv'}
for tbl, fname in files.items():
    df = pd.read_csv(f"{DATA_DIR}/{fname}", dtype=str)
    df['_loaded_at'] = pd.Timestamp.now()
    df['_source_file'] = fname
    df['_run_id'] = 'standalone_run'
    conn.execute(f"INSERT INTO {tbl} SELECT * FROM df")

with open("include/sql/staging_tables.sql", "r") as f:
    conn.execute(f.read())

with open("include/sql/mart_tables.sql", "r") as f:
    conn.execute(f.read())

conn.close()
print("ELT Pipeline Completed Successfully!")
