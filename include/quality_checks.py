import pandas as pd
import numpy as np

def validate_and_clean_orders(df_orders, df_cust, df_prod, df_store):
    df = df_orders.copy()
    df['order_status_clean'] = df['order_status'].astype(str).str.strip().str.upper()
    df['payment_mode_clean'] = df['payment_mode'].astype(str).str.strip().str.upper()
    
    failed_rows = []
    valid_statuses = ['DELIVERED', 'CANCELLED', 'RETURNED']
    
    cust_ids = set(df_cust['customer_id'].dropna())
    prod_ids = set(df_prod['product_id'].dropna())
    store_ids = set(df_store['store_id'].dropna())
    
    for idx, row in df.iterrows():
        reasons = []
        if pd.isnull(row['customer_id']):
            reasons.append("Completeness: customer_id is null")
        if row['order_status_clean'] == 'DELIVERED' and pd.isnull(row['delivery_minutes']):
            reasons.append("Completeness: delivery_minutes missing for DELIVERED order")
        if row['order_status_clean'] not in valid_statuses:
            reasons.append(f"Validity: invalid status '{row['order_status']}'")
        if row['quantity'] <= 0:
            reasons.append(f"Accuracy: non-positive quantity {row['quantity']}")
            
        expected_amt = round(row['quantity'] * row['unit_price'] * (1 - row['discount_pct'] / 100.0), 2)
        if abs(row['order_amount'] - expected_amt) > 0.01:
            reasons.append(f"Accuracy: order_amount mismatch (got {row['order_amount']}, expected {expected_amt})")
            
        if pd.notnull(row['customer_id']) and row['customer_id'] not in cust_ids:
            reasons.append(f"Consistency: customer_id {row['customer_id']} foreign key missing")
        if row['product_id'] not in prod_ids:
            reasons.append(f"Consistency: product_id {row['product_id']} foreign key missing")
        if row['store_id'] not in store_ids:
            reasons.append(f"Consistency: store_id {row['store_id']} foreign key missing")
            
        if reasons:
            r = row.to_dict()
            r['failure_reason'] = "; ".join(reasons)
            failed_rows.append(r)
            
    failed_df = pd.DataFrame(failed_rows)
    failed_ids = set(failed_df['order_id']) if not failed_df.empty else set()
    passed_df = df[~df['order_id'].isin(failed_ids)].copy()
    passed_df = passed_df.drop_duplicates(subset=['order_id'], keep='last')
    
    return passed_df, failed_df
