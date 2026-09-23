import pandas as pd
import logging
from src.validate.quality import validate_curated

def build_curated(staging: dict, run_id: str):
    """Join staging orders/customers/products and create analysis-ready sales rows.

    Required columns include gross_amount, discount_amount, net_amount,
    processed_at_utc, pipeline_run_id, and record_hash.

    Orphan customer/product references must be quarantined, not silently dropped.
    """
    orders = staging['orders']
    customers = staging['customers']
    products = staging['products']

    # 1. Identify and Quarantine Orphans
    valid_customers = orders['customer_id'].isin(customers['customer_id'])
    valid_products = orders['product_id'].isin(products['product_id'])
    
    is_orphan = ~(valid_customers & valid_products)
    orphans = orders[is_orphan].copy()
    
    if not orphans.empty:
        orphans['quarantine_reason'] = 'Orphan reference: missing customer_id or product_id'

    # Keep only valid orders for the curated layer
    valid_orders = orders[~is_orphan].copy()

    # 2. Join Datasets
    curated = valid_orders.merge(
        customers, on='customer_id', how='inner', suffixes=('', '_customer')
    ).merge(
        products, on='product_id', how='inner', suffixes=('', '_product')
    )

    # 3. Calculate Financial Metrics
    curated['gross_amount'] = curated['quantity'] * curated['unit_price']
    
    if 'discount' in curated.columns:
        curated['discount_amount'] = curated['gross_amount'] * curated['discount']
    else:
        curated['discount_amount'] = 0.0
        
    curated['net_amount'] = curated['gross_amount'] - curated['discount_amount']

    # 4. Add Audit Columns
    curated['processed_at_utc'] = pd.Timestamp.utcnow()
    curated['pipeline_run_id'] = run_id
    
    hash_columns = ['order_id', 'customer_id', 'product_id', 'updated_at']
    available_hash_cols = [c for c in hash_columns if c in curated.columns]
    
    curated['record_hash'] = pd.util.hash_pandas_object(
        curated[available_hash_cols], index=False
    ).astype(str)

    # Ensure order_year and order_month exist for audit and partitioning requirements
    date_col = 'order_date' if 'order_date' in curated.columns else 'created_at'
    if date_col in curated.columns:
        curated[date_col] = pd.to_datetime(curated[date_col])
        curated['order_year'] = curated[date_col].dt.year
        curated['order_month'] = curated[date_col].dt.month

    # 5. Run Validation Checks (Goal 2 Quality Assurance)
    validation_errors = validate_curated(curated)
    if validation_errors:
        logging.warning("Data quality validation issues detected in curated dataset:")
        for error in validation_errors:
            logging.warning(f" - {error}")
    else:
        logging.info("Curated dataset passed all data quality validations successfully.")

    return curated, orphans