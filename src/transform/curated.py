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

    # 1. Identify and Quarantine Orphans and Rule Violations
    valid_customers = orders['customer_id'].isin(customers['customer_id'])
    valid_products = orders['product_id'].isin(products['product_id'])
    is_orphan = ~(valid_customers & valid_products)
    
    # Strict Status Allow-List Enforcement
    ALLOWED_STATUSES = ['PENDING', 'PAID', 'PACKED', 'SHIPPED', 'DELIVERED', 'CANCELLED']
    is_invalid_status = ~orders['status'].isin(ALLOWED_STATUSES)
    
    # Quantity Upper Bound (<= 20)
    is_invalid_qty = orders['quantity'] > 20
    
    # Combine all quarantine conditions
    needs_quarantine = is_orphan | is_invalid_status | is_invalid_qty
    quarantine_df = orders[needs_quarantine].copy()
    
    if not quarantine_df.empty:
        quarantine_df['quarantine_reason'] = ""
        quarantine_df.loc[is_orphan, 'quarantine_reason'] += "Orphan reference. "
        quarantine_df.loc[is_invalid_status, 'quarantine_reason'] += "Invalid status. "
        quarantine_df.loc[is_invalid_qty, 'quarantine_reason'] += "Quantity > 20. "

    # Keep only valid orders for the curated layer
    valid_orders = orders[~needs_quarantine].copy()

    # 2. Join Datasets
    curated = valid_orders.merge(
        customers, on='customer_id', how='inner', suffixes=('', '_customer')
    ).merge(
        products, on='product_id', how='inner', suffixes=('', '_product')
    )

    # 3. Flag missing emails (Visible Quality Condition)
    email_col = 'customer_email' if 'customer_email' in curated.columns else 'email'
    curated['is_missing_email'] = curated[email_col].isnull() | curated[email_col].astype(str).str.strip().eq('')

    # 4. Calculate Financial Metrics
    curated['gross_amount'] = curated['quantity'] * curated['unit_price']
    
    if "discount_pct" in curated.columns:
        curated["discount_amount"] = curated["unit_price"] * curated["quantity"] * curated["discount_pct"]
    else:
        curated["discount_amount"] = 0.0

    curated["total_amount"] = (curated["quantity"] * curated["unit_price"]) - curated["discount_amount"]
    curated['net_amount'] = curated['gross_amount'] - curated['discount_amount']

    # 5. Add Audit Columns
    curated['processed_at_utc'] = pd.Timestamp.utcnow()
    curated['pipeline_run_id'] = run_id
    
    hash_columns = ['order_id', 'customer_id', 'product_id', 'updated_at']
    available_hash_cols = [c for c in hash_columns if c in curated.columns]
    
    curated['record_hash'] = pd.util.hash_pandas_object(
        curated[available_hash_cols], index=False
    ).astype(str)

    curated["order_timestamp"] = pd.to_datetime(curated["order_timestamp"])
    curated["order_year"] = curated["order_timestamp"].dt.year
    curated["order_month"] = curated["order_timestamp"].dt.month

    # 6. Run Validation Checks
    validation_errors = validate_curated(curated)
    if validation_errors:
        logging.warning("Data quality validation issues detected in curated dataset:")
        for error in validation_errors:
            logging.warning(f" - {error}")
    else:
        logging.info("Curated dataset passed all data quality validations successfully.")

    return curated, quarantine_df