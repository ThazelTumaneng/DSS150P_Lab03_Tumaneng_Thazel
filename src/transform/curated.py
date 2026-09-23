import pandas as pd

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
    # An order is an orphan if its customer_id or product_id doesn't exist in the dimension tables
    valid_customers = orders['customer_id'].isin(customers['customer_id'])
    valid_products = orders['product_id'].isin(products['product_id'])
    
    is_orphan = ~(valid_customers & valid_products)
    orphans = orders[is_orphan].copy()
    
    if not orphans.empty:
        orphans['quarantine_reason'] = 'Orphan reference: missing customer_id or product_id'

    # Keep only valid orders for the curated layer
    valid_orders = orders[~is_orphan].copy()

    # 2. Join Datasets
    # Use inner joins since orphans are already filtered out
    curated = valid_orders.merge(
        customers, on='customer_id', how='inner', suffixes=('', '_customer')
    ).merge(
        products, on='product_id', how='inner', suffixes=('', '_product')
    )

    # 3. Calculate Financial Metrics
    curated['gross_amount'] = curated['quantity'] * curated['unit_price']
    
    # Check if a discount column exists; if not, default to 0
    if 'discount' in curated.columns:
        curated['discount_amount'] = curated['gross_amount'] * curated['discount']
    else:
        curated['discount_amount'] = 0.0
        
    curated['net_amount'] = curated['gross_amount'] - curated['discount_amount']

    # 4. Add Audit Columns
    curated['processed_at_utc'] = pd.Timestamp.utcnow()
    curated['pipeline_run_id'] = run_id
    
    # Generate a record hash using Pandas hashing utility for data integrity tracking
    hash_columns = ['order_id', 'customer_id', 'product_id', 'updated_at']
    available_hash_cols = [c for c in hash_columns if c in curated.columns]
    
    curated['record_hash'] = pd.util.hash_pandas_object(
        curated[available_hash_cols], index=False
    ).astype(str)

    return curated, orphans
