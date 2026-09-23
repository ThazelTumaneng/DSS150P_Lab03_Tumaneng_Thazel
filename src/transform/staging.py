import pandas as pd
import json
from pathlib import Path

def build_staging(raw_dir, run_id: str):
    """Create cleaned, typed staging datasets.

    Required rules:
    - Deduplicate by business key, keeping greatest updated_at.
    - Parse timestamps as UTC.
    - Normalize emails/cities and flatten product.category.
    - Validate order quantity/status and product price.
    - Add pipeline_run_id and staged_at_utc audit columns.
    - Write invalid records to data/quarantine/ with a reason.

    Return a dict of staging DataFrames and a quarantine DataFrame.
    """
    raw_path = Path(raw_dir)
    now_utc = pd.Timestamp.utcnow()
    quarantine_frames = []

    # 1. Load Data 
    # (pd.json_normalize automatically flattens product.category)
    customers = pd.read_csv(raw_path / 'customers.csv')
    orders = pd.read_csv(raw_path / 'orders.csv')
    
    with open(raw_path / 'products.json', 'r') as f:
        products_data = json.load(f)
    products = pd.json_normalize(products_data)

    # Helper function for deduplication, timestamps, and audit columns
    def standardize_dataset(df, business_key):
        # Parse timestamps as UTC safely
        for col in df.columns:
            if 'at' in col or 'date' in col:
                try:
                    df[col] = pd.to_datetime(df[col], utc=True, format='mixed')
                except (ValueError, TypeError):
                    pass # Ignore if not a valid date string
        
        # Add audit columns
        df['pipeline_run_id'] = run_id
        df['staged_at_utc'] = now_utc
        
        # Deduplicate by business key, keeping greatest updated_at
        if 'updated_at' in df.columns:
            df = df.sort_values('updated_at').drop_duplicates(subset=[business_key], keep='last')
        else:
            df = df.drop_duplicates(subset=[business_key], keep='last')
            
        return df

    # Apply standardizations
    customers = standardize_dataset(customers, 'customer_id')
    orders = standardize_dataset(orders, 'order_id')
    products = standardize_dataset(products, 'product_id')

    # 2. Normalize Specific Strings
    if 'email' in customers.columns:
        customers['email'] = customers['email'].str.lower().str.strip()
    if 'city' in customers.columns:
        customers['city'] = customers['city'].str.title().str.strip()

    # 3. Validate and Quarantine
    # Validate Orders (quantity > 0)
    is_invalid_order = (orders['quantity'] <= 0) | orders['quantity'].isna()
    invalid_orders = orders[is_invalid_order].copy()
    if not invalid_orders.empty:
        invalid_orders['quarantine_reason'] = 'Invalid quantity'
        quarantine_frames.append(invalid_orders)
    orders = orders[~is_invalid_order]

    # Validate Products (price >= 0)
    is_invalid_product = (products['unit_price'] < 0) | products['unit_price'].isna()
    invalid_products = products[is_invalid_product].copy()
    if not invalid_products.empty:
        invalid_products['quarantine_reason'] = 'Negative or missing price'
        quarantine_frames.append(invalid_products)
    products = products[~is_invalid_product]

    # 4. Write Quarantine Records
    if quarantine_frames:
        quarantine_df = pd.concat(quarantine_frames, ignore_index=True)
        # Traverse up from data/raw/run_id=... to place inside data/quarantine/
        quarantine_dir = raw_path.parents[1] / "quarantine" / f"run_id={run_id}"
        quarantine_dir.mkdir(parents=True, exist_ok=True)
        quarantine_df.to_csv(quarantine_dir / "quarantined_records.csv", index=False)
    else:
        quarantine_df = pd.DataFrame()

    # 5. Compile Staging Dictionary
    staging_dfs = {
        'customers': customers,
        'products': products,
        'orders': orders
    }

    return staging_dfs, quarantine_df
