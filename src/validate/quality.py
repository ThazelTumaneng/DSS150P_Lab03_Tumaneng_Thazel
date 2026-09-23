import pandas as pd

def validate_curated(df: pd.DataFrame) -> list[str]:
    """Return a list of human-readable validation errors.

    Minimum checks: order_id uniqueness/non-null, quantity range,
    nonnegative amounts, allowed statuses, required audit fields.
    """
    errors = []
    
    if df.empty:
        return ["DataFrame is empty."]

    # 1. order_id uniqueness and non-null
    if 'order_id' in df.columns:
        if df['order_id'].isnull().any():
            errors.append("Validation failed: 'order_id' contains null values.")
        if df['order_id'].duplicated().any():
            errors.append("Validation failed: 'order_id' contains duplicate values.")
    else:
        errors.append("Validation missing column: 'order_id'.")

    # 2. quantity range check (e.g., between 1 and reasonable max)
    if 'quantity' in df.columns:
        if not df['quantity'].between(1, 10000).all():
            errors.append("Validation failed: 'quantity' values out of expected range [1, 10000].")
    else:
        errors.append("Validation missing column: 'quantity'.")

    # 3. nonnegative amounts check
    if 'total_amount' in df.columns:
        if (df['total_amount'] < 0).any():
            errors.append("Validation failed: 'total_amount' contains negative values.")

    # 4. allowed statuses check (updated with dataset values)
    allowed_statuses = {'PENDING', 'PROCESSING', 'DELIVERED', 'CANCELLED', 'UNKNOWN', 'PAID', 'PACKED', 'SHIPPED'}
    if 'status' in df.columns:
        invalid_statuses = set(df['status'].unique()) - allowed_statuses
        if invalid_statuses:
            errors.append(f"Validation failed: Unexpected status values found: {invalid_statuses}")

    # 5. required audit fields check
    required_audit_fields = {'pipeline_run_id', 'order_year', 'order_month'}
    for field in required_audit_fields:
        if field not in df.columns or df[field].isnull().any():
            errors.append(f"Validation failed: Required audit field '{field}' is missing or contains nulls.")

    return errors