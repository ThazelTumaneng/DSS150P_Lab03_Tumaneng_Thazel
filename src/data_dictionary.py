import pandas as pd
from pathlib import Path

def generate_dictionary():
    data = [
        # Raw Layer
        {"layer": "raw", "column_name": "order_id", "data_type": "string", "nullable": "False", "description": "Unique identifier for the order", "source_field": "order_id", "transformation_rule": "Direct ingestion from source CSV"},
        {"layer": "raw", "column_name": "customer_id", "data_type": "string", "nullable": "False", "description": "Unique identifier for the customer", "source_field": "customer_id", "transformation_rule": "Direct ingestion from source CSV"},
        {"layer": "raw", "column_name": "order_date", "data_type": "string", "nullable": "False", "description": "Timestamp or date when the order was placed", "source_field": "order_date", "transformation_rule": "Direct ingestion from source CSV"},
        {"layer": "raw", "column_name": "product_id", "data_type": "string", "nullable": "False", "description": "Unique identifier for the product", "source_field": "product_id", "transformation_rule": "Direct ingestion from source CSV"},
        {"layer": "raw", "column_name": "quantity", "data_type": "integer", "nullable": "False", "description": "Quantity of items ordered", "source_field": "quantity", "transformation_rule": "Direct ingestion from source CSV"},
        {"layer": "raw", "column_name": "unit_price", "data_type": "float", "nullable": "False", "description": "Price per unit of the product", "source_field": "unit_price", "transformation_rule": "Direct ingestion from source CSV"},
        
        # Curated Layer
        {"layer": "curated", "column_name": "order_id", "data_type": "string", "nullable": "False", "description": "Unique identifier for the order", "source_field": "order_id", "transformation_rule": "Trim whitespace and ensure string type"},
        {"layer": "curated", "column_name": "customer_id", "data_type": "string", "nullable": "False", "description": "Unique identifier for the customer", "source_field": "customer_id", "transformation_rule": "Trim whitespace and ensure string type"},
        {"layer": "curated", "column_name": "order_timestamp", "data_type": "datetime", "nullable": "False", "description": "Standardized datetime format for the order date", "source_field": "order_date", "transformation_rule": "Parsed to standard datetime format (UTC)"},
        {"layer": "curated", "column_name": "product_id", "data_type": "string", "nullable": "False", "description": "Unique identifier for the product", "source_field": "product_id", "transformation_rule": "Trim whitespace and uppercase"},
        {"layer": "curated", "column_name": "quantity", "data_type": "integer", "nullable": "False", "description": "Validated quantity of items ordered", "source_field": "quantity", "transformation_rule": "Cast to integer; filtered out non-positive values"},
        {"layer": "curated", "column_name": "unit_price", "data_type": "float", "nullable": "False", "description": "Validated unit price of the product", "source_field": "unit_price", "transformation_rule": "Cast to float; filtered out negative values"},
        {"layer": "curated", "column_name": "total_amount", "data_type": "float", "nullable": "False", "description": "Calculated total sale amount for the line item", "source_field": "quantity, unit_price", "transformation_rule": "Calculated as quantity * unit_price"},
        {"layer": "curated", "column_name": "order_year", "data_type": "integer", "nullable": "False", "description": "Year extracted from order timestamp for partitioning", "source_field": "order_timestamp", "transformation_rule": "Extracted year component"},
        {"layer": "curated", "column_name": "order_month", "data_type": "integer", "nullable": "False", "description": "Month extracted from order timestamp for partitioning", "source_field": "order_timestamp", "transformation_rule": "Extracted month component"},

        # Presentation Layer
        {"layer": "presentation", "column_name": "order_id", "data_type": "VARCHAR", "nullable": "False", "description": "Primary key for the sales fact table", "source_field": "order_id", "transformation_rule": "Mapped directly from curated layer; unique constraint enforced"},
        {"layer": "presentation", "column_name": "customer_id", "data_type": "VARCHAR", "nullable": "False", "description": "Customer identifier in data warehouse", "source_field": "customer_id", "transformation_rule": "Mapped directly from curated layer"},
        {"layer": "presentation", "column_name": "order_timestamp", "data_type": "TIMESTAMP", "nullable": "False", "description": "Order placement timestamp", "source_field": "order_timestamp", "transformation_rule": "Mapped directly from curated layer"},
        {"layer": "presentation", "column_name": "product_id", "data_type": "VARCHAR", "nullable": "False", "description": "Product identifier", "source_field": "product_id", "transformation_rule": "Mapped directly from curated layer"},
        {"layer": "presentation", "column_name": "quantity", "data_type": "INTEGER", "nullable": "False", "description": "Order quantity", "source_field": "quantity", "transformation_rule": "Mapped directly from curated layer"},
        {"layer": "presentation", "column_name": "unit_price", "data_type": "NUMERIC", "nullable": "False", "description": "Product unit price", "source_field": "unit_price", "transformation_rule": "Mapped directly from curated layer"},
        {"layer": "presentation", "column_name": "total_amount", "data_type": "NUMERIC", "nullable": "False", "description": "Total calculated amount", "source_field": "total_amount", "transformation_rule": "Mapped directly from curated layer; upsert on conflict"}
    ]

    output_path = Path("docs/data_dictionary.csv")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    df = pd.DataFrame(data)
    df.to_csv(output_path, index=False)
    print(f"Successfully populated {output_path}")

if __name__ == "__main__":
    generate_dictionary()