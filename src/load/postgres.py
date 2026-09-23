import pandas as pd
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import inspect, Table, MetaData, create_engine, text
from src.config import DB
from pathlib import Path

db_url = f"postgresql+psycopg2://{DB['user']}:{DB['password']}@{DB['host']}:{DB.get('port', 5432)}/{DB['dbname']}"
db_engine = create_engine(db_url)

def upsert_curated(df, run_id: str) -> int:
    """Load curated.sales_order_lines using rerun-safe UPSERT semantics."""
    if df.empty:
        return 0

    table_name = 'sales_order_lines'
    schema_name = 'curated'

    inspector = inspect(db_engine)
    if not inspector.has_table(table_name, schema=schema_name):
        with db_engine.begin() as connection:
            df.head(0).to_sql(name=table_name, schema=schema_name, con=connection, if_exists='fail', index=False)
            connection.exec_driver_sql(f"ALTER TABLE {schema_name}.{table_name} ADD CONSTRAINT sales_order_lines_pkey PRIMARY KEY (order_id);")

    metadata = MetaData()
    table_obj = Table(table_name, metadata, autoload_with=db_engine, schema=schema_name)

    # 1. Schema Check & Alter
    with db_engine.begin() as conn:
        existing_cols = {c.name for c in table_obj.columns}
        for col in df.columns:
            if col not in existing_cols:
                col_type = "FLOAT" if "amount" in col or "price" in col else "INTEGER" if "year" in col or "month" in col or "quantity" in col else "TEXT"
                conn.exec_driver_sql(f"ALTER TABLE {schema_name}.{table_name} ADD COLUMN {col} {col_type};")
        metadata.clear()
        table_obj = Table(table_name, metadata, autoload_with=db_engine, schema=schema_name)

    # 2. The Missing Insert Logic
    chunksize = 100
    with db_engine.begin() as conn:
        for i in range(0, len(df), chunksize):
            chunk = df.iloc[i:i+chunksize]
            data = chunk.to_dict(orient='records')
            if not data:
                continue
            
            stmt = insert(table_obj).values(data)
            update_cols = {c.name: c for c in stmt.excluded if c.name != 'order_id'}
            
            if 'record_hash' in table_obj.c and 'record_hash' in df.columns:
                upsert_stmt = stmt.on_conflict_do_update(
                    index_elements=['order_id'],
                    set_=update_cols,
                    where=(table_obj.c.record_hash != stmt.excluded.record_hash)
                )
            else:
                upsert_stmt = stmt.on_conflict_do_update(
                    index_elements=['order_id'],
                    set_=update_cols
                )
            conn.execute(upsert_stmt)
            
    # 3. Explicit Return
    return len(df)

def load_partition(df_or_path, year: int, month: int, run_id: str) -> int:
    """Load only a selected year/month partition and record audit.partition_loads."""
    if isinstance(df_or_path, (str, Path)):
        partition_path = Path(df_or_path) / f"order_year={year}" / f"order_month={month}"
        if not partition_path.exists():
            return 0
        df = pd.read_parquet(partition_path)
    else:
        df = df_or_path

    if df.empty:
        return 0

    with db_engine.begin() as conn:
        conn.exec_driver_sql("CREATE SCHEMA IF NOT EXISTS audit;")
        conn.exec_driver_sql("""
            CREATE TABLE IF NOT EXISTS audit.partition_loads (
                load_id SERIAL PRIMARY KEY,
                pipeline_run_id VARCHAR(100),
                order_year INT,
                order_month INT,
                rows_loaded INT,
                loaded_at_utc TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
        """)

    loaded_count = upsert_curated(df, run_id)

    with db_engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO audit.partition_loads (pipeline_run_id, order_year, order_month, rows_loaded)
                VALUES (:run_id, :year, :month, :rows)
            """),
            {"run_id": run_id, "year": year, "month": month, "rows": loaded_count}
        )
    return loaded_count