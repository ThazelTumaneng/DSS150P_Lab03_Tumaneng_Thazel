import pandas as pd
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import inspect
from src.config import DB
from sqlalchemy import create_engine
from pathlib import Path
from sqlalchemy import text

db_url = f"postgresql+psycopg2://{DB['user']}:{DB['password']}@{DB['host']}:{DB.get('port', 5432)}/{DB['dbname']}"
db_engine = create_engine(db_url)

def upsert_curated(df, run_id: str) -> int:
    """Load curated.sales_order_lines using rerun-safe UPSERT semantics.

    Requirement: order_id is the conflict key. A rerun with unchanged records
    must not create duplicate business keys.
    """
    if df.empty:
        return 0

    table_name = 'sales_order_lines'
    schema_name = 'curated'

    # 1. If table doesn't exist, create it using Pandas standard to_sql first
    inspector = inspect(db_engine)
    if not inspector.has_table(table_name, schema=schema_name):
        df.head(0).to_sql(
            name=table_name,
            schema=schema_name,
            con=db_engine,
            if_exists='fail',
            index=False
        )
        # Add primary key constraint to order_id so UPSERT works
        with db_engine.begin() as conn:
            conn.exec_driver_sql(
                f"ALTER TABLE {schema_name}.{table_name} ADD CONSTRAINT sales_order_lines_pkey PRIMARY KEY (order_id);"
            )

    def upsert_method(table, conn, keys, data_iter):
        data = [dict(zip(keys, row)) for row in data_iter]
        stmt = insert(table.table).values(data)
        
        update_cols = {c.name: c for c in stmt.excluded if c.name != 'order_id'}
        
        upsert_stmt = stmt.on_conflict_do_update(
            index_elements=['order_id'],
            set_=update_cols,
            where=(table.table.c.record_hash != stmt.excluded.record_hash)
        )
        
        conn.execute(upsert_stmt)

    # 2. Write data using chunks
    df.to_sql(
        name=table_name,
        schema=schema_name,
        con=db_engine,
        if_exists='append',
        index=False,
        chunksize=100,
        method=upsert_method
    )
    
    return len(df)


def load_partition(df_or_path, year: int, month: int, run_id: str) -> int:
    """Load only a selected year/month partition and record audit.partition_loads."""
    if isinstance(df_or_path, (str, Path)):
        partition_path = Path(df_or_path) / f"order_year={year}" / f"order_month={month}"
        if not partition_path.exists():
            print(f"Partition for {year}-{month:02d} not found at {partition_path}")
            return 0
        df = pd.read_parquet(partition_path)
    else:
        df = df_or_path

    if df.empty:
        print(f"Partition for {year}-{month:02d} is empty.")
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

    # Call upsert_curated directly from this same file/module
    loaded_count = upsert_curated(df, run_id)

    with db_engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO audit.partition_loads (pipeline_run_id, order_year, order_month, rows_loaded)
                VALUES (:run_id, :year, :month, :rows)
            """),
            {"run_id": run_id, "year": year, "month": month, "rows": loaded_count}
        )

    print(f"Successfully loaded partition {year}-{month:02d}: {loaded_count} rows processed and audited.")
    return loaded_count