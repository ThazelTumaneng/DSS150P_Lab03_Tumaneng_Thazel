import time
import statistics
from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine
from src.config import DB

def run_benchmark(curated_path, output_dir, repeats: int = 5) -> dict:
    """Compare the same logical dataset in CSV, JSON Lines, Parquet, and PostgreSQL.

    Capture:
    - storage/file size where applicable
    - write time
    - full-read time
    - filtered-read/query time
    - row count

    Use multiple repetitions and report a median for read/query timing.
    """
    out_path = Path(output_dir)
    out_path.mkdir(parents=True, exist_ok=True)
    
    # Load source curated dataset
    df = pd.read_parquet(curated_path) if str(curated_path).endswith('.parquet') else pd.read_csv(curated_path)
    results = {}
    
    csv_path = out_path / "dataset.csv"
    jsonl_path = out_path / "dataset.jsonl"
    parquet_path = out_path / "dataset.parquet"
    
    # ----------------------------------------------------
    # 1. CSV Benchmark
    # ----------------------------------------------------
    start = time.perf_counter()
    df.to_csv(csv_path, index=False)
    csv_write_time = time.perf_counter() - start
    csv_size = csv_path.stat().st_size
    
    csv_full_reads = []
    for _ in range(repeats):
        start = time.perf_counter()
        _ = pd.read_csv(csv_path)
        csv_full_reads.append(time.perf_counter() - start)
        
    csv_filt_reads = []
    for _ in range(repeats):
        start = time.perf_counter()
        temp_df = pd.read_csv(csv_path)
        _ = temp_df[temp_df['status'] == 'DELIVERED']
        csv_filt_reads.append(time.perf_counter() - start)
        
    results['csv'] = {
        'size_bytes': csv_size,
        'write_time_sec': csv_write_time,
        'full_read_median_sec': statistics.median(csv_full_reads),
        'filtered_read_median_sec': statistics.median(csv_filt_reads),
        'row_count': len(df)
    }

    # ----------------------------------------------------
    # 2. JSON Lines Benchmark
    # ----------------------------------------------------
    start = time.perf_counter()
    df.to_json(jsonl_path, orient='records', lines=True, date_format='iso')
    jsonl_write_time = time.perf_counter() - start
    jsonl_size = jsonl_path.stat().st_size
    
    jsonl_full_reads = []
    for _ in range(repeats):
        start = time.perf_counter()
        _ = pd.read_json(jsonl_path, orient='records', lines=True)
        jsonl_full_reads.append(time.perf_counter() - start)
        
    jsonl_filt_reads = []
    for _ in range(repeats):
        start = time.perf_counter()
        temp_df = pd.read_json(jsonl_path, orient='records', lines=True)
        _ = temp_df[temp_df['status'] == 'DELIVERED']
        jsonl_filt_reads.append(time.perf_counter() - start)
        
    results['json'] = {
        'size_bytes': jsonl_size,
        'write_time_sec': jsonl_write_time,
        'full_read_median_sec': statistics.median(jsonl_full_reads),
        'filtered_read_median_sec': statistics.median(jsonl_filt_reads),
        'row_count': len(df)
    }

    # ----------------------------------------------------
    # 3. Parquet Benchmark
    # ----------------------------------------------------
    start = time.perf_counter()
    df.to_parquet(parquet_path, index=False, compression='snappy')
    parquet_write_time = time.perf_counter() - start
    parquet_size = parquet_path.stat().st_size
    
    parquet_full_reads = []
    for _ in range(repeats):
        start = time.perf_counter()
        _ = pd.read_parquet(parquet_path)
        parquet_full_reads.append(time.perf_counter() - start)
        
    parquet_filt_reads = []
    for _ in range(repeats):
        start = time.perf_counter()
        temp_df = pd.read_parquet(parquet_path)
        _ = temp_df[temp_df['status'] == 'DELIVERED']
        parquet_filt_reads.append(time.perf_counter() - start)
        
    results['parquet'] = {
        'size_bytes': parquet_size,
        'write_time_sec': parquet_write_time,
        'full_read_median_sec': statistics.median(parquet_full_reads),
        'filtered_read_median_sec': statistics.median(parquet_filt_reads),
        'row_count': len(df)
    }

    # ----------------------------------------------------
    # 4. PostgreSQL Benchmark
    # ----------------------------------------------------
    db_url = f"postgresql+psycopg2://{DB['user']}:{DB['password']}@{DB['host']}:{DB.get('port', 5432)}/{DB['dbname']}"
    engine = create_engine(db_url)
    
    pg_full_reads = []
    for _ in range(repeats):
        start = time.perf_counter()
        _ = pd.read_sql("SELECT * FROM curated.sales_order_lines", con=engine)
        pg_full_reads.append(time.perf_counter() - start)
        
    pg_filt_reads = []
    for _ in range(repeats):
        start = time.perf_counter()
        _ = pd.read_sql("SELECT * FROM curated.sales_order_lines WHERE status = 'DELIVERED'", con=engine)
        pg_filt_reads.append(time.perf_counter() - start)
        
    # Get server table size footprint via PostgreSQL function
    with engine.connect() as conn:
        res = conn.exec_driver_sql("SELECT pg_total_relation_size('curated.sales_order_lines');").scalar()
        pg_size = res if res else 0

    results['postgresql'] = {
        'size_bytes': pg_size,
        'write_time_sec': "N/A (already loaded)",
        'full_read_median_sec': statistics.median(pg_full_reads),
        'filtered_read_median_sec': statistics.median(pg_filt_reads),
        'row_count': len(df)
    }

    return results


def write_partitioned_parquet(df, output_dir):
    """Write Parquet partitioned by order_year/order_month."""
    out_path = Path(output_dir)
    out_path.mkdir(parents=True, exist_ok=True)
    
    # Derive year and month columns if they aren't already present
    if 'order_timestamp' in df.columns:
        ts = pd.to_datetime(df['order_timestamp'])
        df['order_year'] = ts.dt.year
        df['order_month'] = ts.dt.month
        
    df.to_parquet(
        out_path,
        index=False,
        partition_cols=['order_year', 'order_month'],
        compression='snappy'
    )

