import argparse
import logging
import sys
import pandas as pd
from src.config import PROJECT_ROOT, DB, SETTINGS
from src.common.audit import new_run_id
from src.extract.files import extract_sources
from src.transform.staging import build_staging
from src.transform.curated import build_curated
from src.load.postgres import upsert_curated

# Configure logging for meaningful error context
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def main():
    parser = argparse.ArgumentParser(description='DSS150P modular pipeline')
    sub = parser.add_subparsers(dest='command', required=True)
    sub.add_parser('validate-env')
    sub.add_parser('extract')
    sub.add_parser('transform')
    sub.add_parser('load')
    sub.add_parser('validate')
    b = sub.add_parser('benchmark'); b.add_argument('--repeats', type=int, default=5)
    p = sub.add_parser('load-partition'); p.add_argument('--year', type=int, required=True); p.add_argument('--month', type=int, required=True)
    sub.add_parser('run-all')
    args = parser.parse_args()

    if args.command == 'validate-env':
        print('PROJECT_ROOT=', PROJECT_ROOT)
        print('DB host/database=', DB['host'], DB['dbname'])
        print('Configured source=', SETTINGS['pipeline']['source_dir'])
        return
    # Wire the modular functions together for standard execution commands
    if args.command in ['run-all', 'load', 'extract', 'transform']:
        run_id = new_run_id()
        logging.info(f"Initializing pipeline run: {run_id}")
        
        # --- EXTRACT ---
        try:
            raw_path = extract_sources(run_id)
            logging.info(f"EXTRACT stage complete. Data at {raw_path}")
        except Exception as e:
            logging.error(f"Pipeline system failure in EXTRACT stage: {str(e)}", exc_info=True)
            sys.exit(1)
            
        if args.command == 'extract':
            return
            
        # --- TRANSFORM ---
        try:
            stg_dfs, quarantine_df = build_staging(raw_path, run_id)
            curated_df, orphans_df = build_curated(stg_dfs, run_id)
            logging.info(f"TRANSFORM stage complete. Curated rows: {len(curated_df)}")
        except Exception as e:
            logging.error(f"Pipeline system failure in TRANSFORM stage: {str(e)}", exc_info=True)
            sys.exit(1)
            
        if args.command == 'transform':
            return
            
        # --- LOAD ---
        try:
            loaded_rows = upsert_curated(curated_df, run_id)
            logging.info(f"LOAD stage complete. Upserted {loaded_rows} records into PostgreSQL.")
        except Exception as e:
            logging.error(f"Pipeline system failure in LOAD stage: {str(e)}", exc_info=True)
            sys.exit(1)

    elif args.command == 'benchmark':
        run_id = new_run_id()
        logging.info(f"Initializing benchmark run: {run_id}")
        
        raw_path = extract_sources(run_id)
        stg_dfs, _ = build_staging(raw_path, run_id)
        curated_df, _ = build_curated(stg_dfs, run_id)
        
        benchmark_dir = PROJECT_ROOT / "data" / "benchmarks" / run_id
        benchmark_dir.mkdir(parents=True, exist_ok=True)
        curated_parquet_path = benchmark_dir / "curated.parquet"
        curated_df.to_parquet(curated_parquet_path, index=False)
        
        partition_output_dir = PROJECT_ROOT / "data" / "partitioned"
        from src.benchmark.storage import run_benchmark, write_partitioned_parquet
        write_partitioned_parquet(curated_df, partition_output_dir)
        
        metrics = run_benchmark(curated_parquet_path, benchmark_dir, repeats=args.repeats)
        
        docs_dir = PROJECT_ROOT / "docs"
        docs_dir.mkdir(parents=True, exist_ok=True)
        results_csv = docs_dir / "benchmark_results.csv"
        
        rows_to_save = []
        for storage_type, data in metrics.items():
            rows_to_save.append({
                'storage_type': storage_type,
                'file_size_bytes': data['size_bytes'],
                'write_seconds': data['write_time_sec'],
                'full_read_seconds': data['full_read_median_sec'],
                'filtered_read_seconds': data['filtered_read_median_sec'],
                'row_count': data['row_count'],
                'notes': 'Measured via benchmark script'
            })
        pd.DataFrame(rows_to_save).to_csv(results_csv, index=False)
        logging.info(f"Benchmark complete. Results saved to {results_csv}")

    elif args.command == 'load-partition':
        run_id = new_run_id()
        logging.info(f"Initializing partition load for Year: {args.year}, Month: {args.month} (Run: {run_id})")
        
        partition_output_dir = PROJECT_ROOT / "data" / "partitioned"
        
        if not partition_output_dir.exists():
            raw_path = extract_sources(run_id)
            stg_dfs, _ = build_staging(raw_path, run_id)
            curated_df, _ = build_curated(stg_dfs, run_id)
            from src.benchmark.storage import write_partitioned_parquet
            write_partitioned_parquet(curated_df, partition_output_dir)
            
        from src.load.postgres import load_partition
        load_partition(partition_output_dir, args.year, args.month, run_id)

    else:
        print(f"Command '{args.command}' is not yet fully implemented for Goal 3/4.")

if __name__ == '__main__':
    main()
