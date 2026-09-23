import argparse
import logging
import sys
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
            
    else:
        print(f"Command '{args.command}' is not yet fully implemented for Goal 3/4.")

if __name__ == '__main__':
    main()
