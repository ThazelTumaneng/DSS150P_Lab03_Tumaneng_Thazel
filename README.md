# DSS150P Laboratory 3 

## Python Version
Python 3.12.10

## Package Versions
- pandas==2.2.3
- pyarrow==17.0.0
- psycopg[binary]==3.2.3
- python-dotenv==1.0.1
- PyYAML==6.0.2

## Why the virtual environment should not be committed to git
Virtual environments contain compiled executables and hardcoded absolute system paths that are tied strictly to the machine that created them. If this folder is committed to version control, any other user pulling the repository onto a different operating system will encounter immediate crashes because the underlying architectyre and file paths do not match.

Git is designed to track incremental changes in human-readable source code, not to store massive directories of static, third-party library files. A .venv folder easily consumes hundreds of megabytes of space, which needlessly bloats the repository history, limits storage efficiency, and heavily slows down cloning operations.

## Main progression
- Goal 1: reproducible environment, modularization, Git, Docker, configuration
- Goal 2: raw -> staging -> curated transformations; audit/error handling; rerun-safe loading
- Goal 3: CSV/JSON/Parquet/PostgreSQL comparison; partitioning; selected-partition load
- Goal 4: Apache Airflow DAG for extract -> transform -> load -> validate

Start with `DSS150P_Laboratory_Activity_3.pdf`.

## Recommended commands
```bash
cp .env.example .env
python -m venv .venv
# activate .venv then:
pip install -r requirements.txt
python -m src.cli validate-env
```
The provided `.env.example` uses `POSTGRES_HOST=localhost` for host-side commands. Docker Compose overrides the application containers to use the service hostname `postgres`.

Docker/PostgreSQL:
```bash
docker compose up -d postgres
docker compose run --rm pipeline python -m src.cli validate-env
```

Airflow in Goal 4:
```bash
docker compose -f docker-compose.yml -f docker-compose.airflow.yml up airflow-init
docker compose -f docker-compose.yml -f docker-compose.airflow.yml up -d airflow-webserver airflow-scheduler
```
Airflow UI: http://localhost:8080 (training credentials: admin/admin; change if reused outside the lab).
