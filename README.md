# DSS150P Laboratory 3 

An end-to-end data engineering pipeline built for DSS150P Lab 03, featuring modular medallion architecture (raw -> staging -> curated -> presentation), parquet partitioning, idempotent PostgreSQL loading via upserts, and Apache Airflow orchestration.

## Python Version
Python 3.12.10

## Core Libraries
- pandas==2.2.3
- pyarrow==17.0.0
- psycopg[binary]==3.2.3
- python-dotenv==1.0.1
- PyYAML==6.0.2

## Orcheatration
- Apache Airflow (Dockerized)

## Storage and Warehouse
- Parquet (Partitioned), PostgreSQL 15 (Docker)

## Why the virtual environment should not be committed to git
Virtual environments contain compiled executables and hardcoded absolute system paths that are tied strictly to the machine that created them. If this folder is committed to version control, any other user pulling the repository onto a different operating system will encounter immediate crashes because the underlying architectyre and file paths do not match.

Git is designed to track incremental changes in human-readable source code, not to store massive directories of static, third-party library files. A .venv folder easily consumes hundreds of megabytes of space, which needlessly bloats the repository history, limits storage efficiency, and heavily slows down cloning operations.

## Project Architecture and Progression
- Goal 1: reproducible environment, project modularization, Git version control, Docker containerization, and centralized configuration management
- Goal 2: raw -> staging -> curated transformations; rigorous audit/error handling; rerun-safe database loading
- Goal 3: File format benchmarks (CSV/JSON/Parquet/PostgreSQL comparison); partitioning strategies (order_year/order_month); selected-partition load
- Goal 4: Full orchestration via Apache Airflow DAG for extract -> transform -> load -> validate


## Setup commands

Initializations:
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

## AI Disclosure
- Setup Guide for Containerization and Airflow (extract, transform, load, validate)
- Code Troubloeshoot and Enhancement

## Optional Challenge: Backfill Reasoning
- When a DAG is configure with daily schedule, Airflow evaluates execution in discrete data intervals. To backfill an entire historical month, Airflow triggers a sequence of daily logical dates. 
- Idempotency guarantees that running the same backfill task multiple times yields the exact same final system state without side effects.
- To prevent duplicate records or inflated sales metrics during concurrent backfills or retries implemet primary key enforcement and upsert strategy.
