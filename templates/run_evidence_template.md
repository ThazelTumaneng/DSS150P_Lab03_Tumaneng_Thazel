# Run Evidence

## Week 4
- Python version: 3.10.12
- Git status/log evidence: 4 goals (different branches for each goal)
- Docker image/container evidence: postgres:15 container running locally (airflow-postgres)
- External configuration evidence: environment variables configured via .env and Airflow connections initialized

## Week 5
- Raw row counts: 50,005
- Staging row counts: 49,898 rows
- Curated row counts: 49,898 rows
- Quarantine row counts: 2 rows
- First load affected rows: none (see evidences folder)
- Second rerun affected rows / evidence of idempotency: none (see evidences folder)

## Week 6
- Benchmark table attached: yes (docs/benchmark_results.csv)
- Partition selected: order_year = 2025 and order_year = 2026
- Partition row count:
    | order_year | order_month | # of rows |
| :--- | :--- | ---: |
| 2025 | 1 | 2,458 |
| 2025 | 2 | 2,283 |
| 2025 | 3 | 2,456 |
| 2025 | 4 | 2,410 |
| 2025 | 5 | 2,540 |
| 2025 | 6 | 2,467 |
| 2025 | 7 | 2,459 |
| 2025 | 8 | 2,510 |
| 2025 | 9 | 2,427 |
| 2025 | 10 | 2,451 |
| 2025 | 11 | 2,366 |
| 2025 | 12 | 2,492 |
| 2026 | 1 | 2,506 |
| 2026 | 2 | 2,286 |
| 2026 | 3 | 2,474 |
| 2026 | 4 | 2,379 |
| 2026 | 5 | 2,500 |
| 2026 | 6 | 2,448 |
| 2026 | 7 | 2,495 |
| 2026 | 8 | 2,540 |
| 2026 | 9 | 951 |
- PostgreSQL verification query: docker exec -it dss150p-postgres psql -U dss150p -d dss150p -c "\dn"

## Week 7
- DAG ID: dss150p_sales_pipeline
- Schedule:
    - First Run Start	2026-09-23, 04:51:38 UTC
    - Last Run Start	2026-09-23, 06:02:36 UTC
    - Max Run Duration	00:30:55
    - Mean Run Duration	00:08:04
    - Min Run Duration	00:00:49
- Parameters used: run_mode, year, month
- Successful run ID: manual__2026-09-23T04:55:16+00:00
- Deliberate failure run ID: manual__2026-09-23T06:02:27+00:00
- Retry/failure-handling evidence: 3 deliberate failure task by renaming order source
- Final recovery run ID: manual__2026-09-23T06:02:27+00:00
