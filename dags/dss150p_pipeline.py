from datetime import datetime, timedelta
from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator

PROJECT = '/opt/airflow/project'

def failure_callback(context):
    """Failure callback that prints detailed task, run, and error context."""
    ti = context.get('task_instance')
    print(f"ALERT: Task Failed!")
    print(f"DAG: {context.get('dag').dag_id}")
    print(f"Task ID: {ti.task_id if ti else 'N/A'}")
    print(f"Run ID: {context.get('run_id')}")
    print(f"Execution Date: {context.get('execution_date')}")
    print(f"Exception: {context.get('exception')}")

DEFAULT_ARGS = {
    'owner': 'dss150p',
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    'execution_timeout': timedelta(minutes=30),
    'on_failure_callback': failure_callback,
}

with DAG(
    dag_id='dss150p_sales_pipeline',
    start_date=datetime(2026, 1, 1),
    # Schedule: Daily at 2:00 AM ('0 2 * * *')[cite: 5, 10]. 
    # This cadence is appropriate because it runs during off-peak hours after daily transactional data has settled.
    schedule='0 2 * * *',
    # Catch-up choice: False[cite: 5, 10]. 
    # Set to False to avoid triggering dozens of historical backfill runs 
    # all at once upon deployment, focusing only on current/future scheduled runs.
    catchup=False,
    default_args=DEFAULT_ARGS,
    params={
        'run_mode': Param('full', enum=['full', 'partition']),
        'year': Param(2026, type='integer'),
        'month': Param(1, type='integer', minimum=1, maximum=12),
    },
    tags=['DSS150P'],
) as dag:
    extract = BashOperator(
        task_id='extract',
        bash_command=f'cd {PROJECT} && PIPELINE_RUN_ID="{{{{ run_id }}}}" python -m src.cli extract',
    )
    transform = BashOperator(
        task_id='transform',
        bash_command=f'cd {PROJECT} && PIPELINE_RUN_ID="{{{{ run_id }}}}" python -m src.cli transform',
    )
    load = BashOperator(
        task_id='load',
        bash_command=(
            f'cd {PROJECT} && PIPELINE_RUN_ID="{{{{ run_id }}}}" '
            '{% if params.run_mode == "partition" %}'
            'python -m src.cli load-partition --year {{ params.year }} --month {{ params.month }}'
            '{% else %}'
            'python -m src.cli load'
            '{% endif %}'
        ),
    )
    validate = BashOperator(
        task_id='validate',
        bash_command=f'cd {PROJECT} && PIPELINE_RUN_ID="{{{{ run_id }}}}" python -m src.cli validate',
    )

    # Dependencies: extract -> transform -> load -> validate[cite: 5, 10]
    extract >> transform >> load >> validate