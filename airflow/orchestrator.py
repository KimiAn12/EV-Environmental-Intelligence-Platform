import sys
from pathlib import Path
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

PROJECT_ROOT = Path(__file__).resolve().parents[1]
API_REQUEST_DIR = PROJECT_ROOT / "api-request"
DBT_PROJECT_DIR = PROJECT_ROOT / "dbt" / "my_project"
DBT_PROFILES_DIR = PROJECT_ROOT / "dbt"

sys.path.append(str(API_REQUEST_DIR))
from insert_records import ingest_ev_data, ingest_weather
from validate_raw_data import run_validation

default_args = {
    'description': 'Orchestrator DAG for Weather API Project',
    'start_date': datetime(2026, 1, 16),
    'catchup': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
}

dag = DAG(
    dag_id = "ev-environmental-intelligence",
    default_args=default_args,
    schedule="@daily",
    tags=["ev", "weather", "dbt", "powerbi"]
)

with dag:
    ingest_weather_task = PythonOperator(
        task_id='ingest_weather_raw',
        python_callable=ingest_weather
    )

    ingest_ev_task = PythonOperator(
        task_id='ingest_ev_raw',
        python_callable=ingest_ev_data
    )

    validate_raw_task = PythonOperator(
        task_id='validate_raw_data',
        python_callable=run_validation
    )

    dbt_run_task = BashOperator(
        task_id='dbt_run',
        bash_command=f'cd "{DBT_PROJECT_DIR}" && dbt run --profiles-dir "{DBT_PROFILES_DIR}"',
    )

    dbt_test_task = BashOperator(
        task_id='dbt_test',
        bash_command=f'cd "{DBT_PROJECT_DIR}" && dbt test --profiles-dir "{DBT_PROFILES_DIR}"',
    )

    [ingest_weather_task, ingest_ev_task] >> validate_raw_task >> dbt_run_task >> dbt_test_task
