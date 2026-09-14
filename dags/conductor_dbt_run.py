"""
Conductor — example dbt DAG for Airflow 3.x.

This DAG runs dbt models from the same immutable GitDagBundle version as
the DAG itself. The Git connection supplies the production ref and safe
repository-relative paths; no mutable IDE workspace is used.
"""

from __future__ import annotations

from datetime import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from conductor_git_bundle import dbt_project_dir

DBT_PROJECT_DIR = dbt_project_dir(__file__)
DBT_PROFILES_DIR = f"{DBT_PROJECT_DIR}/profiles"


with DAG(
    dag_id="conductor_dbt_run",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args={
        "retries": 1,
        "retry_delay": 5,
    },
) as dag:

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt deps --profiles-dir {DBT_PROFILES_DIR} && "
            f"dbt run --profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt test --profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

    dbt_run >> dbt_test
