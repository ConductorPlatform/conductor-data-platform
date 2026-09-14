"""
Conductor — example dbt DAG for Airflow 3.x.

This DAG runs dbt models from the same immutable GitDagBundle version as
the DAG itself. The Git connection supplies the production ref and safe
repository-relative paths; no mutable IDE workspace is used.
"""

from __future__ import annotations

from datetime import datetime
from shlex import quote

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from conductor_git_bundle import dbt_project_dir

DBT_PROJECT_DIR = dbt_project_dir(__file__)
DBT_PROFILES_DIR = f"{DBT_PROJECT_DIR}/profiles"
DBT_ARTIFACT_ROOT = "/opt/airflow/conductor-dbt-artifacts"


def _dbt_command() -> str:
    """Delegate bounded capture to the installed, fixed-argument helper."""

    return (
        f"CONDUCTOR_DBT_PROJECT_DIR={quote(DBT_PROJECT_DIR)} "
        f"CONDUCTOR_DBT_PROFILES_DIR={quote(DBT_PROFILES_DIR)} "
        f"CONDUCTOR_DBT_ARTIFACT_ROOT={quote(DBT_ARTIFACT_ROOT)} "
        "python /opt/airflow/plugins/conductor_dbt_artifacts.py"
    )


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
        bash_command=_dbt_command(),
    )
