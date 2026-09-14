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
DBT_ARTIFACT_ROOT = "/opt/airflow/logs/conductor-dbt-artifacts"


def _dbt_command() -> str:
    """Run from an isolated copy, retaining only bounded run-scoped evidence."""

    source_dir = quote(DBT_PROJECT_DIR)
    profiles_dir = quote(DBT_PROFILES_DIR)
    return f"""
set -eu
run_key=$(printf '%s' "$AIRFLOW_CTX_DAG_RUN_ID" | sha256sum | cut -d ' ' -f 1)
work_dir=$(mktemp -d /tmp/conductor-dbt.XXXXXXXX)
artifact_dir={quote(DBT_ARTIFACT_ROOT)}/$run_key
cleanup() {{ rm -rf "$work_dir"; }}
persist_artifacts() {{
  mkdir -p "$artifact_dir"
  chmod 700 "$artifact_dir"
  for artifact in manifest.json run_results.json; do
    source="$work_dir/target/$artifact"
    if [ -f "$source" ] && [ "$(wc -c < "$source")" -le 5242880 ]; then
      cp "$source" "$artifact_dir/$artifact"
    else
      printf '%s\\n' "missing-or-oversize:$artifact" >> "$artifact_dir/status"
    fi
  done
}}
trap 'persist_artifacts; cleanup' EXIT
cp -a {source_dir}/. "$work_dir/"
cd "$work_dir"
dbt deps --profiles-dir {profiles_dir}
dbt run --profiles-dir {profiles_dir}
dbt test --profiles-dir {profiles_dir}
"""


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
