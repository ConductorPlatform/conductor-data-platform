from __future__ import annotations

import ast
from pathlib import Path


def test_dbt_dag_uses_the_installed_bounded_artifact_helper() -> None:
    source = Path(__file__).resolve().parents[2] / "dags/conductor_dbt_run.py"
    tree = ast.parse(source.read_text())
    function = next(
        node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == "_dbt_command"
    )
    rendered = ast.get_source_segment(source.read_text(), function)

    assert rendered is not None
    assert "CONDUCTOR_DBT_PROJECT_DIR" in rendered
    assert "CONDUCTOR_DBT_PROFILES_DIR" in rendered
    assert "CONDUCTOR_DBT_ARTIFACT_ROOT" in rendered
    assert "/opt/airflow/plugins/conductor_dbt_artifacts.py" in rendered
    assert "dbt deps" not in rendered
