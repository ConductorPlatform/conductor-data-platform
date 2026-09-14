from __future__ import annotations

import ast
from pathlib import Path


def test_dbt_dag_runs_from_an_isolated_copy_and_persists_only_bounded_artifacts() -> None:
    source = Path(__file__).resolve().parents[2] / "dags/conductor_dbt_run.py"
    tree = ast.parse(source.read_text())
    function = next(
        node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == "_dbt_command"
    )
    rendered = ast.get_source_segment(source.read_text(), function)

    assert rendered is not None
    assert "mktemp -d /tmp/conductor-dbt" in rendered
    assert "cp -a {source_dir}/. \"$work_dir/\"" in rendered
    assert "dbt deps" in rendered
    assert "dbt run" in rendered
    assert "dbt test" in rendered
    assert "manifest.json run_results.json" in rendered
    assert "5242880" in rendered
    assert "missing-or-oversize" in rendered
    assert "trap 'persist_artifacts; cleanup' EXIT" in rendered
