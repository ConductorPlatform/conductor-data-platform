from __future__ import annotations

import ast
import importlib.util
import json
from pathlib import Path

import pytest


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


def _load_artifact_helper():
    source = Path(__file__).resolve().parents[2] / "docker/airflow/conductor_dbt_artifacts.py"
    spec = importlib.util.spec_from_file_location("conductor_dbt_artifacts_test", source)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_helper_excludes_bundle_target_when_deps_fails(monkeypatch, tmp_path: Path) -> None:
    module = _load_artifact_helper()
    project = tmp_path / "bundle" / "dbt"
    profiles = project / "profiles"
    profiles.mkdir(parents=True)
    # This must never be published: it existed in the immutable Git bundle
    # before this run, and dbt deps will fail before producing new output.
    (project / "target").mkdir()
    (project / "target" / "manifest.json").write_text('{"stale": true}')
    artifacts = tmp_path / "artifacts"
    project_id = "0123456789abcdef0123456789abcdef"
    commit = "a" * 40
    monkeypatch.setattr(module, "_bundle_commit", lambda _path: commit)
    monkeypatch.setattr(
        module.subprocess,
        "run",
        lambda *_args, **_kwargs: type("Result", (), {"returncode": 1})(),
    )
    monkeypatch.setenv("CONDUCTOR_DBT_PROJECT_DIR", str(project))
    monkeypatch.setenv("CONDUCTOR_DBT_PROFILES_DIR", str(profiles))
    monkeypatch.setenv("CONDUCTOR_DBT_ARTIFACT_ROOT", str(artifacts))
    monkeypatch.setenv("CONDUCTOR_PROJECT_ID", project_id)
    monkeypatch.setenv("CONDUCTOR_RUNTIME_SUBPATH", f"{project_id}/1")
    monkeypatch.setenv("AIRFLOW_CTX_DAG_ID", "dag")
    monkeypatch.setenv("AIRFLOW_CTX_DAG_RUN_ID", "run")
    monkeypatch.setenv("AIRFLOW_CTX_TRY_NUMBER", "1")

    with pytest.raises(SystemExit) as exit_code:
        module.main()

    assert exit_code.value.code == 1
    index_path = next(artifacts.rglob("index.json"))
    index = json.loads(index_path.read_text())
    assert index["stage"] == "deps"
    assert index["exit_code"] == 1
    assert index["files"]["manifest.json"] == {"status": "missing"}
