from __future__ import annotations

from pathlib import Path


ROOT_COMPOSE_PATH = Path(__file__).resolve().parents[2] / "docker-compose.yml"


def test_root_compose_uses_one_worker_managed_airflow_runtime_path() -> None:
    compose = ROOT_COMPOSE_PATH.read_text()

    assert "project-airflow-runtime-image:" in compose
    assert "image: conductor-airflow:latest" in compose
    assert "airflow-image-build" in compose
    assert "lifecycle-worker:" in compose
    assert 'CONDUCTOR_AIRFLOW_PROXY_COOKIE_SECURE: "false"' in compose
    assert "path: ./backend/.env" in compose
    assert "required: false" in compose
    for legacy_service in (
        "airflow-db-init:",
        "airflow-scheduler:",
        "airflow-dag-processor:",
        "airflow-api-server:",
        "airflow-worker:",
        "airflow-dw-",
        "airflow-mktg-",
    ):
        assert legacy_service not in compose


def test_application_default_keeps_production_proxy_cookies_secure() -> None:
    config = (Path(__file__).resolve().parents[1] / "app" / "config.py").read_text()

    assert "airflow_proxy_cookie_secure: bool = True" in config
