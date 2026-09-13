from __future__ import annotations

import importlib.util
from pathlib import Path
from types import ModuleType, SimpleNamespace

import pytest


SCRIPT_PATH = Path(__file__).resolve().parents[2] / "docker/airflow/bootstrap_airflow_users.py"


@pytest.fixture
def bootstrap_module() -> ModuleType:
    spec = importlib.util.spec_from_file_location("bootstrap_airflow_users_test", SCRIPT_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_bootstrap_creates_a_missing_service_account_with_fab_cli(bootstrap_module, monkeypatch):
    calls: list[list[str]] = []

    def run(arguments: list[str]):
        calls.append(arguments)
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(bootstrap_module, "_run", run)

    bootstrap_module._ensure_user("admin", "not-logged", "Admin", "User", "Admin", "admin@test.local")

    assert calls == [
        [
            "airflow",
            "users",
            "create",
            "--username",
            "admin",
            "--password",
            "not-logged",
            "--firstname",
            "Admin",
            "--lastname",
            "User",
            "--role",
            "Admin",
            "--email",
            "admin@test.local",
        ]
    ]


def test_bootstrap_converges_when_a_partial_init_left_the_account_in_place(bootstrap_module, monkeypatch):
    calls: list[list[str]] = []

    def run(arguments: list[str]):
        calls.append(arguments)
        if arguments[2] == "create":
            return SimpleNamespace(returncode=1, stdout="", stderr="already exists")
        return SimpleNamespace(returncode=0, stdout='[{"username": "admin"}]', stderr="")

    monkeypatch.setattr(bootstrap_module, "_run", run)

    bootstrap_module._ensure_user("admin", "not-logged", "Admin", "User", "Admin", "admin@test.local")

    assert calls[0][0:3] == ["airflow", "users", "create"]
    assert calls[1] == ["airflow", "users", "list", "--output", "json"]


def test_bootstrap_fails_when_create_error_cannot_be_confirmed_as_existing(bootstrap_module, monkeypatch):
    def run(arguments: list[str]):
        if arguments[2] == "create":
            return SimpleNamespace(returncode=1, stdout="", stderr="unexpected failure")
        return SimpleNamespace(returncode=0, stdout="[]", stderr="")

    monkeypatch.setattr(bootstrap_module, "_run", run)

    with pytest.raises(RuntimeError, match="Airflow user creation failed"):
        bootstrap_module._ensure_user("admin", "not-logged", "Admin", "User", "Admin", "admin@test.local")
