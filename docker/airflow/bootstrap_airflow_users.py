#!/usr/bin/env python3
"""Converge the fixed project service accounts through Airflow's FAB CLI.

The provisioning retry path may re-enter after one account was created.  Each
account is created through ``airflow users create``; if that command reports an
existing account, the script confirms it through the same FAB CLI before
continuing.  Passwords are read only from the container environment and are
never written to stdout or stderr.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from collections.abc import Iterable


_USERS = (
    ("AIRFLOW_ADMIN_USER", "AIRFLOW_ADMIN_PASSWORD", "Admin", "User", "Admin", "admin@conductor.local"),
    ("AIRFLOW_DEV_USER", "AIRFLOW_DEV_PASSWORD", "Developer", "User", "User", "dev@conductor.local"),
    ("AIRFLOW_VIEWER_USER", "AIRFLOW_VIEWER_PASSWORD", "Viewer", "User", "Viewer", "viewer@conductor.local"),
    (
        "AIRFLOW_INTEGRATION_USER",
        "AIRFLOW_INTEGRATION_PASSWORD",
        "Integration",
        "User",
        "Viewer",
        "integration@conductor.local",
    ),
)


def _run(arguments: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(arguments, check=False, capture_output=True, text=True)


def _existing_usernames() -> set[str]:
    result = _run(["airflow", "users", "list", "--output", "json"])
    if result.returncode:
        raise RuntimeError("Unable to verify existing Airflow users after a create failure")
    try:
        payload = json.loads(result.stdout)
    except json.JSONDecodeError as error:
        raise RuntimeError("Airflow users list did not return JSON") from error

    users: Iterable[object]
    if isinstance(payload, dict):
        users = payload.get("users", [])
    else:
        users = payload
    if not isinstance(users, list):
        raise RuntimeError("Airflow users list returned an unexpected JSON payload")
    return {
        username
        for item in users
        if isinstance(item, dict) and isinstance((username := item.get("username")), str)
    }


def _ensure_user(
    username: str,
    password: str,
    first_name: str,
    last_name: str,
    role: str,
    email: str,
) -> None:
    result = _run(
        [
            "airflow",
            "users",
            "create",
            "--username",
            username,
            "--password",
            password,
            "--firstname",
            first_name,
            "--lastname",
            last_name,
            "--role",
            role,
            "--email",
            email,
        ]
    )
    if result.returncode == 0:
        return
    if username in _existing_usernames():
        return
    raise RuntimeError(f"Airflow user creation failed for {username!r}")


def main() -> int:
    for username_key, password_key, first_name, last_name, role, email in _USERS:
        _ensure_user(
            os.environ[username_key],
            os.environ[password_key],
            first_name,
            last_name,
            role,
            email,
        )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (KeyError, RuntimeError) as error:
        print(f"Airflow service-account bootstrap failed: {error}", file=sys.stderr)
        raise SystemExit(1) from error
