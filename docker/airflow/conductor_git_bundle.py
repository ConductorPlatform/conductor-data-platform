"""Conductor's credential-safe adapter for Airflow's native GitDagBundle.

The adapter deliberately delegates all clone, refresh and version selection
behaviour to ``GitDagBundle``.  Its only responsibilities are reading the
project-local connection metadata and providing HTTPS credentials through an
askpass file instead of a credential-bearing repository URL.
"""

from __future__ import annotations

import os
import stat
import tempfile
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from airflow.providers.git.bundles import git as git_bundle_module
from airflow.providers.git.bundles.git import GitDagBundle
from airflow.providers.common.compat.sdk import BaseHook
from airflow.providers.git.hooks.git import GitHook

_CONNECTION_ID = "conductor_git"
_REQUIRED_EXTRA_KEYS = frozenset({"conductor_tracking_ref", "conductor_dags_path", "conductor_dbt_path"})
_TOKEN_FILE = Path("/run/secrets/conductor-git-token")


def _connection_metadata(connection_id: str) -> tuple[str, str, str]:
    connection = BaseHook.get_connection(connection_id)
    extras = connection.extra_dejson
    if not _REQUIRED_EXTRA_KEYS.issubset(extras):
        raise ValueError("Conductor Git connection is missing repository path metadata")
    tracking_ref = extras["conductor_tracking_ref"]
    dags_path = extras["conductor_dags_path"]
    dbt_path = extras["conductor_dbt_path"]
    if not all(isinstance(value, str) and value for value in (tracking_ref, dags_path, dbt_path)):
        raise ValueError("Conductor Git connection has invalid repository path metadata")
    return tracking_ref, dags_path, dbt_path


class ConductorGitHook(GitHook):
    """GitHook variant that keeps HTTPS tokens out of repository URLs and logs."""

    def _process_git_auth_url(self) -> None:
        # The provider's default hook embeds the token into repo_url.  Keep the
        # URL credential-free; configure_hook_env supplies auth to git instead.
        return

    @contextmanager
    def configure_hook_env(self) -> Iterator[None]:
        if not isinstance(self.repo_url, str) or not self.repo_url.startswith("https://"):
            with super().configure_hook_env():
                yield
            return

        try:
            token_stat = _TOKEN_FILE.stat()
        except FileNotFoundError as exc:
            raise ValueError("Conductor Git token file is unavailable") from exc
        if not stat.S_ISREG(token_stat.st_mode) or token_stat.st_mode & 0o077:
            raise ValueError("Conductor Git token file is not private")

        with tempfile.TemporaryDirectory(prefix="conductor-git-") as directory:
            root = Path(directory)
            askpass_file = root / "askpass"
            askpass_file.write_text(
                "#!/bin/sh\n"
                "case \"$1\" in\n"
                "  *Username*) printf '%s\\n' \"${CONDUCTOR_GIT_USERNAME:-oauth2}\" ;;\n"
                "  *Password*) cat \"$CONDUCTOR_GIT_TOKEN_FILE\" ;;\n"
                "  *) exit 1 ;;\n"
                "esac\n"
            )
            os.chmod(askpass_file, stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR)
            previous = dict(self.env)
            environment = {
                "CONDUCTOR_GIT_TOKEN_FILE": str(_TOKEN_FILE),
                "CONDUCTOR_GIT_USERNAME": self.user_name or "oauth2",
                "GIT_ASKPASS": str(askpass_file),
                "GIT_TERMINAL_PROMPT": "0",
            }
            previous_process_environment = {key: os.environ.get(key) for key in environment}
            self.env.update(environment)
            os.environ.update(environment)
            try:
                yield
            finally:
                self.env = previous
                for key, previous_value in previous_process_environment.items():
                    if previous_value is None:
                        os.environ.pop(key, None)
                    else:
                        os.environ[key] = previous_value


class ConductorGitDagBundle(GitDagBundle):
    """Native GitDagBundle using deterministic project connection metadata."""

    def __init__(self, *, git_conn_id: str = _CONNECTION_ID, **kwargs) -> None:
        tracking_ref, dags_path, _ = _connection_metadata(git_conn_id)
        # GitDagBundle eagerly constructs its GitHook.  The provider hard-codes
        # that class at module scope, so substitute only during construction;
        # ConductorGitHook overrides the provider's credential-in-URL behavior
        # while the native bundle keeps its clone/refresh/version mechanics.
        original_hook = git_bundle_module.GitHook
        git_bundle_module.GitHook = ConductorGitHook
        try:
            super().__init__(
                tracking_ref=tracking_ref,
                subdir=dags_path,
                git_conn_id=git_conn_id,
                **kwargs,
            )
        finally:
            git_bundle_module.GitHook = original_hook


def dbt_project_dir(dag_file: str, *, git_conn_id: str = _CONNECTION_ID) -> str:
    """Resolve dbt_path inside the full immutable bundle checkout.

    ``GitDagBundle.path`` is the configured DAG subdirectory.  Walking upward
    by the configured DAG path depth yields its repository root without ever
    consulting an IDE workspace or a mutable branch checkout.
    """

    _, dags_path, dbt_path = _connection_metadata(git_conn_id)
    dag_path = Path(dag_file)
    repository_root = dag_path.parent
    for _ in Path(dags_path).parts:
        repository_root = repository_root.parent
    repository_root = repository_root.resolve()
    try:
        dag_path.resolve().relative_to(repository_root)
    except ValueError as exc:
        raise ValueError("Conductor DAG path escapes the immutable Git bundle") from exc

    project_dir = (repository_root / dbt_path).resolve()
    try:
        project_dir.relative_to(repository_root)
    except ValueError as exc:
        raise ValueError("Conductor dbt path escapes the immutable Git bundle") from exc
    if not project_dir.is_dir() or not (project_dir / "dbt_project.yml").is_file():
        raise ValueError("Conductor dbt project is missing from the immutable Git bundle")
    return str(project_dir)
