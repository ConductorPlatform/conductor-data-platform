"""Server-side synchronization of the one supported GitDagBundle connection."""

from __future__ import annotations

import json
from dataclasses import dataclass

import httpx
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.git_config import GitConfig
from app.services.airflow_session import AirflowSessionManager
from app.services.crypto import decrypt_token
from app.services.project_airflow_context import ProjectAirflowContext

_CONNECTION_ID = "conductor_git"


class GitDagBundleSyncError(RuntimeError):
    """A project Git setting could not be applied to its Airflow runtime."""


@dataclass(frozen=True)
class GitDagConnectionPayload:
    connection_id: str
    conn_type: str
    host: str
    login: str
    password: str
    extra: str

    def as_dict(self) -> dict[str, str]:
        return {
            "connection_id": self.connection_id,
            "conn_type": self.conn_type,
            "host": self.host,
            "login": self.login,
            "password": self.password,
            "extra": self.extra,
        }


def git_dag_connection_payload(config: GitConfig) -> GitDagConnectionPayload:
    """Build the credential-bearing request body without logging or persisting it."""

    if config.auth_type != "token" or not config.credentials_encrypted:
        raise ValueError("The MVP GitDagBundle path requires an HTTPS token configuration")
    if not config.repo_url.startswith("https://"):
        raise ValueError("The MVP GitDagBundle path requires an HTTPS repository URL")
    metadata = {
        "conductor_tracking_ref": config.default_branch,
        "conductor_dags_path": config.dags_path,
        "conductor_dbt_path": config.dbt_path,
    }
    return GitDagConnectionPayload(
        connection_id=_CONNECTION_ID,
        conn_type="git",
        host=config.repo_url,
        login="oauth2",
        password=decrypt_token(config.credentials_encrypted),
        extra=json.dumps(metadata, separators=(",", ":"), sort_keys=True),
    )


async def sync_git_dag_connection(
    *,
    context: ProjectAirflowContext,
    config: GitConfig,
    db: AsyncSession,
    session_manager: AirflowSessionManager | None = None,
) -> None:
    """Create, update, or revoke only the deterministic project Git connection.

    The bearer token and repository credential are held only in process memory
    for this request.  Failure leaves the database setting intact so an admin
    can retry the same update; it is never exposed in the error returned to the
    caller.
    """

    manager = session_manager or AirflowSessionManager()
    admin_context = ProjectAirflowContext(
        project_id=context.project_id,
        deployment_id=context.deployment_id,
        deployment_generation=context.deployment_generation,
        airflow_base_url=context.airflow_base_url,
        account_key="admin",
    )
    access_token = await manager.get_access_token(admin_context, db)
    headers = {"Authorization": f"Bearer {access_token}"}
    endpoint = f"{context.airflow_base_url}/api/v2/connections/{_CONNECTION_ID}"

    try:
        async with httpx.AsyncClient() as client:
            if config.auth_type != "token":
                response = await client.delete(endpoint, headers=headers)
                if response.status_code in (200, 204, 404):
                    return
                raise GitDagBundleSyncError("Airflow rejected Git connection revocation")

            payload = git_dag_connection_payload(config).as_dict()
            current = await client.get(endpoint, headers=headers)
            if current.status_code == 404:
                response = await client.post(
                    f"{context.airflow_base_url}/api/v2/connections",
                    headers=headers,
                    json=payload,
                )
            elif 200 <= current.status_code < 300:
                response = await client.patch(endpoint, headers=headers, json=payload)
            else:
                raise GitDagBundleSyncError("Airflow rejected Git connection lookup")
    except httpx.HTTPError as exc:
        raise GitDagBundleSyncError("Airflow Git connection is unavailable") from exc

    if not 200 <= response.status_code < 300:
        raise GitDagBundleSyncError("Airflow rejected Git connection update")
