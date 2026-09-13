from __future__ import annotations

import httpx
import redis.asyncio as aioredis
from fastapi import HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.models.project_deployment import ProjectDeployment
from app.services.crypto import decrypt_token
from app.services.project_airflow_context import ProjectAirflowContext


class AirflowSessionManager:
    """Manages Airflow session cookies with Redis caching (55min TTL)."""

    def __init__(self) -> None:
        self._redis: aioredis.Redis | None = None

    async def _get_redis(self) -> aioredis.Redis:
        if self._redis is None:
            self._redis = aioredis.from_url(settings.redis_url, decode_responses=True)
        return self._redis

    async def get_session(self, context: ProjectAirflowContext, db: AsyncSession) -> str:
        """Get a cached service session without exposing credential material."""
        cache_key = (
            f"airflow_session:{context.deployment_id}:"
            f"{context.deployment_generation}:{context.account_key}"
        )
        deployment = await db.get(ProjectDeployment, context.deployment_id)
        if (
            deployment is None
            or deployment.project_id != context.project_id
            or deployment.generation != context.deployment_generation
        ):
            raise HTTPException(status_code=404, detail="Airflow not provisioned")

        redis = await self._get_redis()
        cached = await redis.get(cache_key)
        if cached:
            return cached

        if context.account_key == "admin":
            username = deployment.airflow_admin_user
            encrypted_password = deployment.airflow_admin_password_encrypted
        elif context.account_key == "dev":
            username = deployment.airflow_dev_user
            encrypted_password = deployment.airflow_dev_password_encrypted
        else:
            username = deployment.airflow_viewer_user
            encrypted_password = deployment.airflow_viewer_password_encrypted

        try:
            password = decrypt_token(encrypted_password)
            async with httpx.AsyncClient() as client:
                login_response = await client.post(
                    f"{context.airflow_base_url}/api/v1/login/",
                    data={"username": username, "password": password},
                    headers={"Content-Type": "application/x-www-form-urlencoded"},
                )
        except Exception as error:
            raise HTTPException(status_code=502, detail="Airflow authentication failed") from error

        if login_response.status_code != 200:
            raise HTTPException(status_code=502, detail="Airflow authentication failed")

        session_cookie = login_response.cookies.get("session")
        if not session_cookie:
            for cookie in login_response.cookies.jar:
                if cookie.name == "session":
                    session_cookie = cookie.value
                    break

        if session_cookie:
            await redis.setex(cache_key, 3300, session_cookie)  # 55 min TTL
        return session_cookie or ""
