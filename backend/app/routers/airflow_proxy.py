from __future__ import annotations

from datetime import UTC, datetime, timedelta
from urllib.parse import quote, unquote, urlsplit

import httpx
from fastapi import APIRouter, Depends, HTTPException, Request, Response, status
from fastapi.responses import RedirectResponse
from jose import jwt
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth.deps import get_current_user, get_current_user_from_access_token
from app.auth.jwt import decode_token
from app.config import settings
from app.database import get_db_session
from app.models.user import User
from app.services.airflow_session import AirflowSessionManager
from app.services.project_airflow_context import (
    ProjectAirflowContext,
    resolve_project_airflow_context,
)

router = APIRouter()

_PROXY_COOKIE_NAME = "airflow_proxy_session"
_READ_METHODS = frozenset({"GET", "HEAD"})
_REQUEST_HEADERS_TO_DROP = frozenset(
    {
        "accept-encoding",
        "authorization",
        "connection",
        "content-length",
        "cookie",
        "host",
        "keep-alive",
        "proxy-authenticate",
        "proxy-authorization",
        "te",
        "trailer",
        "transfer-encoding",
        "upgrade",
    }
)
_RESPONSE_HEADERS_TO_DROP = _REQUEST_HEADERS_TO_DROP | frozenset(
    {"content-encoding", "location", "set-cookie"}
)


def _permission_for_method(method: str) -> tuple[str, str]:
    if method.upper() in _READ_METHODS:
        return "project.dag.view", "read"
    return "project.dag.run", "write"


def _validate_proxy_path(path: str) -> str:
    """Accept only a relative Airflow path that cannot alter the trusted host."""
    decoded_path = unquote(path)
    if (
        decoded_path.startswith(("/", "\\"))
        or "\\" in decoded_path
        or "://" in decoded_path
        or any(segment in {".", ".."} for segment in decoded_path.split("/"))
    ):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Proxy path not found")
    return decoded_path


def _target_url(context: ProjectAirflowContext, path: str) -> str:
    parsed_base_url = urlsplit(context.airflow_base_url)
    if (
        parsed_base_url.scheme not in {"http", "https"}
        or not parsed_base_url.hostname
        or parsed_base_url.username
        or parsed_base_url.password
        or parsed_base_url.query
        or parsed_base_url.fragment
    ):
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail="Airflow unavailable")
    safe_path = quote(path, safe="/-._~!$&'()*+,;=:@")
    return f"{context.airflow_base_url.rstrip('/')}/{safe_path}"


def _forward_request_headers(request: Request) -> dict[str, str]:
    return {
        name: value
        for name, value in request.headers.items()
        if name.lower() not in _REQUEST_HEADERS_TO_DROP
        and not name.lower().startswith("x-forwarded-")
    }


def _forward_response_headers(headers: httpx.Headers) -> dict[str, str]:
    return {
        name: value
        for name, value in headers.items()
        if name.lower() not in _RESPONSE_HEADERS_TO_DROP
    }


async def _get_proxy_session_user(
    slug: str,
    request: Request,
    db: AsyncSession = Depends(get_db_session),
) -> User:
    authorization = request.headers.get("authorization")
    if authorization:
        scheme, separator, access_token = authorization.partition(" ")
        if scheme.lower() != "bearer" or not separator or not access_token:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid or expired token"
            )
        return await get_current_user_from_access_token(access_token, db)

    token = request.cookies.get(_PROXY_COOKIE_NAME)
    payload = decode_token(token) if token else None
    if (
        payload is None
        or payload.get("type") != "airflow_proxy_session"
        or payload.get("slug") != slug
        or not isinstance(payload.get("sub"), str)
    ):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Proxy session required"
        )

    user = (await db.execute(select(User).where(User.id == payload["sub"]))).scalar_one_or_none()
    if user is None or not user.is_active:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Proxy session required"
        )
    return user


def _issue_proxy_session(response: Response, slug: str, user: User) -> None:
    expires_at = datetime.now(UTC) + timedelta(seconds=settings.airflow_proxy_session_ttl_seconds)
    token = jwt.encode(
        {
            "sub": user.id,
            "slug": slug,
            "exp": expires_at,
            "type": "airflow_proxy_session",
        },
        settings.secret_key,
        algorithm=settings.algorithm,
    )
    response.set_cookie(
        key=_PROXY_COOKIE_NAME,
        value=token,
        max_age=settings.airflow_proxy_session_ttl_seconds,
        path=f"/api/v1/projects/{slug}/airflow-proxy",
        secure=settings.airflow_proxy_cookie_secure,
        httponly=True,
        samesite="lax",
    )


@router.api_route(
    "/projects/{slug}/airflow-proxy/{path:path}",
    methods=["GET", "HEAD", "POST", "PUT", "PATCH", "DELETE"],
)
async def airflow_proxy(
    slug: str,
    path: str,
    request: Request,
    user: User = Depends(_get_proxy_session_user),
    db: AsyncSession = Depends(get_db_session),
):
    resource, action = _permission_for_method(request.method)
    context = await resolve_project_airflow_context(slug, user, db, resource, action)
    safe_path = _validate_proxy_path(path)
    target_url = _target_url(context, safe_path)
    session = await AirflowSessionManager().get_session(context, db)
    body = await request.body()

    try:
        async with httpx.AsyncClient(follow_redirects=False) as client:
            resp = await client.request(
                method=request.method,
                url=target_url,
                content=body,
                headers=_forward_request_headers(request),
                cookies={"session": session},
                params=list(request.query_params.multi_items()),
            )
    except httpx.HTTPError as error:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY, detail="Airflow unavailable"
        ) from error

    if resp.is_error or resp.is_redirect:
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail="Airflow unavailable")

    return Response(
        content=resp.content,
        status_code=resp.status_code,
        headers=_forward_response_headers(resp.headers),
    )


@router.get("/projects/{slug}/airflow-iframe/{path:path}")
async def airflow_iframe(
    slug: str,
    path: str,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db_session),
):
    """Bootstrap a short-lived, project-scoped browser proxy session."""
    resource, action = _permission_for_method("GET")
    await resolve_project_airflow_context(slug, user, db, resource, action)
    safe_path = _validate_proxy_path(path)
    response = RedirectResponse(
        url=f"/api/v1/projects/{slug}/airflow-proxy/{quote(safe_path, safe='/')}",
        status_code=status.HTTP_303_SEE_OTHER,
    )
    _issue_proxy_session(response, slug, user)
    return response
