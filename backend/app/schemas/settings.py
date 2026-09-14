from __future__ import annotations

from datetime import datetime
from pathlib import PurePosixPath
from typing import Literal
from urllib.parse import urlsplit

from pydantic import BaseModel, field_validator, model_validator


class GitConfigResponse(BaseModel):
    repo_url: str
    auth_type: str
    default_branch: str
    dbt_path: str
    dags_path: str
    has_credentials: bool = False  # Backward-compatible generic secret indicator
    has_token: bool = False
    created_at: datetime
    updated_at: datetime


class GitConfigUpdateRequest(BaseModel):
    repo_url: str | None = None
    auth_type: Literal["https", "token", "ssh"] | None = None
    token: str | None = None
    credentials: str | None = None  # Backward compatibility for older clients/SSH
    default_branch: str | None = None
    dbt_path: str | None = None
    dags_path: str | None = None
    webhook_secret: str | None = None

    @field_validator("repo_url", "auth_type")
    @classmethod
    def reject_null_for_non_nullable_fields(cls, value: str | None) -> str:
        if value is None:
            raise ValueError("Field may be omitted, but it must not be null")
        return value

    @field_validator("repo_url")
    @classmethod
    def reject_credentials_in_repo_url(cls, value: str) -> str:
        parsed = urlsplit(value)
        if parsed.username is not None or parsed.password is not None:
            raise ValueError("Repository URL must not contain embedded credentials")
        return value

    @field_validator("token", "credentials")
    @classmethod
    def reject_blank_credentials(cls, value: str | None) -> str | None:
        if value is not None and not value.strip():
            raise ValueError("Credential must not be blank")
        return value

    @field_validator("dbt_path", "dags_path")
    @classmethod
    def require_canonical_repository_directory(cls, value: str | None) -> str | None:
        if value is None:
            return value
        if not value or value in (".", "./") or "\\" in value or "\x00" in value or value.startswith("/"):
            raise ValueError("Path must be a repository-relative POSIX directory")
        path = PurePosixPath(value)
        if str(path) != value or any(part in ("", ".", "..") for part in path.parts):
            raise ValueError("Path must be a repository-relative POSIX directory")
        return str(path)

    @field_validator("default_branch")
    @classmethod
    def reject_unsafe_git_ref(cls, value: str | None) -> str | None:
        if value is None:
            return value
        invalid_characters = " ~^:?*[\\"
        components = value.split("/")
        if (
            not value
            or value == "@"
            or value.startswith("-")
            or value.startswith("/")
            or value.endswith("/")
            or value.endswith(".")
            or ".." in value
            or "@{" in value
            or any(character in value for character in invalid_characters)
            or any(ord(character) < 32 or ord(character) == 127 for character in value)
            or any(
                component in ("", ".", "..")
                or component.startswith(".")
                or component.endswith(".lock")
                for component in components
            )
        ):
            raise ValueError("Production branch is not a safe Git ref")
        return value

    @model_validator(mode="after")
    def reject_ambiguous_credentials(self):
        if self.token is not None and self.credentials is not None:
            raise ValueError("Provide either token or credentials, not both")
        return self


class EnvironmentResponse(BaseModel):
    id: str
    name: str
    branch_name: str
    is_protected: bool
    is_active: bool
    created_at: datetime


class EnvironmentCreateRequest(BaseModel):
    name: str
    branch_name: str
    is_protected: bool = False


class EnvironmentUpdateRequest(BaseModel):
    name: str | None = None
    branch_name: str | None = None
    is_protected: bool | None = None
    is_active: bool | None = None


class ProjectSettingsResponse(BaseModel):
    self_approve_enabled: bool


class ProjectSettingsUpdateRequest(BaseModel):
    self_approve_enabled: bool | None = None
