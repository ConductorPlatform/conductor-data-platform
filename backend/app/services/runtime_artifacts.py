"""Trusted, reconstructible runtime artifact generation for project Compose stacks."""

from __future__ import annotations

import os
import re
import shutil
import stat
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import quote, urlsplit
from uuid import uuid4

from app.services.lifecycle_errors import InvalidTemplateError
from app.services.project_lifecycle import validate_runtime_parameters

_PROJECT_ID_PATTERN = re.compile(r"^[0-9a-f]{32}$")
_SLUG_PATTERN = re.compile(r"^[a-z0-9](?:[a-z0-9-]{0,62})$")
_DNS_LABEL_PATTERN = re.compile(r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$")
_NETWORK_NAME_PATTERN = re.compile(r"^[a-z0-9][a-z0-9_.-]{0,62}$")
_IMAGE_REFERENCE_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._/@:+-]{0,255}$")
_TEMPLATE_ROOT = Path(__file__).resolve().parents[1] / "runtime_templates"
_TRUSTED_TEMPLATE_VERSIONS = frozenset({"v1"})


@dataclass(frozen=True)
class RuntimeArtifactSpec:
    """All desired runtime inputs reconstructed from the project deployment row."""

    project_id: str
    generation: int
    template_version: str
    compose_project_name: str
    project_slug: str
    airflow_external_url: str
    airflow_db_name: str
    airflow_db_role: str
    airflow_db_password: str
    airflow_admin_user: str
    airflow_admin_password: str
    airflow_dev_user: str
    airflow_dev_password: str
    airflow_viewer_user: str
    airflow_viewer_password: str
    airflow_integration_user: str
    airflow_integration_password: str
    warehouse_db_name: str
    warehouse_db_role: str
    warehouse_db_password: str
    warehouse_schema: str
    parameters: dict[str, object]


@dataclass(frozen=True)
class RuntimeArtifact:
    """Private paths to generated files; secret values are deliberately excluded."""

    project_id: str
    generation: int
    template_version: str
    runtime_directory: Path
    compose_path: Path
    env_path: Path


class RuntimeArtifactWriter:
    """Copy trusted Compose templates and write their allowlisted ENV atomically."""

    def __init__(
        self,
        *,
        runtime_root: Path,
        runtime_ingress_network: str = "conductor-runtime-ingress",
        airflow_image: str = "conductor-airflow:latest",
        airflow_database_host: str = "host.docker.internal",
        airflow_database_port: int = 5432,
        warehouse_host: str = "host.docker.internal",
        warehouse_port: int = 5433,
        template_root: Path = _TEMPLATE_ROOT,
        secret_root: Path | None = None,
        artifact_root: Path | None = None,
        runtime_uid: int = 50000,
        runtime_gid: int = 0,
    ) -> None:
        self._runtime_root = Path(os.path.abspath(runtime_root))
        self._runtime_ingress_network = runtime_ingress_network
        self._airflow_image = airflow_image
        self._airflow_database_host = airflow_database_host
        self._airflow_database_port = airflow_database_port
        self._warehouse_host = warehouse_host
        self._warehouse_port = warehouse_port
        self._template_root = template_root.resolve()
        self._secret_root = Path(os.path.abspath(secret_root or runtime_root))
        self._artifact_root = Path(os.path.abspath(artifact_root or runtime_root))
        self._runtime_uid = runtime_uid
        self._runtime_gid = runtime_gid

    def runtime_directory(self, *, project_id: str, generation: int) -> Path:
        _validate_project_id(project_id)
        _validate_generation(generation)
        runtime_directory = self._runtime_root / project_id / str(generation)
        try:
            runtime_directory.relative_to(self._runtime_root)
        except ValueError as exc:
            raise ValueError("Runtime artifact path escapes CONDUCTOR_RUNTIME_ROOT") from exc
        return runtime_directory

    def git_token_path(self, *, project_id: str, generation: int) -> Path:
        """Return the token boundary in the daemon-managed secret volume."""

        return self.secret_directory(project_id=project_id, generation=generation) / "git-token"

    def runtime_subpath(self, *, project_id: str, generation: int) -> str:
        """Return the only project-scoped volume subpath exposed to a runtime."""

        _validate_project_id(project_id)
        _validate_generation(generation)
        return f"{project_id}/{generation}"

    def secret_directory(self, *, project_id: str, generation: int) -> Path:
        return _ensure_private_generation_directory(
            self._secret_root, project_id, generation,
            owner_uid=self._runtime_uid, owner_gid=self._runtime_gid,
        )

    def artifact_directory(self, *, project_id: str, generation: int) -> Path:
        return _ensure_private_generation_directory(
            self._artifact_root, project_id, generation,
            owner_uid=self._runtime_uid, owner_gid=self._runtime_gid,
        )

    def write_git_token(self, *, project_id: str, generation: int, token: str) -> Path:
        """Atomically materialize the Git credential outside Compose and Airflow metadata."""

        if not token or any(character in token for character in ("\x00", "\n", "\r")):
            raise ValueError("Git token must be a non-empty single-line value")
        token_path = self.git_token_path(project_id=project_id, generation=generation)
        _reject_symlink(token_path, "Runtime Git token file")
        _atomic_write(
            token_path, token.encode(), owner_uid=self._runtime_uid, owner_gid=self._runtime_gid
        )
        return token_path

    def revoke_git_token(self, *, project_id: str, generation: int) -> None:
        """Remove the private token file without following a hostile symlink."""

        token_path = self.git_token_path(project_id=project_id, generation=generation)
        _reject_symlink(token_path, "Runtime Git token file")
        try:
            token_path.unlink()
        except FileNotFoundError:
            return

    def render(self, spec: RuntimeArtifactSpec) -> RuntimeArtifact:
        """Render a private, deterministic artifact from trusted desired state only."""

        template_path = self._template_path(spec.template_version)
        _validate_spec(
            spec,
            runtime_ingress_network=self._runtime_ingress_network,
            airflow_image=self._airflow_image,
            airflow_database_host=self._airflow_database_host,
            airflow_database_port=self._airflow_database_port,
            warehouse_host=self._warehouse_host,
            warehouse_port=self._warehouse_port,
        )
        runtime_directory = self.runtime_directory(
            project_id=spec.project_id,
            generation=spec.generation,
        )
        self._ensure_private_project_directory(spec.project_id)
        compose_path = runtime_directory / "compose.yaml"
        env_path = runtime_directory / ".env"
        compose_content = template_path.read_bytes()
        env_content = _serialize_env(
            _allowlisted_env(
                spec,
                runtime_ingress_network=self._runtime_ingress_network,
                airflow_image=self._airflow_image,
                airflow_database_host=self._airflow_database_host,
                airflow_database_port=self._airflow_database_port,
                warehouse_host=self._warehouse_host,
                warehouse_port=self._warehouse_port,
            )
        ).encode()

        _publish_generation_atomically(
            runtime_directory=runtime_directory,
            compose_content=compose_content,
            env_content=env_content,
        )
        # Docker validates `volume.subpath` before starting a container. Create
        # both roots before any Compose operation so an initially absent token
        # is represented by an absent file, never a daemon-created directory.
        self.secret_directory(project_id=spec.project_id, generation=spec.generation)
        self.artifact_directory(project_id=spec.project_id, generation=spec.generation)
        return RuntimeArtifact(
            project_id=spec.project_id,
            generation=spec.generation,
            template_version=spec.template_version,
            runtime_directory=runtime_directory,
            compose_path=compose_path,
            env_path=env_path,
        )

    def _template_path(self, template_version: str) -> Path:
        if template_version not in _TRUSTED_TEMPLATE_VERSIONS:
            raise InvalidTemplateError("Runtime template version is not allowlisted")
        template_path = (self._template_root / template_version / "compose.yaml").resolve()
        try:
            template_path.relative_to(self._template_root)
        except ValueError as exc:
            raise InvalidTemplateError("Runtime template path escapes trusted templates") from exc
        if not template_path.is_file():
            raise InvalidTemplateError("Runtime template version is not installed")
        return template_path

    def _ensure_private_project_directory(self, project_id: str) -> None:
        root_descriptor = _open_private_runtime_root(self._runtime_root)
        try:
            project_descriptor = _open_private_child_directory(root_descriptor, project_id)
        finally:
            os.close(root_descriptor)
        os.close(project_descriptor)


def runtime_artifact_metadata(artifact: RuntimeArtifact) -> dict[str, str | int]:
    """Return secret-free metadata suitable for registry records and diagnostics."""

    return {
        "project_id": artifact.project_id,
        "generation": artifact.generation,
        "template_version": artifact.template_version,
        "compose_path": str(artifact.compose_path),
    }


def _validate_project_id(project_id: str) -> None:
    if not isinstance(project_id, str) or not _PROJECT_ID_PATTERN.fullmatch(project_id):
        raise ValueError("project_id must be exactly 32 lowercase hexadecimal characters")


def _validate_generation(generation: int) -> None:
    if isinstance(generation, bool) or not isinstance(generation, int) or generation < 1:
        raise ValueError("generation must be a positive integer")


def _validate_spec(
    spec: RuntimeArtifactSpec,
    *,
    runtime_ingress_network: str,
    airflow_image: str,
    airflow_database_host: str,
    airflow_database_port: int,
    warehouse_host: str,
    warehouse_port: int,
) -> None:
    _validate_project_id(spec.project_id)
    _validate_generation(spec.generation)
    validate_runtime_parameters(spec.parameters)

    expected_compose_name = f"conductor-p-{spec.project_id}"
    expected_database_name = f"conductor_airflow_{spec.project_id}"
    if spec.compose_project_name != expected_compose_name:
        raise ValueError("compose_project_name does not match the immutable project identity")
    if spec.airflow_db_name != expected_database_name or spec.airflow_db_role != expected_database_name:
        raise ValueError("Airflow database identity does not match the immutable project identity")
    expected_warehouse_name = f"conductor_warehouse_{spec.project_id}"
    if spec.warehouse_db_name != expected_warehouse_name or spec.warehouse_db_role != expected_warehouse_name:
        raise ValueError("Warehouse database identity does not match the immutable project identity")
    if spec.warehouse_schema != "analytics":
        raise ValueError("Warehouse schema must be the supported analytics schema")
    if not _SLUG_PATTERN.fullmatch(spec.project_slug):
        raise ValueError("project_slug must be a canonical project slug")

    _airflow_external_host(spec.airflow_external_url)
    _validate_runtime_ingress_network(runtime_ingress_network)
    _validate_airflow_image(airflow_image)
    _validate_airflow_database_host(airflow_database_host)
    _validate_airflow_database_port(airflow_database_port)
    _validate_airflow_database_host(warehouse_host)
    _validate_airflow_database_port(warehouse_port)

    for name, value in _allowlisted_env(
        spec,
        runtime_ingress_network=runtime_ingress_network,
        airflow_image=airflow_image,
        airflow_database_host=airflow_database_host,
        airflow_database_port=airflow_database_port,
        warehouse_host=warehouse_host,
        warehouse_port=warehouse_port,
    ).items():
        if not value or "\x00" in value or "\n" in value or "\r" in value:
            raise ValueError(f"{name} must be a non-empty single-line value")


def _allowlisted_env(
    spec: RuntimeArtifactSpec,
    *,
    runtime_ingress_network: str,
    airflow_image: str,
    airflow_database_host: str,
    airflow_database_port: int,
    warehouse_host: str,
    warehouse_port: int,
) -> dict[str, str]:
    """Return the only values the static template may interpolate."""

    airflow_host = _airflow_external_host(spec.airflow_external_url)
    return {
        "COMPOSE_PROJECT_NAME": spec.compose_project_name,
        "CONDUCTOR_PROJECT_ID": spec.project_id,
        "CONDUCTOR_TEMPLATE_VERSION": spec.template_version,
        "CONDUCTOR_RUNTIME_INGRESS_NETWORK": runtime_ingress_network,
        "AIRFLOW_IMAGE": airflow_image,
        "PROJECT_SLUG": spec.project_slug,
        "AIRFLOW_EXTERNAL_HOST": airflow_host,
        "AIRFLOW_INTERNAL_ALIAS": f"airflow-{spec.project_id}",
        "AIRFLOW_DATABASE_HOST": airflow_database_host,
        "AIRFLOW_DATABASE_PORT": str(airflow_database_port),
        "AIRFLOW_DB_NAME": spec.airflow_db_name,
        "AIRFLOW_DB_ROLE": spec.airflow_db_role,
        "AIRFLOW_DB_PASSWORD_URLENCODED": quote(spec.airflow_db_password, safe=""),
        "AIRFLOW_ADMIN_USER": spec.airflow_admin_user,
        "AIRFLOW_ADMIN_PASSWORD": spec.airflow_admin_password,
        "AIRFLOW_DEV_USER": spec.airflow_dev_user,
        "AIRFLOW_DEV_PASSWORD": spec.airflow_dev_password,
        "AIRFLOW_VIEWER_USER": spec.airflow_viewer_user,
        "AIRFLOW_VIEWER_PASSWORD": spec.airflow_viewer_password,
        "AIRFLOW_INTEGRATION_USER": spec.airflow_integration_user,
        "AIRFLOW_INTEGRATION_PASSWORD": spec.airflow_integration_password,
        "WAREHOUSE_HOST": warehouse_host,
        "WAREHOUSE_PORT": str(warehouse_port),
        "WAREHOUSE_DB_NAME": spec.warehouse_db_name,
        "WAREHOUSE_DB_ROLE": spec.warehouse_db_role,
        "WAREHOUSE_DB_PASSWORD_URLENCODED": quote(spec.warehouse_db_password, safe=""),
        "WAREHOUSE_SCHEMA": spec.warehouse_schema,
        "CONDUCTOR_RUNTIME_SUBPATH": f"{spec.project_id}/{spec.generation}",
    }


def _validate_runtime_ingress_network(value: str) -> None:
    if not isinstance(value, str) or not _NETWORK_NAME_PATTERN.fullmatch(value):
        raise ValueError("runtime_ingress_network must be a canonical Docker network name")


def _validate_airflow_image(value: str) -> None:
    if not isinstance(value, str) or not _IMAGE_REFERENCE_PATTERN.fullmatch(value):
        raise ValueError("airflow_image must be a canonical container image reference")


def _validate_airflow_database_host(value: str) -> None:
    if (
        not isinstance(value, str)
        or len(value) > 253
        or any(not _DNS_LABEL_PATTERN.fullmatch(label) for label in value.split("."))
    ):
        raise ValueError("airflow_database_host must be a canonical DNS hostname")


def _validate_airflow_database_port(value: int) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= 65535:
        raise ValueError("airflow_database_port must be between 1 and 65535")


def _serialize_env(values: dict[str, str]) -> str:
    return "".join(f"{name}='{_escape_compose_env_literal(value)}'\n" for name, value in values.items())


def _escape_compose_env_literal(value: str) -> str:
    return value.replace("\\", "\\\\").replace("'", "\\'")


def _airflow_external_host(value: str) -> str:
    """Return a DNS-safe host for interpolation into a Traefik rule."""

    try:
        parsed_url = urlsplit(value)
        port = parsed_url.port
    except ValueError as exc:
        raise ValueError("airflow_external_url must use a canonical HTTPS hostname") from exc

    host = parsed_url.hostname
    if (
        parsed_url.scheme != "https"
        or not host
        or parsed_url.username
        or parsed_url.password
        or port is not None
        or parsed_url.path
        or parsed_url.query
        or parsed_url.fragment
        or len(host) > 253
        or any(not _DNS_LABEL_PATTERN.fullmatch(label) for label in host.split("."))
    ):
        raise ValueError("airflow_external_url must use a canonical HTTPS hostname")
    return host


def _open_private_runtime_root(path: Path) -> int:
    try:
        path_stat = path.lstat()
    except FileNotFoundError:
        path.mkdir(mode=0o700, parents=True)
    else:
        if stat.S_ISLNK(path_stat.st_mode):
            raise ValueError("CONDUCTOR_RUNTIME_ROOT must not be a symlink")
        if not stat.S_ISDIR(path_stat.st_mode):
            raise ValueError("CONDUCTOR_RUNTIME_ROOT must be a directory")
    return _open_private_directory(path)


def _ensure_private_generation_directory(
    root: Path,
    project_id: str,
    generation: int,
    *,
    owner_uid: int,
    owner_gid: int,
) -> Path:
    """Create a no-follow project/generation directory under one trusted root."""

    _validate_project_id(project_id)
    _validate_generation(generation)
    root_descriptor = _open_private_runtime_root(root)
    try:
        project_descriptor = _open_private_child_directory(
            root_descriptor, project_id, owner_uid=owner_uid, owner_gid=owner_gid
        )
        try:
            generation_descriptor = _open_private_child_directory(
                project_descriptor, str(generation), owner_uid=owner_uid, owner_gid=owner_gid
            )
        finally:
            os.close(project_descriptor)
    finally:
        os.close(root_descriptor)
    os.close(generation_descriptor)
    return root / project_id / str(generation)


def _open_private_child_directory(
    parent_descriptor: int,
    name: str,
    *,
    owner_uid: int | None = None,
    owner_gid: int | None = None,
) -> int:
    try:
        os.mkdir(name, mode=0o700, dir_fd=parent_descriptor)
    except FileExistsError:
        pass
    try:
        child_stat = os.stat(name, dir_fd=parent_descriptor, follow_symlinks=False)
    except FileNotFoundError as exc:
        raise ValueError("Runtime artifact directory disappeared during creation") from exc
    if stat.S_ISLNK(child_stat.st_mode):
        raise ValueError("Runtime artifact path component must not be a symlink")
    if not stat.S_ISDIR(child_stat.st_mode):
        raise ValueError("Runtime artifact path component must be a directory")
    descriptor = _open_private_directory(name, dir_fd=parent_descriptor)
    if owner_uid is not None and owner_gid is not None:
        _set_runtime_owner(descriptor, owner_uid, owner_gid)
    return descriptor


def _open_private_directory(path: str | Path, *, dir_fd: int | None = None) -> int:
    flags = os.O_RDONLY | os.O_DIRECTORY | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags, dir_fd=dir_fd)
    except OSError as exc:
        raise ValueError("Runtime artifact path component must not be a symlink") from exc
    try:
        if not stat.S_ISDIR(os.fstat(descriptor).st_mode):
            raise ValueError("Runtime artifact path component must be a directory")
        os.fchmod(descriptor, 0o700)
    except Exception:
        os.close(descriptor)
        raise
    return descriptor


def _atomic_write(
    destination: Path,
    content: bytes,
    *,
    owner_uid: int | None = None,
    owner_gid: int | None = None,
) -> None:
    temporary = destination.with_name(f".{destination.name}.{uuid4().hex}.tmp")
    try:
        with temporary.open("xb", buffering=0) as file_handle:
            os.chmod(temporary, 0o600)
            file_handle.write(content)
            file_handle.flush()
            os.fsync(file_handle.fileno())
        os.replace(temporary, destination)
        os.chmod(destination, 0o600)
        if owner_uid is not None and owner_gid is not None:
            _set_runtime_owner_path(destination, owner_uid, owner_gid)
        _fsync_directory(destination.parent)
    finally:
        if temporary.exists():
            temporary.unlink()


def _fsync_directory(directory: Path) -> None:
    descriptor = os.open(directory, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _set_runtime_owner(descriptor: int, uid: int, gid: int) -> None:
    """Make project volume subpaths readable by the fixed Airflow identity.

    The lifecycle worker runs as root. Unit tests run as an unprivileged user,
    so they preserve their temporary-file ownership while exercising the same
    no-follow and mode semantics.
    """

    if os.geteuid() == 0:
        os.fchown(descriptor, uid, gid)
    os.fchmod(descriptor, 0o700)


def _set_runtime_owner_path(path: Path, uid: int, gid: int) -> None:
    if os.geteuid() == 0:
        os.chown(path, uid, gid, follow_symlinks=False)
    os.chmod(path, 0o600)


def _publish_generation_atomically(
    *,
    runtime_directory: Path,
    compose_content: bytes,
    env_content: bytes,
) -> None:
    """Publish both private files together, or retain the prior immutable generation."""

    parent = runtime_directory.parent
    _reject_symlink(runtime_directory, "Runtime artifact generation")
    if runtime_directory.exists():
        _verify_existing_generation(runtime_directory, compose_content, env_content)
        return

    staging_directory = parent / f".{runtime_directory.name}.{uuid4().hex}.tmp"
    try:
        staging_directory.mkdir(mode=0o700)
        staging_directory.chmod(0o700)
        _atomic_write(staging_directory / "compose.yaml", compose_content)
        _atomic_write(staging_directory / ".env", env_content)
        os.replace(staging_directory, runtime_directory)
        _fsync_directory(parent)
    except FileExistsError:
        _reject_symlink(runtime_directory, "Runtime artifact generation")
        if runtime_directory.exists():
            _verify_existing_generation(runtime_directory, compose_content, env_content)
            return
        raise
    finally:
        if staging_directory.exists():
            shutil.rmtree(staging_directory)


def _verify_existing_generation(
    runtime_directory: Path,
    compose_content: bytes,
    env_content: bytes,
) -> None:
    _reject_symlink(runtime_directory, "Runtime artifact generation")
    compose_path = runtime_directory / "compose.yaml"
    env_path = runtime_directory / ".env"
    descriptors: list[int] = []
    try:
        compose_descriptor = _open_existing_regular_file(compose_path)
        descriptors.append(compose_descriptor)
        env_descriptor = _open_existing_regular_file(env_path)
        descriptors.append(env_descriptor)
        if (
            not runtime_directory.is_dir()
            or _read_file_descriptor(compose_descriptor) != compose_content
            or _read_file_descriptor(env_descriptor) != env_content
        ):
            raise ValueError("Runtime generation already exists with different artifacts")
        runtime_directory.chmod(0o700)
        os.fchmod(compose_descriptor, 0o600)
        os.fchmod(env_descriptor, 0o600)
    finally:
        for descriptor in descriptors:
            os.close(descriptor)


def _open_existing_regular_file(path: Path) -> int:
    try:
        path_stat = path.lstat()
    except FileNotFoundError as exc:
        raise ValueError("Runtime artifact generation is incomplete") from exc
    if stat.S_ISLNK(path_stat.st_mode):
        raise ValueError("Runtime artifact file must not be a symlink")

    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise ValueError("Runtime artifact file must not be a symlink") from exc
    try:
        if not stat.S_ISREG(os.fstat(descriptor).st_mode):
            raise ValueError("Runtime artifact file must be a regular file")
    except Exception:
        os.close(descriptor)
        raise
    return descriptor


def _read_file_descriptor(descriptor: int) -> bytes:
    chunks: list[bytes] = []
    while chunk := os.read(descriptor, 1024 * 1024):
        chunks.append(chunk)
    return b"".join(chunks)


def _reject_symlink(path: Path, description: str) -> None:
    try:
        path_stat = path.lstat()
    except FileNotFoundError:
        return
    if stat.S_ISLNK(path_stat.st_mode):
        raise ValueError(f"{description} must not be a symlink")
