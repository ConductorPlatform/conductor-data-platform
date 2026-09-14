from __future__ import annotations

import json
import os
import shutil
import stat
import subprocess
from dataclasses import replace
from pathlib import Path
from urllib.parse import quote

import pytest

from app.services.lifecycle_errors import InvalidParametersError, InvalidTemplateError
from app.services.runtime_artifacts import (
    RuntimeArtifactSpec,
    RuntimeArtifactWriter,
    runtime_artifact_metadata,
)

PROJECT_ID = "0123456789abcdef0123456789abcdef"
FIXTURE_SECRETS = (
    "fixture-airflow-db-password:/?#[]!$&'()*+,;=%%",
    "fixture-airflow-admin-password",
    "fixture-airflow-dev-password",
    "fixture-airflow-viewer-password",
    "fixture-airflow-integration-password",
    "fixture-warehouse-password:/?#[]!$&'()*+,;=%%",
)
TEMPLATE_PATH = Path(__file__).resolve().parents[1] / "app/runtime_templates/v1/compose.yaml"
ENV_FIXTURE_PATH = Path(__file__).resolve().parent / "fixtures/runtime-v1.env"


@pytest.fixture
def runtime_spec() -> RuntimeArtifactSpec:
    return RuntimeArtifactSpec(
        project_id=PROJECT_ID,
        generation=3,
        template_version="v1",
        compose_project_name=f"conductor-p-{PROJECT_ID}",
        project_slug="analytics",
        airflow_external_url="https://analytics.airflow.example.test",
        airflow_db_name=f"conductor_airflow_{PROJECT_ID}",
        airflow_db_role=f"conductor_airflow_{PROJECT_ID}",
        airflow_db_password=FIXTURE_SECRETS[0],
        airflow_admin_user="admin",
        airflow_admin_password=FIXTURE_SECRETS[1],
        airflow_dev_user="dev",
        airflow_dev_password=FIXTURE_SECRETS[2],
        airflow_viewer_user="viewer",
        airflow_viewer_password=FIXTURE_SECRETS[3],
        airflow_integration_user="integration",
        airflow_integration_password=FIXTURE_SECRETS[4],
        warehouse_db_name=f"conductor_warehouse_{PROJECT_ID}",
        warehouse_db_role=f"conductor_warehouse_{PROJECT_ID}",
        warehouse_db_password=FIXTURE_SECRETS[5],
        warehouse_schema="analytics",
        parameters={},
    )


def test_runtime_artifact_path_rejects_path_traversal_and_invalid_generations(tmp_path: Path) -> None:
    writer = RuntimeArtifactWriter(runtime_root=tmp_path)

    for project_id in ("../project", "a" * 31, "A" * 32):
        with pytest.raises(ValueError, match="32 lowercase hexadecimal"):
            writer.runtime_directory(project_id=project_id, generation=1)
    for generation in (0, -1, True, "../1"):
        with pytest.raises(ValueError, match="generation"):
            writer.runtime_directory(project_id=PROJECT_ID, generation=generation)  # type: ignore[arg-type]


def test_template_lookup_rejects_unknown_or_path_like_versions(tmp_path: Path, runtime_spec: RuntimeArtifactSpec) -> None:
    writer = RuntimeArtifactWriter(runtime_root=tmp_path)

    for template_version in ("v2", "../v1", "v1/../../etc", ""):
        with pytest.raises(InvalidTemplateError):
            writer.render(replace(runtime_spec, template_version=template_version))


def test_template_lookup_rejects_non_allowlisted_template_even_when_installed(
    tmp_path: Path, runtime_spec: RuntimeArtifactSpec
) -> None:
    template_root = tmp_path / "templates"
    installed_v2_template = template_root / "v2" / "compose.yaml"
    installed_v2_template.parent.mkdir(parents=True)
    installed_v2_template.write_text("services: {}\n")
    writer = RuntimeArtifactWriter(runtime_root=tmp_path / "runtime", template_root=template_root)

    with pytest.raises(InvalidTemplateError, match="not allowlisted"):
        writer.render(replace(runtime_spec, template_version="v2"))


def test_runtime_artifacts_reject_forbidden_template_parameters(
    tmp_path: Path, runtime_spec: RuntimeArtifactSpec
) -> None:
    writer = RuntimeArtifactWriter(runtime_root=tmp_path)

    with pytest.raises(InvalidParametersError, match="empty object"):
        writer.render(replace(runtime_spec, parameters={"command": ["sh", "-c", "id"]}))


def test_runtime_root_and_artifact_directories_are_private(tmp_path: Path, runtime_spec: RuntimeArtifactSpec) -> None:
    artifact = RuntimeArtifactWriter(runtime_root=tmp_path).render(runtime_spec)

    assert stat.S_IMODE(tmp_path.stat().st_mode) == 0o700
    assert stat.S_IMODE((tmp_path / PROJECT_ID).stat().st_mode) == 0o700
    assert stat.S_IMODE(artifact.runtime_directory.stat().st_mode) == 0o700


def test_runtime_git_token_is_private_and_rejects_symlink_escape(
    tmp_path: Path, runtime_spec: RuntimeArtifactSpec
) -> None:
    writer = RuntimeArtifactWriter(runtime_root=tmp_path)
    artifact = writer.render(runtime_spec)

    token_path = writer.write_git_token(
        project_id=runtime_spec.project_id,
        generation=runtime_spec.generation,
        token="test-git-token",
    )

    assert token_path.parent == artifact.runtime_directory
    assert token_path.read_text() == "test-git-token"
    assert stat.S_IMODE(token_path.stat().st_mode) == 0o600
    token_path.unlink()
    token_path.symlink_to(tmp_path / "outside-token")
    with pytest.raises(ValueError, match="must not be a symlink"):
        writer.write_git_token(
            project_id=runtime_spec.project_id,
            generation=runtime_spec.generation,
            token="another-token",
        )


@pytest.mark.parametrize("symlink_component", ["runtime-root", "project-id", "generation"])
def test_runtime_artifacts_reject_preseeded_symlink_components(
    tmp_path: Path,
    runtime_spec: RuntimeArtifactSpec,
    symlink_component: str,
) -> None:
    outside_directory = tmp_path / "outside"
    outside_directory.mkdir()
    runtime_root = tmp_path / "runtime"
    if symlink_component == "runtime-root":
        runtime_root.symlink_to(outside_directory, target_is_directory=True)
    elif symlink_component == "project-id":
        runtime_root.mkdir()
        (runtime_root / PROJECT_ID).symlink_to(outside_directory, target_is_directory=True)
    else:
        expected_artifact = RuntimeArtifactWriter(runtime_root=tmp_path / "expected").render(runtime_spec)
        shutil.copytree(expected_artifact.runtime_directory, outside_directory, dirs_exist_ok=True)
        runtime_root.mkdir()
        project_directory = runtime_root / PROJECT_ID
        project_directory.mkdir()
        (project_directory / str(runtime_spec.generation)).symlink_to(outside_directory, target_is_directory=True)

    writer = RuntimeArtifactWriter(runtime_root=runtime_root)

    with pytest.raises(ValueError, match="symlink"):
        writer.render(runtime_spec)

    assert not (outside_directory / "3").exists()


def test_existing_generation_rejects_content_identical_leaf_file_symlinks(
    tmp_path: Path, runtime_spec: RuntimeArtifactSpec
) -> None:
    writer = RuntimeArtifactWriter(runtime_root=tmp_path)
    artifact = writer.render(runtime_spec)
    outside_compose = tmp_path / "outside-compose.yaml"
    outside_env = tmp_path / "outside.env"
    outside_compose.write_bytes(artifact.compose_path.read_bytes())
    outside_env.write_bytes(artifact.env_path.read_bytes())
    artifact.compose_path.unlink()
    artifact.env_path.unlink()
    artifact.compose_path.symlink_to(outside_compose)
    artifact.env_path.symlink_to(outside_env)

    with pytest.raises(ValueError, match="must not be a symlink"):
        writer.render(runtime_spec)


def test_runtime_env_serialization_preserves_compose_literals_and_uri_encodes_database_password(
    tmp_path: Path, runtime_spec: RuntimeArtifactSpec
) -> None:
    spec = replace(
        runtime_spec,
        airflow_admin_user=r"admin$literal'\\path",
        airflow_db_password="pa@ss word?",
    )
    artifact = RuntimeArtifactWriter(runtime_root=tmp_path).render(spec)
    rendered_env = artifact.env_path.read_text()

    assert r"AIRFLOW_ADMIN_USER='admin$literal\'\\\\path'" in rendered_env
    assert "AIRFLOW_DB_PASSWORD_URLENCODED='pa%40ss%20word%3F'" in rendered_env
    assert "AIRFLOW_DB_PASSWORD=" not in rendered_env


def test_runtime_artifacts_allow_a_trusted_task_specific_ingress_network(
    tmp_path: Path, runtime_spec: RuntimeArtifactSpec
) -> None:
    artifact = RuntimeArtifactWriter(
        runtime_root=tmp_path,
        runtime_ingress_network="conductor-t35719905-runtime-ingress",
    ).render(runtime_spec)

    assert "CONDUCTOR_RUNTIME_INGRESS_NETWORK='conductor-t35719905-runtime-ingress'" in artifact.env_path.read_text()


def test_runtime_artifacts_allow_a_trusted_airflow_database_endpoint(
    tmp_path: Path, runtime_spec: RuntimeArtifactSpec
) -> None:
    artifact = RuntimeArtifactWriter(
        runtime_root=tmp_path,
        airflow_database_host="host.docker.internal",
        airflow_database_port=15432,
    ).render(runtime_spec)

    rendered_env = artifact.env_path.read_text()
    assert "AIRFLOW_DATABASE_HOST='host.docker.internal'" in rendered_env
    assert "AIRFLOW_DATABASE_PORT='15432'" in rendered_env


@pytest.mark.parametrize("host, port", [("bad/host", 5432), ("host.docker.internal", 0)])
def test_runtime_artifacts_reject_unsafe_airflow_database_endpoints(
    tmp_path: Path, runtime_spec: RuntimeArtifactSpec, host: str, port: int
) -> None:
    with pytest.raises(ValueError, match="airflow_database_(host|port)"):
        RuntimeArtifactWriter(
            runtime_root=tmp_path,
            airflow_database_host=host,
            airflow_database_port=port,
        ).render(runtime_spec)


@pytest.mark.parametrize("network", ["", "CONDUCTOR-INGRESS", "network/name", "network space"])
def test_runtime_artifacts_reject_unsafe_ingress_network_names(
    tmp_path: Path, runtime_spec: RuntimeArtifactSpec, network: str
) -> None:
    with pytest.raises(ValueError, match="canonical Docker network name"):
        RuntimeArtifactWriter(runtime_root=tmp_path, runtime_ingress_network=network).render(runtime_spec)


def test_template_uses_uri_encoded_database_password() -> None:
    template = TEMPLATE_PATH.read_text()

    assert "${AIRFLOW_DB_PASSWORD_URLENCODED}" in template
    assert "${AIRFLOW_DB_PASSWORD}" not in template


def test_generated_env_keeps_dollars_literal_and_uri_encodes_database_password(
    tmp_path: Path, runtime_spec: RuntimeArtifactSpec
) -> None:
    if shutil.which("docker") is None:
        pytest.skip("Docker CLI is required for Compose semantic validation")

    spec = replace(
        runtime_spec,
        airflow_admin_user="admin$HOST_SECRET",
        airflow_db_password="pa@ss word?",
    )
    env_path = RuntimeArtifactWriter(runtime_root=tmp_path).render(spec).env_path
    environment = os.environ | {"HOST_SECRET": "host-environment-leak"}
    result = subprocess.run(
        [
            "docker",
            "compose",
            "--env-file",
            str(env_path),
            "-f",
            str(TEMPLATE_PATH),
            "config",
            "--format",
            "json",
        ],
        check=False,
        capture_output=True,
        env=environment,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    config = json.loads(result.stdout)
    rendered_environment = config["services"]["airflow-init"]["environment"]
    assert "host-environment-leak" not in json.dumps(rendered_environment)
    assert "HOST_SECRET" in rendered_environment["AIRFLOW_ADMIN_USER"]
    assert "pa%40ss%20word%3F" in rendered_environment["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"]


@pytest.mark.parametrize(
    "airflow_external_url",
    [
        "https://good.example`) || PathPrefix(`/`) || Host(`attacker.example",
        "https://analytics..example.test",
        "https://-analytics.example.test",
        "https://analytics.example.test/with-path",
    ],
)
def test_runtime_artifacts_reject_unsafe_airflow_route_hostnames(
    tmp_path: Path, runtime_spec: RuntimeArtifactSpec, airflow_external_url: str
) -> None:
    writer = RuntimeArtifactWriter(runtime_root=tmp_path)

    with pytest.raises(ValueError, match="canonical HTTPS hostname"):
        writer.render(replace(runtime_spec, airflow_external_url=airflow_external_url))


def test_airflow_init_uses_container_environment_not_compose_interpolated_credentials() -> None:
    template = TEMPLATE_PATH.read_text()
    init_service = template.split("  airflow-api-server:", maxsplit=1)[0]

    for variable in (
        "AIRFLOW_ADMIN_USER",
        "AIRFLOW_ADMIN_PASSWORD",
        "AIRFLOW_DEV_USER",
        "AIRFLOW_DEV_PASSWORD",
        "AIRFLOW_VIEWER_USER",
        "AIRFLOW_VIEWER_PASSWORD",
        "AIRFLOW_INTEGRATION_USER",
        "AIRFLOW_INTEGRATION_PASSWORD",
    ):
        assert f"{variable}: ${{{variable}}}" in init_service
    assert "AIRFLOW__CORE__AUTH_MANAGER" in init_service
    assert "airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager" in init_service
    assert "python /home/airflow/bootstrap_airflow_users.py" in init_service
    assert "${AIRFLOW_IMAGE:-conductor-airflow:latest}" in template


def test_canonical_airflow_image_includes_fab_and_resumable_cli_bootstrap() -> None:
    dockerfile = (TEMPLATE_PATH.parents[4] / "docker" / "airflow" / "Dockerfile").read_text()
    bootstrap = (TEMPLATE_PATH.parents[4] / "docker" / "airflow" / "bootstrap_airflow_users.py").read_text()

    assert '"apache-airflow-providers-fab"' in dockerfile
    assert "bootstrap_airflow_users.py" in dockerfile
    assert '"users",\n            "create",' in bootstrap
    assert '"users", "list", "--output", "json"' in bootstrap


def test_runtime_artifact_resolves_configured_airflow_image(
    tmp_path: Path, runtime_spec: RuntimeArtifactSpec
) -> None:
    if shutil.which("docker") is None:
        pytest.skip("Docker CLI is required for Compose semantic validation")

    airflow_image = "registry.example.test/conductor-airflow:acceptance"
    artifact = RuntimeArtifactWriter(runtime_root=tmp_path, airflow_image=airflow_image).render(runtime_spec)
    result = subprocess.run(
        [
            "docker",
            "compose",
            "--env-file",
            str(artifact.env_path),
            "-f",
            str(artifact.compose_path),
            "config",
            "--format",
            "json",
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    resolved = json.loads(result.stdout)
    assert resolved["services"]["airflow-init"]["image"] == airflow_image


def test_failed_second_artifact_write_never_publishes_a_partial_generation(
    tmp_path: Path, runtime_spec: RuntimeArtifactSpec, monkeypatch: pytest.MonkeyPatch
) -> None:
    writer = RuntimeArtifactWriter(runtime_root=tmp_path)
    spec = replace(runtime_spec, generation=4)
    real_write = __import__("app.services.runtime_artifacts", fromlist=["_atomic_write"])._atomic_write

    def fail_env_write(destination: Path, content: bytes) -> None:
        if destination.name == ".env":
            raise OSError("fixture env write failure")
        real_write(destination, content)

    monkeypatch.setattr("app.services.runtime_artifacts._atomic_write", fail_env_write)

    with pytest.raises(OSError, match="fixture env write failure"):
        writer.render(spec)

    assert not (tmp_path / PROJECT_ID / "4").exists()


def test_runtime_artifacts_are_atomic_private_and_deterministic(
    tmp_path: Path, runtime_spec: RuntimeArtifactSpec, monkeypatch: pytest.MonkeyPatch
) -> None:
    writer = RuntimeArtifactWriter(runtime_root=tmp_path)
    replace_calls: list[tuple[Path, Path]] = []
    real_replace = __import__("os").replace

    def recording_replace(source: Path, destination: Path) -> None:
        replace_calls.append((source, destination))
        real_replace(source, destination)

    monkeypatch.setattr("app.services.runtime_artifacts.os.replace", recording_replace)

    first = writer.render(runtime_spec)
    first_compose = first.compose_path.read_bytes()
    first_env = first.env_path.read_bytes()
    second = writer.render(runtime_spec)

    assert second.runtime_directory == tmp_path / PROJECT_ID / "3"
    assert first_compose == second.compose_path.read_bytes()
    assert first_env == second.env_path.read_bytes()
    assert {destination.name for _, destination in replace_calls} == {"compose.yaml", ".env", "3"}
    assert len(replace_calls) == 3
    assert stat.S_IMODE(second.runtime_directory.stat().st_mode) == 0o700
    assert stat.S_IMODE(second.compose_path.stat().st_mode) == 0o600
    assert stat.S_IMODE(second.env_path.stat().st_mode) == 0o600
    assert not list(second.runtime_directory.glob(".*.tmp"))


def test_runtime_artifact_metadata_is_reconstructible_and_secret_free(
    tmp_path: Path, runtime_spec: RuntimeArtifactSpec
) -> None:
    artifact = RuntimeArtifactWriter(runtime_root=tmp_path).render(runtime_spec)
    metadata = runtime_artifact_metadata(artifact)
    rendered_compose = artifact.compose_path.read_text()

    assert metadata == {
        "project_id": PROJECT_ID,
        "generation": 3,
        "template_version": "v1",
        "compose_path": str(artifact.compose_path),
    }
    for secret in FIXTURE_SECRETS:
        assert secret not in rendered_compose
        assert secret not in json.dumps(metadata)


def test_trusted_template_has_required_normalized_compose_semantics() -> None:
    if shutil.which("docker") is None:
        pytest.skip("Docker CLI is required for Compose semantic validation")

    result = subprocess.run(
        [
            "docker",
            "compose",
            "--env-file",
            str(ENV_FIXTURE_PATH),
            "-f",
            str(TEMPLATE_PATH),
            "config",
            "--format",
            "json",
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    config = json.loads(result.stdout)

    assert set(config["services"]) == {
        "project-redis",
        "airflow-init",
        "airflow-api-server",
        "airflow-scheduler",
        "airflow-dag-processor",
        "airflow-worker",
    }
    assert "postgres" not in config["services"]
    assert "traefik" not in config["services"]
    assert set(config["volumes"]) == {"dags", "logs"}
    assert set(config["networks"]) == {"default", "ingress"}
    assert config["networks"]["default"]["name"] == f"conductor-p-{PROJECT_ID}_default"
    assert config["networks"]["default"].get("internal") is not True
    assert config["networks"]["ingress"]["external"] is True
    assert config["networks"]["ingress"]["name"] == "conductor-runtime-ingress"

    for resource in [*config["services"].values(), *config["volumes"].values(), config["networks"]["default"]]:
        labels = resource["labels"]
        assert labels["conductor.managed"] == "true"
        assert labels["conductor.project_id"] == PROJECT_ID
        assert labels["conductor.template_version"] == "v1"

    for service in config["services"].values():
        assert "ports" not in service

    api_service = config["services"]["airflow-api-server"]
    assert api_service["labels"]["traefik.enable"] == "false"
    assert api_service["networks"]["ingress"]["aliases"] == [f"airflow-{PROJECT_ID}"]

    init_service = config["services"]["airflow-init"]
    assert init_service["extra_hosts"] in (
        ["host.docker.internal:host-gateway"],
        ["host.docker.internal=host-gateway"],
    )
    init_command = " ".join(init_service["command"])
    for secret in FIXTURE_SECRETS[1:]:
        assert secret not in init_command
    assert "python /home/airflow/bootstrap_airflow_users.py" in init_command
    result_backend = init_service["environment"]["AIRFLOW__CELERY__RESULT_BACKEND"]
    assert result_backend == (
        f"db+postgresql://conductor_airflow_{PROJECT_ID}:"
        f"{quote(FIXTURE_SECRETS[0], safe='')}@host.docker.internal:5432/"
        f"conductor_airflow_{PROJECT_ID}"
    )

    assert "workspace-session-manager" not in config["services"]


def test_template_labels_and_non_secret_artifact_outputs_never_leak_fixture_secrets(
    tmp_path: Path, runtime_spec: RuntimeArtifactSpec
) -> None:
    if shutil.which("docker") is None:
        pytest.skip("Docker CLI is required for Compose semantic validation")

    artifact = RuntimeArtifactWriter(runtime_root=tmp_path).render(runtime_spec)
    result = subprocess.run(
        [
            "docker",
            "compose",
            "--env-file",
            str(ENV_FIXTURE_PATH),
            "-f",
            str(TEMPLATE_PATH),
            "config",
            "--format",
            "json",
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    config = json.loads(result.stdout)
    non_secret_outputs = [
        json.dumps(
            {
                "labels": [service["labels"] for service in config["services"].values()],
                "volume_labels": [volume["labels"] for volume in config["volumes"].values()],
                "network_labels": config["networks"]["default"]["labels"],
            },
            sort_keys=True,
        ),
        json.dumps(runtime_artifact_metadata(artifact), sort_keys=True),
        artifact.compose_path.read_text(),
    ]
    for secret in FIXTURE_SECRETS:
        assert all(secret not in value for value in non_secret_outputs)
