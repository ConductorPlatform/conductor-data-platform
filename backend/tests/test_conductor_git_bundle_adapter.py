from __future__ import annotations

import importlib.util
import os
import sys
from contextlib import contextmanager
from pathlib import Path
from types import ModuleType
from uuid import uuid4


def _load_bundle_module() -> ModuleType:
    class GitHook:
        @contextmanager
        def configure_hook_env(self):
            yield

    class GitDagBundle:
        pass

    class BaseHook:
        @staticmethod
        def get_connection(_connection_id):
            raise AssertionError("connection lookup is not part of this adapter regression")

    modules = {
        "airflow": ModuleType("airflow"),
        "airflow.providers": ModuleType("airflow.providers"),
        "airflow.providers.git": ModuleType("airflow.providers.git"),
        "airflow.providers.git.bundles": ModuleType("airflow.providers.git.bundles"),
        "airflow.providers.git.bundles.git": ModuleType("airflow.providers.git.bundles.git"),
        "airflow.providers.git.hooks": ModuleType("airflow.providers.git.hooks"),
        "airflow.providers.git.hooks.git": ModuleType("airflow.providers.git.hooks.git"),
        "airflow.providers.common": ModuleType("airflow.providers.common"),
        "airflow.providers.common.compat": ModuleType("airflow.providers.common.compat"),
        "airflow.providers.common.compat.sdk": ModuleType("airflow.providers.common.compat.sdk"),
    }
    modules["airflow.providers.git.bundles.git"].GitDagBundle = GitDagBundle
    modules["airflow.providers.git.hooks.git"].GitHook = GitHook
    modules["airflow.providers.common.compat.sdk"].BaseHook = BaseHook
    previous = {name: sys.modules.get(name) for name in modules}
    sys.modules.update(modules)
    try:
        source = Path(__file__).resolve().parents[2] / "docker/airflow/conductor_git_bundle.py"
        module_name = f"conductor_git_bundle_test_{uuid4().hex}"
        spec = importlib.util.spec_from_file_location(module_name, source)
        assert spec is not None and spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module
    finally:
        for name, prior in previous.items():
            if prior is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = prior


def test_private_https_hook_applies_askpass_to_process_and_native_hook_env(tmp_path, monkeypatch) -> None:
    module = _load_bundle_module()
    token_file = tmp_path / "git-token"
    token_file.write_text("tracked-private-token")
    token_file.chmod(0o600)
    monkeypatch.setattr(module, "_TOKEN_FILE", token_file)
    hook = module.ConductorGitHook.__new__(module.ConductorGitHook)
    hook.repo_url = "https://git.example.test/team/project.git"
    hook.user_name = "oauth2"
    hook.env = {"GIT_SSH_COMMAND": "ssh -o StrictHostKeyChecking=yes"}
    original = {name: os.environ.get(name) for name in ("GIT_ASKPASS", "CONDUCTOR_GIT_TOKEN_FILE", "GIT_TERMINAL_PROMPT")}

    with hook.configure_hook_env():
        assert hook.env["CONDUCTOR_GIT_TOKEN_FILE"] == str(token_file)
        assert hook.env["GIT_TERMINAL_PROMPT"] == "0"
        assert os.environ["CONDUCTOR_GIT_TOKEN_FILE"] == str(token_file)
        assert os.environ["GIT_TERMINAL_PROMPT"] == "0"
        askpass = Path(os.environ["GIT_ASKPASS"])
        assert askpass.is_file()
        assert "tracked-private-token" not in askpass.read_text()

    assert hook.env == {"GIT_SSH_COMMAND": "ssh -o StrictHostKeyChecking=yes"}
    for name, value in original.items():
        assert os.environ.get(name) == value
