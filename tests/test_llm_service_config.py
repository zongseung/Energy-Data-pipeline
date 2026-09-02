import re
import secrets
import shutil
import stat
import subprocess
from pathlib import Path


COMPOSE = Path("docker/llm-demo/compose.yml").read_text()
LOADER = Path("docker/llm-demo/load-secrets.sh")
PROVISIONER = Path("docker/llm-demo/provision-secrets.sh")
MONGO_INIT = Path("docker/llm-demo/init-mongo-users.js")
SECRET_DIR = Path("/tmp/energy-llm-plan-secrets")


def test_sensitive_values_are_not_compose_environment_or_env_file():
    assert "env_file:" not in COMPOSE
    for name in (
        "OPENAI_API_KEY",
        "JWT_SECRET",
        "JWT_REFRESH_SECRET",
        "CREDS_KEY",
        "CREDS_IV",
        "ENERGY_MCP_DSN",
        "DB_PASSWORD",
    ):
        assert not re.search(rf"^\s+{name}:\s+", COMPOSE, re.M)


def test_services_mount_secrets_and_use_loader():
    assert LOADER.exists()
    assert COMPOSE.count("/usr/local/bin/load-secrets:ro") >= 2
    assert "ENERGY_MCP_MODE: workflow" in COMPOSE
    assert "mongod --auth" in COMPOSE


def test_secret_loader_never_traces_or_prints_values():
    assert LOADER.exists()
    text = LOADER.read_text()
    assert "set -x" not in text
    assert "echo $" not in text
    assert "/run/secrets/" in text


def test_provisioner_rejects_bad_arguments_and_creates_owner_only_files():
    assert PROVISIONER.exists()
    assert subprocess.run(
        [PROVISIONER, "relative"], capture_output=True, text=True
    ).returncode != 0

    shutil.rmtree(SECRET_DIR, ignore_errors=True)
    try:
        stdin = f"{secrets.token_hex(16)}\n{secrets.token_hex(16)}\n"
        assert subprocess.run(
            [PROVISIONER, str(SECRET_DIR), "extra"],
            input=stdin,
            capture_output=True,
            text=True,
        ).returncode != 0
        result = subprocess.run(
            [PROVISIONER, str(SECRET_DIR)],
            input=stdin,
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0
        assert stat.S_IMODE(SECRET_DIR.stat().st_mode) == 0o700
        expected = {
            "jwt_secret",
            "jwt_refresh_secret",
            "creds_key",
            "creds_iv",
            "mongo_root_password",
            "librechat_mongo_password",
            "energy_mcp_mongo_password",
            "openai_api_key",
            "postgres_readonly_password",
            "energy_mcp_dsn",
        }
        assert {path.name for path in SECRET_DIR.iterdir()} == expected
        assert all(
            stat.S_IMODE(path.stat().st_mode) == 0o600
            for path in SECRET_DIR.iterdir()
        )
    finally:
        shutil.rmtree(SECRET_DIR, ignore_errors=True)


def test_mongo_bootstrap_is_idempotent_and_scopes_application_roles():
    assert MONGO_INIT.exists()
    text = MONGO_INIT.read_text()
    assert text.count("getUser(") == 2
    assert "admin.auth('root', rootPassword)" in text
    assert "ensureUser('LibreChat', 'librechat_app'" in text
    assert "ensureUser('energy_mcp', 'energy_mcp_app'" in text
    assert "console.log" not in text
    assert "print(" not in text


def test_compose_uses_required_secret_files_and_health_dependencies():
    source = '${LLM_SECRET_DIR:?LLM_SECRET_DIR\ub97c \uc124\uc815\ud558\uc138\uc694}'
    assert COMPOSE.count(source) == 10
    assert COMPOSE.count("condition: service_healthy") >= 3
    assert COMPOSE.count("healthcheck:") >= 3
    assert "mongo_root_password" in COMPOSE
    assert "librechat_mongo_password" in COMPOSE
    assert "energy_mcp_mongo_password" in COMPOSE


def test_mcp_image_installs_the_tracked_lockfile_and_loader():
    dockerfile = Path("docker/llm-demo/mcp.Dockerfile").read_text()
    assert "ghcr.io/astral-sh/uv:0.8.17" in dockerfile
    assert "uv sync --frozen --no-dev" in dockerfile
    assert "COPY mcp-server /opt/mcp-server" in dockerfile
    assert "COPY docker/llm-demo/load-secrets.sh" in dockerfile
