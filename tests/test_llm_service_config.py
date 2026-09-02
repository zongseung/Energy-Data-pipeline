import os
import re
import stat
import subprocess
from pathlib import Path

import yaml


COMPOSE_PATH = Path("docker/llm-demo/compose.yml")
COMPOSE = COMPOSE_PATH.read_text()
CONFIG = yaml.safe_load(COMPOSE)
LOADER = Path("docker/llm-demo/load-secrets.sh")
PROVISIONER = Path("docker/llm-demo/provision-secrets.sh")
MONGO_INIT = Path("docker/llm-demo/init-mongo-users.js")
MONGO_HEALTH = Path("docker/llm-demo/mongo-healthcheck.js")
MONGO_BOOTSTRAP = Path("docker/llm-demo/bootstrap-mongo.sh")


def _write_loader_secrets(secret_dir: Path) -> dict[str, str]:
    values = {
        "openai_api_key": "dummy-openai-key ",
        "jwt_secret": "dummy-jwt",
        "jwt_refresh_secret": "dummy-refresh",
        "creds_key": "dummy-creds-key",
        "creds_iv": "dummy-creds-iv",
        "librechat_mongo_password": "a1b2c3d4",
        "energy_mcp_mongo_password": "d4c3b2a1",
        "postgres_readonly_password": "dummy db password ",
        "energy_mcp_dsn": "postgresql://demo_ro:dummy%20db%20password%20@pgbouncer:5432/pv",
    }
    secret_dir.mkdir(mode=0o700)
    for name, value in values.items():
        (secret_dir / name).write_text(value)
    return values


def _run_loader(secret_dir: Path, profile: str, check: str, values: dict[str, str]):
    env = os.environ.copy()
    env["LOAD_SECRETS_DIR"] = str(secret_dir)
    env.update({f"WANT_{name.upper()}": value for name, value in values.items()})
    return subprocess.run(
        [LOADER, profile, "/bin/sh", "-c", check],
        capture_output=True,
        text=True,
        env=env,
    )


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


def test_loader_profiles_execute_commands_and_preserve_trailing_spaces(tmp_path):
    values = _write_loader_secrets(tmp_path / "secrets")
    checks = {
        "librechat": """
            [ "$OPENAI_API_KEY" = "$WANT_OPENAI_API_KEY" ] &&
            [ "$JWT_SECRET" = "$WANT_JWT_SECRET" ] &&
            [ "$JWT_REFRESH_SECRET" = "$WANT_JWT_REFRESH_SECRET" ] &&
            [ "$CREDS_KEY" = "$WANT_CREDS_KEY" ] &&
            [ "$CREDS_IV" = "$WANT_CREDS_IV" ] &&
            [ "$MONGO_URI" = "mongodb://librechat_app:$WANT_LIBRECHAT_MONGO_PASSWORD@mongodb:27017/LibreChat?authSource=LibreChat" ]
        """,
        "energy-mcp": """
            [ "$OPENAI_API_KEY" = "$WANT_OPENAI_API_KEY" ] &&
            [ "$ENERGY_MCP_DSN" = "$WANT_ENERGY_MCP_DSN" ] &&
            [ "$ENERGY_MCP_MONGO_URI" = "mongodb://energy_mcp_app:$WANT_ENERGY_MCP_MONGO_PASSWORD@mongodb:27017/energy_mcp?authSource=energy_mcp" ]
        """,
        "pgbouncer": '[ "$DB_PASSWORD" = "$WANT_POSTGRES_READONLY_PASSWORD" ]',
    }

    for profile, check in checks.items():
        result = _run_loader(tmp_path / "secrets", profile, check, values)
        assert result.returncode == 0
        assert result.stdout == ""
        assert result.stderr == ""


def test_loader_is_posix_and_rejects_missing_command_without_reading_secrets(tmp_path):
    assert LOADER.read_text().startswith("#!/bin/sh\n")
    assert stat.S_IMODE(LOADER.stat().st_mode) == 0o555
    env = os.environ | {"LOAD_SECRETS_DIR": str(tmp_path / "missing")}
    result = subprocess.run(
        [LOADER, "librechat"], capture_output=True, text=True, env=env
    )
    assert result.returncode != 0
    assert "명령" in result.stderr


def test_loader_never_traces_or_prints_submitted_values(tmp_path):
    values = _write_loader_secrets(tmp_path / "secrets")
    result = _run_loader(tmp_path / "secrets", "unknown", ":", values)
    output = result.stdout + result.stderr
    assert result.returncode != 0
    assert all(value not in output for value in values.values())
    text = LOADER.read_text()
    assert "set -x" not in text
    assert "echo $" not in text


def test_provisioner_refuses_existing_or_incomplete_destinations(tmp_path):
    assert stat.S_IMODE(PROVISIONER.stat().st_mode) == 0o555
    assert subprocess.run(
        [PROVISIONER, "relative"], capture_output=True, text=True
    ).returncode != 0
    assert subprocess.run(
        [PROVISIONER, str(tmp_path / "extra"), "extra"],
        capture_output=True,
        text=True,
    ).returncode != 0
    existing = tmp_path / "existing"
    existing.mkdir()
    marker = existing / "marker"
    marker.write_text("unchanged")
    submitted = "dummy-openai\ndummy-postgres\n"
    result = subprocess.run(
        [PROVISIONER, str(existing)], input=submitted, capture_output=True, text=True
    )
    assert result.returncode != 0
    assert marker.read_text() == "unchanged"

    interrupted = tmp_path / "interrupted"
    result = subprocess.run(
        [PROVISIONER, str(interrupted)],
        input="dummy-openai\n",
        capture_output=True,
        text=True,
    )
    assert result.returncode != 0
    assert not interrupted.exists()
    assert not list(tmp_path.glob(".interrupted.tmp.*"))
    assert "dummy-openai" not in result.stdout + result.stderr


def test_provisioner_rejects_unsafe_postgres_password_without_partial_output(tmp_path):
    secret_dir = tmp_path / "unsafe"
    submitted = 'dummy-openai\nunsafe"password\n'
    result = subprocess.run(
        [PROVISIONER, str(secret_dir)],
        input=submitted,
        capture_output=True,
        text=True,
    )
    assert result.returncode != 0
    assert not secret_dir.exists()
    assert not list(tmp_path.glob(".unsafe.tmp.*"))
    assert all(value not in result.stdout + result.stderr for value in submitted.splitlines())


def test_provisioner_atomically_creates_owner_only_complete_set(tmp_path):
    secret_dir = tmp_path / "complete"
    openai_key = "dummy-openai "
    postgres_password = "dummy postgres "
    result = subprocess.run(
        [PROVISIONER, str(secret_dir)],
        input=f"{openai_key}\n{postgres_password}\n",
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0
    assert result.stdout == ""
    assert openai_key not in result.stderr
    assert postgres_password not in result.stderr
    assert stat.S_IMODE(secret_dir.stat().st_mode) == 0o700
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
    assert {path.name for path in secret_dir.iterdir()} == expected
    assert all(stat.S_IMODE(path.stat().st_mode) == 0o600 for path in secret_dir.iterdir())
    assert (secret_dir / "openai_api_key").read_text() == openai_key
    assert (secret_dir / "postgres_readonly_password").read_text() == postgres_password
    assert (secret_dir / "energy_mcp_dsn").read_text().endswith(
        "dummy%20postgres%20@pgbouncer:5432/pv"
    )
    assert not list(tmp_path.glob(".complete.tmp.*"))


def test_mongo_bootstrap_is_idempotent_and_scopes_application_roles():
    text = MONGO_INIT.read_text()
    assert text.count("getUser(") == 2
    assert "admin.auth('root', rootPassword)" in text
    assert "ensureUser('LibreChat', 'librechat_app'" in text
    assert "ensureUser('energy_mcp', 'energy_mcp_app'" in text
    assert ".trim()" not in text
    assert "console.log" not in text
    assert "print(" not in text


def test_mongo_healthcheck_reads_secret_from_file_not_process_argv():
    mongodb = CONFIG["services"]["mongodb"]
    assert mongodb["command"] == ["mongod", "--auth", "--bind_ip_all"]
    assert mongodb["healthcheck"]["test"] == [
        "CMD",
        "mongosh",
        "--quiet",
        "--file",
        "/usr/local/share/mongo-healthcheck.js",
    ]
    assert "./mongo-healthcheck.js:/usr/local/share/mongo-healthcheck.js:ro" in mongodb[
        "volumes"
    ]
    text = MONGO_HEALTH.read_text()
    assert "/run/secrets/mongo_root_password" in text
    assert "--password" not in text
    assert "console.log" not in text


def test_existing_volume_bootstrap_starts_only_mongo_then_initializes_and_verifies(tmp_path):
    assert MONGO_BOOTSTRAP.exists()
    assert stat.S_IMODE(MONGO_BOOTSTRAP.stat().st_mode) == 0o555
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    log = tmp_path / "docker.log"
    fake_docker = fake_bin / "docker"
    fake_docker.write_text('#!/bin/sh\nprintf "%s\\n" "$*" >> "$FAKE_DOCKER_LOG"\n')
    fake_docker.chmod(0o755)
    env = os.environ | {
        "PATH": f"{fake_bin}:{os.environ['PATH']}",
        "FAKE_DOCKER_LOG": str(log),
        "LLM_SECRET_DIR": str(tmp_path / "secrets"),
        "LLM_PUBLIC_BASE_URL": "http://example.invalid",
    }
    result = subprocess.run([MONGO_BOOTSTRAP], capture_output=True, text=True, env=env)
    assert result.returncode == 0
    assert result.stdout == ""
    assert result.stderr == ""
    compose = f"compose --env-file /dev/null -f {COMPOSE_PATH.resolve()}"
    assert log.read_text().splitlines() == [
        f"{compose} up -d --no-deps mongodb",
        f"{compose} exec -T mongodb mongosh --quiet --eval quit(db.runCommand({{ping:1}}).ok ? 0 : 2)",
        f"{compose} exec -T mongodb mongosh --quiet --file /docker-entrypoint-initdb.d/init-mongo-users.js",
        f"{compose} exec -T mongodb mongosh --quiet --file /usr/local/share/mongo-healthcheck.js",
    ]


def test_services_own_exact_secrets_and_health_dependencies():
    services = CONFIG["services"]
    assert services["pgbouncer"]["entrypoint"] == [
        "/usr/local/bin/load-secrets",
        "pgbouncer",
        "/entrypoint.sh",
    ]
    assert "./load-secrets.sh:/usr/local/bin/load-secrets:ro" in services[
        "pgbouncer"
    ]["volumes"]
    assert services["librechat"]["entrypoint"] == [
        "/usr/local/bin/load-secrets",
        "librechat",
    ]
    assert "./load-secrets.sh:/usr/local/bin/load-secrets:ro" in services[
        "librechat"
    ]["volumes"]
    assert services["energy-mcp"]["environment"]["ENERGY_MCP_MODE"] == "workflow"
    assert services["pgbouncer"]["secrets"] == ["postgres_readonly_password"]
    assert services["energy-mcp"]["secrets"] == [
        "openai_api_key",
        "energy_mcp_dsn",
        "energy_mcp_mongo_password",
    ]
    assert services["mongodb"]["secrets"] == [
        "mongo_root_password",
        "librechat_mongo_password",
        "energy_mcp_mongo_password",
    ]
    assert services["librechat"]["secrets"] == [
        "openai_api_key",
        "jwt_secret",
        "jwt_refresh_secret",
        "creds_key",
        "creds_iv",
        "librechat_mongo_password",
    ]
    assert services["energy-mcp"]["depends_on"] == {
        "mongodb": {"condition": "service_healthy"},
        "pgbouncer": {"condition": "service_started"},
    }
    assert services["librechat"]["depends_on"] == {
        "mongodb": {"condition": "service_healthy"},
        "energy-mcp": {"condition": "service_healthy"},
    }
    assert services["nginx"]["depends_on"] == {
        "librechat": {"condition": "service_healthy"},
        "energy-mcp": {"condition": "service_healthy"},
    }
    assert all("healthcheck" in services[name] for name in ("mongodb", "energy-mcp", "librechat"))


def test_compose_uses_required_secret_files():
    source = '${LLM_SECRET_DIR:?LLM_SECRET_DIR\ub97c \uc124\uc815\ud558\uc138\uc694}'
    assert COMPOSE.count(source) == 10
    assert set(CONFIG["secrets"]) == {
        "openai_api_key",
        "jwt_secret",
        "jwt_refresh_secret",
        "creds_key",
        "creds_iv",
        "postgres_readonly_password",
        "energy_mcp_dsn",
        "mongo_root_password",
        "librechat_mongo_password",
        "energy_mcp_mongo_password",
    }


def test_mcp_image_installs_the_tracked_lockfile_and_loader():
    dockerfile = Path("docker/llm-demo/mcp.Dockerfile").read_text()
    assert Path("mcp-server/uv.lock").exists()
    assert "ghcr.io/astral-sh/uv:0.8.17" in dockerfile
    assert "uv sync --frozen --no-dev" in dockerfile
    assert "COPY mcp-server /opt/mcp-server" in dockerfile
    assert "COPY docker/llm-demo/load-secrets.sh" in dockerfile
