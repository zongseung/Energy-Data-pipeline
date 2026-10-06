"""LLM 데모 스택 설정 검증.

compose 배선 검증 7건은 2026-09-09 에 제거했다 — 브랜치의 시크릿 파일 방식
(load-secrets/provision-secrets 로 주입, 가입 차단, 0.0.0.0 바인딩)을 채택하지
않고 기존 운영 설정(Tailscale 전용 바인딩·자유가입·compose 환경변수 주입)을
유지하기로 했기 때문이다. 스크립트 자체는 머지돼 있고 아래 테스트가 지킨다 —
나중에 시크릿 방식으로 넘어갈 때 배선 검증을 되살리면 된다.
"""
import json
import os
import re
import stat
import subprocess
from pathlib import Path

import yaml


COMPOSE_PATH = Path("llm/librechat/compose.yml")
COMPOSE = COMPOSE_PATH.read_text()
CONFIG = yaml.safe_load(COMPOSE)
LOADER = Path("llm/librechat/load-secrets.sh")
PROVISIONER = Path("llm/librechat/provision-secrets.sh")
MONGO_INIT = Path("llm/librechat/init-mongo-users.js")
MONGO_HEALTH = Path("llm/librechat/mongo-healthcheck.js")
MONGO_BOOTSTRAP = Path("llm/librechat/bootstrap-mongo.sh")

MONGO_HARNESS = r"""
const fs = require('fs');
const realRead = fs.readFileSync;
const secrets = {
  mongo_root_password: 'file-root-password',
  librechat_mongo_password: 'file-librechat-password',
  energy_mcp_mongo_password: 'file-energy-password',
  ...JSON.parse(process.env.MONGO_TEST_SECRETS || '{}'),
};
fs.readFileSync = (path, encoding) => {
  if (String(path).startsWith('/run/secrets/')) return secrets[String(path).split('/').pop()];
  return realRead(path, encoding);
};

const users = JSON.parse(process.env.MONGO_TEST_USERS);
const events = [];
let authenticated = false;
const target = (database) => ({
  auth(user, password) {
    events.push({ op: 'auth', database, user });
    if (database === 'admin' && users.admin?.root === password) {
      authenticated = true;
      return true;
    }
    return false;
  },
  getUser(user) {
    events.push({ op: 'getUser', database, user });
    if (users.admin?.root && !authenticated) throw new Error('unauthorized');
    return users[database]?.[user] ? { user } : null;
  },
  createUser({ user, pwd, roles }) {
    events.push({ op: 'createUser', database, user, roles });
    if (users.admin?.root && !authenticated) throw new Error('unauthorized');
    users[database] ??= {};
    users[database][user] = pwd;
  },
});
global.db = { getSiblingDB: target };

try {
  require(process.env.MONGO_INIT_PATH);
  process.stdout.write(JSON.stringify({ ok: true, events }));
} catch (error) {
  process.stdout.write(JSON.stringify({ ok: false, events, error: error.message }));
  process.exitCode = 3;
}
"""


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


def _run_mongo_init(users, secrets=None):
    env = os.environ | {
        "MONGO_INIT_PATH": str(MONGO_INIT.resolve()),
        "MONGO_TEST_USERS": json.dumps(users),
        "MONGO_TEST_SECRETS": json.dumps(secrets or {}),
    }
    result = subprocess.run(
        ["node", "-e", MONGO_HARNESS], capture_output=True, text=True, env=env
    )
    return result, json.loads(result.stdout)


def _render_compose(path: Path, env: dict[str, str]):
    result = subprocess.run(
        ["docker", "compose", "--env-file", "/dev/null", "-f", path, "config"],
        capture_output=True,
        text=True,
        env=os.environ | env,
    )
    assert result.returncode == 0, result.stderr
    return yaml.safe_load(result.stdout)


def test_production_mcp_connects_the_existing_clarification_workflow():
    mcp = CONFIG["services"]["energy-mcp"]
    assert mcp["environment"]["ENERGY_MCP_MODE"] == "workflow"
    assert "ENERGY_MCP_APPROVAL_BASE_URL" in mcp["environment"]
    assert mcp["env_file"] == ["mcp.env"]
    assert "mongodb" in mcp["depends_on"]


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


def test_loader_rejects_empty_or_linebreak_only_secrets_before_exec(tmp_path):
    cases = (
        ("librechat", "jwt_secret", b""),
        ("energy-mcp", "energy_mcp_dsn", b"\n\n"),
        ("pgbouncer", "postgres_readonly_password", b"\n"),
        ("librechat", "openai_api_key", b"\r\n"),
    )
    for index, (profile, empty_name, content) in enumerate(cases):
        case_dir = tmp_path / f"{profile}-{index}"
        values = _write_loader_secrets(case_dir)
        (case_dir / empty_name).write_bytes(content)
        marker = tmp_path / f"{profile}.ran"

        result = _run_loader(
            case_dir,
            profile,
            f'touch "$WANT_MARKER"',
            values | {"marker": str(marker)},
        )

        assert result.returncode != 0
        assert not marker.exists()
        assert empty_name in result.stderr
        assert all(value not in result.stdout + result.stderr for value in values.values())


def test_loader_is_posix_and_rejects_missing_command_without_reading_secrets(tmp_path):
    assert LOADER.read_text().startswith("#!/bin/sh\n")
    mode = stat.S_IMODE(LOADER.stat().st_mode)
    assert mode & stat.S_IXUSR
    assert not mode & stat.S_IWOTH
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
    mode = stat.S_IMODE(PROVISIONER.stat().st_mode)
    assert mode & stat.S_IXUSR
    assert not mode & stat.S_IWOTH
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


def test_mongo_init_fresh_state_creates_root_then_scoped_app_users():
    result, payload = _run_mongo_init({"admin": {}, "LibreChat": {}, "energy_mcp": {}})
    assert result.returncode == 0
    assert payload["ok"] is True
    assert [(event["op"], event["database"], event["user"]) for event in payload["events"]] == [
        ("auth", "admin", "root"),
        ("getUser", "admin", "root"),
        ("createUser", "admin", "root"),
        ("auth", "admin", "root"),
        ("getUser", "LibreChat", "librechat_app"),
        ("createUser", "LibreChat", "librechat_app"),
        ("getUser", "energy_mcp", "energy_mcp_app"),
        ("createUser", "energy_mcp", "energy_mcp_app"),
    ]
    created = [event for event in payload["events"] if event["op"] == "createUser"]
    assert created[1]["roles"] == [{"role": "readWrite", "db": "LibreChat"}]
    assert created[2]["roles"] == [{"role": "readWrite", "db": "energy_mcp"}]


def test_mongo_init_root_only_state_authenticates_then_creates_app_users():
    result, payload = _run_mongo_init(
        {"admin": {"root": "file-root-password"}, "LibreChat": {}, "energy_mcp": {}}
    )
    assert result.returncode == 0
    assert payload["events"][0] == {"op": "auth", "database": "admin", "user": "root"}
    assert not any(
        event["op"] == "getUser" and event["database"] == "admin"
        for event in payload["events"]
    )
    assert [
        (event["database"], event["user"])
        for event in payload["events"]
        if event["op"] == "createUser"
    ] == [("LibreChat", "librechat_app"), ("energy_mcp", "energy_mcp_app")]


def test_mongo_init_completed_state_is_safe_to_rerun():
    result, payload = _run_mongo_init(
        {
            "admin": {"root": "file-root-password"},
            "LibreChat": {"librechat_app": "file-librechat-password"},
            "energy_mcp": {"energy_mcp_app": "file-energy-password"},
        }
    )
    assert result.returncode == 0
    assert payload["events"][0] == {"op": "auth", "database": "admin", "user": "root"}
    assert not any(event["op"] == "createUser" for event in payload["events"])


def test_mongo_init_wrong_existing_root_password_fails_closed_without_values():
    result, payload = _run_mongo_init(
        {"admin": {"root": "different-password"}, "LibreChat": {}, "energy_mcp": {}}
    )
    assert result.returncode != 0
    assert payload["ok"] is False
    assert payload["error"] == "Mongo root 인증 실패"
    assert not any(event["op"] == "createUser" for event in payload["events"])
    assert "file-root-password" not in result.stdout + result.stderr


def test_mongo_init_rejects_empty_secrets_before_any_database_mutation():
    result, payload = _run_mongo_init(
        {"admin": {}, "LibreChat": {}, "energy_mcp": {}},
        {"energy_mcp_mongo_password": "\n\n"},
    )

    assert result.returncode != 0
    assert payload["ok"] is False
    assert payload["events"] == []
    assert payload["error"] == "필수 secret 파일이 비어 있습니다: energy_mcp_mongo_password"


def test_mongo_healthcheck_accepts_the_same_crlf_secret_as_initialization(tmp_path):
    harness = tmp_path / "healthcheck-harness.js"
    harness.write_text(
        """
const fs = require('fs');
const realRead = fs.readFileSync;
fs.readFileSync = (path, encoding) => String(path).endsWith('mongo_root_password')
  ? 'file-root-password\\r\\n'
  : realRead(path, encoding);
global.db = { getSiblingDB: () => ({
  auth: (_user, password) => password === 'file-root-password',
  runCommand: () => ({ ok: 1 }),
}) };
global.quit = (code) => { process.exitCode = code; };
require(process.env.MONGO_HEALTH_PATH);
"""
    )
    result = subprocess.run(
        ["node", harness],
        capture_output=True,
        text=True,
        env=os.environ | {"MONGO_HEALTH_PATH": str(MONGO_HEALTH.resolve())},
    )
    assert result.returncode == 0


def test_existing_volume_bootstrap_starts_only_mongo_then_initializes_and_verifies(tmp_path):
    assert MONGO_BOOTSTRAP.exists()
    mode = stat.S_IMODE(MONGO_BOOTSTRAP.stat().st_mode)
    assert mode & stat.S_IXUSR
    assert not mode & stat.S_IWOTH
    compose = f"compose --env-file /dev/null -f {COMPOSE_PATH.resolve()}"
    for scenario, readiness in (
        (
            "fresh",
            [
                f"{compose} exec -T mongodb mongosh --quiet --file /usr/local/share/mongo-healthcheck.js",
                f"{compose} exec -T mongodb mongosh --quiet --eval quit(db.runCommand({{ping:1}}).ok ? 0 : 2)",
            ],
        ),
        (
            "rooted",
            [f"{compose} exec -T mongodb mongosh --quiet --file /usr/local/share/mongo-healthcheck.js"],
        ),
    ):
        case_dir = tmp_path / scenario
        fake_bin = case_dir / "bin"
        fake_bin.mkdir(parents=True)
        log = case_dir / "docker.log"
        marker = case_dir / "initialized"
        fake_docker = fake_bin / "docker"
        fake_docker.write_text(
            """#!/bin/sh
printf '%s\\n' "$*" >> "$FAKE_DOCKER_LOG"
case "$*" in
  *init-mongo-users.js*) : > "$FAKE_MONGO_MARKER" ;;
  *mongo-healthcheck.js*)
    [ "$FAKE_MONGO_SCENARIO" = rooted ] || [ -e "$FAKE_MONGO_MARKER" ] || exit 1
    ;;
  *'--eval quit(db.runCommand({ping:1}).ok ? 0 : 2)'*)
    [ "$FAKE_MONGO_SCENARIO" = fresh ] || exit 1
    ;;
esac
"""
        )
        fake_docker.chmod(0o755)
        (fake_bin / "sleep").write_text("#!/bin/sh\nexit 0\n")
        (fake_bin / "sleep").chmod(0o755)
        env = os.environ | {
            "PATH": f"{fake_bin}:{os.environ['PATH']}",
            "FAKE_DOCKER_LOG": str(log),
            "FAKE_MONGO_MARKER": str(marker),
            "FAKE_MONGO_SCENARIO": scenario,
            "LLM_SECRET_DIR": str(case_dir / "secrets"),
            "LLM_APPROVAL_PUBLIC_ORIGIN": "http://example.invalid:8099",
            "LLM_EXPORT_PUBLIC_ORIGIN": "http://example.invalid:8098",
        }
        result = subprocess.run(
            [MONGO_BOOTSTRAP], capture_output=True, text=True, env=env
        )
        assert result.returncode == 0
        assert result.stdout == ""
        assert result.stderr == ""
        assert log.read_text().splitlines() == [
            f"{compose} up -d --no-deps mongodb",
            *readiness,
            f"{compose} exec -T mongodb mongosh --quiet --file /docker-entrypoint-initdb.d/init-mongo-users.js",
            f"{compose} exec -T mongodb mongosh --quiet --file /usr/local/share/mongo-healthcheck.js",
        ]
        assert "--password" not in log.read_text()


def _nginx_location_directives(nginx: str, path: str) -> list[tuple[str, str]]:
    location = re.search(
        rf"^\s*location {re.escape(path)} \{{(?P<body>.*?)^\s*\}}",
        nginx,
        re.M | re.S,
    )
    assert location is not None
    return [
        tuple(line.rstrip(";").split(maxsplit=1))
        for raw_line in location["body"].splitlines()
        if (line := raw_line.split("#", 1)[0].strip())
    ]


def test_approval_proxy_parser_keeps_duplicate_header_directives():
    directives = _nginx_location_directives(
        """
        location /approval/ {
            proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
            proxy_set_header Host $host;
        }
        """,
        "/approval/",
    )

    assert ("proxy_set_header", "X-Forwarded-For $proxy_add_x_forwarded_for") in directives
    assert ("proxy_set_header", "Host $host") in directives


def test_librechat_uses_server_instructions_and_approval_proxy():
    nginx = Path("llm/librechat/nginx.conf").read_text()
    servers = yaml.safe_load(Path("llm/librechat/librechat.yaml").read_text())["mcpServers"]
    directives = _nginx_location_directives(nginx, "/approval/")

    assert {
        name: config.get("serverInstructions")
        for name, config in servers.items()
        if "serverInstructions" in config
    } == {"energy-db": True}
    assert nginx.index("location /approval/") < nginx.index("location / {")
    assert ("proxy_pass", "http://energy-mcp:8000") in directives
    assert ("access_log", "off") in directives
    assert not any(
        name == "proxy_set_header" and value.startswith("X-Forwarded-For")
        for name, value in directives
    )
