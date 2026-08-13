"""energy_mcp.server 단위 테스트. DB 없이 통과한다 — psycopg2 커넥션을 모킹한다."""

from __future__ import annotations

from unittest.mock import MagicMock

import psycopg2
import psycopg2.errors
import pytest

from energy_mcp import server


@pytest.fixture(autouse=True)
def dsn_env(monkeypatch):
    monkeypatch.setenv(server.DSN_ENV, "postgresql://researcher:x@example.invalid:5432/pv")
    monkeypatch.delenv(server.ROW_LIMIT_ENV, raising=False)
    monkeypatch.delenv(server.TIMEOUT_ENV, raising=False)


class FakeCursor:
    """psycopg2 cursor를 흉내낸다. `with conn.cursor() as cur:` 패턴을 지원한다."""

    def __init__(self, *, description=None, rows=None, error_on_query=None):
        self.description = description
        self._rows = list(rows or [])
        self._error_on_query = error_on_query
        self.executed: list[str] = []

    def execute(self, sql, params=None):
        self.executed.append(sql)
        # SET statement_timeout 은 항상 통과시키고, 실제 쿼리에서만 에러를 낸다.
        if self._error_on_query is not None and not sql.startswith("SET "):
            raise self._error_on_query

    def fetchmany(self, n):
        rows = self._rows[:n]
        self._rows = self._rows[n:]
        return rows

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class FakeConnection:
    def __init__(self, cursor: FakeCursor):
        self._cursor = cursor
        self.readonly_calls: list[dict] = []
        self.rollback_called = False
        self.close_called = False

    def set_session(self, **kwargs):
        self.readonly_calls.append(kwargs)

    def cursor(self):
        return self._cursor

    def rollback(self):
        self.rollback_called = True

    def close(self):
        self.close_called = True


def _patch_connect(monkeypatch, fake_conn: FakeConnection):
    monkeypatch.setattr(server, "_connect", lambda dsn: fake_conn)


# ---------------------------------------------------------------------------
# 읽기전용 강제
# ---------------------------------------------------------------------------


def test_write_query_is_rejected_by_read_only_session(monkeypatch):
    """INSERT 는 read-only 세션에서 DB가 거부한다 — 그 에러가 읽을 수 있는
    메시지로 변환되는지, 그리고 커넥션에 set_session(readonly=True)가 실제로
    걸리는지 확인한다."""
    cursor = FakeCursor(
        error_on_query=psycopg2.errors.ReadOnlySqlTransaction(
            "cannot execute INSERT in a read-only transaction"
        )
    )
    conn = FakeConnection(cursor)
    _patch_connect(monkeypatch, conn)

    with pytest.raises(RuntimeError) as exc_info:
        server._execute("INSERT INTO research.plants (plant_id) VALUES (1)")

    assert "read-only" in str(exc_info.value)
    # 애플리케이션 층에서 read-only 세션을 실제로 요청했는지 검증한다.
    assert conn.readonly_calls == [{"readonly": True, "autocommit": False}]
    # 실패했어도 커넥션은 정리돼야 한다.
    assert conn.rollback_called
    assert conn.close_called


@pytest.mark.parametrize("verb", ["UPDATE", "DELETE", "DROP TABLE"])
def test_various_write_verbs_are_rejected(monkeypatch, verb):
    cursor = FakeCursor(
        error_on_query=psycopg2.errors.ReadOnlySqlTransaction(
            f"cannot execute {verb} in a read-only transaction"
        )
    )
    conn = FakeConnection(cursor)
    _patch_connect(monkeypatch, conn)

    with pytest.raises(RuntimeError):
        server._execute(f"{verb} research.plants SET x = 1")


def test_multi_statement_is_rejected_before_touching_db(monkeypatch):
    """세미콜론으로 문장을 이어붙이면 커넥션을 열기도 전에 거부돼야 한다
    (그래야 뒷 문장이 statement_timeout 을 덮어쓸 길이 없다)."""
    connect_calls = []
    monkeypatch.setattr(server, "_connect", lambda dsn: connect_calls.append(dsn))

    with pytest.raises(ValueError, match="하나의 SQL 문장"):
        server._execute("SELECT 1; DROP TABLE research.plants")

    assert connect_calls == []


# ---------------------------------------------------------------------------
# 행수 상한 · 절단 표시
# ---------------------------------------------------------------------------


def test_row_limit_truncates_and_flags_response(monkeypatch):
    monkeypatch.setenv(server.ROW_LIMIT_ENV, "3")

    description = [MagicMock(name="col_a"), MagicMock(name="col_b")]
    for d, n in zip(description, ["plant_id", "gen_kwh"]):
        d.name = n
    rows = [(i, i * 10) for i in range(10)]  # 10행, 상한 3행

    cursor = FakeCursor(description=description, rows=rows)
    conn = FakeConnection(cursor)
    _patch_connect(monkeypatch, conn)

    result = server._execute("SELECT plant_id, gen_kwh FROM research.generation")

    assert result["truncated"] is True
    assert result["row_count"] == 3
    assert len(result["rows"]) == 3
    assert "note" in result and "3" in result["note"]


def test_under_limit_is_not_truncated(monkeypatch):
    monkeypatch.setenv(server.ROW_LIMIT_ENV, "10")

    description = [MagicMock(name="col_a")]
    description[0].name = "plant_id"
    rows = [(1,), (2,)]

    cursor = FakeCursor(description=description, rows=rows)
    conn = FakeConnection(cursor)
    _patch_connect(monkeypatch, conn)

    result = server._execute("SELECT plant_id FROM research.plants")

    assert result["truncated"] is False
    assert result["row_count"] == 2
    assert "note" not in result


# ---------------------------------------------------------------------------
# 값 직렬화 (Decimal/날짜 등 JSON 비호환 타입)
# ---------------------------------------------------------------------------


def test_decimal_and_date_values_are_jsonable(monkeypatch):
    import datetime
    from decimal import Decimal

    description = [MagicMock(), MagicMock()]
    description[0].name = "gen_kwh"
    description[1].name = "timestamp"
    rows = [(Decimal("123.456"), datetime.datetime(2026, 1, 1, 9, 0))]

    cursor = FakeCursor(description=description, rows=rows)
    conn = FakeConnection(cursor)
    _patch_connect(monkeypatch, conn)

    result = server._execute("SELECT gen_kwh, timestamp FROM research.generation")

    row = result["rows"][0]
    assert row["gen_kwh"] == "123.456"  # 문자열로 보존 (float 반올림 방지)
    assert row["timestamp"] == "2026-01-01T09:00:00"


# ---------------------------------------------------------------------------
# 에러 메시지 — 내부 정보 노출 금지
# ---------------------------------------------------------------------------


def test_missing_dsn_raises_readable_error(monkeypatch):
    monkeypatch.delenv(server.DSN_ENV, raising=False)

    with pytest.raises(RuntimeError, match=server.DSN_ENV):
        server._execute("SELECT 1")


def test_connection_failure_does_not_leak_traceback_object(monkeypatch):
    def boom(dsn):
        raise psycopg2.OperationalError("could not connect to server")

    monkeypatch.setattr(server, "_connect", boom)

    with pytest.raises(RuntimeError) as exc_info:
        server._execute("SELECT 1")

    message = str(exc_info.value)
    assert "DB 연결 실패" in message
    assert "Traceback" not in message


# ---------------------------------------------------------------------------
# 스키마 리소스 마크다운 렌더링 (DB 없이 — 행 튜플만 넣어본다)
# ---------------------------------------------------------------------------


def test_schema_markdown_includes_known_pitfalls_without_db():
    md = server._render_schema_markdown(
        [
            ("plants", "발전소 마스터", "data_quality", "text", "정상/시간별무효/전면무효/미검증"),
            ("plants", "발전소 마스터", "hourly_valid_from", "date", "시간별을 믿어도 되는 시작일"),
        ]
    )

    assert "research.plants" in md
    assert "data_quality" in md
    # 정적 함정 요약은 DB COMMENT 유무와 무관하게 항상 포함돼야 한다.
    assert "미검증" in md
    assert "영흥태양광" in md
    assert "is_aggregate" in md
    assert "구간시작" in md
