"""energy_mcp.hints — SQL 오류 교정 힌트 매핑 (의존성 없는 순수 모듈을 경로로 로드)."""
import importlib.util
from pathlib import Path

_spec = importlib.util.spec_from_file_location(
    "energy_mcp_hints",
    Path(__file__).parents[1] / "mcp-server" / "energy_mcp" / "hints.py",
)
hints = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(hints)


def test_round_cast_hint():
    msg = 'SQL 오류: function round(double precision, integer) does not exist'
    assert "::numeric" in hints.hint_for(msg)


def test_unknown_relation_lists_all_views():
    msg = 'SQL 오류: relation "research.power_plants" does not exist'
    hint = hints.hint_for(msg)
    assert "research.plants" in hint and "research.demand_5min" in hint


def test_unknown_column_suggests_probe():
    msg = 'SQL 오류: column "generation_kwh" does not exist'
    assert "LIMIT 1" in hints.hint_for(msg)


def test_timeout_suggests_narrowing():
    assert "기간" in hints.hint_for("canceling statement due to statement timeout")


def test_multi_statement_syntax():
    assert "단일 SELECT" in hints.hint_for('syntax error at or near ";"')


def test_division_by_zero():
    assert "NULLIF" in hints.hint_for("division by zero")


def test_unknown_error_returns_none():
    assert hints.hint_for("deadlock detected") is None
