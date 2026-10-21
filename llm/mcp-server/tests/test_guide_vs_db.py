"""안내문·힌트가 실제 DB 와 어긋나면 실패한다. ENERGY_MCP_TEST_DSN(읽기전용 계정) 이 있을 때만 돈다."""
import os

import psycopg2
import pytest

from energy_mcp import hints, server

DSN = os.environ.get("ENERGY_MCP_TEST_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="ENERGY_MCP_TEST_DSN 없음")


def _query(sql):
    with psycopg2.connect(DSN) as conn, conn.cursor() as cur:
        cur.execute(sql)
        return cur.fetchall()


def test_known_views_and_guide_list_exactly_the_research_relations():
    in_db = {name for (name,) in _query(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'research'")}
    known = {v.strip().removeprefix("research.") for v in hints.KNOWN_VIEWS.split("(")[0].split(",") if v.strip()}
    assert known == in_db, f"hints.KNOWN_VIEWS 와 DB 가 다르다: 추가 {in_db - known}, 삭제 {known - in_db}"
    guide = server.LEGACY_INSTRUCTIONS
    assert [v for v in sorted(in_db - {"oil_hourly_raw"}) if v not in guide] == []


def test_range_specs_columns_exist():
    cols = {}
    for table, column in _query(
            "SELECT table_name, column_name FROM information_schema.columns WHERE table_schema = 'research'"):
        cols.setdefault(table, set()).add(column)
    for view, (time_col, groups) in server.RANGE_SPECS.items():
        assert {time_col, *groups} <= cols.get(view, set()), view
