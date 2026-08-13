"""데모 쿼리 배터리 판정 로직 검증.

배터리는 LLM 개선 여부를 읽는 계기판이다. 계기판이 틀리면 개선을 퇴행으로,
퇴행을 개선으로 잘못 읽는다. 실제로 1·2차 라운드에서 두 종류의 오판이 났다.

  - 1차 smp-daily: SQL 이 `column "station_type" does not exist` 로 실패했는데
    정규식만 맞아서 ✅ 통과로 집계됐다 (거짓 통과).
  - 1·2차 period-bounds: `EXTRACT(YEAR..)=2026 AND EXTRACT(MONTH..)=7` 이라는
    완전히 옳은 SQL 이 리터럴 '2026-07' 만 인정하는 정규식에 걸려 실패했다
    (거짓 실패).

두 오판이 재발하지 않는지만 본다.
"""

import re
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

from demo_query_battery import _ym, _ymd, judge  # noqa: E402

FLAGS = re.I | re.S

EXTRACT_JULY = (
    "SELECT SUM(gen_kwh) FROM research.generation "
    "WHERE EXTRACT(YEAR FROM timestamp) = 2026 "
    "AND EXTRACT(MONTH FROM timestamp) = 7 AND fuel_type = 'solar'"
)


@pytest.mark.parametrize("sql, expected", [
    (EXTRACT_JULY, True),                              # EXTRACT 분해형도 정답이다
    ("WHERE timestamp >= '2026-07-01'", True),         # 리터럴도 정답이다
    ("WHERE date_trunc('month', timestamp) = '2026-07-01'", True),
    ("WHERE EXTRACT(YEAR FROM timestamp)=2026 "
     "AND EXTRACT(MONTH FROM timestamp)=3", False),    # 다른 달은 거부해야 한다
    ("WHERE timestamp >= '2025-07-01'", False),        # 다른 해도 거부해야 한다
])
def test_year_month_accepts_both_notations(sql, expected):
    assert bool(re.search(_ym(2026, 7), sql, FLAGS)) is expected


@pytest.mark.parametrize("sql, expected", [
    ("WHERE timestamp::date = '2026-08-10'", True),
    ("WHERE region='land' AND timestamp >= '20260810'", True),  # 압축 표기
    ("WHERE timestamp >= '20260811'", False),
    ("WHERE timestamp::date = '2026-08-01'", False),
])
def test_year_month_day_accepts_both_notations(sql, expected):
    assert bool(re.search(_ymd(2026, 8, 10), sql, FLAGS)) is expected


def test_zero_padding_uses_format_spec():
    """'0{day}' 로 쓰면 day=10 일 때 '010' 이 붙어 아홉 자리가 된다."""
    assert "20260810" in _ymd(2026, 8, 10)
    assert "202608010" not in _ymd(2026, 8, 10)


CASE = {"id": "t", "q": "", "sql_must": [r"smp_hourly"]}


def test_errored_final_sql_is_not_a_pass():
    """정규식이 맞아도 마지막 SQL 이 에러로 끝났으면 실패다."""
    ok, note = judge(CASE, [("SELECT 1 FROM research.smp_hourly", True)], ["boom"], False)
    assert ok is False
    assert "에러" in note


def test_successful_final_sql_passes():
    ok, _ = judge(CASE, [("SELECT 1 FROM research.smp_hourly", False)], [], False)
    assert ok is True


def test_missing_pattern_fails_even_when_sql_succeeds():
    ok, note = judge(CASE, [("SELECT 1 FROM research.plants", False)], [], False)
    assert ok is False
    assert "필수 패턴 누락" in note


RECOVER_CASE = {"id": "r", "q": "", "recovered": True}


def test_recovery_counts_only_the_last_call():
    """에러 뒤 재시도가 성공하면 회복 성공, 끝까지 에러면 실패."""
    assert judge(RECOVER_CASE, [("bad", True), ("good", False)], ["e"], False)[0] is True
    assert judge(RECOVER_CASE, [("bad", True)], ["e"], False)[0] is False


def test_no_sql_at_all_fails():
    ok, note = judge(CASE, [], [], False)
    assert ok is False
    assert "SQL 실행 없음" in note
