"""시간별 유가 수집기 검증.

네트워크 없이 도는 부분만 본다 — 캔들 파싱, 병합 우선순위, 증분 윈도우.

병합 우선순위가 이 수집기의 핵심이다. Hyperliquid 는 **진행 중인 캔들도** 돌려
주는데 그 값은 시간이 끝나면 바뀐다. 새로 받은 값이 기존 값을 덮지 않으면
미완성 캔들이 영원히 남는다.
"""

from __future__ import annotations

import polars as pl
import pytest

from fetch_data.oil import oil_hourly as oh


def _candle(t, o, h, l, c, v, n):  # noqa: E741
    return {"t": t, "o": str(o), "h": str(h), "l": str(l), "c": str(c), "v": str(v), "n": n}


def test_candles_to_df_prefixes_columns_by_symbol():
    df = oh._candles_to_df([_candle(1000, 80, 81, 79, 80.5, 12.5, 3)], "wti")
    assert df.columns == ["t", "wti_o", "wti_h", "wti_l", "wti_c", "wti_v", "wti_n"]
    assert df["wti_c"][0] == pytest.approx(80.5)
    assert df["wti_n"][0] == 3


def test_candles_to_df_empty_keeps_schema():
    """빈 응답도 스키마를 유지해야 뒤의 join 이 깨지지 않는다."""
    df = oh._candles_to_df([], "brent")
    assert df.is_empty()
    assert "brent_c" in df.columns


def test_merge_prefers_new_rows(tmp_path):
    """같은 t 는 새 값이 이긴다 — 진행 중 캔들이 확정값으로 갱신되는 경로."""
    existing = pl.DataFrame({"t": [1000, 2000], "wti_c": [80.0, 81.0]})
    new = pl.DataFrame({"t": [2000], "wti_c": [99.0]})       # 2000 이 확정됨

    oh.merge_and_save(new, existing, tmp_path)
    got = oh.load_existing(tmp_path).sort("t")

    assert got["t"].to_list() == [1000, 2000]
    assert got["wti_c"].to_list() == [80.0, 99.0]            # 81.0 이 아니라 99.0


def test_merge_keeps_existing_when_new_is_empty(tmp_path):
    existing = pl.DataFrame({"t": [1000], "wti_c": [80.0]})
    oh.merge_and_save(pl.DataFrame(schema={"t": pl.Int64}), existing, tmp_path)
    assert oh.load_existing(tmp_path)["t"].to_list() == [1000]


def test_saved_file_is_world_readable(tmp_path):
    """file_fdw 로 postgres(uid 999)가 읽는다 — 소유자 전용 권한이면 못 읽는다."""
    oh.merge_and_save(pl.DataFrame({"t": [1], "wti_c": [1.0]}),
                      pl.DataFrame(schema={"t": pl.Int64}), tmp_path)
    mode = (tmp_path / oh.CUMULATIVE_FILE).stat().st_mode
    assert mode & 0o044, "그룹·기타 읽기 권한이 없으면 file_fdw 가 실패한다"


def test_load_existing_missing_file_returns_empty(tmp_path):
    assert oh.load_existing(tmp_path).is_empty()


def test_column_names_match_the_foreign_table():
    """CSV 헤더와 sql/research/oil_fdw.sql 의 외부 테이블 컬럼이 어긋나면
    조회가 통째로 깨진다. 한쪽만 고치는 사고를 막는다."""
    from pathlib import Path

    ddl = Path("sql/research/oil_fdw.sql").read_text(encoding="utf-8")
    for sym in oh.COINS:
        for suffix in ("o", "h", "l", "c", "v", "n"):
            assert f"{sym}_{suffix}" in ddl, f"{sym}_{suffix} 가 외부 테이블 정의에 없다"
    assert "ts_kst" in ddl
