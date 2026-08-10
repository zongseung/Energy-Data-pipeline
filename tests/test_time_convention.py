"""
수집기 시간규약(hour-ending vs 구간시작) 회귀 방지 테스트.

한국 전력 데이터의 시간 라벨 1~24는 **구간종료(hour-ending)** 표기다.
즉 라벨 N = 구간 [N-1시, N시). 코어 `generation.timestamp`는 **구간시작**을 쓰기로 했으므로
수집기는 라벨 N을 (N-1)시로 옮겨야 한다.

현재 코드베이스에는 두 부류가 섞여 있고, 이 테스트는 그 현재 동작을 **있는 그대로 고정**한다.
(이 테스트는 "옳은 동작"이 아니라 "지금 동작"을 박아두는 것이 목적이다.
 보정이 필요한 경로는 docs/time-convention-audit.md 의 표를 따른다.)

  · 라벨 N -> (N-1)시  (구간시작, 보정 불필요)
      fetch_data/common/utils.py::parse_hour_column        (남부 두 경로가 공유하는 라벨 파서)
      fetch_data/pv/nambu_backfill.py::_rows_from_api_payload
      fetch_data/pv/nambu_collect.py::collect_and_save     (위와 동일 로직의 인라인 복제본)
      fetch_data/pv/ekr_collect.py::_to_long
  · 라벨 N -> N시      (구간종료 그대로, 1시간 늦음 -> 뷰에서 -1h 필요)
      fetch_data/gen/transform_gen.py::transform_wide_to_long
      fetch_data/wind/namdong_collect.py::transform_wide_to_long
      fetch_data/pv/nambu_transform.py::preprocess_nambu  (남부 레거시 매핑)

**남부 경로는 공용 함수가 없다.** `nambu_collect.py:125-126` 과 `nambu_backfill.py:134-135` 는
같은 계산의 독립된 인라인 복제본이고, `nambu_collect` 는 `parse_hour_column` 만 import 한다.
한쪽만 고치면 두 규약이 갈라지므로 **두 경로를 각각 구동**한다 —
특히 `nambu_collect` 는 매일 09:30 KST 로 도는 라이브 수집기라 앞으로 생길 남부 행을 전부 만든다.

DB / 네트워크 없이 순수 함수만으로 통과한다.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from fetch_data.common.utils import parse_hour_column  # noqa: E402
from fetch_data.gen import transform_gen  # noqa: E402
from fetch_data.pv import ekr_collect, nambu_backfill, nambu_transform  # noqa: E402
from fetch_data.pv.namdong_transform import extract_hour  # noqa: E402
from fetch_data.wind import namdong_collect as wind_namdong  # noqa: E402

DAY = "2026-06-15"
NEXT_DAY = "2026-06-16"


# =========================================================
# 라벨 N -> (N-1)시  (구간시작으로 보정하는 경로)
# =========================================================
def test_parse_hour_column_라벨을_0based_구간시작으로_변환():
    """공용 파서: qhorgen01 -> 0시, qhorgen24 -> 23시 (구간종료 라벨을 -1 보정)."""
    assert parse_hour_column("qhorgen01") == 0
    assert parse_hour_column("qhorgen12") == 11
    assert parse_hour_column("qhorgen24") == 23
    assert parse_hour_column("H01") == 0


def test_nambu_백필경로는_구간시작으로_적재():
    """남부 백필 경로(`nambu_backfill._rows_from_api_payload`): qhorgen01 -> 당일 00시.

    라이브 수집기(`nambu_collect`)는 이 함수를 쓰지 않는 **별도 복제본**이다.
    그쪽은 test_nambu_라이브_수집기도_구간시작으로_적재 가 따로 구동한다.
    """
    payload = {
        "ymd": DAY,
        "hogi": "1",
        "gencd": "TEST01",
        "ipptnm": "테스트태양광",
        "qvodgen": "10",
        "qvodavg": "1",
        "qvodmax": "2",
        "qvodmin": "0",
        "qhorgen01": "11",
        "qhorgen12": "22",
        "qhorgen24": "33",
    }
    out = nambu_backfill._rows_from_api_payload([payload])
    ts = dict(zip(out["generation"], out["datetime"]))

    assert ts[11.0] == pd.Timestamp(f"{DAY} 00:00")
    assert ts[22.0] == pd.Timestamp(f"{DAY} 11:00")
    assert ts[33.0] == pd.Timestamp(f"{DAY} 23:00")
    # 구간시작 규약이면 익일로 넘어가는 값이 없어야 한다.
    assert out["datetime"].max() < pd.Timestamp(NEXT_DAY)


def _시각계산_두줄(rel_path: str) -> list[str]:
    """수집기 소스에서 hour0/datetime 계산 두 줄만 꺼낸다."""
    src = (PROJECT_ROOT / rel_path).read_text(encoding="utf-8")
    lines = [
        line.strip()
        for line in src.splitlines()
        if 'df_long["hour0"]' in line or 'df_long["datetime"]' in line
    ]
    assert len(lines) == 2, f"{rel_path} 의 시각 계산 줄 구조가 바뀌었다: {lines}"
    return lines


def test_nambu_라이브_수집기도_구간시작으로_적재():
    """라이브 수집기(nambu_collect)의 **인라인 복제본**을 소스에서 직접 구동한다.

    `collect_and_save` 는 async + aiohttp 세션 + DB 엔진이 필요해 통째로 호출할 수 없다.
    시각 계산 두 줄만 추출해 실행한다 — backfill 쪽만 통과하고 이쪽이 갈라지는 상황을 막는다.
    이 경로가 매일 09:30 KST 로 돌며 앞으로 생길 남부 행을 전부 만든다.
    """
    collect_lines = _시각계산_두줄("fetch_data/pv/nambu_collect.py")

    # 두 파일이 같은 계산의 복제본이라는 사실 자체를 고정한다.
    assert collect_lines == _시각계산_두줄("fetch_data/pv/nambu_backfill.py")

    ns = {
        "pd": pd,
        "parse_hour_column": parse_hour_column,
        "df_long": pd.DataFrame(
            {"h_str": ["qhorgen01", "qhorgen12", "qhorgen24"], "ymd": [DAY] * 3}
        ),
    }
    exec("\n".join(collect_lines), ns)

    assert list(ns["df_long"]["datetime"]) == [
        pd.Timestamp(f"{DAY} 00:00"),
        pd.Timestamp(f"{DAY} 11:00"),
        pd.Timestamp(f"{DAY} 23:00"),
    ]


def test_ekr_는_구간시작으로_적재():
    """EKR(영암/율치): '1시' -> 당일 00시, '24시' -> 당일 23시."""
    rows = [{"시설명": "율치", "날짜": DAY, "1시": 11, "12시": 22, "24시": 33}]
    out = ekr_collect._to_long(rows, year=2026)
    ts = dict(zip(out["generation"], out["timestamp"]))

    assert ts[11.0] == pd.Timestamp(f"{DAY} 00:00")
    assert ts[22.0] == pd.Timestamp(f"{DAY} 11:00")
    assert ts[33.0] == pd.Timestamp(f"{DAY} 23:00")
    assert out["plant_name"].unique().tolist() == ["율치태양광"]


# =========================================================
# 라벨 N -> N시  (구간종료 그대로 = 1시간 늦음, 뷰에서 -1h 필요)
# =========================================================
def test_gen_transform_은_구간종료_라벨을_그대로_보존():
    """KOEN 비태양광: '1시 발전량' -> 당일 01시, '24시 발전량' -> 익일 00시 (미보정)."""
    wide = pd.DataFrame([{
        "발전구분": "영흥",
        "호기": "1",
        "일자": DAY,
        "1시 발전량(MWh)": 11,
        "12시 발전량(MWh)": 22,
        "24시 발전량(MWh)": 33,
    }])
    out = transform_gen.transform_wide_to_long(wide)
    ts = dict(zip(out["generation"], out["timestamp"]))

    assert ts[11.0] == pd.Timestamp(f"{DAY} 01:00")   # 실제 구간은 00~01시
    assert ts[22.0] == pd.Timestamp(f"{DAY} 12:00")   # 실제 구간은 11~12시
    assert ts[33.0] == pd.Timestamp(f"{NEXT_DAY} 00:00")  # 24시 -> 익일 00시
    assert out["plant_name"].unique().tolist() == ["영흥_1"]


def test_wind_namdong_은_구간종료_라벨을_그대로_보존():
    """남동 풍력 API: qhorGen01 -> 당일 01시, qhorGen24 -> 익일 00시 (미보정)."""
    wide = pd.DataFrame([{
        "dgenYmd": DAY,
        "ipptNam": "영흥풍력",
        "hogi": "1",
        "qhorGen01": 11,
        "qhorGen12": 22,
        "qhorGen24": 33,
    }])
    out = wind_namdong.transform_wide_to_long(wide)
    ts = dict(zip(out["generation"], out["timestamp"]))

    assert ts[11.0] == pd.Timestamp(f"{DAY} 01:00")
    assert ts[22.0] == pd.Timestamp(f"{DAY} 12:00")
    assert ts[33.0] == pd.Timestamp(f"{NEXT_DAY} 00:00")
    assert out["plant_name"].unique().tolist() == ["영흥풍력 1"]


def test_nambu_레거시_transform_은_1시간_늦은_매핑(tmp_path, monkeypatch):
    """남부 레거시 매핑(pv_nambu 시절): qhorgen01 -> 당일 01시.

    2025-12-31 이전 남부 데이터가 1시간 늦게 적재된 원인이며,
    현행 경로(test_nambu_현행_수집경로는_구간시작으로_적재)와 1시간 어긋난다.
    """
    raw_dir = tmp_path / "raw"
    out_dir = tmp_path / "processed"
    raw_dir.mkdir()
    specs = tmp_path / "specs.csv"

    pd.DataFrame([{"발전소명": "테스트태양광발전소", "설비용량": 1.0}]).to_csv(
        specs, index=False, encoding="utf-8-sig"
    )
    pd.DataFrame([{
        "ymd": DAY,
        "ipptnm": "테스트태양광발전소",
        "qhorgen01": 11,
        "qhorgen12": 22,
        "qhorgen24": 33,
    }]).to_csv(raw_dir / "nambu_bulk_TEST_1.csv", index=False)

    monkeypatch.setattr(nambu_transform, "RAW_DIR", raw_dir)
    monkeypatch.setattr(nambu_transform, "PROCESSED_DIR", out_dir)
    monkeypatch.setattr(nambu_transform, "SPECS_FILE", specs)

    nambu_transform.preprocess_nambu()

    out = pd.read_csv(out_dir / "nambu_processed_TEST_1.csv", parse_dates=["timestamp"])
    ts = dict(zip(out["generation"], out["timestamp"]))

    assert ts[11] == pd.Timestamp(f"{DAY} 01:00")
    assert ts[22] == pd.Timestamp(f"{DAY} 12:00")
    assert ts[33] == pd.Timestamp(f"{NEXT_DAY} 00:00")


# =========================================================
# 원천 라벨 파서 (KOEN CSV 태양광/비태양광 공통 헤더)
# =========================================================
@pytest.mark.parametrize(
    "col, expected",
    [("1시 발전량(KWh)", 1), ("12시 발전량(MWh)", 12), ("24시 발전량(KWh)", 24)],
)
def test_koen_라벨_파서는_1based_원본값을_반환(col, expected):
    """KOEN CSV 헤더는 태양광(KWh)·비태양광(MWh)이 동일 포맷.

    태양광 경로는 이 값에서 -1 해 구간시작으로 적재하고
    (fetch_data/pv/namdong_collect.py:310),
    비태양광 경로는 -1 하지 않는다(fetch_data/gen/transform_gen.py:93).
    같은 원천·같은 헤더인데 처리가 갈리는 지점이므로 파서 반환값을 고정한다.
    """
    assert extract_hour(col) == expected
    assert transform_gen.HOUR_RE.match(col).group(1) == str(expected)
