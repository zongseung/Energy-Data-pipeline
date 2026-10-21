"""남부 파일데이터 23종 -> generation 코어 적재.

파일이 정본인 이유 ([[nambu-filedata-ingest]]):
  · API(PwrGenTran/PwrSunLightInfo)는 **2015-10-23 부터만** 준다 — 2013-01-01~2015-10-22 은 파일뿐.
  · 겹치는 구간은 파일 컬럼N == API qhorgenN 으로 값이 동일하다(2016-08-15 24컬럼 대조 확인).
  · API 원천 공백(2020-09~10 · 2020-12~2021-03 · 2021-09~10)을 파일이 100% 메운다.
  · DB 의 옛 source='backfill' 767,662행은 1시간 밀려 있다(옛 nambu_transform.py 가 1-based 로 읽음).

시간 규약: **컬럼N -> (N-1)시**. 파일 1~24 는 hour-ending 표기이고 적재는 구간시작이다
([[kpx-smp-hour-ending]] 과 동일). 검증 — 태양광 21종 중 18종이 (N-1) 변환에서 피크 12시,
일 '총량' == 시간별 합계가 태양광 전종 0건 불일치.

파일이 끊기는 2026-03-01 이후(풍력은 2026-04-01)는 건드리지 않는다 — 그 구간은 API 가 정본이다.
그래서 삭제 범위를 **각 파일의 실제 날짜 범위**로 한정한다.

사용:
    uv run python -m pipeline.fetch_data.nambu.file_load --dry-run   # 계획만
    uv run python -m pipeline.fetch_data.nambu.file_load             # 적재
    uv run python -m pipeline.fetch_data.nambu.file_load 하동본부     # 일부
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text

from pipeline.fetch_data.common.db_utils import resolve_db_url
from pipeline.fetch_data.common.logger import get_logger
from pipeline.fetch_data.common.paths import DATA_DIR

logger = get_logger(__name__)

SRC_DIR = DATA_DIR / "nambu"
OPERATOR = "nambu"
HOURS = [str(i) for i in range(1, 25)]

# 파일명 -> (source_key, fuel_type, region)
# 근거: 겹치는 구간 시간별 값 대조 + 사양정보(15030079) 설치용량 대조 + 공식 코드표 69행.
# 호기는 파일의 '호기'/'단계' 값을 그대로 unit_no 로 쓴다.
# 월별 스키마 2종(삼척사택·안동소내)은 generation(시간별)에 들어갈 수 없어 제외했다.
PLANTS: dict[str, tuple[str, str, str]] = {
    # 기존 plants 에 있던 계열 (source_key 는 P9 에서 부여한 값)
    "남제주소내":         ("namjeju-onsite",      "solar", "jeju"),
    "부산복합자재창고":    ("busan-warehouse",     "solar", "mainland"),
    "부산수처리장":       ("busan-watertreat",    "solar", "mainland"),
    "신인천소내":         ("sinincheon-onsite",   "solar", "mainland"),
    "신인천전망대":       ("sinincheon-view",     "solar", "mainland"),
    "신인천해수구취수구":  ("sinincheon-intake",   "solar", "mainland"),
    "부산신항":          ("busan-newport",       "solar", "mainland"),
    "부산운동장":         ("busan-1400kwp",       "solar", "mainland"),  # DB 이름이 잘못됨(실은 부산본부 1,400kW)
    "부산본부":          ("busan-complex",       "solar", "mainland"),  # 호기 1·2
    "삼척소내":          ("samcheok-onsite",     "solar", "mainland"),  # 호기 1~4 (4호기는 신규)
    "한경풍력":          ("hangyeong-wind",      "wind",  "jeju"),      # 단계 1·2
    # 파일에만 있던 신규 계열
    "하동본부":          ("hadong-hq",           "solar", "mainland"),  # 호기 1~6
    "하동변전소":         ("hadong-substation",   "solar", "mainland"),
    "하동보건소":         ("hadong-health",       "solar", "mainland"),
    "하동정수장":         ("hadong-waterplant",   "solar", "mainland"),
    "하동공설운동장":      ("hadong-stadium",      "solar", "mainland"),
    "하동하수처리장":      ("hadong-sewage",       "solar", "mainland"),
    "행원소수력":         ("haengwon-hydro",      "solar", "jeju"),      # 소수력 부지의 태양광
    "영월본부":          ("yeongwol-hq",         "solar", "mainland"),
    "영월철도부지":       ("yeongwol-rail",       "solar", "mainland"),
    "인천수산정수장":      ("incheon-waterplant",  "solar", "mainland"),
    "위미2리":           ("wimi2ri",             "solar", "jeju"),
    "성산풍력":          ("seongsan-wind",       "wind",  "jeju"),      # 단계 1·2
}

_ENSURE = text("""
    INSERT INTO plants (source_key, plant_name, unit_no, fuel_type, operator, region)
    VALUES (:sk, :name, :unit, :fuel, :op, :region)
    ON CONFLICT (operator, source_key, unit_no) DO NOTHING
""")
_IDS = text("""
    SELECT unit_no, plant_id FROM plants
     WHERE operator = :op AND source_key = :sk
""")
_WIPE = text("""
    DELETE FROM generation
     WHERE plant_id = :pid AND timestamp >= :lo AND timestamp <= :hi
""")
_INSERT = text("""
    INSERT INTO generation (timestamp, plant_id, gen_kwh, source)
    VALUES (:ts, :pid, :kwh, 'file')
    ON CONFLICT (timestamp, plant_id)
    DO UPDATE SET gen_kwh = EXCLUDED.gen_kwh, source = 'file'
""")


def to_long(path: Path) -> pd.DataFrame:
    """wide(1~24열) -> long. 컬럼N 은 (N-1)시다. 호기/단계 컬럼명 차이를 흡수한다."""
    d = pd.read_csv(path, dtype=str)
    if "년월일" not in d.columns:
        raise ValueError(f"{path.name}: 시간별 스키마가 아니다 ({list(d.columns)[:3]})")
    unit_col = "호기" if "호기" in d.columns else "단계"
    # 호기 라벨만 빠진 행이 있다 (신인천해수구취수구 365행 — 값은 정상).
    # 단일 호기 설비면 그 값으로 채우고, 다중 호기면 어디 것인지 모르니 멈춘다.
    if d[unit_col].isna().any():
        present = d[unit_col].dropna().unique()
        if len(present) != 1:
            raise ValueError(
                f"{path.name}: 호기 결측 {d[unit_col].isna().sum()}행인데 호기가 {list(present)} 여러 개다"
            )
        logger.warning(f"{path.name}: 호기 결측 {d[unit_col].isna().sum()}행 -> '{present[0]}' 로 채운다")
        d[unit_col] = d[unit_col].fillna(present[0])
    d = d.melt(id_vars=["년월일", unit_col], value_vars=HOURS,
               var_name="h", value_name="kwh")
    d["timestamp"] = pd.to_datetime(d["년월일"]) + pd.to_timedelta(d["h"].astype(int) - 1, unit="h")
    d["kwh"] = pd.to_numeric(d["kwh"], errors="coerce")
    d["unit_no"] = d[unit_col].astype(str).str.strip()
    return d.dropna(subset=["timestamp", "unit_no"])[["timestamp", "unit_no", "kwh"]]


def load_one(conn, name: str, path: Path, dry: bool) -> int:
    sk, fuel, region = PLANTS[name]
    long = to_long(path)
    lo, hi = long["timestamp"].min(), long["timestamp"].max()
    units = sorted(long["unit_no"].unique())

    if not dry:
        for u in units:
            conn.execute(_ENSURE, {"sk": sk, "name": name, "unit": u,
                                   "fuel": fuel, "op": OPERATOR, "region": region})
    ids = {u: p for u, p in conn.execute(_IDS, {"op": OPERATOR, "sk": sk}).fetchall()}
    missing = [u for u in units if u not in ids]
    if missing and not dry:
        raise RuntimeError(f"{name}: plants 등록 실패 unit={missing}")

    rows = long.dropna(subset=["kwh"])
    logger.info(f"[{name}] {sk} unit={units} {lo.date()}~{hi.date()} "
                f"{len(rows):,}행 (plant_id={[ids.get(u) for u in units]})")
    if dry:
        return len(rows)

    for u in units:
        conn.execute(_WIPE, {"pid": ids[u], "lo": lo, "hi": hi})
    recs = [{"ts": t, "pid": ids[u], "kwh": float(k)}
            for t, u, k in zip(rows["timestamp"], rows["unit_no"], rows["kwh"])]
    for i in range(0, len(recs), 5000):
        conn.execute(_INSERT, recs[i:i + 5000])
    return len(recs)


def run(only: list[str] | None = None, dry: bool = False) -> int:
    paths = {}
    for p in sorted(SRC_DIR.glob("15*.csv")):
        nm = p.name.split("_", 1)[1][:-4]
        if nm in PLANTS and (not only or nm in only):
            paths[nm] = p
    unmapped = sorted({p.name.split("_", 1)[1][:-4] for p in SRC_DIR.glob("15*.csv")} - PLANTS.keys())
    if unmapped:
        logger.info(f"매핑에 없어 건너뛴 파일: {unmapped} (월별 스키마는 generation 에 못 들어간다)")

    eng = create_engine(resolve_db_url())
    total = 0
    for nm, path in paths.items():
        with eng.begin() as conn:          # 파일 단위 트랜잭션 — 멱등이라 재실행 가능
            total += load_one(conn, nm, path, dry)
    logger.info(f"{'[계획] ' if dry else ''}합계 {total:,}행 / {len(paths)}종")
    return total


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("names", nargs="*", help="일부만 (기본 전체)")
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()
    run(a.names or None, a.dry_run)
