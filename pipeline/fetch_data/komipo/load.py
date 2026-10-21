"""중부발전(KOMIPO) parquet -> generation 코어 적재.

원본은 `collect.py` 가 쌓은 `data/komipo/{station}_{year}.parquet` (B552521 renewEnergy).
**원본 parquet 은 고치지 않는다** — 커서가 parquet 기반이라 지우면 다음 수집이 되받아오고,
재검증도 못 하게 된다. 교정은 전부 이 적재 단계의 규칙이다.

## 2026-10-21 실측으로 정한 것

단위는 **kWh**다. 제주대 태양광(공식 설비 1,087kW) 일합을 공공데이터
`15140015`(지역별 태양광 발전실적, 일단위 MWh) 와 967일 대조 → 비율 중앙값 1.09.
시간 패턴도 정상(06~19시, 12시 피크).

**공식 공개값과 약 9% 차이가 난다** (중앙값 1.0922, 날마다 1.03~1.15 변동).
단위 문제가 아니다 — 고정 비율이 아니기 때문이다. 상명풍력을 공식 시간별
`15119454` 와 대조해도 최선이 상관 0.637(오프셋 -1h)로 맞지 않는다.
**parquet=발전단(gross) / 공식=거래·송전단(net) 으로 추정**하되 확정은 못 했다.
남부 PwrGenTran 의 qhorgen(발전) vs qhortrn(송전) 비가 94.8% 였던 것과 같은 구조로 보인다.
→ source='komipo-api' 로 구분해 넣고, 공식값과 다름을 아는 채로 쓴다.

## 전처리 규칙

1. **전구간 0 설비 제외** — 세종 매봉산 풍력(21,979행) · 신보령 태양광 2호기(15,260행).
   plants 도 만들지 않는다.
2. **이름 분절 합침** — 보령 '수상태양광'/'수상 태양광'(공백 하나 차이)은 같은 설비다.
   2018-11~2020-05-12 공백있음 → 2020-05-13~2024-07-31 공백없음 → 2024-08~ 공백있음 으로
   표기가 두 번 바뀌었다. 시각 겹침 0건, 피크 둘 다 12시, p99 833/902 가 공식 1,172kW 아래로 일관.
   같은 source_key 를 줘서 한 계열로 모은다.
3. **스파이크 -> NULL** — 설비별 p99(>0 값 기준) 의 3배 초과. 112행.
   성읍태양광 2호기 51,732(공식 997kW 의 52배) · 인천 연료전지 1호기 7,192,739(p99 의 480배) 등.
4. **태양광 야간(21~04시) >0 -> NULL** — 20행. 3번과 14행 겹치고 6행이 추가로 걸린다.
   보령 태양광 4호기 21·22시 703kWh 는 p99 배수로는 안 잡힌다(367*3=1,101).
   세종 태양광 1호기 14행은 전부 00시인데 그날 01~23시 합과 비율이 1.8~inf 로 일정하지 않아
   '일 총량이 0시 칸에 들어간 것'으로 보인다 — 시간별 발전량이 아니므로 버린다.

사용:
    uv run python -m pipeline.fetch_data.komipo.load --dry-run
    uv run python -m pipeline.fetch_data.komipo.load
"""
from __future__ import annotations

import argparse
import glob

import pandas as pd
from sqlalchemy import create_engine, text

from pipeline.fetch_data.common.db_utils import resolve_db_url
from pipeline.fetch_data.common.logger import get_logger
from pipeline.fetch_data.common.paths import DATA_DIR

logger = get_logger(__name__)

SRC_DIR = DATA_DIR / "komipo"
OPERATOR = "jungbu"   # 한국중부발전 (디렉터리명 komipo 는 기관코드 B552521 약칭)
SOURCE = "jungbu-api"
SPIKE_FACTOR = 3.0          # 설비 p99 의 몇 배를 이상치로 볼지
NIGHT = set(range(21, 24)) | set(range(0, 5))   # 태양광이 0 이어야 하는 시간

# (unitterm) -> (source_key, fuel_type)   ※ 본부는 region 과 site_name 으로 들어간다
# 전구간 0 인 2종은 일부러 빼 뒀다 — 여기 없으면 적재되지 않는다.
UNITS: dict[tuple[str, str], tuple[str, str]] = {
    ("보령발전본부", "태양광 1호기"):        ("boryeong-pv-1",       "solar"),
    ("보령발전본부", "태양광 3호기"):        ("boryeong-pv-3",       "solar"),
    ("보령발전본부", "태양광 4호기"):        ("boryeong-pv-4",       "solar"),
    ("보령발전본부", "수상태양광"):          ("boryeong-floating-pv", "solar"),  # ← 분절
    ("보령발전본부", "수상 태양광"):         ("boryeong-floating-pv", "solar"),  # ← 같은 설비
    ("보령발전본부", "순수드림센터 1호기"):   ("boryeong-sunsu-dream", "solar"),
    ("보령발전본부", "에너지월드 1호기"):     ("boryeong-energy-world", "solar"),
    ("보령발전본부", "제1소수력"):           ("boryeong-hydro-1",    "hydro"),
    ("보령발전본부", "제2소수력"):           ("boryeong-hydro-2",    "hydro"),
    ("보령발전본부", "연료전지 2호기"):       ("boryeong-fc-2",       "fuel_cell"),
    ("신보령발전본부", "태양광 1호기"):       ("sinboryeong-pv-1",    "solar"),
    ("신보령발전본부", "제1소수력"):          ("sinboryeong-hydro-1", "hydro"),
    ("신보령발전본부", "제2소수력"):          ("sinboryeong-hydro-2", "hydro"),
    ("인천발전본부", "태양광 1호기"):         ("incheon-pv-1",        "solar"),
    ("인천발전본부", "태양광 2호기"):         ("incheon-pv-2",        "solar"),
    ("인천발전본부", "태양광 3호기"):         ("incheon-pv-3",        "solar"),
    ("인천발전본부", "아시아드 태양광 1호기"): ("incheon-asiad-pv-1",  "solar"),
    ("인천발전본부", "아시아드 태양광 2호기"): ("incheon-asiad-pv-2",  "solar"),
    # 인천 연료전지는 2022-12-05/06 에 '호기' -> '단계' 로 표기가 바뀐 분절이다.
    # p99 가 1호기 14,979 / 1단계 14,518, 2호기 15,893 / 2단계 15,865 로 짝이 맞는다.
    ("인천발전본부", "연료전지 1호기"):        ("incheon-fc-1",        "fuel_cell"),
    ("인천발전본부", "연료전지 1단계"):        ("incheon-fc-1",        "fuel_cell"),
    ("인천발전본부", "연료전지 2호기"):        ("incheon-fc-2",        "fuel_cell"),
    ("인천발전본부", "연료전지 2단계"):        ("incheon-fc-2",        "fuel_cell"),
    ("세종발전본부", "태양광 1호기"):          ("sejong-pv-1",         "solar"),
    ("세종발전본부", "연료전지 1호기"):        ("sejong-fc-1",         "fuel_cell"),
    ("제주발전본부", "태양광 1호기"):          ("jeju-pv-1",           "solar"),
    ("제주발전본부", "태양광 2호기"):          ("jeju-pv-2",           "solar"),
    ("제주발전본부", "제주 본부 태양광 2호기"): ("jeju-hq-pv-2",        "solar"),
    ("제주발전본부", "제주대 태양광"):          ("jejudae-pv",          "solar"),
    ("제주발전본부", "성읍태양광 1호기"):       ("seongeup-pv-1",       "solar"),
    ("제주발전본부", "성읍태양광 2호기"):       ("seongeup-pv-2",       "solar"),
    ("제주발전본부", "상명풍력"):              ("sangmyeong-wind",     "wind"),
}
DROPPED = {("세종발전본부", "매봉산 풍력"), ("신보령발전본부", "신보령 태양광 2호기")}

_ENSURE = text("""
    INSERT INTO plants (source_key, plant_name, unit_no, fuel_type, operator, region, site_name)
    VALUES (:sk, :name, '1', :fuel, :op, :region, :site)
    ON CONFLICT (operator, source_key, unit_no) DO NOTHING
""")
_ID = text("SELECT plant_id FROM plants WHERE operator=:op AND source_key=:sk AND unit_no='1'")
_WIPE = text("DELETE FROM generation WHERE plant_id=:pid AND timestamp>=:lo AND timestamp<=:hi")
_INSERT = text("""
    INSERT INTO generation (timestamp, plant_id, gen_kwh, source)
    VALUES (:ts, :pid, :kwh, :src)
    ON CONFLICT (timestamp, plant_id)
    DO UPDATE SET gen_kwh = EXCLUDED.gen_kwh, source = EXCLUDED.source
""")


def read_all() -> pd.DataFrame:
    files = sorted(glob.glob(str(SRC_DIR / "*.parquet")))
    if not files:
        raise FileNotFoundError(f"parquet 없음: {SRC_DIR}")
    d = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
    return d.drop_duplicates(subset=["siteterm", "unitterm", "timestamp"], keep="last")


def clean(d: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    """전처리 규칙 적용. (정리된 df, 통계) 반환."""
    stat = {"입력": len(d)}
    key = list(zip(d["siteterm"], d["unitterm"]))
    d = d.assign(_key=key)

    unknown = sorted({k for k in set(key) if k not in UNITS and k not in DROPPED})
    if unknown:
        raise RuntimeError(
            f"UNITS 에 없는 설비 {len(unknown)}종 — 자동 추가하지 않는다. "
            f"확인 후 매핑을 넣고 다시 돌려라: {unknown}"
        )
    d = d[d["_key"].isin(UNITS)].copy()
    stat["전구간0 제외"] = stat["입력"] - len(d)

    d["source_key"] = [UNITS[k][0] for k in d["_key"]]
    d["fuel_type"] = [UNITS[k][1] for k in d["_key"]]
    d["gen_kwh"] = pd.to_numeric(d["daypower"], errors="coerce")

    # ③ 스파이크: 설비(source_key) 별 p99 의 N배 초과
    spike = pd.Series(False, index=d.index)
    for sk, g in d.groupby("source_key"):
        nz = g.loc[g["gen_kwh"] > 0, "gen_kwh"]
        if nz.empty:
            continue
        spike |= (d["source_key"] == sk) & (d["gen_kwh"] > nz.quantile(0.99) * SPIKE_FACTOR)
    stat["스파이크 NULL"] = int(spike.sum())

    # ④ 태양광 야간
    night = (d["fuel_type"] == "solar") & d["timestamp"].dt.hour.isin(NIGHT) & (d["gen_kwh"] > 0)
    stat["야간 NULL"] = int(night.sum())
    stat["둘 다 걸린 행"] = int((spike & night).sum())

    d.loc[spike | night, "gen_kwh"] = None
    stat["적재"] = len(d)
    return d, stat


def run(dry: bool = False) -> int:
    d, stat = clean(read_all())
    for k, v in stat.items():
        logger.info(f"  {k}: {v:,}")

    eng = create_engine(resolve_db_url())
    total = 0
    for (sk, fuel), g in d.groupby(["source_key", "fuel_type"]):
        site = g["siteterm"].iloc[0]
        latest_name = g.loc[g["timestamp"].idxmax(), "unitterm"]   # 분절이면 최근 표기를 쓴다
        region = "jeju" if site.startswith("제주") else "mainland"
        lo, hi = g["timestamp"].min(), g["timestamp"].max()
        logger.info(f"[{sk}] {fuel} {site} {lo.date()}~{hi.date()} {len(g):,}행 \"{latest_name}\"")
        if dry:
            total += len(g)
            continue
        with eng.begin() as conn:
            conn.execute(_ENSURE, {"sk": sk, "name": latest_name, "fuel": fuel,
                                   "op": OPERATOR, "region": region, "site": site})
            pid = conn.execute(_ID, {"op": OPERATOR, "sk": sk}).scalar()
            if pid is None:
                raise RuntimeError(f"{sk}: plants 등록 실패")
            conn.execute(_WIPE, {"pid": pid, "lo": lo, "hi": hi})
            recs = [{"ts": t, "pid": pid, "kwh": (None if pd.isna(v) else float(v)), "src": SOURCE}
                    for t, v in zip(g["timestamp"], g["gen_kwh"])]
            for i in range(0, len(recs), 5000):
                conn.execute(_INSERT, recs[i:i + 5000])
            total += len(recs)
    logger.info(f"{'[계획] ' if dry else ''}합계 {total:,}행 / {d['source_key'].nunique()}계열")
    return total


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    run(ap.parse_args().dry_run)
