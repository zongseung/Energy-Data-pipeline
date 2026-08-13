"""
한국농어촌공사 영암/율치 태양광 발전량 수집 (data.go.kr odcloud 15005796).

- 연간 갱신: 2019~ '통합 발전량 현황' 연 1파일(영암1차/영암2차/율치 × 365일, 시간별 wide, kWh).
  2026 데이터는 미등록(차기 등록 2027-01-06) → 스케줄은 연 1회.
- OAS에서 uddi를 **동적 enumerate**(연도별 최신 '현황' 파일) → 비동기 수집 → wide→long →
  generation/plants 코어에 upsert (operator='ekr', fuel='solar').
- 컬럼 스키마가 연도마다 다름(구 `01시`/`날짜`/`발전기명` ↔ 신 `1시`/`월`·`일`/`시설명`) → 패턴 기반 처리.
- 시간 `N시`는 구간종료 표기로 보고 (N-1)시 구간시작으로 적재.
"""
from __future__ import annotations

import argparse
import asyncio
import re
from typing import Optional

import aiohttp
import pandas as pd

from fetch_data.common.config import get_nambu_api_key
from fetch_data.common.db_utils import resolve_db_url
from fetch_data.common.generation_core import upsert_generation
from fetch_data.common.logger import get_logger

logger = get_logger(__name__)

DATASET = "15005796"
OAS_URL = f"https://infuser.odcloud.kr/oas/docs?namespace={DATASET}/v1"
API_BASE = f"https://api.odcloud.kr/api/{DATASET}/v1"
OPERATOR = "ekr"   # 한국농어촌공사
FUEL = "solar"

# 시설명/발전기명 약칭 → 정식 발전소명
_PLANT_NAME = {
    "영암1차": "영암1차태양광",
    "영암2차": "영암2차태양광",
    "율치": "율치태양광",
}


def _norm_plant(raw) -> str:
    s = str(raw).strip()
    return _PLANT_NAME.get(s, s)


async def list_year_uddis(session: aiohttp.ClientSession) -> dict[int, str]:
    """OAS에서 연도별 '통합 발전량 현황' uddi(연도별 최신 스냅샷) 반환: {year: uddi}.

    구 per-plant 데이터(진도 등, 단위/스키마 상이)는 '현황'이 아니므로 제외.
    새 연도(예: 2026)가 등록되면 자동 포함된다.
    """
    async with session.get(OAS_URL, timeout=40) as r:
        oas = await r.json()
    best: dict[int, tuple] = {}   # year -> (score, uddi)
    for path, item in oas.get("paths", {}).items():
        title = ((item.get("get") or {}).get("summary") or "").strip()
        if "현황" not in title:
            continue
        m = re.search(r"(20\d{2})(\d{2})(\d{2})", title)
        if m:
            year, datekey = int(m.group(1)), m.group(1) + m.group(2) + m.group(3)
        else:
            m2 = re.search(r"(\d{2})/(\d{2})/(20\d{2})", title)  # 12/31/2019
            if not m2:
                continue
            year, datekey = int(m2.group(3)), m2.group(3) + m2.group(1) + m2.group(2)
        # 연도별 최신 datekey + '영암 태양광' 명시본 우선
        score = (datekey, 1 if "영암 태양광 발전소 발전량 현황" in title else 0)
        uddi = path.split("uddi:")[-1].strip("/")
        if year not in best or score > best[year][0]:
            best[year] = (score, uddi)
    return {y: u for y, (_, u) in sorted(best.items())}


async def _fetch_uddi(session: aiohttp.ClientSession, key: str, uddi: str) -> list[dict]:
    rows: list[dict] = []
    page = 1
    while True:
        params = {"serviceKey": key, "page": str(page), "perPage": "2000"}
        async with session.get(f"{API_BASE}/uddi:{uddi}", params=params, timeout=40) as r:
            if r.status != 200:
                logger.warning(f"[ekr] HTTP {r.status} uddi={uddi[:8]} page={page}")
                break
            d = await r.json()
        data = d.get("data", []) or []
        rows.extend(data)
        if not data or len(rows) >= (d.get("totalCount") or 0):
            break
        page += 1
    return rows


def _to_long(rows: list[dict], year: int) -> pd.DataFrame:
    cols = ["timestamp", "plant_name", "generation"]
    if not rows:
        return pd.DataFrame(columns=cols)
    df = pd.DataFrame(rows)
    plant_col = next((c for c in ("시설명", "발전기명", "발전소명") if c in df.columns), None)
    if plant_col is not None:
        df["plant_name"] = df[plant_col].map(_norm_plant)
    else:
        # 2019~2021 파일에는 시설명 컬럼이 아예 없다 — 원천이 1차/2차를 구분해
        # 주지 않아 사업 전체가 한 계열로만 온다. '_합계' 같은 접미사를 붙이면
        # "다른 행과 중복되니 빼야 할 줄"로 읽히는데, 141·142(2022~)와 기간이
        # 겹치지 않아 실제로는 이 기간의 유일한 원천이다. 사업명 그대로 둔다.
        # (upsert_generation 이 (plant_name, unit_no) 로 plant_id 를 찾으므로
        #  이 문자열을 바꾸면 DB의 plants.plant_name 도 함께 바꿔야 한다.)
        df["plant_name"] = "영암태양광"

    if "날짜" in df.columns:
        df["date"] = pd.to_datetime(df["날짜"], errors="coerce")
    elif "월" in df.columns and "일" in df.columns:
        df["date"] = pd.to_datetime(dict(
            year=year,
            month=pd.to_numeric(df["월"], errors="coerce"),
            day=pd.to_numeric(df["일"], errors="coerce"),
        ), errors="coerce")
    else:
        raise ValueError(f"날짜 컬럼 없음: {list(df.columns)[:8]}")

    # 시간 컬럼: '10시' / '10h' / '10시(h)' / '1시' 등 (앞자리 숫자 + 시|h)
    hour_cols = [c for c in df.columns if re.match(r"^\s*\d+\s*(시|h)", str(c))]
    long = df.melt(id_vars=["plant_name", "date"], value_vars=hour_cols,
                   var_name="h", value_name="generation")
    long["hour"] = long["h"].str.extract(r"(\d+)").astype(int)
    long["timestamp"] = long["date"] + pd.to_timedelta(long["hour"] - 1, unit="h")
    long["generation"] = pd.to_numeric(long["generation"], errors="coerce")
    return long[cols].dropna(subset=["timestamp", "plant_name"])


async def _collect(years: Optional[list[int]] = None) -> pd.DataFrame:
    key = get_nambu_api_key()
    if not key:
        raise RuntimeError("NAMBU_API_KEY(서비스키)가 설정되지 않았습니다.")
    async with aiohttp.ClientSession() as session:
        year_uddi = await list_year_uddis(session)
        if years:
            year_uddi = {y: u for y, u in year_uddi.items() if y in years}
        items = list(year_uddi.items())
        logger.info(f"[ekr] 대상 연도: {[y for y, _ in items]}")
        results = await asyncio.gather(*[_fetch_uddi(session, key, u) for _, u in items])
    parts = [_to_long(rows, y) for (y, _), rows in zip(items, results)]
    out = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame(columns=["timestamp", "plant_name", "generation"])
    return out


def run(years: Optional[list[int]] = None, db_url: Optional[str] = None) -> int:
    """영암/율치 PV를 generation/plants 코어에 적재. 적재 행수 반환."""
    df = asyncio.run(_collect(years))
    logger.info(f"[ekr] 변환 {len(df)}행")
    return upsert_generation(df, operator=OPERATOR, fuel_type=FUEL, db_url=resolve_db_url(db_url))


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="한국농어촌공사 영암/율치 PV 수집 (odcloud 15005796)")
    ap.add_argument("--years", nargs="*", type=int, default=None, help="연도 필터(없으면 전체)")
    ap.add_argument("--db-url", default=None)
    args = ap.parse_args()
    print("적재 행수:", run(years=args.years, db_url=args.db_url))
