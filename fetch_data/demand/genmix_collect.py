"""
KPX 실시간 발전원별 발전량 5분 수집기 (계통기준, 전국).

전력거래소 '실시간 전력수급현황' 페이지가 당일치 5분 자료를 `var ictArr = [...]`
JSON 으로 페이지 안에 그대로 싣는다. **인증키가 필요 없다.**
(공공데이터포털 PwrAmountByGen API 는 같은 자료지만 서비스별 활용신청이 필요하다.)

이미 있는 research.demand_5min 은 수요·예비력만 담고 발전원 구분이 없다.
이 수집기가 그 빈자리를 채운다 — 특히 **태양광이 전력시장·PPA·BTM 세 갈래로**
나뉘어 자가소비(BTM)까지 잡힌다.

주의:
  · 페이지는 **당일치만** 싣는다. 하루 288칸이 시간이 지나며 채워지고, 지나간
    날은 다시 못 받는다 → 5분마다 돌려야 구멍이 안 생긴다.
  · 아직 안 온 칸은 regDate 가 "0" 이다. 걸러낸다.
  · PPA·BTM 은 KPX 가 명시한 대로 계량값이 아니라 추정치다.
  · 시각은 KST naive (demand_5min 과 동일).

실행:
    uv run python -m fetch_data.demand.genmix_collect
"""
from __future__ import annotations

import argparse
import json
import re
from datetime import datetime
from typing import Optional

import requests

from fetch_data.common.logger import get_logger
from fetch_data.demand.database import (
    get_demand_engine,
    upsert_gen_mix_5min,
)

logger = get_logger(__name__)

URL = "https://new.kpx.or.kr/powerinfoSubmain.es?mid=a10606030000"
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"

# 페이지 필드 → 우리 컬럼
_FIELDS = {
    "sunlight": "solar_market",
    "ppa": "solar_ppa",
    "btm": "solar_btm",
    "windPower": "wind",
    "nuclearPower": "nuclear",
    "gas": "gas",
    "totCoal": "coal_total",
    "localCoal": "coal_domestic",
    "oil": "oil",
    "waterPower": "hydro",
    "raisingWater": "pumped",
    "newRenewable": "renewable_total",
    "neweMw": "renewable_new",
    "reneweMw": "renewable_renew",
    "essMw": "ess",
}


def _num(value) -> Optional[float]:
    try:
        return float(str(value).replace(",", "").replace(" ", ""))
    except (TypeError, ValueError):
        return None


def fetch_records() -> list[dict]:
    """페이지에서 당일 5분 발전원별 레코드를 뽑는다."""
    html = requests.get(URL, headers={"User-Agent": USER_AGENT}, timeout=60).text
    m = re.search(r"var ictArr = (\[.*?\]);", html, re.S)
    if not m:
        raise RuntimeError("페이지에서 ictArr 를 찾지 못했다 — KPX 페이지 구조 변경 의심")

    records = []
    for row in json.loads(m.group(1)):
        reg = str(row.get("regDate", "0")).strip()
        if not reg or reg == "0":
            continue  # 아직 안 채워진 칸
        rec = {"timestamp": datetime.strptime(reg, "%Y-%m-%d %H:%M")}
        for src, col in _FIELDS.items():
            rec[col] = _num(row.get(src))
        records.append(rec)
    return records


def run(db_url: Optional[str] = None) -> int:
    records = fetch_records()
    if not records:
        logger.warning("[genmix] 수집된 레코드 없음")
        return 0
    engine = get_demand_engine(db_url)
    n = upsert_gen_mix_5min(engine, records)
    logger.info(
        f"[genmix] {n}행 UPSERT ({records[0]['timestamp']} ~ {records[-1]['timestamp']})"
    )
    return n


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="KPX 실시간 발전원별 5분 수집")
    ap.add_argument("--db-url", default=None)
    args = ap.parse_args()
    print("적재 행수:", run(db_url=args.db_url))
