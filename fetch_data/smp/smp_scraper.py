"""
KPX SMP 스크래퍼 (외부 SMP_collector 레포의 SMPScraper 이식).

수집 흐름(공통):
  1) GET 으로 페이지를 받아 _csrf 토큰을 추출 (POST가 필요한 경우)
  2) POST(또는 GET)로 데이터 조회
  3) BeautifulSoup+lxml로 HTML <table>을 rowspan/colspan 인식 그리드로 확장

차단 대응: 403/429 또는 본문에 firewall/blocked 감지 시 BLOCKED 반환.
요청 간 랜덤 딜레이로 IP 차단을 회피한다(외부 레포와 동일).

이 모듈은 '원시 그리드(2D list)'와 HTTP 세션만 책임진다.
값 해석/정규화/적재는 호출 측(smp_collect/smp_aggregate/smp_realtime)에서 한다.
"""

from __future__ import annotations

import random
import time
from typing import List, Optional

import requests
from bs4 import BeautifulSoup

from fetch_data.common.logger import get_logger
from fetch_data.constants import SMPAPI

logger = get_logger(__name__)

BLOCK_MARKERS = ("firewall", "blocked", "access denied")


def make_session() -> requests.Session:
    """User-Agent가 설정된 requests 세션을 생성합니다."""
    s = requests.Session()
    s.headers.update({"User-Agent": SMPAPI.USER_AGENT})
    return s


def _expand_table_to_grid(table) -> List[List[str]]:
    """HTML <table>를 rowspan/colspan을 반영한 2D 문자열 그리드로 확장합니다.

    KPX 표는 시간/구분 셀에 rowspan을, 헤더에 colspan을 쓰므로
    단순 행별 td 추출로는 열이 어긋난다. 그리드 확장으로 정렬을 보장한다.
    """
    rows = table.find_all("tr")
    grid: dict = {}
    occupied: set = set()
    for ri, tr in enumerate(rows):
        ci = 0
        for cell in tr.find_all(["th", "td"]):
            while (ri, ci) in occupied:
                ci += 1
            rowspan = int(cell.get("rowspan", 1) or 1)
            colspan = int(cell.get("colspan", 1) or 1)
            text = cell.get_text(strip=True)
            for dr in range(rowspan):
                for dc in range(colspan):
                    grid[(ri + dr, ci + dc)] = text
                    occupied.add((ri + dr, ci + dc))
            ci += colspan
    if not grid:
        return []
    n_rows = max(r for (r, _) in grid) + 1
    n_cols = max(c for (_, c) in grid) + 1
    return [[grid.get((r, c), "") for c in range(n_cols)] for r in range(n_rows)]


def _is_blocked(status: int, text: str) -> bool:
    if status in (403, 429):
        return True
    low = text.lower()
    return any(m in low for m in BLOCK_MARKERS)


def fetch_grid(
    session: requests.Session,
    url: str,
    mid: str,
    *,
    post: bool = False,
    post_data: Optional[dict] = None,
    extra_params: Optional[dict] = None,
    table_index: int = 0,
) -> Optional[List[List[str]]]:
    """KPX 페이지에서 표 하나를 그리드로 가져옵니다.

    Args:
        post: True면 GET으로 _csrf를 얻은 뒤 POST로 데이터를 조회.
        post_data: POST 본문에 합칠 추가 파라미터(issue_date, yy 등). _csrf는 자동 주입.
        extra_params: GET/POST 쿼리스트링에 합칠 추가 파라미터.
        table_index: 페이지에 표가 여러 개일 때 선택할 인덱스.

    Returns:
        2D 문자열 그리드. 차단/빈 응답이면 None.
    """
    params = {"mid": mid}
    if extra_params:
        params.update(extra_params)
    headers = {"Referer": f"{url}?mid={mid}&device=pc"}
    timeout = SMPAPI.TIMEOUT_SEC

    last_err: Optional[str] = None
    for attempt in range(1, SMPAPI.MAX_RETRIES + 1):
        try:
            resp = session.get(url, params=params, headers=headers, timeout=timeout)
            if _is_blocked(resp.status_code, resp.text):
                last_err = f"BLOCKED(GET status={resp.status_code})"
                logger.warning(f"[SMP] {last_err} — {SMPAPI.COOL_OFF_SEC}s 쿨오프 (attempt {attempt})")
                time.sleep(SMPAPI.COOL_OFF_SEC)
                continue
            if resp.status_code != 200:
                last_err = f"HTTP_{resp.status_code}(GET)"
                logger.warning(f"[SMP] {last_err}")
                return None

            html = resp.text
            if post:
                soup = BeautifulSoup(html, "lxml")
                csrf_el = soup.find("input", {"name": "_csrf"})
                csrf = csrf_el.get("value") if csrf_el else None
                if not csrf:
                    last_err = "no _csrf"
                    logger.warning(f"[SMP] {last_err} — {SMPAPI.COOL_OFF_SEC}s 쿨오프 (attempt {attempt})")
                    time.sleep(SMPAPI.COOL_OFF_SEC)
                    continue
                time.sleep(random.uniform(*SMPAPI.WAIT_RANGE))
                body = dict(post_data or {})
                body["_csrf"] = csrf
                resp = session.post(url, params=params, data=body, headers=headers, timeout=timeout)
                if _is_blocked(resp.status_code, resp.text):
                    last_err = f"BLOCKED(POST status={resp.status_code})"
                    logger.warning(f"[SMP] {last_err} — {SMPAPI.COOL_OFF_SEC}s 쿨오프 (attempt {attempt})")
                    time.sleep(SMPAPI.COOL_OFF_SEC)
                    continue
                if resp.status_code != 200:
                    last_err = f"HTTP_{resp.status_code}(POST)"
                    logger.warning(f"[SMP] {last_err}")
                    return None
                html = resp.text

            soup = BeautifulSoup(html, "lxml")
            tables = soup.find_all("table")
            if not tables or table_index >= len(tables):
                last_err = f"no table (found {len(tables)})"
                logger.warning(f"[SMP] {last_err} @ {url}?mid={mid}")
                return None
            grid = _expand_table_to_grid(tables[table_index])
            if not grid:
                last_err = "empty grid"
                return None
            return grid

        except requests.Timeout:
            last_err = "TIMEOUT"
            logger.warning(f"[SMP] TIMEOUT (attempt {attempt}) @ {url}")
            time.sleep(SMPAPI.COOL_OFF_SEC)
        except Exception as e:  # noqa: BLE001
            last_err = f"ERROR: {str(e)[:80]}"
            logger.warning(f"[SMP] {last_err} (attempt {attempt})")
            time.sleep(random.uniform(*SMPAPI.WAIT_RANGE))

    logger.error(f"[SMP] 수집 실패({last_err}) @ {url}?mid={mid}")
    return None
