from __future__ import annotations


import argparse
import asyncio
import re
import xml.etree.ElementTree as ET
from urllib.parse import urlencode
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Iterable, Optional

import aiohttp
import pandas as pd
from sqlalchemy import create_engine, text

from fetch_data.common.config import get_nambu_api_key
from fetch_data.common.db_utils import resolve_db_url, redact_db_url
from fetch_data.common.logger import get_logger
from fetch_data.common.utils import parse_hour_column
from fetch_data.constants import NamebuAPI
from notify.slack_notifier import send_slack_message

logger = get_logger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[2]

ENDPOINT = NamebuAPI.ENDPOINT


def _validate_yyyymmdd(s: str) -> str:
    if not re.fullmatch(r"\d{8}", s):
        raise ValueError(f"YYYYMMDD 형식이어야 합니다: {s!r}")
    datetime.strptime(s, "%Y%m%d")
    return s


def _to_date(s: str) -> date:
    return datetime.strptime(_validate_yyyymmdd(s), "%Y%m%d").date()


def _to_yyyymmdd(d: date) -> str:
    return d.strftime("%Y%m%d")


def _log_debug(msg: str, debug: bool, debug_log: Optional[list[str]]) -> None:
    if not debug:
        return
    print(msg)
    if debug_log is None:
        return
    if len(debug_log) >= 50:
        return
    debug_log.append(msg)


async def _fetch_api_days(
    session: aiohttp.ClientSession,
    api_key: str,
    start_str: str,
    end_str: str,
    gencd: str,
    hogi: int,
    debug: bool = False,
    debug_log: Optional[list[str]] = None,
) -> list[dict]:
    params = {
        "pageNo": "1",
        "numOfRows": "100",
        "strSdate": start_str,
        "strEdate": end_str,
        "strOrgCd": gencd,
        "strHoki": str(hogi),
    }
    # 일부 키는 이미 URL 인코딩된 상태로 제공되므로, 그 경우 직접 쿼리를 구성한다.
    key_is_encoded = "%" in api_key
    if key_is_encoded:
        query = f"serviceKey={api_key}&{urlencode(params)}"
        url = f"{ENDPOINT}?{query}"
        req_kwargs = {"url": url}
    else:
        req_kwargs = {"url": ENDPOINT, "params": {"serviceKey": api_key, **params}}
    try:
        async with session.get(timeout=20, **req_kwargs) as resp:
            text_body = await resp.text()
            if resp.status != 200:
                if debug:
                    _log_debug(
                        f"  - serviceKey: {'encoded (raw query)' if key_is_encoded else 'plain (params)'}",
                        debug,
                        debug_log,
                    )
                    _log_debug(
                        f"  - HTTP {resp.status} for {start_str}~{end_str} {gencd}_{hogi}",
                        debug,
                        debug_log,
                    )
                    _log_debug(f"  - body: {text_body[:300]}", debug, debug_log)
                return []
            root = ET.fromstring(text_body)
            if debug:
                result_code = root.findtext(".//resultCode")
                result_msg = root.findtext(".//resultMsg")
                if result_code or result_msg:
                    _log_debug(
                        f"  - API resultCode={result_code} resultMsg={result_msg}",
                        debug,
                        debug_log,
                    )
            # 응답 포맷이 두 가지:
            # 1) <items><item>...</item></items>
            # 2) <items><ymd>...</ymd>...</items>
            items = root.findall(".//item")
            if items:
                return [{child.tag: child.text for child in item} for item in items]

            items_node = root.find(".//items")
            if items_node is not None:
                return [{child.tag: child.text for child in items_node}]
            return []
    except Exception:
        if debug:
            _log_debug(
                f"  - API 예외: {start_str}~{end_str} {gencd}_{hogi}",
                debug,
                debug_log,
            )
        return []


def _iter_dates(start: date, end: date) -> Iterable[date]:
    cur = start
    while cur <= end:
        yield cur
        cur += timedelta(days=1)


def _get_targets(engine, gencd: Optional[str], hogi: Optional[int]) -> list[dict]:
    q = """
    SELECT
      gencd,
      hogi,
      MAX(plant_name) AS plant_name
    FROM nambu_generation
    GROUP BY gencd, hogi
    ORDER BY gencd, hogi
    """
    df = pd.read_sql(text(q), engine.connect())
    targets = []
    for row in df.itertuples(index=False):
        tgencd = str(row.gencd).strip()
        thogi = int(row.hogi)
        if gencd and tgencd != gencd:
            continue
        if hogi is not None and thogi != hogi:
            continue
        targets.append({"gencd": tgencd, "hogi": thogi, "plant_name": getattr(row, "plant_name", None)})
    return targets


def _find_incomplete_days(engine, gencd: str, hogi: int, start: date, end: date) -> list[date]:
    q = text(
        """
        SELECT
          DATE(datetime) AS d,
          COUNT(DISTINCT EXTRACT(HOUR FROM datetime)) AS hours
        FROM nambu_generation
        WHERE gencd = :gencd
          AND hogi = :hogi
          AND datetime >= :start_dt
          AND datetime < :end_dt
        GROUP BY DATE(datetime)
        """
    )
    start_dt = datetime.combine(start, datetime.min.time())
    end_dt = datetime.combine(end + timedelta(days=1), datetime.min.time())
    with engine.connect() as conn:
        rows = conn.execute(q, {"gencd": gencd, "hogi": hogi, "start_dt": start_dt, "end_dt": end_dt}).fetchall()

    complete = {r[0] for r in rows if int(r[1] or 0) >= 24}
    incomplete = [d for d in _iter_dates(start, end) if d not in complete]
    return incomplete


def _rows_from_api_payload(payloads: list[dict]) -> pd.DataFrame:
    df_raw = pd.DataFrame(payloads)
    v_vars = [c for c in df_raw.columns if c.startswith("qhorgen")]
    df_long = df_raw.melt(
        id_vars=["ymd", "hogi", "gencd", "ipptnm", "qvodgen", "qvodavg", "qvodmax", "qvodmin"],
        value_vars=v_vars,
        var_name="h_str",
        value_name="generation",
    )
    df_long["hour0"] = df_long["h_str"].apply(parse_hour_column).astype(int)
    df_long["datetime"] = pd.to_datetime(df_long["ymd"]) + pd.to_timedelta(df_long["hour0"], unit="h")
    df_long["generation"] = pd.to_numeric(df_long["generation"], errors="coerce").fillna(0)
    df_long["daily_total"] = pd.to_numeric(df_long["qvodgen"], errors="coerce")
    df_long["daily_avg"] = pd.to_numeric(df_long["qvodavg"], errors="coerce")
    df_long["daily_max"] = pd.to_numeric(df_long["qvodmax"], errors="coerce")
    df_long["daily_min"] = pd.to_numeric(df_long["qvodmin"], errors="coerce")
    df_long["plant_name"] = df_long["ipptnm"]
    df_long["hogi"] = pd.to_numeric(df_long["hogi"], errors="coerce").astype("Int64")

    return df_long[
        [
            "datetime",
            "gencd",
            "plant_name",
            "hogi",
            "generation",
            "daily_total",
            "daily_avg",
            "daily_max",
            "daily_min",
        ]
    ].dropna(subset=["datetime", "gencd", "hogi"])


async def backfill(
    engine,
    api_key: str,
    targets: list[dict],
    start: date,
    end: date,
    sleep_sec: float,
    debug: bool,
    debug_log: Optional[list[str]] = None,
) -> tuple[int, int]:
    total_days = 0
    total_rows = 0

    async with aiohttp.ClientSession() as session:
        for t in targets:
            gencd = t["gencd"]
            hogi = t["hogi"]
            name = t.get("plant_name") or f"{gencd}_{hogi}"

            missing_days = _find_incomplete_days(engine, gencd, hogi, start, end)
            if not missing_days:
                print(f"✅ {name}: 누락 없음")
                continue

            print(f"📡 {name}: 누락/미완성 {len(missing_days)}일 백필")
            for d in missing_days:
                day_str = _to_yyyymmdd(d)
                payloads = await _fetch_api_days(
                    session,
                    api_key,
                    day_str,
                    day_str,
                    gencd,
                    hogi,
                    debug=debug,
                    debug_log=debug_log,
                )
                if not payloads:
                    # 일부 날짜는 end가 다음날이어야 응답되는 케이스 보정
                    next_day = _to_yyyymmdd(d + timedelta(days=1))
                    payloads = await _fetch_api_days(
                        session,
                        api_key,
                        day_str,
                        next_day,
                        gencd,
                        hogi,
                        debug=debug,
                        debug_log=debug_log,
                    )
                payloads = [p for p in payloads if (p.get("ymd") or "").replace("-", "") == day_str]

                if not payloads:
                    print(f"  - {day_str}: API 응답 없음/실패")
                    await asyncio.sleep(sleep_sec)
                    continue

                df = _rows_from_api_payload(payloads)
                if df.empty:
                    print(f"  - {day_str}: 변환 결과 없음")
                    await asyncio.sleep(sleep_sec)
                    continue

                day_start = datetime.combine(d, datetime.min.time())
                day_end = day_start + timedelta(days=1)

                with engine.begin() as conn:
                    conn.execute(
                        text(
                            """
                            DELETE FROM nambu_generation
                            WHERE gencd = :gencd
                              AND hogi = :hogi
                              AND datetime >= :day_start
                              AND datetime < :day_end
                            """
                        ),
                        {"gencd": gencd, "hogi": hogi, "day_start": day_start, "day_end": day_end},
                    )
                    df.to_sql("nambu_generation", con=conn, if_exists="append", index=False)

                total_days += 1
                total_rows += len(df)
                print(f"  - {day_str}: ✅ {len(df)}행 적재")

                await asyncio.sleep(sleep_sec)

    return total_days, total_rows


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", default=None, help="백필 시작일(YYYYMMDD), 기본값: 20260101")
    parser.add_argument("--end", default=None, help="백필 종료일(YYYYMMDD), 기본값: 어제")
    parser.add_argument("--gencd", default=None, help="특정 발전소 코드만")
    parser.add_argument("--hogi", default=None, type=int, help="특정 호기만")
    parser.add_argument("--sleep-sec", default=0.05, type=float, help="API 호출 간 대기(초)")
    parser.add_argument("--slack", action="store_true", help="Slack 알림 전송")
    parser.add_argument(
        "--db-url",
        default=None,
        help="DB 접속 문자열 직접 지정 (예: postgresql+psycopg2://user:pass@localhost:5435/pv_data)",
    )
    parser.add_argument("--debug", action="store_true", help="API 응답 디버그 로그 출력")
    parser.add_argument("--debug-slack", action="store_true", help="디버그 로그를 Slack으로 전송 (최대 50줄)")
    args = parser.parse_args()

    api_key = get_nambu_api_key()
    db_url = resolve_db_url(args.db_url)
    if not api_key:
        raise RuntimeError("NAMBU_API_KEY가 설정되어 있지 않습니다.")
    if not db_url:
        raise RuntimeError("DB_URL(또는 PV_DATABASE_URL/LOCAL_DB_URL)이 설정되어 있지 않습니다.")

    default_start = "20260101"
    default_end = (date.today() - timedelta(days=1)).strftime("%Y%m%d")
    start_str = args.start or default_start
    end_str = args.end or default_end

    start = _to_date(start_str)
    end = _to_date(end_str)
    if end < start:
        raise ValueError("end가 start보다 빠릅니다.")

    logger.info(f"DB_URL: {redact_db_url(db_url)}")
    engine = create_engine(db_url)
    targets = _get_targets(engine, args.gencd, args.hogi)
    if not targets:
        raise RuntimeError("대상 발전소(gencd/hogi)를 찾지 못했습니다. (nambu_generation에 기존 데이터가 있어야 합니다.)")

    title = f"Nambu backfill {start_str}~{_to_yyyymmdd(end)}"
    if args.slack:
        send_slack_message(f"[Nambu PV 백필 시작]\n- 기간: {start_str}~{_to_yyyymmdd(end)}\n- 대상: {len(targets)}개")

    try:
        debug_log: Optional[list[str]] = [] if args.debug_slack else None
        days, rows = asyncio.run(
            backfill(engine, api_key, targets, start, end, args.sleep_sec, args.debug, debug_log)
        )
        msg = f"{title}\n- 처리 일수: {days}\n- 적재 행수: {rows}"
        logger.info(msg)
        if args.slack:
            send_slack_message(f"[Nambu PV 백필 완료]\n{msg}")
        if args.slack and args.debug_slack and debug_log:
            debug_msg = "\n".join(debug_log)
            send_slack_message(f"[Nambu PV 디버그]\n{debug_msg}")
    except Exception as e:
        err = f"{title}\n- 에러: {type(e).__name__}: {e}"
        logger.error(err)
        if args.slack:
            send_slack_message(f"[Nambu PV 백필 실패]\n{err}")
        raise


if __name__ == "__main__":
    main()
