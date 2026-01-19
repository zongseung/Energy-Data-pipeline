import asyncio
import os
import re
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import List, Optional, Tuple

import aiohttp
import requests
from dotenv import load_dotenv
from prefect import flow, task

BASE = "https://www.koenergy.kr"
MENU_CD = "FN0912020217"

CSV_URL = f"{BASE}/kosep/gv/nf/dt/nfdt21/csvDown.do"
MAIN_URL = f"{BASE}/kosep/gv/nf/dt/nfdt21/main.do"

# 저장 폴더 (원하면 바꾸기)
PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(PROJECT_ROOT / ".env")
_output_dir_env = os.getenv("NAMDONG_OUTPUT_DIR")
OUTPUT_DIR = Path(_output_dir_env) if _output_dir_env else (PROJECT_ROOT / "pv_data_raw")

SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")
NAMDONG_START_DATE = os.getenv("NAMDONG_START_DATE")
NAMDONG_ORG_NO = os.getenv("NAMDONG_ORG_NO", "").strip()
NAMDONG_HOKI_S = os.getenv("NAMDONG_HOKI_S", "").strip()
NAMDONG_HOKI_E = os.getenv("NAMDONG_HOKI_E", "").strip()
NAMDONG_PAGE_INDEX = os.getenv("NAMDONG_PAGE_INDEX", "1").strip() or "1"


# -------------------------
# Utils
# -------------------------
def _sanitize_filename(s: str) -> str:
    s = s.strip()
    s = re.sub(r"[^\w\-.가-힣 ]+", "_", s)
    s = re.sub(r"\s+", "_", s)
    return s[:180]


def _prompt(msg: str, default: Optional[str] = None) -> str:
    if default is not None:
        s = input(f"{msg} [{default}]: ").strip()
        return s or default
    return input(f"{msg}: ").strip()


def _validate_yyyymmdd(date_str: str) -> str:
    if not re.fullmatch(r"\d{8}", date_str):
        raise ValueError("YYYYMMDD 형식(예: 20210101)이어야 합니다.")
    datetime.strptime(date_str, "%Y%m%d")
    return date_str


def _to_date_yyyymmdd(s: str) -> date:
    return datetime.strptime(s, "%Y%m%d").date()


def _to_yyyymmdd(d: date) -> str:
    return d.strftime("%Y%m%d")


def _month_end(d: date) -> date:
    if d.month == 12:
        next_month = date(d.year + 1, 1, 1)
    else:
        next_month = date(d.year, d.month + 1, 1)
    return next_month - timedelta(days=1)


def split_by_month(date_s: str, date_e: str) -> List[Tuple[str, str]]:
    start = _to_date_yyyymmdd(date_s)
    end = _to_date_yyyymmdd(date_e)
    if end < start:
        raise ValueError("종료일이 시작일보다 빠릅니다.")

    ranges: List[Tuple[str, str]] = []
    cur = start
    while cur <= end:
        me = _month_end(cur)
        chunk_end = me if me <= end else end
        ranges.append((_to_yyyymmdd(cur), _to_yyyymmdd(chunk_end)))
        cur = chunk_end + timedelta(days=1)
    return ranges


def prompt_page_index(default: str = "1") -> str:
    raw = input(f"pageIndex [{default}]: ").strip()
    if not raw:
        return default
    if not raw.isdigit():
        print(f"⚠️ pageIndex가 숫자가 아님({raw!r}) -> {default}로 처리")
        return default
    return raw


def build_main_url(page_index: str, org_no: str, hoki_s: str, hoki_e: str, date_s: str, date_e: str) -> str:
    # 실제 Referer 형태 유지
    return (
        f"{MAIN_URL}"
        f"?pageIndex={page_index}&menuCd={MENU_CD}&xmlText="
        f"&strOrgNo={org_no}&strHokiS={hoki_s}&strHokiE={hoki_e}"
        f"&strDateS={date_s}&strDateE={date_e}"
    )


def tag_for_filename(org_no: str, hoki_s: str, hoki_e: str) -> str:
    if not org_no and not hoki_s and not hoki_e:
        return "전체"
    parts = [org_no if org_no else "ALLORG"]
    if hoki_s or hoki_e:
        hs = hoki_s if hoki_s else "ALL"
        he = hoki_e if hoki_e else "ALL"
        parts.append(f"H{hs}-{he}")
    return "_".join(parts)


def is_probably_csv(body: bytes) -> bool:
    # HTML 에러 페이지/너무 작은 응답 방지
    head = body.lstrip()[:80].lower()
    if head.startswith(b"<!doctype") or head.startswith(b"<html") or b"<head" in head:
        return False
    # 너무 작으면 실패로 간주(경험상 476 bytes 같은 경우)
    if len(body) < 2000:
        return False
    # 쉼표가 거의 없으면 CSV 아닐 가능성
    if body[:2000].count(b",") < 5:
        return False
    return True


# -------------------------
# Slack notify
# -------------------------
def send_slack_message(text: str, webhook_url: Optional[str] = None) -> None:
    if webhook_url is None:
        webhook_url = SLACK_WEBHOOK_URL
    if not webhook_url:
        print("SLACK_WEBHOOK_URL이 설정되어 있지 않습니다. Slack 전송 스킵.")
        return

    try:
        resp = requests.post(webhook_url, json={"text": text}, timeout=5)
        if resp.status_code != 200:
            print(f"Slack 전송 실패: {resp.status_code}, {resp.text}")
    except Exception as e:
        print(f"Slack 전송 중 예외 발생: {e}")


@task(name="Slack 성공 알림", retries=0)
def notify_slack_success(flow_name: str, details: str) -> None:
    msg = f"[{flow_name} 완료]\n{details}"
    send_slack_message(msg)


@task(name="Slack 실패 알림", retries=0)
def notify_slack_failure(flow_name: str, error_msg: str) -> None:
    msg = f"[{flow_name} 실패]\n- 에러: {error_msg}"
    send_slack_message(msg)


# -------------------------
# Backfill helpers
# -------------------------
def _parse_date_range_from_filename(name: str) -> Optional[Tuple[str, str]]:
    m = re.search(r"_(\d{8})-(\d{8})\.csv$", name)
    if not m:
        return None
    return m.group(1), m.group(2)


def get_latest_collected_date(output_dir: Path) -> Optional[date]:
    if not output_dir.exists():
        return None
    latest: Optional[date] = None
    for fp in output_dir.glob("south_pv_*.csv"):
        parsed = _parse_date_range_from_filename(fp.name)
        if not parsed:
            continue
        _, end_str = parsed
        try:
            end_dt = _to_date_yyyymmdd(end_str)
        except Exception:
            continue
        if latest is None or end_dt > latest:
            latest = end_dt
    return latest


def resolve_backfill_range(
    output_dir: Path,
    target_start: Optional[str],
    target_end: Optional[str],
) -> Tuple[date, date]:
    if target_start:
        start_dt = _to_date_yyyymmdd(_validate_yyyymmdd(target_start))
    else:
        latest = get_latest_collected_date(output_dir)
        if latest:
            start_dt = latest + timedelta(days=1)
        elif NAMDONG_START_DATE:
            start_dt = _to_date_yyyymmdd(_validate_yyyymmdd(NAMDONG_START_DATE))
        else:
            start_dt = date.today() - timedelta(days=365)

    if target_end:
        end_dt = _to_date_yyyymmdd(_validate_yyyymmdd(target_end))
    else:
        end_dt = date.today() - timedelta(days=1)

    return start_dt, end_dt


# -------------------------
# Main downloader
# -------------------------
async def download_monthly_csvs(
    page_index: str,
    org_no: str,
    hoki_s: str,
    hoki_e: str,
    date_s: str,
    date_e: str,
    sleep_sec: int = 5,
) -> List[Path]:
    month_ranges = split_by_month(date_s, date_e)
    print(f"\n총 {len(month_ranges)}개 구간(월 단위)으로 분할합니다.")
    for i, (ds, de) in enumerate(month_ranges, start=1):
        print(f"  {i:>2}. {ds} ~ {de}")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    tag = tag_for_filename(org_no, hoki_s, hoki_e)

    headers_common = {
        "Origin": BASE,
        "Content-Type": "application/x-www-form-urlencoded",
        "User-Agent": "Mozilla/5.0",
    }

    saved_files: List[Path] = []

    async with aiohttp.ClientSession(headers={"User-Agent": headers_common["User-Agent"]}) as session:
        for idx, (ds, de) in enumerate(month_ranges, start=1):
            main_url = build_main_url(page_index, org_no, hoki_s, hoki_e, ds, de)

            # 쿠키 확보용 GET (400/500이면 다음으로 넘어가게 처리)
            try:
                async with session.get(main_url, timeout=30) as r1:
                    r1.raise_for_status()
            except Exception as e:
                print(f"[FAIL] ({idx}/{len(month_ranges)}) main.do GET 실패 {ds}~{de}: {e}")
                if idx < len(month_ranges):
                    await asyncio.sleep(sleep_sec)
                continue

            data = {
                "pageIndex": page_index,
                "menuCd": MENU_CD,
                "xmlText": "",
                "strOrgNo": org_no,   # 빈값이면 전체
                "strHokiS": hoki_s,   # 빈값이면 전체
                "strHokiE": hoki_e,   # 빈값이면 전체
                "strDateS": ds,
                "strDateE": de,
                "ptSignature": "",
            }

            post_headers = dict(headers_common)
            post_headers["Referer"] = main_url

            try:
                async with session.post(CSV_URL, data=data, headers=post_headers, timeout=120) as r2:
                    r2.raise_for_status()
                    content_type = (r2.headers.get("Content-Type", "") or "").lower()
                    body = await r2.read()
            except Exception as e:
                print(f"[FAIL] ({idx}/{len(month_ranges)}) csvDown POST 실패 {ds}~{de}: {e}")
                if idx < len(month_ranges):
                    await asyncio.sleep(sleep_sec)
                continue

            if "csv" not in content_type or not is_probably_csv(body):
                print(f"[FAIL] ({idx}/{len(month_ranges)}) {ds}~{de} 비정상 응답")
                print(f"  Content-Type: {content_type}")
                print(f"  Size: {len(body)} bytes")
                print(f"  Head(200): {body[:200]}")
            else:
                out_name = _sanitize_filename(f"south_pv_{tag}_{ds}-{de}.csv")
                out_path = OUTPUT_DIR / out_name
                out_path.write_bytes(body)
                saved_files.append(out_path)
                print(f"[OK] ({idx}/{len(month_ranges)}) Saved: {out_path} ({len(body)} bytes)")

            if idx < len(month_ranges):
                print(f"...{sleep_sec}초 대기 후 다음 구간 수집")
                await asyncio.sleep(sleep_sec)

    return saved_files


@task(name="남동발전 CSV 수집", retries=3, retry_delay_seconds=300)
def collect_namdong_csv(
    page_index: str,
    org_no: str,
    hoki_s: str,
    hoki_e: str,
    start_dt: date,
    end_dt: date,
    sleep_sec: int = 5,
) -> List[Path]:
    return asyncio.run(
        download_monthly_csvs(
            page_index=page_index,
            org_no=org_no,
            hoki_s=hoki_s,
            hoki_e=hoki_e,
            date_s=_to_yyyymmdd(start_dt),
            date_e=_to_yyyymmdd(end_dt),
            sleep_sec=sleep_sec,
        )
    )


@flow(name="Daily Namdong PV Collection Flow", log_prints=True)
def daily_namdong_collection_flow(
    target_start: Optional[str] = None,
    target_end: Optional[str] = None,
    sleep_sec: int = 5,
) -> List[Path]:
    print(f"\n{'='*60}")
    print("남동발전 PV 수집 플로우 시작")
    print(f"실행 시각: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}\n")

    try:
        start_dt, end_dt = resolve_backfill_range(OUTPUT_DIR, target_start, target_end)
        if start_dt > end_dt:
            print("수집 대상이 없습니다. 최신 상태입니다.")
            notify_slack_success.submit(
                "Namdong PV",
                "- 수집 대상 없음 (최신 상태)"
            )
            return []

        print(f"수집 기간: {start_dt} ~ {end_dt}")
        saved_files = collect_namdong_csv.submit(
            NAMDONG_PAGE_INDEX,
            NAMDONG_ORG_NO,
            NAMDONG_HOKI_S,
            NAMDONG_HOKI_E,
            start_dt,
            end_dt,
            sleep_sec,
        ).result()

        notify_slack_success.submit(
            "Namdong PV",
            f"- 기간: {_to_yyyymmdd(start_dt)}~{_to_yyyymmdd(end_dt)}\n"
            f"- 저장 파일 수: {len(saved_files)}\n"
            f"- 저장 폴더: {OUTPUT_DIR}"
        )

        return saved_files

    except Exception as e:
        error_msg = f"{type(e).__name__}: {e}"
        print(f"\n플로우 실행 중 에러 발생: {error_msg}")
        notify_slack_failure.submit("Namdong PV", error_msg)
        raise


def main():
    page_index = prompt_page_index("1")

    org_no = _prompt("발전소 코드(strOrgNo) - 전체는 엔터", "").strip()
    hoki_s = _prompt("호기 시작(strHokiS) - 전체는 엔터", "").strip()
    hoki_e = _prompt("호기 종료(strHokiE) - 전체는 엔터", "").strip()

    while True:
        try:
            date_s = _validate_yyyymmdd(_prompt("시작일(YYYYMMDD)", "20220101"))
            date_e = _validate_yyyymmdd(_prompt("종료일(YYYYMMDD)", "20221231"))
            if _to_date_yyyymmdd(date_e) < _to_date_yyyymmdd(date_s):
                raise ValueError("종료일이 시작일보다 빠릅니다.")
            break
        except Exception as e:
            print(f"입력 오류: {e}")

    sleep_sec_raw = _prompt("월별 다운로드 간 대기(초)", "5")
    sleep_sec = 5 if not sleep_sec_raw.isdigit() else int(sleep_sec_raw)

    saved = asyncio.run(
        download_monthly_csvs(
            page_index=page_index,
            org_no=org_no,
            hoki_s=hoki_s,
            hoki_e=hoki_e,
            date_s=date_s,
            date_e=date_e,
            sleep_sec=sleep_sec,
        )
    )

    print("\n완료.")
    print(f"- 저장 폴더: {OUTPUT_DIR}")
    print(f"- 저장 파일 수: {len(saved)}")


if __name__ == "__main__":
    main()
