"""
남동발전(KOEN) 비태양광 발전원 - 시간대별 발전실적 배치 병렬 수집기.

대상 발전원 (koenergy.kr 메뉴):
- 해양소수력 (nfdt24 / FN0912020219)
- 연료전지   (nfdt25 / FN0912020220)
- 화력       (nfdt26 / FN0912020221)

수집 방식 (태양광 namdong_collect_pv 와 동일 패턴):
1) main.do GET 으로 세션 쿠키(JSESSIONID) 확보
2) csvDown.do POST 로 기간별 CSV 다운로드 (Referer = main.do)

기간은 '월 단위'로 분할하고, 분할된 구간을 asyncio.Semaphore 로 묶어
배치 병렬(동시 실행 상한 = concurrency)로 비동기 수집한다.
기본값은 최근 5개월(완결된 월 기준).

위치 데이터:
사이트 응답에는 위경도가 없으므로, 수집된 '발전구분'을
fetch_data.gen.locations 의 발전본부 좌표에 매핑해 별도 CSV로 저장한다.

DB 적재는 추후. 현재는 CSV 저장까지만 수행한다.

사용 예:
    # 최근 5개월, 3개 발전원 전체
    uv run python -m fetch_data.gen.namdong_gen_collect

    # 기간/발전원/동시성 지정
    uv run python -m fetch_data.gen.namdong_gen_collect \
        --start 20251201 --end 20260430 \
        --types ocean_hydro,fuel_cell --concurrency 4
"""

from __future__ import annotations

import argparse
import asyncio
import re
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import aiohttp
import pandas as pd

from fetch_data.common.koen import (
    get_koen_ssl_context,
    is_probably_csv,
    read_csv_flexible,
    split_by_month,
)
from fetch_data.common.logger import get_logger
from fetch_data.constants import NamdongGenAPI
from fetch_data.gen.capacities import resolve_capacity
from fetch_data.gen.locations import resolve_location

logger = get_logger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_RAW_DIR = PROJECT_ROOT / "gen_data_raw"
DEFAULT_LOC_PATH = PROJECT_ROOT / "gen_data" / "namdong_gen_plant_locations.csv"
DEFAULT_CAP_PATH = PROJECT_ROOT / "gen_data" / "namdong_gen_plant_capacities.csv"

USER_AGENT = "Mozilla/5.0"


# -------------------------
# 날짜 유틸
# -------------------------
from fetch_data.common.date_utils import (
    month_end as _month_end,
    add_months as _add_months,
    to_yyyymmdd as _to_str,
    validate_yyyymmdd as _validate_yyyymmdd,
    to_date_yyyymmdd as _to_date,
)


def recent_n_months(n: int, ref: Optional[date] = None) -> Tuple[date, date]:
    """완결된 최근 n개월의 (시작일, 종료일)을 반환.

    예) ref=2026-05-26, n=5 -> (2025-12-01, 2026-04-30)
    """
    if n < 1:
        raise ValueError("n은 1 이상이어야 합니다.")
    ref = ref or date.today()
    first_this = date(ref.year, ref.month, 1)
    end = first_this - timedelta(days=1)  # 전월 말일
    months_total = end.year * 12 + (end.month - 1) - (n - 1)
    sy, sm = divmod(months_total, 12)
    start = date(sy, sm + 1, 1)
    return start, end


# koenergy.kr 가 데이터를 제공하는 하한(이 이전 월은 빈 응답). 탐지 시작점.
EARLIEST_FLOOR = date(2021, 1, 1)


# -------------------------
# 응답 검증 / 디코딩
# -------------------------
from fetch_data.common.utils import decode_bytes as decode_csv_bytes


# -------------------------
# 다운로드 (비동기)
# -------------------------
def _build_main_url(page: str, menu_cd: str, ds: str = "", de: str = "") -> str:
    return (
        f"{NamdongGenAPI.main_url(page)}"
        f"?pageIndex=1&menuCd={menu_cd}&xmlText="
        f"&strOrgNo=&strHokiS=&strHokiE=&strDateS={ds}&strDateE={de}"
    )


async def _fetch_chunk(
    session: aiohttp.ClientSession,
    sem: asyncio.Semaphore,
    gen_key: str,
    cfg: dict,
    ds: str,
    de: str,
    out_dir: Path,
    retries: int = 2,
    backoff_sec: float = 3.0,
) -> Optional[Path]:
    """한 발전원의 한 달치 CSV를 POST로 받아 저장. 실패 시 None."""
    page, menu_cd, label = cfg["page"], cfg["menu_cd"], cfg["label"]
    main_url = _build_main_url(page, menu_cd, ds, de)
    csv_url = NamdongGenAPI.csv_url(page)

    data = {
        "pageIndex": "1",
        "menuCd": menu_cd,
        "xmlText": "",
        "strOrgNo": "",
        "strHokiS": "",
        "strHokiE": "",
        "strDateS": ds,
        "strDateE": de,
        "ptSignature": "",
    }
    headers = {
        "Origin": NamdongGenAPI.BASE_URL,
        "Content-Type": "application/x-www-form-urlencoded",
        "Referer": main_url,
        "User-Agent": USER_AGENT,
    }

    async with sem:
        for attempt in range(1, retries + 2):
            try:
                async with session.post(csv_url, data=data, headers=headers, timeout=120) as r:
                    r.raise_for_status()
                    content_type = (r.headers.get("Content-Type", "") or "").lower()
                    body = await r.read()
            except Exception as e:
                logger.warning(f"[{label}] {ds}~{de} POST 실패(시도 {attempt}): {e}")
                if attempt <= retries:
                    await asyncio.sleep(backoff_sec * attempt)
                continue

            if "csv" not in content_type or not is_probably_csv(body):
                logger.warning(
                    f"[{label}] {ds}~{de} 비정상 응답(시도 {attempt}) "
                    f"ct={content_type} size={len(body)} head={body[:120]!r}"
                )
                if attempt <= retries:
                    await asyncio.sleep(backoff_sec * attempt)
                continue

            out_path = out_dir / f"koen_{gen_key}_{ds}-{de}.csv"
            out_path.write_text(decode_csv_bytes(body), encoding="utf-8-sig")
            logger.info(f"[{label}] OK {ds}~{de} -> {out_path.name} ({len(body)} bytes, utf-8)")
            return out_path

    logger.error(f"[{label}] {ds}~{de} 최종 실패")
    return None


async def _month_has_data(session: aiohttp.ClientSession, cfg: dict, d: date) -> bool:
    """해당 월에 데이터가 있는지(빈 응답이 아닌지) 확인."""
    ds, de = _to_str(d), _to_str(_month_end(d))
    main_url = _build_main_url(cfg["page"], cfg["menu_cd"], ds, de)
    data = {
        "pageIndex": "1", "menuCd": cfg["menu_cd"], "xmlText": "",
        "strOrgNo": "", "strHokiS": "", "strHokiE": "",
        "strDateS": ds, "strDateE": de, "ptSignature": "",
    }
    headers = {
        "Origin": NamdongGenAPI.BASE_URL,
        "Content-Type": "application/x-www-form-urlencoded",
        "Referer": main_url,
        "User-Agent": USER_AGENT,
    }
    try:
        async with session.post(
            NamdongGenAPI.csv_url(cfg["page"]), data=data, headers=headers, timeout=60
        ) as r:
            body = await r.read()
        return is_probably_csv(body)
    except Exception:
        return False


async def detect_available_range(
    session: aiohttp.ClientSession,
    cfg: dict,
    floor: date = EARLIEST_FLOOR,
    ref: Optional[date] = None,
) -> Optional[Tuple[date, date]]:
    """사이트가 제공하는 (가장 오래된 월, 가장 최신 월)을 자동 탐지.

    - 최신: 직전 완결월부터 뒤로 가며 첫 데이터월 (최대 6개월 탐색)
    - 최초: [floor, 최신] 구간에서 데이터가 처음 나타나는 월을 이분 탐색
            (한번 데이터가 시작되면 이후 계속 존재한다는 단조성 가정)
    """
    ref = ref or date.today()
    label = cfg["label"]

    # 최신 가용월
    latest: Optional[date] = None
    probe = _add_months(date(ref.year, ref.month, 1), -1)
    for _ in range(6):
        if await _month_has_data(session, cfg, probe):
            latest = probe
            break
        probe = _add_months(probe, -1)
    if latest is None:
        logger.warning(f"[{label}] 최신 가용월 탐지 실패")
        return None

    # 최초 가용월 (이분 탐색)
    floor = floor.replace(day=1)
    lo = floor.year * 12 + (floor.month - 1)
    hi = latest.year * 12 + (latest.month - 1)
    best = hi
    while lo <= hi:
        mid = (lo + hi) // 2
        y, m = divmod(mid, 12)
        if await _month_has_data(session, cfg, date(y, m + 1, 1)):
            best = mid
            hi = mid - 1
        else:
            lo = mid + 1
    ey, em = divmod(best, 12)
    earliest = date(ey, em + 1, 1)
    logger.info(f"[{label}] 가용 범위 자동 탐지: {earliest} ~ {_month_end(latest)}")
    return earliest, latest


async def _download_type(
    gen_key: str,
    cfg: dict,
    sem: asyncio.Semaphore,
    out_root: Path,
    month_ranges: Optional[List[Tuple[str, str]]] = None,
    floor: date = EARLIEST_FLOOR,
    ref: Optional[date] = None,
) -> List[Path]:
    """한 발전원: 쿠키 1회 확보 후 월별 구간을 배치 병렬 수집.

    month_ranges 가 None 이면 사이트 가용 범위를 자동 탐지(최초~최신)한다.
    """
    out_dir = out_root / gen_key
    out_dir.mkdir(parents=True, exist_ok=True)

    connector = aiohttp.TCPConnector(ssl=get_koen_ssl_context())
    async with aiohttp.ClientSession(
        headers={"User-Agent": USER_AGENT}, connector=connector
    ) as session:
        # 세션 쿠키(JSESSIONID) 확보
        try:
            async with session.get(_build_main_url(cfg["page"], cfg["menu_cd"]), timeout=30) as r:
                r.raise_for_status()
        except Exception as e:
            logger.warning(f"[{cfg['label']}] 쿠키 확보 GET 실패(계속 진행): {e}")

        if month_ranges is None:
            rng = await detect_available_range(session, cfg, floor, ref)
            if rng is None:
                return []
            start, end = rng
            month_ranges = split_by_month(start, _month_end(end))
            logger.info(f"[{cfg['label']}] {len(month_ranges)}개월 수집 예정")

        tasks = [
            _fetch_chunk(session, sem, gen_key, cfg, ds, de, out_dir)
            for ds, de in month_ranges
        ]
        saved = await asyncio.gather(*tasks)

    return [p for p in saved if p is not None]


async def download_all(
    gen_keys: List[str],
    concurrency: int,
    out_root: Path,
    start: Optional[date] = None,
    end: Optional[date] = None,
    full: bool = False,
    floor: date = EARLIEST_FLOOR,
) -> Dict[str, List[Path]]:
    """선택한 발전원들을 동시에, 월별 구간은 공용 세마포어로 배치 병렬 수집.

    full=True 면 발전원별로 사이트 가용 범위(최초~최신)를 자동 탐지해 전부 수집.
    그렇지 않으면 [start, end] 를 월 단위로 분할해 수집.
    """
    if full:
        month_ranges = None
        logger.info(f"전체 자동 수집 모드 (탐지 하한 {floor}) — 발전원별 가용 범위 탐지")
    else:
        if start is None or end is None:
            raise ValueError("full=False 이면 start/end 가 필요합니다.")
        month_ranges = split_by_month(start, end)
        logger.info(f"수집 기간 {_to_str(start)}~{_to_str(end)} / 월 구간 {len(month_ranges)}개")

    sem = asyncio.Semaphore(concurrency)
    results: Dict[str, List[Path]] = {}

    async def run_one(key: str):
        results[key] = await _download_type(
            key, NamdongGenAPI.GEN_TYPES[key], sem, out_root,
            month_ranges=month_ranges, floor=floor,
        )

    await asyncio.gather(*(run_one(k) for k in gen_keys))
    return results


# -------------------------
# 발전소 위치 CSV 생성
# -------------------------
def build_plant_locations(
    results: Dict[str, List[Path]],
    out_path: Path,
) -> pd.DataFrame:
    """수집 CSV들의 '발전구분'을 위경도/주소에 매핑해 위치 CSV로 저장.

    Returns:
        DataFrame[gen_type, plant, lat, lon, address, site, matched]
    """
    rows: List[dict] = []
    seen: set = set()

    for gen_key, files in results.items():
        label = NamdongGenAPI.GEN_TYPES[gen_key]["label"]
        for fp in files:
            try:
                df = read_csv_flexible(fp)
            except Exception as e:
                logger.warning(f"[위치] 읽기 실패 {fp.name}: {e}")
                continue
            if "발전구분" not in df.columns:
                continue
            for plant in df["발전구분"].astype(str).str.strip().unique():
                key = (gen_key, plant)
                if not plant or key in seen:
                    continue
                seen.add(key)
                loc = resolve_location(plant)
                rows.append(
                    {
                        "gen_type": label,
                        "plant": plant,
                        "lat": loc["lat"] if loc else None,
                        "lon": loc["lon"] if loc else None,
                        "address": loc["address"] if loc else None,
                        "site": loc["site"] if loc else None,
                        "matched": bool(loc),
                    }
                )

    loc_df = pd.DataFrame(
        rows, columns=["gen_type", "plant", "lat", "lon", "address", "site", "matched"]
    ).sort_values(["gen_type", "plant"]).reset_index(drop=True)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    loc_df.to_csv(out_path, index=False, encoding="utf-8-sig")

    unmatched = loc_df.loc[~loc_df["matched"], "plant"].tolist()
    logger.info(f"[위치] 발전소 {len(loc_df)}개 -> {out_path}")
    if unmatched:
        logger.warning(f"[위치] 미매핑(좌표 없음) {len(unmatched)}개: {unmatched}")
    return loc_df


def rebuild_locations_from_raw(
    raw_dir: Path = DEFAULT_RAW_DIR,
    loc_path: Path = DEFAULT_LOC_PATH,
    gen_keys: Optional[List[str]] = None,
) -> pd.DataFrame:
    """디스크에 저장된 '모든' 원본 CSV에서 위치 CSV를 재생성한다.

    build_plant_locations 는 단일 수집 run 의 메모리 결과(results)만 반영하므로,
    일부 발전원만 수집한 run 이 위치 CSV를 덮어쓰면 나머지 발전원이 사라진다.
    이 함수는 raw_dir 의 발전원별 원본을 전부 스캔해 항상 완전한 위치 CSV를 만든다.
    """
    gen_keys = gen_keys or list(NamdongGenAPI.GEN_TYPES.keys())
    results: Dict[str, List[Path]] = {}
    for key in gen_keys:
        cat_dir = Path(raw_dir) / key
        files = sorted(cat_dir.glob("koen_*.csv")) if cat_dir.exists() else []
        results[key] = files
        logger.info(f"[위치재생성] {key}: 원본 {len(files)}개")
    return build_plant_locations(results, loc_path)


# -------------------------
# 호기별 위치 + 정격용량 CSV 생성
# -------------------------
def build_plant_capacities(
    raw_dir: Path = DEFAULT_RAW_DIR,
    out_path: Path = DEFAULT_CAP_PATH,
    gen_keys: Optional[List[str]] = None,
) -> pd.DataFrame:
    """디스크의 원본 CSV에서 (발전구분, 호기) 조합을 모아, 호기 단위로
    위치(locations) + 정격용량(capacities)을 결합한 CSV를 생성한다.

    plant_name(= 발전구분_호기)은 transform_gen 의 규칙과 동일하므로
    시간별 long CSV(gen_data/{category}_long.csv)와 plant_name 으로 바로 조인된다.

    Returns:
        DataFrame[gen_type, plant_name, plant, hogi, lat, lon, address, site,
                  capacity_mw, capacity_confidence, capacity_source]
    """
    gen_keys = gen_keys or list(NamdongGenAPI.GEN_TYPES.keys())
    rows: List[dict] = []
    seen: set = set()

    for gen_key in gen_keys:
        label = NamdongGenAPI.GEN_TYPES[gen_key]["label"]
        cat_dir = Path(raw_dir) / gen_key
        files = sorted(cat_dir.glob("koen_*.csv")) if cat_dir.exists() else []
        for fp in files:
            try:
                df = read_csv_flexible(fp)
            except Exception as e:
                logger.warning(f"[용량] 읽기 실패 {fp.name}: {e}")
                continue
            if "발전구분" not in df.columns or "호기" not in df.columns:
                continue
            combos = (
                df[["발전구분", "호기"]]
                .astype(str)
                .apply(lambda s: s.str.strip())
                .drop_duplicates()
            )
            for plant, hogi in combos.itertuples(index=False):
                plant_name = f"{plant}_{hogi}"
                key = (gen_key, plant_name)
                if not plant or not hogi or key in seen:
                    continue
                seen.add(key)
                loc = resolve_location(plant)
                cap = resolve_capacity(plant_name)
                rows.append(
                    {
                        "gen_type": label,
                        "plant_name": plant_name,
                        "plant": plant,
                        "hogi": hogi,
                        "lat": loc["lat"] if loc else None,
                        "lon": loc["lon"] if loc else None,
                        "address": loc["address"] if loc else None,
                        "site": loc["site"] if loc else None,
                        "capacity_mw": cap["capacity_mw"] if cap else None,
                        "capacity_confidence": cap["confidence"] if cap else "미확인",
                        "capacity_source": cap["source"] if cap else None,
                    }
                )

    cap_df = pd.DataFrame(
        rows,
        columns=[
            "gen_type", "plant_name", "plant", "hogi",
            "lat", "lon", "address", "site",
            "capacity_mw", "capacity_confidence", "capacity_source",
        ],
    ).sort_values(["gen_type", "plant_name"]).reset_index(drop=True)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    cap_df.to_csv(out_path, index=False, encoding="utf-8-sig")

    no_cap = cap_df.loc[cap_df["capacity_mw"].isna(), "plant_name"].tolist()
    logger.info(f"[용량] 호기 {len(cap_df)}개 -> {out_path}")
    if no_cap:
        logger.warning(f"[용량] 용량 미확정 {len(no_cap)}개: {no_cap}")
    return cap_df


# -------------------------
# 실행
# -------------------------
def run(
    gen_keys: List[str],
    start: Optional[date] = None,
    end: Optional[date] = None,
    full: bool = False,
    concurrency: int = 4,
    out_root: Path = DEFAULT_RAW_DIR,
    loc_path: Path = DEFAULT_LOC_PATH,
    floor: date = EARLIEST_FLOOR,
) -> Dict[str, List[Path]]:
    logger.info("=" * 60)
    logger.info("남동발전 비태양광(해양소수력/연료전지/화력) 수집 시작")
    mode = "전체 자동(최초~현재)" if full else f"{start}~{end}"
    logger.info(f"대상 발전원: {gen_keys} / 모드: {mode} / 동시성: {concurrency}")
    logger.info("=" * 60)

    results = asyncio.run(
        download_all(
            gen_keys, concurrency, out_root,
            start=start, end=end, full=full, floor=floor,
        )
    )

    total_files = sum(len(v) for v in results.values())
    for key, files in results.items():
        label = NamdongGenAPI.GEN_TYPES[key]["label"]
        logger.info(f"  - {label}: {len(files)}개 파일")
    logger.info(f"총 저장 파일: {total_files}개 (루트: {out_root})")

    # 위치/용량 CSV는 이번 run 의 결과만이 아니라 디스크의 모든 발전원 원본을 스캔해
    # 재생성한다(부분 수집이 CSV를 불완전하게 덮어쓰는 문제 방지).
    rebuild_locations_from_raw(raw_dir=out_root, loc_path=loc_path)
    build_plant_capacities(raw_dir=out_root)
    return results


def _parse_types(s: Optional[str]) -> List[str]:
    valid = list(NamdongGenAPI.GEN_TYPES.keys())
    if not s:
        return valid
    keys = [t.strip() for t in s.split(",") if t.strip()]
    bad = [k for k in keys if k not in valid]
    if bad:
        raise SystemExit(f"알 수 없는 발전원: {bad} (가능: {valid})")
    return keys


def main():
    parser = argparse.ArgumentParser(
        description="남동발전 비태양광(해양소수력/연료전지/화력) 시간대별 발전실적 배치 병렬 수집"
    )
    parser.add_argument(
        "--full",
        action="store_true",
        help="가장 오래된 가용월부터 현재까지 전부 자동 수집 (start/end/months 무시)",
    )
    parser.add_argument("--start", default=None, help="시작일 YYYYMMDD")
    parser.add_argument("--end", default=None, help="종료일 YYYYMMDD")
    parser.add_argument("--months", type=int, default=5, help="최근 N개월 (start/end/full 미지정 시, 기본 5)")
    parser.add_argument(
        "--types",
        default=None,
        help=f"수집 발전원 콤마구분 (기본 전체). 가능: {','.join(NamdongGenAPI.GEN_TYPES.keys())}",
    )
    parser.add_argument("--concurrency", type=int, default=4, help="동시 다운로드 상한 (기본 4)")
    parser.add_argument("--out", default=str(DEFAULT_RAW_DIR), help="원본 CSV 저장 루트")
    parser.add_argument(
        "--rebuild-locations",
        action="store_true",
        help="수집 생략, 디스크의 기존 원본 전체에서 위치 CSV + 호기별 용량 CSV만 재생성",
    )
    args = parser.parse_args()

    gen_keys = _parse_types(args.types)

    if args.rebuild_locations:
        rebuild_locations_from_raw(raw_dir=Path(args.out), gen_keys=gen_keys)
        build_plant_capacities(raw_dir=Path(args.out), gen_keys=gen_keys)
        return

    if args.full:
        run(
            gen_keys=gen_keys,
            full=True,
            concurrency=args.concurrency,
            out_root=Path(args.out),
        )
        return

    if args.start or args.end:
        if not (args.start and args.end):
            raise SystemExit("--start 와 --end 는 함께 지정해야 합니다.")
        start = _to_date(_validate_yyyymmdd(args.start))
        end = _to_date(_validate_yyyymmdd(args.end))
    else:
        start, end = recent_n_months(args.months)

    run(
        gen_keys=gen_keys,
        start=start,
        end=end,
        concurrency=args.concurrency,
        out_root=Path(args.out),
    )


if __name__ == "__main__":
    main()
