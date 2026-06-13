"""
남동발전(KOEN) 비태양광 발전원 자동화 파이프라인: 수집 -> 변환.

이 모듈은 두 단계를 하나로 잇는 오케스트레이터다.
  1) 수집  : fetch_data.gen.namdong_gen_collect (gen_data_raw/{category}/koen_*.csv)
  2) 변환  : fetch_data.gen.transform_gen        (gen_data/{category}_long.csv, wind 패턴)

수집 모드:
- latest (기본): 이미 수집된 원본에서 가장 최신 월을 찾아, 그 직전부터(overlap) 직전 완결월까지
                 '새로 생긴 월'만 증분 수집. (수집본이 없으면 최근 N개월로 폴백)
- months       : 최근 N개월 재수집
- range        : --start ~ --end 기간 수집
- full         : 사이트 가용 범위(최초~최신) 전체 수집

DB 적재는 이 모듈의 범위가 아니다(추후 wind식 upsert 모듈에서 처리).

사용 예:
    # 최신 증분 수집 + 변환 (기본)
    uv run python -m fetch_data.gen.pipeline

    # 최근 6개월 재수집 + 변환
    uv run python -m fetch_data.gen.pipeline --months 6

    # 특정 카테고리만, 기간 지정
    uv run python -m fetch_data.gen.pipeline --types ocean_hydro --start 20260101 --end 20260430

    # 수집 생략하고 기존 원본만 변환
    uv run python -m fetch_data.gen.pipeline --no-collect
"""

from __future__ import annotations

import argparse
import re
from datetime import date, datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from fetch_data.common.logger import get_logger
from fetch_data.gen import namdong_collect as collect
from fetch_data.gen import transform_gen as transform

logger = get_logger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_RAW_DIR = transform.DEFAULT_RAW_DIR
DEFAULT_OUT_DIR = transform.DEFAULT_OUT_DIR
CATEGORIES = transform.CATEGORIES

# 수집본이 하나도 없을 때 latest 모드의 폴백 개월 수
DEFAULT_FALLBACK_MONTHS = 5

_FNAME_RE = re.compile(r"_(\d{8})-(\d{8})\.csv$")


# =========================================================
# 증분(latest) 수집 범위 계산
# =========================================================
def latest_collected_month(category: str, raw_dir: Path = DEFAULT_RAW_DIR) -> Optional[date]:
    """카테고리 원본 파일명에서 가장 최신 수집 월(해당 월 1일)을 추출."""
    cat_dir = Path(raw_dir) / category
    if not cat_dir.exists():
        return None
    months: List[date] = []
    for fp in cat_dir.glob("koen_*.csv"):
        m = _FNAME_RE.search(fp.name)
        if not m:
            continue
        try:
            end = datetime.strptime(m.group(2), "%Y%m%d").date()
        except ValueError:
            continue
        months.append(date(end.year, end.month, 1))
    return max(months) if months else None


def resolve_incremental_range(
    gen_keys: List[str],
    raw_dir: Path = DEFAULT_RAW_DIR,
    overlap_months: int = 1,
    ref: Optional[date] = None,
) -> Optional[Tuple[date, date]]:
    """증분 수집 범위 (start, end) 계산.

    - end   : 직전 완결월의 말일 (recent_n_months(1) 기준)
    - start : 카테고리별 '최신 수집월'에서 overlap_months 만큼 뒤로 당긴 월들 중 가장 이른 월.
              (경계 월을 재수집해 뒤늦게 갱신된 값을 반영)
    수집본이 전혀 없으면 None 을 반환(호출측에서 폴백 처리).
    """
    _, end = collect.recent_n_months(1, ref)  # 직전 완결월 말일

    starts: List[date] = []
    for key in gen_keys:
        lm = latest_collected_month(key, raw_dir)
        if lm is not None:
            starts.append(collect._add_months(lm, -max(0, overlap_months)))

    if not starts:
        return None

    start = max(min(starts), collect.EARLIEST_FLOOR.replace(day=1))
    if start > end:
        # 이미 최신까지 수집됨 -> 경계 월(직전 완결월)만 재확인
        start = date(end.year, end.month, 1)
    return start, end


# =========================================================
# 파이프라인
# =========================================================
def collect_step(
    gen_keys: List[str],
    mode: str,
    *,
    months: int = DEFAULT_FALLBACK_MONTHS,
    start: Optional[date] = None,
    end: Optional[date] = None,
    overlap_months: int = 1,
    concurrency: int = 4,
    raw_dir: Path = DEFAULT_RAW_DIR,
) -> None:
    """수집 단계. mode: latest | months | range | full."""
    if mode == "full":
        logger.info("[수집] full — 사이트 가용 범위 전체 자동 수집")
        collect.run(gen_keys=gen_keys, full=True, concurrency=concurrency, out_root=raw_dir)
        return

    if mode == "range":
        if not (start and end):
            raise ValueError("range 모드는 start/end 가 필요합니다.")
        s, e = start, end
    elif mode == "months":
        s, e = collect.recent_n_months(months)
    else:  # latest (증분)
        rng = resolve_incremental_range(gen_keys, raw_dir, overlap_months)
        if rng is None:
            s, e = collect.recent_n_months(months)
            logger.info(f"[수집] latest — 기존 수집본 없음, 최근 {months}개월로 폴백")
        else:
            s, e = rng

    logger.info(f"[수집] {mode} — {s} ~ {e}")
    collect.run(gen_keys=gen_keys, start=s, end=e, concurrency=concurrency, out_root=raw_dir)


def run_pipeline(
    gen_keys: Optional[List[str]] = None,
    *,
    mode: str = "latest",
    months: int = DEFAULT_FALLBACK_MONTHS,
    start: Optional[date] = None,
    end: Optional[date] = None,
    overlap_months: int = 1,
    concurrency: int = 4,
    collect_data: bool = True,
    raw_dir: Path = DEFAULT_RAW_DIR,
    out_dir: Path = DEFAULT_OUT_DIR,
) -> Dict[str, Path]:
    """수집(옵션) -> 변환 -> 카테고리별 long CSV 저장. 저장 경로 dict 반환."""
    gen_keys = gen_keys or CATEGORIES

    logger.info("=" * 60)
    logger.info(f"비태양광 파이프라인 시작 | 카테고리={gen_keys} | 수집={collect_data}({mode})")
    logger.info("=" * 60)

    if collect_data:
        collect_step(
            gen_keys, mode,
            months=months, start=start, end=end,
            overlap_months=overlap_months, concurrency=concurrency, raw_dir=raw_dir,
        )
    else:
        logger.info("[수집] 생략 — 기존 원본만 변환")

    paths = transform.run(categories=gen_keys, raw_dir=raw_dir, out_dir=out_dir)
    logger.info(f"파이프라인 완료 — {len(paths)}개 데이터셋: {[p.name for p in paths.values()]}")
    return paths


# =========================================================
# CLI
# =========================================================
def _parse_types(s: Optional[str]) -> List[str]:
    if not s:
        return CATEGORIES
    keys = [t.strip() for t in s.split(",") if t.strip()]
    bad = [k for k in keys if k not in CATEGORIES]
    if bad:
        raise SystemExit(f"알 수 없는 카테고리: {bad} (가능: {CATEGORIES})")
    return keys


def main() -> None:
    parser = argparse.ArgumentParser(
        description="남동발전 비태양광 수집 -> 변환 자동화 파이프라인"
    )
    parser.add_argument("--types", default=None,
                        help=f"카테고리 콤마구분 (기본 전체). 가능: {','.join(CATEGORIES)}")
    parser.add_argument("--full", action="store_true", help="가용 범위 전체 수집")
    parser.add_argument("--months", type=int, default=DEFAULT_FALLBACK_MONTHS,
                        help=f"최근 N개월 수집 (기본 {DEFAULT_FALLBACK_MONTHS}, latest 폴백에도 사용)")
    parser.add_argument("--start", default=None, help="시작일 YYYYMMDD (range 모드)")
    parser.add_argument("--end", default=None, help="종료일 YYYYMMDD (range 모드)")
    parser.add_argument("--overlap-months", type=int, default=1,
                        help="latest 모드에서 경계 월 재수집 폭 (기본 1)")
    parser.add_argument("--concurrency", type=int, default=4, help="동시 다운로드 상한 (기본 4)")
    parser.add_argument("--no-collect", action="store_true", help="수집 생략, 기존 원본만 변환")
    parser.add_argument("--raw-dir", default=str(DEFAULT_RAW_DIR), help="원본 CSV 루트")
    parser.add_argument("--out-dir", default=str(DEFAULT_OUT_DIR), help="long CSV 저장 디렉터리")
    args = parser.parse_args()

    gen_keys = _parse_types(args.types)

    # 모드 결정
    if args.full:
        mode, start, end = "full", None, None
    elif args.start or args.end:
        if not (args.start and args.end):
            raise SystemExit("--start 와 --end 는 함께 지정해야 합니다.")
        mode = "range"
        start = collect._to_date(collect._validate_yyyymmdd(args.start))
        end = collect._to_date(collect._validate_yyyymmdd(args.end))
    elif args.months != DEFAULT_FALLBACK_MONTHS:
        mode, start, end = "months", None, None
    else:
        mode, start, end = "latest", None, None

    run_pipeline(
        gen_keys=gen_keys,
        mode=mode,
        months=args.months,
        start=start,
        end=end,
        overlap_months=args.overlap_months,
        concurrency=args.concurrency,
        collect_data=not args.no_collect,
        raw_dir=Path(args.raw_dir),
        out_dir=Path(args.out_dir),
    )


if __name__ == "__main__":
    main()
