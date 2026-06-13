"""
남동발전(KOEN) 비태양광 발전원 원본 CSV -> wind 패턴 long 변환 모듈.

대상: gen_data_raw/{ocean_hydro,fuel_cell,thermal}/koen_*.csv (월 단위, 와이드 33컬럼)

타깃 스키마 (기존 wind_* 테이블과 동일):
    [timestamp, plant_name, generation]
- timestamp : 날짜+시각 단일 컬럼. hour 24 -> 다음날 00시 (wind 규칙)
- plant_name: 발전구분 + "_" + 호기 (단일 호기여도 항상 접미사)
- generation: kWh. 원본 헤더는 'MWh'지만 실제 값은 kWh 이므로 단위변환 없이 그대로 사용.

집계 컬럼(총량/평균/최대/최소)은 시간별 값에서 재계산 가능한 파생값이라 모두 버린다.

카테고리별로 따로 변환해 카테고리당 하나의 long DataFrame을 만든다(적재 시 각각 별도 테이블).

사용 예:
    # 3개 카테고리 전부 변환해 gen_data/{category}_long.csv 로 저장
    uv run python -m fetch_data.gen.transform_gen

    # 일부 카테고리만
    uv run python -m fetch_data.gen.transform_gen --types ocean_hydro,fuel_cell
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd

from fetch_data.common.logger import get_logger
from fetch_data.constants import NamdongGenAPI
from fetch_data.pv.namdong_transform import read_csv_flexible

logger = get_logger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_RAW_DIR = PROJECT_ROOT / "gen_data_raw"
DEFAULT_OUT_DIR = PROJECT_ROOT / "gen_data"

CATEGORIES: List[str] = list(NamdongGenAPI.GEN_TYPES.keys())  # ocean_hydro, fuel_cell, thermal

# "1시 발전량(MWh)" 같은 시간별 발전량 컬럼 매칭
HOUR_RE = re.compile(r"^\s*(\d+)\s*시\s*발전량")

# long 결과 컬럼 (wind 패턴)
LONG_COLUMNS = ["timestamp", "plant_name", "generation"]


# =========================================================
# 결합 / 변환
# =========================================================
def merge_category_wide(category: str, raw_dir: Path = DEFAULT_RAW_DIR) -> pd.DataFrame:
    """한 카테고리의 월별 원본 CSV를 모두 읽어 하나의 와이드 DataFrame으로 결합."""
    cat_dir = Path(raw_dir) / category
    files = sorted(cat_dir.glob("koen_*.csv"))  # 파일명=기간 → 날짜순 정렬
    if not files:
        raise FileNotFoundError(f"{cat_dir}/koen_*.csv 파일이 없습니다.")

    df = pd.concat([read_csv_flexible(fp) for fp in files], ignore_index=True)
    df["발전구분"] = df["발전구분"].astype(str).str.strip()
    df["호기"] = df["호기"].astype(str).str.strip()
    logger.info(f"[{category}] {len(files)}개 파일 결합 -> 와이드 {df.shape}")
    return df


def build_plant_name(df: pd.DataFrame) -> pd.Series:
    """발전구분 + 호기. 단일 호기여도 항상 '_호기' 접미사를 붙인다."""
    return df["발전구분"] + "_" + df["호기"]


def transform_wide_to_long(df: pd.DataFrame) -> pd.DataFrame:
    """와이드(시간별 24컬럼) -> long [timestamp, plant_name, generation] (wind 패턴)."""
    hour_cols = [c for c in df.columns if HOUR_RE.match(c)]
    if not hour_cols:
        raise ValueError("시간별 발전량 컬럼(N시 발전량...)을 찾지 못했습니다.")

    work = df.copy()
    work["plant_name"] = build_plant_name(work)

    long = work.melt(
        id_vars=["일자", "plant_name"],
        value_vars=hour_cols,
        var_name="hour_col",
        value_name="generation",
    )
    long["hour_num"] = long["hour_col"].str.extract(HOUR_RE.pattern).astype(int)

    # timestamp: 날짜 + 시각. hour 24 -> 다음날 00시 (wind 규칙)
    base = pd.to_datetime(long["일자"], errors="coerce").dt.normalize()
    long["timestamp"] = base + pd.to_timedelta(long["hour_num"] % 24, unit="h")
    long.loc[long["hour_num"] == 24, "timestamp"] += pd.Timedelta(days=1)

    # generation: 라벨은 MWh지만 실제 kWh -> 변환 없이 숫자화만
    long["generation"] = pd.to_numeric(long["generation"], errors="coerce")

    return (
        long[LONG_COLUMNS]
        .dropna(subset=["timestamp"])
        .sort_values(["plant_name", "timestamp"])
        .reset_index(drop=True)
    )


def transform_category(category: str, raw_dir: Path = DEFAULT_RAW_DIR) -> pd.DataFrame:
    """한 카테고리: 원본 결합 -> long 변환 (+ 무손실 검증)."""
    wide = merge_category_wide(category, raw_dir)
    long = transform_wide_to_long(wide)
    _verify_lossless(category, wide, long)
    logger.info(
        f"[{category}] long {long.shape} | 발전소 {long['plant_name'].nunique()}개 "
        f"| {long['timestamp'].min()} ~ {long['timestamp'].max()}"
    )
    return long


def transform_all(
    categories: Optional[List[str]] = None,
    raw_dir: Path = DEFAULT_RAW_DIR,
) -> Dict[str, pd.DataFrame]:
    """여러 카테고리를 각각 변환해 {category: long DataFrame} 으로 반환."""
    categories = categories or CATEGORIES
    return {cat: transform_category(cat, raw_dir) for cat in categories}


# =========================================================
# 검증 / 저장
# =========================================================
def _verify_lossless(category: str, wide: pd.DataFrame, long: pd.DataFrame) -> None:
    """시간별 셀 총합 == long generation 총합(무손실), (timestamp, plant_name) 중복 0 확인."""
    hour_cols = [c for c in wide.columns if HOUR_RE.match(c)]
    wide_sum = pd.to_numeric(wide[hour_cols].stack(), errors="coerce").sum()
    long_sum = long["generation"].sum()
    if abs(long_sum - wide_sum) > 1.0:
        logger.warning(f"[{category}] 합계 불일치! long={long_sum:,.1f} wide={wide_sum:,.1f}")
    dup = int(long.duplicated(["timestamp", "plant_name"]).sum())
    if dup:
        logger.warning(f"[{category}] (timestamp, plant_name) 중복 {dup}건")


def save_datasets(
    datasets: Dict[str, pd.DataFrame],
    out_dir: Path = DEFAULT_OUT_DIR,
) -> Dict[str, Path]:
    """{category: long} 을 gen_data/{category}_long.csv 로 저장."""
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    paths: Dict[str, Path] = {}
    for category, df in datasets.items():
        out_path = out_dir / f"{category}_long.csv"
        df.to_csv(out_path, index=False, encoding="utf-8-sig")
        paths[category] = out_path
        logger.info(f"saved: {out_path}  ({len(df):,} rows)")
    return paths


def run(
    categories: Optional[List[str]] = None,
    raw_dir: Path = DEFAULT_RAW_DIR,
    out_dir: Path = DEFAULT_OUT_DIR,
) -> Dict[str, Path]:
    """원본 결합 -> 변환 -> 카테고리별 long CSV 저장."""
    datasets = transform_all(categories, raw_dir)
    return save_datasets(datasets, out_dir)


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
        description="남동발전 비태양광 원본 CSV -> wind 패턴 long 변환 (카테고리별 데이터셋)"
    )
    parser.add_argument(
        "--types",
        default=None,
        help=f"변환 카테고리 콤마구분 (기본 전체). 가능: {','.join(CATEGORIES)}",
    )
    parser.add_argument("--raw-dir", default=str(DEFAULT_RAW_DIR), help="원본 CSV 루트")
    parser.add_argument("--out-dir", default=str(DEFAULT_OUT_DIR), help="long CSV 저장 디렉터리")
    args = parser.parse_args()

    run(
        categories=_parse_types(args.types),
        raw_dir=Path(args.raw_dir),
        out_dir=Path(args.out_dir),
    )


if __name__ == "__main__":
    main()
