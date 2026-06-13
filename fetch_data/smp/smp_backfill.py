"""
과거 SMP 데이터 백필 (CSV → 공통 DB).

KPX 라이브 페이지는 2013-02-03 이후만 조회 가능하다. 그 이전(육지 2001-05-01,
제주 2010-01-01 ~)이나 원본에 누락된 날짜는 CSV로만 채울 수 있다.

모드:
  --mode master (기본): long-form CSV (컬럼 date,time,price).
      외부 SMP_collector 레포의 smp_{region}_master.csv 형식.
  --mode wide          : wide CSV (기간/날짜 + 1시~24시 [+ 최대/최소/가중평균]).
      EPSIS/KPX prime 원본 형식. cp949/utf-8 자동 감지.
      시간열 -> smp_hourly, 통계열(있으면) -> smp_weighted_avg(daily).

시간 변환(공통): KPX hour-ending 라벨 N시 -> 구간시작 (N-1)시.
  (01:00:00/1시 -> 00:00 ... 24:00:00/24시 -> 같은날 23:00)

사용 예:
    # long-form (전체 백필)
    uv run python -m fetch_data.smp.smp_backfill --mode master \
        --file /tmp/smp_collector_ref/data/smp_jeju_master.csv --region jeju

    # wide (EPSIS 원본으로 결측일 보충)
    uv run python -m fetch_data.smp.smp_backfill --mode wide \
        --file /path/to/jeju_missing.csv --region jeju
"""

from __future__ import annotations

import argparse
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional, Tuple

import pandas as pd

from fetch_data.common.logger import get_logger
from fetch_data.smp import _common as C

logger = get_logger(__name__)

CSV_ENCODINGS = ("cp949", "utf-8-sig", "utf-8")
_HOUR_COL_RE = re.compile(r"^\s*(\d{1,2})\s*시\s*$")
# wide 통계열 라벨 -> smp_weighted_avg 컬럼
_STAT_LABELS = {"최대": "smp_max", "최소": "smp_min", "가중평균": "weighted_avg"}


def _hour_label_to_offset(time_str: str) -> Optional[int]:
    """KPX hour-ending 라벨 -> 구간시작 시(0~23) 오프셋.

    long-form의 'NN:00:00', wide의 'N시' 모두 첫 숫자(N)를 받아 N-1 반환.
    """
    s = str(time_str).strip()
    if not s:
        return None
    m = re.match(r"\s*(\d{1,2})", s)
    if not m:
        return None
    n = int(m.group(1))
    if not (1 <= n <= 24):
        return None
    return n - 1


# ----------------------------------------------------------------------
# long-form (master)
# ----------------------------------------------------------------------

def load_master_csv(file_path: str, region: str) -> pd.DataFrame:
    """long-form master CSV -> smp_hourly 적재용 DataFrame(timestamp, region, price)."""
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"CSV를 찾을 수 없습니다: {path}")

    df = pd.read_csv(path)
    required = {"date", "time", "price"}
    if not required.issubset(df.columns):
        raise ValueError(f"master CSV 컬럼이 예상과 다릅니다: {list(df.columns)} (필요: {required})")

    df["_date"] = pd.to_datetime(df["date"], errors="coerce")
    df["_offset"] = df["time"].map(_hour_label_to_offset)
    df["price"] = pd.to_numeric(
        df["price"].astype(str).str.replace(",", "", regex=False), errors="coerce"
    )

    df = df.dropna(subset=["_date", "_offset"])
    df["timestamp"] = df.apply(
        lambda r: r["_date"].to_pydatetime() + timedelta(hours=int(r["_offset"])), axis=1
    )
    df["region"] = region

    out = df[["timestamp", "region", "price"]].dropna(subset=["price"])
    out = out.drop_duplicates(subset=["timestamp", "region"]).reset_index(drop=True)
    logger.info(f"[backfill][{region}] master CSV 로드: {len(out)}행 ({path.name})")
    return out


# ----------------------------------------------------------------------
# wide (EPSIS/prime 원본)
# ----------------------------------------------------------------------

def _read_csv_any_encoding(path: Path) -> pd.DataFrame:
    last_err: Optional[Exception] = None
    for enc in CSV_ENCODINGS:
        try:
            return pd.read_csv(path, encoding=enc)
        except Exception as e:  # noqa: BLE001
            last_err = e
    raise ValueError(f"CSV 인코딩 인식 실패({CSV_ENCODINGS}): {last_err}")


def load_wide_csv(file_path: str, region: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """wide CSV -> (hourly_df, wavg_df).

    구조: [날짜열, '1시'..'24시'(+ '최대'/'최소'/'가중평균')].
    - 첫 열을 날짜로 본다(헤더명 무관, 보통 '기간'/'날짜').
    - 'N시' 헤더 24개를 시간열로 식별(라벨 N -> 구간시작 (N-1)시).
    - 통계 3열이 있으면 smp_weighted_avg(daily)로 반환.
    """
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"CSV를 찾을 수 없습니다: {path}")

    df = _read_csv_any_encoding(path)
    if df.shape[1] < 2:
        raise ValueError(f"wide CSV 열이 너무 적습니다: {list(df.columns)}")

    date_col = df.columns[0]
    # 시간열 식별: 헤더가 'N시'
    hour_cols: List[Tuple[str, int]] = []  # (컬럼명, 구간시작 offset)
    for col in df.columns[1:]:
        m = _HOUR_COL_RE.match(str(col))
        if m:
            n = int(m.group(1))
            if 1 <= n <= 24:
                hour_cols.append((col, n - 1))
    if not hour_cols:
        raise ValueError(
            f"'N시' 형태 시간열을 찾지 못했습니다. 헤더: {list(df.columns)}"
        )

    # 통계열 식별(선택)
    stat_cols = {label: col for col in df.columns for label, _ in _STAT_LABELS.items()
                 if str(col).strip() == label}

    df["_date"] = pd.to_datetime(
        df[date_col].astype(str).str.replace("/", "-", regex=False).str.strip(),
        errors="coerce",
    )
    df = df.dropna(subset=["_date"])

    hourly_rows = []
    wavg_rows = []
    for _, r in df.iterrows():
        d = r["_date"].to_pydatetime()
        for col, offset in hour_cols:
            price = C.parse_price(r[col])
            if price is None:
                continue
            hourly_rows.append(
                {"timestamp": d + timedelta(hours=offset), "region": region, "price": price}
            )
        if stat_cols:
            wavg = {
                "period_type": "daily",
                "period": d.date(),
                "region": region,
                "weighted_avg": None,
                "smp_max": None,
                "smp_min": None,
            }
            for label, key in _STAT_LABELS.items():
                if label in stat_cols:
                    wavg[key] = C.parse_price(r[stat_cols[label]])
            if any(wavg[k] is not None for k in ("weighted_avg", "smp_max", "smp_min")):
                wavg_rows.append(wavg)

    hourly_df = (
        pd.DataFrame(hourly_rows)
        .drop_duplicates(subset=["timestamp", "region"])
        .reset_index(drop=True)
    )
    wavg_df = pd.DataFrame(wavg_rows)
    logger.info(
        f"[backfill][{region}] wide CSV 로드: 시간별 {len(hourly_df)}행, "
        f"일별통계 {len(wavg_df)}행 ({path.name})"
    )
    return hourly_df, wavg_df


# ----------------------------------------------------------------------
# EPSIS 시간별 wide CSV의 '가중평균' 컬럼 -> 일별 공식 가중평균
# ----------------------------------------------------------------------

def load_epsis_daily_wavg(file_path: str, region: str) -> pd.DataFrame:
    """EPSIS 시간별 wide CSV에서 '가중평균' 컬럼만 추출 -> daily 가중평균.

    CSV 구조: 기간(날짜), 1시..24시, 최대, 최소, 가중평균 (cp949).
    반환: smp_weighted_avg 적재용(period_type=daily, region, price_type=smp, weighted_avg).
    """
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"CSV를 찾을 수 없습니다: {path}")

    df = _read_csv_any_encoding(path)
    date_col = df.columns[0]
    wavg_col = next((c for c in df.columns if str(c).strip() == "가중평균"), None)
    if wavg_col is None:
        raise ValueError(f"'가중평균' 컬럼을 찾지 못했습니다. 헤더: {list(df.columns)}")

    df["_date"] = pd.to_datetime(
        df[date_col].astype(str).str.replace("/", "-", regex=False).str.strip(),
        errors="coerce",
    )
    df["_wavg"] = pd.to_numeric(
        df[wavg_col].astype(str).str.replace(",", "", regex=False), errors="coerce"
    )
    df = df.dropna(subset=["_date", "_wavg"])

    out = pd.DataFrame(
        {
            "period_type": "daily",
            "period": df["_date"].dt.date,
            "region": region,
            "price_type": "smp",
            "weighted_avg": df["_wavg"].values,
        }
    ).drop_duplicates(subset=["period_type", "period", "region", "price_type"])
    logger.info(f"[backfill][{region}] EPSIS 일별 가중평균 로드: {len(out)}행 ({path.name})")
    return out


# ----------------------------------------------------------------------
# 실행
# ----------------------------------------------------------------------

def run_backfill(
    mode: str,
    file_path: str,
    region: str,
    db_url: Optional[str] = None,
) -> int:
    """과거 데이터 백필 실행. 반환: 적재 행수."""
    if region not in ("land", "jeju"):
        raise ValueError(f"region must be 'land' or 'jeju': {region}")

    if mode == "epsis-daily":
        df = load_epsis_daily_wavg(file_path, region)
        if df.empty:
            logger.warning(f"[backfill][{region}] 적재할 일별 가중평균이 없습니다.")
            return 0
        n = C.upsert_weighted_avg(df, db_url=db_url)
        logger.info(
            f"[backfill][{region}] epsis-daily 완료: {n}행 "
            f"({df['period'].min()} ~ {df['period'].max()})"
        )
        return n

    if mode == "master":
        df = load_master_csv(file_path, region)
        if df.empty:
            logger.warning(f"[backfill][{region}] 적재할 데이터가 없습니다.")
            return 0
        n = C.upsert_hourly(df, db_url=db_url)
        logger.info(
            f"[backfill][{region}] 완료: {n}행 "
            f"({df['timestamp'].min()} ~ {df['timestamp'].max()})"
        )
        return n

    if mode == "wide":
        hourly_df, wavg_df = load_wide_csv(file_path, region)
        if hourly_df.empty:
            logger.warning(f"[backfill][{region}] 적재할 시간별 데이터가 없습니다.")
            return 0
        engine = C.get_engine_for(db_url)
        n = C.upsert_hourly(hourly_df, engine=engine)
        if not wavg_df.empty:
            C.upsert_weighted_avg(wavg_df, engine=engine)
        logger.info(
            f"[backfill][{region}] wide 완료: 시간별 {n}행 "
            f"({hourly_df['timestamp'].min()} ~ {hourly_df['timestamp'].max()})"
        )
        return n

    raise ValueError(f"지원하지 않는 mode: {mode} (master | wide | epsis-daily)")


def main() -> None:
    parser = argparse.ArgumentParser(description="과거 SMP CSV 백필")
    parser.add_argument("--mode", choices=["master", "wide", "epsis-daily"], default="master")
    parser.add_argument("--file", required=True, help="CSV 경로")
    parser.add_argument("--region", choices=["land", "jeju"], required=True)
    parser.add_argument("--db-url", default=None)
    args = parser.parse_args()
    run_backfill(args.mode, args.file, args.region, args.db_url)


if __name__ == "__main__":
    main()
