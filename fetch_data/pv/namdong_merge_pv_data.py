from __future__ import annotations

import re
from pathlib import Path
from typing import List, Optional

import pandas as pd

DEFAULT_INPUT_DIR = Path.cwd() / "pv_data_raw"   # 수집 스크립트 OUTPUT_DIR과 맞추기
DEFAULT_OUT_PATH  = Path.cwd() / "pv_data" / "south_pv_all_long.csv"


def normalize_columns(cols: List[str]) -> List[str]:
    s = pd.Index(cols).astype(str)
    s = (
        s.str.replace("\n", " ", regex=False)
         .str.replace("\r", " ", regex=False)
         .str.replace("\t", " ", regex=False)
         .str.strip()
         .str.replace(r"\s+", " ", regex=True)
    )
    return list(s)


def read_csv_flexible(fp: Path) -> pd.DataFrame:
    """
    - cp949/euc-kr/utf-8-sig/utf-8 순서로 시도
    - 콤마 뒤 공백 자동 제거(skipinitialspace=True) -> ' 호기' 같은 문제 해결
    - python engine 사용(깨진 행이 섞인 경우가 있어 C엔진보다 안전)
    """
    encodings = ["cp949", "euc-kr", "utf-8-sig", "utf-8"]

    last_err: Optional[Exception] = None
    for enc in encodings:
        try:
            df = pd.read_csv(
                fp,
                encoding=enc,
                sep=",",
                engine="python",
                index_col=False,
                skipinitialspace=True,   # ★ 핵심: 콤마 뒤 공백 제거
            )
            df.columns = normalize_columns(df.columns.tolist())
            return df
        except Exception as e:
            last_err = e

    raise RuntimeError(f"CSV 읽기 실패: {fp} / last_err={last_err}")


def hour_columns(df: pd.DataFrame) -> List[str]:
    # 예: "1시 발전량(KWh)" / "10시 발전량(KWh)" 등
    cols = [c for c in df.columns if re.search(r"^\d+\s*시\s*발전량", c)]
    return cols


def extract_hour(col: str) -> int:
    m = re.search(r"^(\d+)\s*시", col)
    if not m:
        raise ValueError(f"시간 컬럼 파싱 실패: {col}")
    return int(m.group(1))


# -------------------------
# Main
# -------------------------
def merge_to_long(input_dir: Path, out_path: Path) -> None:
    input_dir = Path(input_dir)
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    files = sorted(input_dir.glob("south_pv_*.csv"))
    if not files:
        raise FileNotFoundError(f"입력 파일 없음: {input_dir}/south_pv_*.csv")

    long_parts: List[pd.DataFrame] = []

    for fp in files:
        print(f"read: {fp.name}")
        df = read_csv_flexible(fp)

        # 기본 컬럼 보정 (사이트 원본 기준)
        # 발전소명 컬럼이 없으면 "발전구분"을 발전소명으로 사용 (전체 조회일 때 이게 사실상 발전소명)
        if "발전소명" not in df.columns:
            if "발전구분" in df.columns:
                df["발전소명"] = df["발전구분"]
            else:
                raise ValueError(f"필수 컬럼(발전구분) 누락: {fp.name} / cols={df.columns.tolist()[:15]}")

        # 호기 정보가 있으면 발전소명에 호기 번호 추가 (예: "영흥태양광_1", "영흥태양광_2")
        if "호기" in df.columns:
            df["호기"] = df["호기"].astype(str).str.strip()
            # 호기가 1개인 발전소는 호기 번호 생략, 여러 개인 경우만 추가
            hogi_counts = df.groupby("발전소명")["호기"].nunique()
            multi_hogi_plants = hogi_counts[hogi_counts > 1].index.tolist()
            
            def add_hogi_suffix(row):
                if row["발전소명"] in multi_hogi_plants:
                    return f"{row['발전소명']}_{row['호기']}"
                return row["발전소명"]
            
            df["발전소명"] = df.apply(add_hogi_suffix, axis=1)
            print(f"  -> 호기 구분 적용: {len(multi_hogi_plants)}개 발전소")

        # 일자 컬럼명도 케이스별로 정리
        if "일자" not in df.columns:
            # 가끔 '일자' 대신 다른 형태면 여기서 추가 매핑
            raise ValueError(f"필수 컬럼(일자) 누락: {fp.name} / cols={df.columns.tolist()[:15]}")

        # 시간별 발전량 컬럼 찾기
        hcols = hour_columns(df)
        if not hcols:
            raise ValueError(f"시간별 발전량 컬럼을 못 찾음: {fp.name} / cols={df.columns.tolist()[:30]}")

        # melt (요청하신 방식: value_cols 미리 만들지 말고, regex로 잡은 hcols만 value_vars로)
        df_long = pd.melt(
            df,
            id_vars=["일자", "발전소명"],
            value_vars=hcols,
            var_name="시간",
            value_name="발전량",
        )

        # 시간 정수화(1~24)
        df_long["시간"] = df_long["시간"].apply(extract_hour).astype("int64")

        # 데이터 타입 정리(선택)
        # df_long["일자"] = pd.to_datetime(df_long["일자"], errors="coerce")
        df_long["발전량"] = pd.to_numeric(df_long["발전량"], errors="coerce")

        long_parts.append(df_long)

    merged = pd.concat(long_parts, ignore_index=True)

    # 저장: utf-8-sig 추천(엑셀 호환) / 분석용이면 utf-8도 OK
    merged.to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"\n✅ merged saved: {out_path}")
    print(f"rows: {len(merged):,} / plants: {merged['발전소명'].nunique():,} / hours: {merged['시간'].nunique():,}")


if __name__ == "__main__":
    merge_to_long(DEFAULT_INPUT_DIR, DEFAULT_OUT_PATH)
