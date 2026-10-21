"""매일 수집된 ASOS 데이터를 asos_all_merged.csv 에 누적하는 모듈.

원래 CLI 스크립트였고(파일 경로만 받았다) flow 가 그 함수를 재사용하면서
"일별 CSV 를 만들어 경로를 넘기는" 구조가 됐다. 그 일별 파일이 병합·DB 적재 뒤에도
지워지지 않아 `data/` 루트에 80개가 쌓였다(2026-10-21 정리).

지금은 **DataFrame 을 직접 받을 수 있다** — flow 는 그 길을 쓰므로 일별 파일이 생기지 않는다.
CLI(`python -m pipeline.fetch_data.weather.merge_asos <csv>`)는 그대로 둔다.

2026-10-21 에 `prefect_flows/merge_to_all.py` 에서 여기로 옮겼다 — 수집기
(`asos_collect.py`)가 flow 패키지를 import 하는 역방향 의존이었다.
"""
import pandas as pd
from pathlib import Path
from datetime import datetime

from pipeline.fetch_data.common.paths import DATA_DIR

DATA_DIR.mkdir(parents=True, exist_ok=True)
DEFAULT_MERGED_CSV = DATA_DIR / "asos_all_merged.csv"


def merge_to_all_csv(new_data: "str | Path | pd.DataFrame",
                     merged_csv_path: str | Path = DEFAULT_MERGED_CSV):
    """새로 수집된 ASOS 데이터를 통합 CSV 에 누적한다.

    Parameters
    ----------
    new_data : str | Path | pandas.DataFrame
        새 데이터. **DataFrame 을 주면 중간 파일을 만들지 않는다**(flow 가 쓰는 길).
        경로를 주면 그 CSV 를 읽는다(CLI 호환).
    merged_csv_path : str | Path
        통합 CSV 경로 (기본값: data/asos_all_merged.csv)
    """
    merged_csv_path = Path(merged_csv_path)

    print(f"\n{'='*80}")
    print(f"데이터 통합 시작")
    print(f"{'='*80}")

    if isinstance(new_data, pd.DataFrame):
        df_new = new_data.copy()
        print(f"새 데이터: {len(df_new)}건 (DataFrame 직접 전달 — 중간 파일 없음)")
    else:
        new_csv_path = Path(new_data)
        if not new_csv_path.exists():
            raise FileNotFoundError(f"새 파일을 찾을 수 없습니다: {new_csv_path}")
        print(f"새 파일: {new_csv_path}")
        df_new = pd.read_csv(new_csv_path, encoding="utf-8-sig")
        print(f"새 데이터: {len(df_new)}건")
    print(f"통합 파일: {merged_csv_path}")
    
    merged_file = merged_csv_path
    
    if merged_file.exists():
        print(f"\n기존 통합 파일 읽는 중...")
        df_merged = pd.read_csv(merged_csv_path, encoding="utf-8-sig")
        print(f"기존 데이터: {len(df_merged)}건")
        
        print(f"\n데이터 합치는 중...")
        df_combined = pd.concat([df_merged, df_new], ignore_index=True)
        print(f"합친 데이터: {len(df_combined)}건")
        
        # hour 컬럼이 있으면 date + station_name + hour 기준 중복 제거
        if "hour" in df_combined.columns and "date" in df_combined.columns:
            print(f"\n중복 제거 중... (date, station_name, hour 기준)")
            if df_combined["date"].dtype == "object":
                df_combined["date"] = pd.to_datetime(df_combined["date"], format="mixed")
            before = len(df_combined)
            df_combined = df_combined.drop_duplicates(
                subset=["date", "station_name", "hour"],
                keep="last",
            )
            after = len(df_combined)
            print(f"중복 제거 전: {before}건 → 후: {after}건 (제거: {before - after}건)")
        elif "date" in df_combined.columns:
            # hour 없으면 date + station_name 기준
            print(f"\n중복 제거 중... (date, station_name 기준)")
            if df_combined["date"].dtype == "object":
                df_combined["date"] = pd.to_datetime(df_combined["date"], format="mixed")
            before = len(df_combined)
            df_combined = df_combined.drop_duplicates(
                subset=["date", "station_name"],
                keep="last",
            )
            after = len(df_combined)
            print(f"중복 제거 전: {before}건 → 후: {after}건 (제거: {before - after}건)")
        else:
            print("경고: 'date' 컬럼이 없습니다. 중복 제거를 건너뜁니다.")
    else:
        print(f"\n통합 파일이 없습니다. 새로 생성합니다.")
        df_combined = df_new.copy()
    
    # 날짜 순 정렬
    if "date" in df_combined.columns:
        df_combined["date"] = pd.to_datetime(df_combined["date"], format="mixed")

    
    print(f"\n통합 파일 저장 중...")
    df_combined.to_csv(merged_csv_path, index=False, encoding="utf-8-sig")
    
    print(f"\n{'='*80}")
    print(f"통합 완료!")
    print(f"저장 경로: {merged_csv_path}")
    print(f"총 데이터: {len(df_combined)}건")
    print(f"저장 시각: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*80}\n")
    
    return str(merged_csv_path)


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 2:
        print("사용법: python -m pipeline.fetch_data.weather.merge_asos <새_CSV_경로> [통합_CSV_경로]")
        print("\n예시:")
        print("  python -m pipeline.fetch_data.weather.merge_asos data/asos/asos_20241203_20241203.csv")
        sys.exit(1)
    
    new_csv = sys.argv[1]
    merged_csv = sys.argv[2] if len(sys.argv) > 2 else DEFAULT_MERGED_CSV
    merge_to_all_csv(new_csv, merged_csv)
