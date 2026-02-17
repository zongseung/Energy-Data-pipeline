import pandas as pd
import os
from pathlib import Path
from datetime import datetime, timedelta
import re

# 1. 경로 설정
PROJECT_ROOT = Path(__file__).parent.parent.parent
RAW_DIR = PROJECT_ROOT / "pv_data_raw"
PROCESSED_DIR = PROJECT_ROOT / "pv_data_processed"
SPECS_FILE = PROJECT_ROOT / "한국남부발전(주)_태양광발전기 사양정보_20250630.csv"

# 2. 발전소 명칭 정규화 함수 (유지)
def normalize_name(name):
    if not isinstance(name, str): return ""
    name = re.sub(r"한국남부발전\(주\)_", "", name)
    name = re.sub(r"태양광발전실적", "", name)
    name = re.sub(r"태양광발전소", "", name)
    name = re.sub(r"태양광", "", name)
    name = re.sub(r"발전소", "", name)
    name = re.sub(r"\(주\)", "", name)
    name = name.replace(" ", "").strip()
    return name

def preprocess_nambu():
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    
    # 사양 정보 로드
    try:
        specs_df = pd.read_csv(SPECS_FILE, encoding='utf-8-sig')
    except Exception:
        specs_df = pd.read_csv(SPECS_FILE, encoding='cp949')
    
    specs_df['norm_name'] = specs_df['발전소명'].apply(normalize_name)
    
    raw_files = list(RAW_DIR.glob("nambu_bulk_*.csv"))
    if not raw_files:
        print("[!] 전처리할 원본 파일이 없습니다.")
        return

    print(f"--- 총 {len(raw_files)}개 파일 전처리 및 시간축 전환 시작 ---")

    for file_path in raw_files:
        df = pd.read_csv(file_path)
        if df.empty: continue
        
        # 가. 날짜 형식 정리 및 누락 체크 (기존 기능 유지)
        df['ymd'] = pd.to_datetime(df['ymd'])
        start_date = df['ymd'].min()
        end_date = df['ymd'].max()
        full_range = pd.date_range(start=start_date, end=end_date, freq='D')
        missing_dates = full_range.difference(df['ymd'])
        
        if not missing_dates.empty:
            print(f"  [!] {file_path.name}: 누락된 날짜 {len(missing_dates)}일 발견")

        # 나. 사양 정보와 Merge (기존 기능 유지)
        df['norm_name'] = df['ipptnm'].apply(normalize_name)
        merged_df = pd.merge(df, specs_df, on='norm_name', how='left')
        
        # 다. 시간축 스위칭 (Wide to Long) - ⭐ 추가된 기능
        # qhorgen01 ~ qhorgen24 컬럼 찾기
        value_vars = [c for c in merged_df.columns if c.startswith('qhorgen')]
        # 나머지 모든 컬럼은 고정(ID)
        id_vars = [c for c in merged_df.columns if c not in value_vars and c != 'norm_name']
        
        # Melt 실행
        df_long = merged_df.melt(
            id_vars=id_vars,
            value_vars=value_vars,
            var_name='hour_str',
            value_name='generation'
        )
        
        # 라. Timestamp 생성 및 정렬
        # 'qhorgen14' -> 14 (정수) 추출
        df_long['hour'] = df_long['hour_str'].str.extract('(\d+)').astype(int)
        
        # 실제 시간 객체 생성 (ymd 날짜 + hour 시간)
        df_long['timestamp'] = df_long['ymd'] + pd.to_timedelta(df_long['hour'], unit='h')
        
        # 시간순 정렬
        df_long = df_long.sort_values('timestamp').reset_index(drop=True)
        
        # 불필요한 임시 컬럼 제거
        df_long = df_long.drop(columns=['hour_str', 'hour'])

        # 마. 발전소별 개별 저장
        out_name = file_path.name.replace("bulk", "processed")
        df_long.to_csv(PROCESSED_DIR / out_name, index=False, encoding="utf-8-sig")
        print(f"  [OK] {file_path.name} -> {out_name} (변환 및 병합 완료)")

if __name__ == "__main__":
    preprocess_nambu()