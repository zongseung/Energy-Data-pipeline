"""
PV 데이터 DB 적재 스크립트

남부발전/남동발전 태양광 발전 데이터를 PostgreSQL에 적재합니다.
- pv_nambu: 남부발전 데이터
- pv_namdong: 남동발전 데이터
- plant_info_nambu: 남부발전 발전소 정보
- plant_info_namdong: 남동발전 발전소 정보
"""

import pandas as pd
import re
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv

# 프로젝트 경로 설정
PROJECT_ROOT = Path(__file__).parent
load_dotenv(PROJECT_ROOT / ".env")

# DB 모듈 임포트
from fetch_data.pv.database import (
    get_engine,
    init_db,
    drop_all_tables,
    get_namdong_location,
)

# 데이터 경로 (NAS)
PV_DATA_DIR = PROJECT_ROOT / "pv_data"
NAMBU_PROCESSED_DIR = PV_DATA_DIR / "pv_nambu_data_processed"
NAMDONG_DATA_FILE = PV_DATA_DIR / "pv_namdong_data" / "south_pv_all_long.csv"

# ========================================
# 남부발전 위경도 매핑
# ========================================

NAMBU_PLANT_LOCATIONS = {
    '997D': {'lat': 33.2373862, 'lon': 126.3418842},   # 남제주
    '997G': {'lat': 35.0586428, 'lon': 128.8157557},   # 부산신항
    '997N': {'lat': 35.0586428, 'lon': 128.8157557},   # 부산복합자재
    'B997': {'lat': 35.0870019, 'lon': 128.9989357},   # 부산본부
    '997Q': {'lat': 35.2591938, 'lon': 129.2235041},   # 부산수처리장
    '997R': {'lat': 35.1902253, 'lon': 129.0563480},   # 부산운동장
    '9987': {'lat': 35.1157106, 'lon': 129.0428212},   # 부산역선상
    '997S': {'lat': 37.536111, 'lon': 126.602318},     # 신인천
    '997Y': {'lat': 37.536111, 'lon': 126.602318},     # 신인천
    '8760': {'lat': 37.536111, 'lon': 126.602318},     # 신인천소내
    '9985': {'lat': 37.536111, 'lon': 126.602318},     # 신인천주차장
    '9988': {'lat': 37.536111, 'lon': 126.602318},     # 신인천북측
    '9989': {'lat': 37.536111, 'lon': 126.602318},     # 신인천 1,2단계
    '9979': {'lat': 37.3335822, 'lon': 127.4795795},   # 이천D
    'S997': {'lat': 37.1902416, 'lon': 129.3387384},   # 삼척
}


def clean_spec(text_val):
    """설치용량/설치각에서 숫자 추출"""
    if pd.isna(text_val):
        return None
    match = re.search(r'(\d+\.?\d*)', str(text_val))
    return float(match.group(1)) if match else None


# ========================================
# 남부발전 적재
# ========================================

def ingest_nambu():
    """남부발전 데이터 적재"""
    print("\n" + "=" * 60)
    print("남부발전 데이터 적재 시작")
    print("=" * 60)

    files = sorted(list(NAMBU_PROCESSED_DIR.glob("nambu_processed_*.csv")))
    if not files:
        print("  [!] 남부발전 가공 파일이 없습니다.")
        return

    print(f"  총 {len(files)}개 파일 발견")

    engine = get_engine()
    total_rows = 0
    plant_info_list = []

    for file_path in files:
        print(f"\n  처리 중: {file_path.name}")

        df = pd.read_csv(file_path, encoding='utf-8-sig')
        if df.empty:
            continue

        # 데이터 타입 변환
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df['generation'] = pd.to_numeric(df['generation'], errors='coerce').fillna(0)
        df['hogi'] = df['hogi'].astype(str)

        # 중복 제거
        df = df.drop_duplicates(subset=['timestamp', 'gencd', 'hogi'], keep='first')

        # gencd에서 위경도 매핑
        def get_location(row):
            gencd = row['gencd']
            if gencd in NAMBU_PLANT_LOCATIONS:
                return pd.Series(NAMBU_PLANT_LOCATIONS[gencd])
            return pd.Series({'lat': None, 'lon': None})

        df[['lat', 'lon']] = df.apply(get_location, axis=1)

        # pv_nambu 테이블에 적재할 데이터 준비
        df_pv = df[['timestamp', 'gencd', 'ipptnm', 'hogi', 'generation', 'lat', 'lon']].copy()
        df_pv.columns = ['timestamp', 'plant_id', 'plant_name', 'hogi', 'generation', 'lat', 'lon']

        # DB 적재 (upsert 방식)
        df_pv.to_sql('pv_nambu', con=engine, if_exists='append', index=False,
                     method='multi', chunksize=5000)
        total_rows += len(df_pv)
        print(f"    -> {len(df_pv):,}건 적재 완료")

        # 발전소 정보 수집 (첫 행만)
        first_row = df.iloc[0]
        plant_info = {
            'plant_id': first_row['gencd'],
            'plant_name': first_row['ipptnm'],
            'hogi': first_row['hogi'],
            'address': first_row.get('발전소 주소지', None),
            'capacity_kw': clean_spec(first_row.get('설치용량', None)),
            'angle_deg': clean_spec(first_row.get('설치각', None)),
            'lat': first_row.get('lat', None),
            'lon': first_row.get('lon', None),
        }
        plant_info_list.append(plant_info)

    # 발전소 정보 테이블 적재
    if plant_info_list:
        df_plant = pd.DataFrame(plant_info_list)
        df_plant = df_plant.drop_duplicates(subset=['plant_id', 'hogi'], keep='first')
        df_plant.to_sql('plant_info_nambu', con=engine, if_exists='replace', index=False)
        print(f"\n  [plant_info_nambu] {len(df_plant)}개 발전소 정보 적재")

    print(f"\n  [pv_nambu] 총 {total_rows:,}건 적재 완료")


# ========================================
# 남동발전 적재
# ========================================

def ingest_namdong():
    """남동발전 데이터 적재"""
    print("\n" + "=" * 60)
    print("남동발전 데이터 적재 시작")
    print("=" * 60)

    if not NAMDONG_DATA_FILE.exists():
        print(f"  [!] 남동발전 파일이 없습니다: {NAMDONG_DATA_FILE}")
        return

    print(f"  파일 로드 중: {NAMDONG_DATA_FILE.name}")

    df = pd.read_csv(NAMDONG_DATA_FILE, encoding='utf-8-sig')
    print(f"  원본 데이터: {len(df):,}건")

    # 타임스탬프 생성 (일자 + 시간)
    df['일자'] = pd.to_datetime(df['일자'])
    df['timestamp'] = df['일자'] + pd.to_timedelta(df['시간'], unit='h')

    # 발전량 숫자 변환
    df['발전량'] = pd.to_numeric(df['발전량'], errors='coerce').fillna(0)

    # 중복 제거
    df = df.drop_duplicates(subset=['timestamp', '발전소명'], keep='first')

    # 위경도 매핑
    def add_location(row):
        loc = get_namdong_location(row['발전소명'])
        return pd.Series(loc)

    df[['lat', 'lon']] = df.apply(add_location, axis=1)

    # pv_namdong 테이블용 데이터
    df_pv = df[['timestamp', '발전소명', '발전량', 'lat', 'lon']].copy()
    df_pv.columns = ['timestamp', 'plant_name', 'generation', 'lat', 'lon']

    # DB 적재
    engine = get_engine()
    df_pv.to_sql('pv_namdong', con=engine, if_exists='append', index=False,
                 method='multi', chunksize=10000)
    print(f"  [pv_namdong] {len(df_pv):,}건 적재 완료")

    # 발전소 정보 테이블
    plants = df['발전소명'].unique()
    plant_info_list = []
    for plant_name in plants:
        loc = get_namdong_location(plant_name)
        plant_info_list.append({
            'plant_name': plant_name,
            'lat': loc['lat'],
            'lon': loc['lon'],
        })

    df_plant = pd.DataFrame(plant_info_list)
    df_plant.to_sql('plant_info_namdong', con=engine, if_exists='replace', index=False)
    print(f"  [plant_info_namdong] {len(df_plant)}개 발전소 정보 적재")


# ========================================
# 메인 실행
# ========================================

def run_full_ingestion(reset_db: bool = True):
    """
    전체 데이터 적재 실행

    Args:
        reset_db: True면 테이블 초기화 후 적재
    """
    print("\n" + "=" * 60)
    print("PV 데이터 전체 적재 시작")
    print(f"시작 시각: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    if reset_db:
        print("\n[1/4] 테이블 초기화 중...")
        drop_all_tables()
        init_db()
        print("  테이블 초기화 완료")
    else:
        print("\n[1/4] 테이블 확인 중...")
        init_db()

    print("\n[2/4] 남부발전 데이터 적재...")
    ingest_nambu()

    print("\n[3/4] 남동발전 데이터 적재...")
    ingest_namdong()

    print("\n[4/4] 적재 완료!")
    print("\n" + "=" * 60)
    print("전체 적재 완료!")
    print(f"종료 시각: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="PV 데이터 DB 적재")
    parser.add_argument("--no-reset", action="store_true", help="테이블 초기화 없이 추가 적재")
    parser.add_argument("--nambu-only", action="store_true", help="남부발전만 적재")
    parser.add_argument("--namdong-only", action="store_true", help="남동발전만 적재")

    args = parser.parse_args()

    if args.nambu_only:
        init_db()
        ingest_nambu()
    elif args.namdong_only:
        init_db()
        ingest_namdong()
    else:
        run_full_ingestion(reset_db=not args.no_reset)
