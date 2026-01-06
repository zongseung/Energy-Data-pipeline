import pandas as pd
import os
import re
from sqlalchemy import create_engine, text, types
from pathlib import Path
from dotenv import load_dotenv

# 1. 환경 설정 및 DB 연결
PROJECT_ROOT = Path(__file__).parent
load_dotenv(PROJECT_ROOT / ".env")

DB_URL = os.getenv("LOCAL_DB_URL") 
engine = create_engine(DB_URL)

# --- [정제 로직] ---
def clean_spec(text_val):
    if pd.isna(text_val): return None
    match = re.search(r'(\d+\.?\d*)', str(text_val))
    return match.group(1) if match else None

# --- [수동 위경도 데이터] ---
MANUAL_PLANT_DATA = [
    {'gcd': '997D', 'hg': '1', 'lat': 33.2373862, 'lon': 126.3418842}, # 남제주
    {'gcd': '997G', 'hg': '1', 'lat': 35.0586428, 'lon': 128.8157557}, # 부산신항
    {'gcd': '997N', 'hg': '1', 'lat': 35.0586428, 'lon': 128.8157557}, # 부산복합자재
    {'gcd': 'B997', 'hg': '1', 'lat': 35.0870019, 'lon': 128.9989357}, # 부산본부 1
    {'gcd': 'B997', 'hg': '2', 'lat': 35.0870019, 'lon': 128.9989357}, # 부산본부 2
    {'gcd': '997Q', 'hg': '1', 'lat': 35.2591938, 'lon': 129.2235041}, # 부산수처리장
    {'gcd': '997R', 'hg': '1', 'lat': 35.1902253, 'lon': 129.0563480}, # 부산운동장
    {'gcd': '9987', 'hg': '1', 'lat': 35.1157106, 'lon': 129.0428212}, # 부산역선상
    {'gcd': '997S', 'hg': '1', 'lat': 37.536111, 'lon': 126.602318, 'cap': '1742', 'ang': '20', 'mod': '475W x 3,668', 'inv': '250kW x 7EA'},
    {'gcd': '997Y', 'hg': '1', 'lat': 37.536111, 'lon': 126.602318, 'cap': '907.2', 'ang': '23', 'mod': '360W x 2,520', 'inv': '250kW x 4EA'},
    {'gcd': '8760', 'hg': '1', 'lat': 37.536111, 'lon': 126.602318}, # 신인천소내
    {'gcd': '9985', 'hg': '1', 'lat': 37.536111, 'lon': 126.602318}, # 신인천주차장
    {'gcd': '9988', 'hg': '1', 'lat': 37.536111, 'lon': 126.602318}, # 신인천북측
    {'gcd': '9989', 'hg': '1', 'lat': 37.536111, 'lon': 126.602318}, # 신인천 1,2단계
    {'gcd': '9979', 'hg': '1', 'lat': 37.3335822, 'lon': 127.4795795}, # 이천D
    {'gcd': 'S997', 'hg': '1', 'lat': 37.1902416, 'lon': 129.3387384, 'cap': '999', 'ang': '25', 'mod': '360W x 2,775', 'inv': '500kW x 2EA'},
    {'gcd': 'S997', 'hg': '2', 'lat': 37.1902416, 'lon': 129.3387384, 'cap': '990.45', 'ang': '25', 'mod': '355W x 2,790', 'inv': '500kW x 2EA'},
    {'gcd': 'S997', 'hg': '3', 'lat': 37.1902416, 'lon': 129.3387384, 'cap': '2002.32', 'ang': '25', 'mod': '360W x 5,562', 'inv': '500kW x 1EA, 1500kW x 1EA'}
]

def run_ingestion():
    PROCESSED_DIR = PROJECT_ROOT / "pv_data_processed"
    files = sorted(list(PROCESSED_DIR.glob("nambu_processed_*.csv")))
    
    if not files:
        print("❌ 가공된 CSV 파일이 없습니다.")
        return

    # --- 1단계: 테이블 초기화 및 스키마 설정 (renit_db.py 스타일) ---
    print("🧹 1. 테이블 초기화 및 스키마 설정 중...")
    with engine.begin() as conn:
        # 기존 테이블 삭제
        conn.execute(text("DROP TABLE IF EXISTS pv_generation CASCADE;"))
        conn.execute(text("DROP TABLE IF EXISTS plant_info CASCADE;"))
        
        # pv_generation 테이블 생성 (Primary Key 추가로 데이터 중복 방지)
        conn.execute(text("""
            CREATE TABLE pv_generation (
                timestamp TIMESTAMP NOT NULL,
                ipptnm TEXT NOT NULL,
                hogi TEXT NOT NULL,
                generation FLOAT,
                gencd TEXT,
                PRIMARY KEY (timestamp, ipptnm, hogi) -- 중복 데이터 방지 핵심!
            );
        """))
    print("✅ 테이블 스키마 생성 완료")

    # --- 2단계: 마스터 테이블(plant_info) 생성 ---
    print("🏠 2. 마스터 테이블(plant_info) 구축 중...")
    sample_dfs = []
    for f in files:
        df_tmp = pd.read_csv(f, encoding='utf-8-sig').iloc[:1]
        sample_dfs.append(df_tmp)
    
    df_master = pd.concat(sample_dfs).drop_duplicates(['gencd', 'hogi'])
    df_master['capacity_kw'] = df_master['설치용량'].apply(clean_spec)
    df_master['angle_deg'] = df_master['설치각'].apply(clean_spec)
    
    df_manual = pd.DataFrame(MANUAL_PLANT_DATA)
    df_master = pd.merge(df_master, df_manual, left_on=['gencd', 'hogi'], right_on=['gcd', 'hg'], how='left')
    
    master_cols = {
        'gencd': 'gencd', 'hogi': 'hogi', 'ipptnm': 'plant_name', 
        '발전소 주소지': 'address', 'capacity_kw': 'capacity_kw', 
        'angle_deg': 'angle_deg', 'lat': 'lat', 'lon': 'lon',
        'mod': 'module_spec', 'inv': 'inverter_spec'
    }
    df_master_final = df_master[list(master_cols.keys())].rename(columns=master_cols)
    df_master_final.to_sql('plant_info', con=engine, if_exists='replace', index=False)

    # --- 3단계: 시계열 데이터(pv_generation) 적재 및 중복 제거 ---
    print(f"📈 3. 시계열 데이터 적재 시작 ({len(files)}개 파일)...")
    for file_path in files:
        df = pd.read_csv(file_path, encoding='utf-8-sig')
        
        # 데이터 타입 변환 및 결측치 처리
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df['generation'] = pd.to_numeric(df['generation'], errors='coerce').fillna(0)
        
        # [중요!] CSV 내부의 중복 데이터 제거 (renit_db 로직)
        df = df.drop_duplicates(subset=['timestamp', 'ipptnm', 'hogi'], keep='first')
        
        # 필요한 컬럼만 추출하여 적재
        df_gen = df[['timestamp', 'ipptnm', 'hogi', 'generation', 'gencd']]
        df_gen.to_sql('pv_generation', con=engine, if_exists='append', index=False)
        print(f"  - ✅ {file_path.name} 이관 완료 ({len(df_gen)}행)")

    print("\n🎉 모든 데이터가 중복 없이 깨끗하게 저장되었습니다!")

if __name__ == "__main__":
    run_ingestion()