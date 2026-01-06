import pandas as pd
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv
from pathlib import Path

# 1. 환경 설정 및 DB 연결
PROJECT_ROOT = Path(__file__).parent
load_dotenv(PROJECT_ROOT / ".env")

# 로컬에서 실행하므로 .env의 LOCAL_DB_URL을 우선 참조하게 설정
DB_URL = os.getenv("LOCAL_DB_URL") or os.getenv("DB_URL")
engine = create_engine(DB_URL)

def inspect_tables():
    with engine.connect() as conn:
        print("\n" + "="*50)
        print("🔍 PostgreSQL 데이터 적재 상태 점검")
        print("="*50)

        # 1. 테이블별 행 수(Row Count) 확인
        print("\n📊 [1. 테이블 요약]")
        count_query = """
        SELECT 'plant_info' as table_name, COUNT(*) as row_count FROM plant_info
        UNION ALL
        SELECT 'pv_generation' as table_name, COUNT(*) as row_count FROM pv_generation
        """
        summary = pd.read_sql(text(count_query), conn)
        print(summary)

        # 2. 마스터 테이블 (plant_info) 상세 확인
        print("\n🏠 [2. 마스터 테이블 샘플 (발전소별 고정 정보)]")
        # 새로 추가된 capacity_kw, lat, lon 등의 컬럼이 잘 들어갔는지 확인
        master_data = pd.read_sql(text("SELECT * FROM plant_info ORDER BY gencd, hogi LIMIT 10"), conn)
        print(master_data)

        # 3. 데이터 정합성 체크 (Join 테스트)
        print("\n🔗 [3. 테이블 조인 테스트 (정상 연결 여부)]")
        # p.plant_name 대신 실제 컬럼명인 p.ipptnm을 사용합니다.
        join_query = """
        SELECT 
            g.timestamp, 
            p.ipptnm AS plant_name, 
            g.generation, 
            p.lat, 
            p.lon
        FROM pv_generation g
        JOIN plant_info p ON g.gencd = p.gencd AND g.hogi = p.hogi
        ORDER BY g.timestamp DESC
        LIMIT 5
        """
        join_data = pd.read_sql(text(join_query), conn)
        join_data = pd.read_sql(text(join_query), conn)
        if join_data.empty:
            print("⚠️ 경고: 테이블 조인 결과가 없습니다! gencd/hogi 값이 일치하는지 확인하세요.")
        else:
            print("✅ 조인 성공: 발전량 데이터와 발전소 정보가 잘 매칭됩니다.")
            print(join_data)

if __name__ == "__main__":
    inspect_tables()