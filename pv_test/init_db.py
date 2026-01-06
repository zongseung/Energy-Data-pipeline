"""
CSV -> PostgreSQL 변환 (남동발전/남부발전 테이블 분리)

특징:
- 비동기 청크 기반 데이터 로딩
- 남동발전/남부발전 테이블 분리
- 남부발전 CSV에서 발전소 주소 → 좌표 추출
"""

import asyncio
import os
import re
import time
from pathlib import Path
from typing import Optional, Tuple
from concurrent.futures import ThreadPoolExecutor

import aiohttp
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

# =============================================================================
# 설정
# =============================================================================
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "pv-db"),
    "port": int(os.getenv("DB_PORT", 5432)),
    "dbname": os.getenv("DB_NAME", "pv_data"),
    "user": os.getenv("DB_USER", "pv_admin"),
    "password": os.getenv("DB_PASS", "pv_secure_2024"),
}

# 입력 파일 경로
INPUT_DIR = Path("/app/input")
NAMDONG_CSV = INPUT_DIR / "south_pv_all_long.csv"
NAMBU_DIR = INPUT_DIR / "nambu"
NAMBU_INFO_CSV = INPUT_DIR / "한국남부발전(주)_태양광발전기 사양정보_20250630.csv"

# API 키
VWORLD_API_KEY = os.getenv("VWORLD_API_KEY", "")
KAKAO_API_KEY = os.getenv("KAKAO_REST_API_KEY", "")

# 청크 설정
CHUNK_SIZE = 10000

# =============================================================================
# 발전소 좌표 하드코딩 (API 실패 대비)
# =============================================================================
PLANT_COORDS = {
    # 남동발전
    "영흥태양광": (37.2594, 126.4281),
    "영동태양광": (37.7519, 128.8761),
    "삼천포태양광": (34.9454, 128.0680),
    "여수태양광": (34.7604, 127.6622),
    "예천태양광": (36.6469, 128.4375),
    "구미태양광": (36.1195, 128.3446),
    "광양항세방태양광": (34.9156, 127.6800),
    "두산엔진MG태양광": (35.2100, 128.5800),
    "탑선태양광": (35.2733, 126.7297),
    "경상대태양광": (35.1582, 128.1002),
    "고흥만 수상태양광": (34.5500, 127.2800),
    # 남부발전 주요 지역
    "부산": (35.1796, 129.0756),
    "인천": (37.4563, 126.7052),
    "서귀포": (33.2541, 126.5601),
    "영월": (37.1839, 128.4618),
    "하동": (35.0670, 127.7513),
    "제주": (33.4996, 126.5312),
    "울산": (35.5384, 129.3114),
    "광양": (34.9406, 127.6958),
}


def normalize_plant_name(name: str) -> str:
    """발전소명 정규화"""
    name = re.sub(r"\s+", "", str(name))
    name = re.sub(r"[#＃_]", "", name)
    name = re.sub(r"[-－]", "", name)
    name = re.sub(r"\d+", "", name)
    return name.lower()


def extract_city_from_address(address: str) -> Optional[str]:
    """주소에서 도시명 추출"""
    if not address:
        return None
    patterns = [
        r"(부산|인천|서귀포|영월|하동|제주|울산|광양|서울|대구|대전|광주)",
        r"(경상남도|경상북도|전라남도|전라북도|충청남도|충청북도|강원도|경기도)",
    ]
    for pattern in patterns:
        match = re.search(pattern, str(address))
        if match:
            return match.group(1)
    return None


# =============================================================================
# 지오코딩 (비동기)
# =============================================================================
async def geocode_nominatim(session: aiohttp.ClientSession, address: str) -> Optional[Tuple[float, float]]:
    """Nominatim API로 주소 → 좌표 변환"""
    url = "https://nominatim.openstreetmap.org/search"
    params = {
        "q": address,
        "format": "json",
        "limit": 1,
        "countrycodes": "kr",
    }
    headers = {"User-Agent": "PV-Dashboard/1.0"}
    
    try:
        async with session.get(url, params=params, headers=headers, timeout=10) as resp:
            if resp.status == 200:
                data = await resp.json()
                if data:
                    return (float(data[0]["lat"]), float(data[0]["lon"]))
    except Exception as e:
        print(f"[Nominatim ERROR] {address}: {e}")
    return None


async def geocode_kakao(session: aiohttp.ClientSession, address: str) -> Optional[Tuple[float, float]]:
    """카카오 API로 주소 → 좌표 변환"""
    if not KAKAO_API_KEY:
        return None
    
    url = "https://dapi.kakao.com/v2/local/search/keyword.json"
    headers = {"Authorization": f"KakaoAK {KAKAO_API_KEY}"}
    params = {"query": address, "size": 1}
    
    try:
        async with session.get(url, params=params, headers=headers, timeout=5) as resp:
            if resp.status == 200:
                data = await resp.json()
                if data.get("documents"):
                    doc = data["documents"][0]
                    return (float(doc["y"]), float(doc["x"]))
    except Exception as e:
        print(f"[Kakao ERROR] {address}: {e}")
    return None


async def geocode_address(session: aiohttp.ClientSession, address: str, plant_name: str = "") -> Tuple[float, float]:
    """주소 → 좌표 변환 (하드코딩 → Kakao → Nominatim)"""
    # 1. 발전소명으로 하드코딩 좌표 확인
    base_name = normalize_plant_name(plant_name)
    for key, coords in PLANT_COORDS.items():
        if normalize_plant_name(key) in base_name or base_name in normalize_plant_name(key):
            print(f"[하드코딩] {plant_name} -> {coords}")
            return coords
    
    # 2. 주소에서 도시명 추출해서 하드코딩 확인
    city = extract_city_from_address(address)
    if city and city in PLANT_COORDS:
        coords = PLANT_COORDS[city]
        print(f"[도시 기반] {plant_name} ({city}) -> {coords}")
        return coords
    
    # 3. Kakao API
    coords = await geocode_kakao(session, address)
    if coords:
        print(f"[Kakao] {address} -> {coords}")
        return coords
    
    await asyncio.sleep(0.5)  # Rate limiting
    
    # 4. Nominatim API
    coords = await geocode_nominatim(session, address)
    if coords:
        print(f"[Nominatim] {address} -> {coords}")
        return coords
    
    # 기본값 (한국 중심)
    print(f"[WARN] 좌표 변환 실패: {plant_name} / {address}")
    return (35.9078, 127.7669)


# =============================================================================
# 데이터베이스 초기화
# =============================================================================
def get_connection():
    """PostgreSQL 연결"""
    return psycopg2.connect(**DB_CONFIG)


def init_tables(conn):
    """테이블 생성"""
    cursor = conn.cursor()
    
    # 남동발전 발전량 테이블
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS namdong_generation (
            id SERIAL PRIMARY KEY,
            datetime TIMESTAMP,
            plant_name VARCHAR(100),
            hour INTEGER,
            generation REAL
        )
    """)
    
    # 남부발전 발전량 테이블
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS nambu_generation (
            id SERIAL PRIMARY KEY,
            datetime TIMESTAMP,
            gencd VARCHAR(20),
            plant_name VARCHAR(200),
            hogi INTEGER,
            generation REAL,
            daily_total REAL,
            daily_avg REAL,
            daily_max REAL,
            daily_min REAL
        )
    """)
    
    # 남동발전 발전소 정보 테이블
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS namdong_plants (
            id SERIAL PRIMARY KEY,
            plant_name VARCHAR(100) UNIQUE,
            base_name VARCHAR(100),
            latitude REAL,
            longitude REAL
        )
    """)
    
    # 남부발전 발전소 정보 테이블
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS nambu_plants (
            id SERIAL PRIMARY KEY,
            plant_name VARCHAR(200) UNIQUE,
            address VARCHAR(500),
            capacity VARCHAR(500),
            install_angle VARCHAR(100),
            latitude REAL,
            longitude REAL
        )
    """)
    
    # 인덱스 생성
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_namdong_datetime ON namdong_generation(datetime)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_namdong_plant ON namdong_generation(plant_name)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_nambu_datetime ON nambu_generation(datetime)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_nambu_plant ON nambu_generation(plant_name)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_nambu_gencd ON nambu_generation(gencd)")
    
    conn.commit()
    print("테이블 생성 완료")


def clear_tables(conn):
    """테이블 데이터 삭제"""
    cursor = conn.cursor()
    cursor.execute("TRUNCATE TABLE namdong_generation, nambu_generation, namdong_plants, nambu_plants RESTART IDENTITY")
    conn.commit()
    print("기존 데이터 삭제 완료")


# =============================================================================
# 청크 기반 데이터 로딩
# =============================================================================
def load_namdong_chunks(conn):
    """남동발전 데이터 청크 단위 로딩"""
    if not NAMDONG_CSV.exists():
        print(f"[WARN] 남동발전 CSV 없음: {NAMDONG_CSV}")
        return
    
    print(f"\n남동발전 데이터 로딩: {NAMDONG_CSV}")
    cursor = conn.cursor()
    total_rows = 0
    
    for chunk_idx, chunk in enumerate(pd.read_csv(NAMDONG_CSV, encoding="utf-8-sig", chunksize=CHUNK_SIZE)):
        # 컬럼명 매핑
        chunk = chunk.rename(columns={
            "일자": "datetime",
            "발전소명": "plant_name", 
            "시간": "hour",
            "발전량": "generation"
        })
        
        # 데이터 삽입
        records = chunk[["datetime", "plant_name", "hour", "generation"]].values.tolist()
        execute_values(
            cursor,
            "INSERT INTO namdong_generation (datetime, plant_name, hour, generation) VALUES %s",
            records
        )
        conn.commit()
        
        total_rows += len(chunk)
        print(f"  청크 {chunk_idx + 1}: {len(chunk):,}건 (누적: {total_rows:,}건)")
    
    print(f"남동발전 총 {total_rows:,}건 로딩 완료")


def load_nambu_chunks(conn):
    """남부발전 데이터 청크 단위 로딩"""
    nambu_files = list(NAMBU_DIR.glob("nambu_processed_*.csv"))
    if not nambu_files:
        print(f"[WARN] 남부발전 CSV 없음: {NAMBU_DIR}")
        return
    
    print(f"\n남부발전 데이터 로딩: {len(nambu_files)}개 파일")
    cursor = conn.cursor()
    total_rows = 0
    
    for file_path in nambu_files:
        print(f"  파일: {file_path.name}")
        
        for chunk_idx, chunk in enumerate(pd.read_csv(file_path, encoding="utf-8", chunksize=CHUNK_SIZE)):
            # 필요한 컬럼만 선택
            records = []
            for _, row in chunk.iterrows():
                records.append((
                    row.get("timestamp") or row.get("ymd"),
                    row.get("gencd", ""),
                    row.get("발전소명") or row.get("ipptnm", ""),
                    int(row.get("hogi", 1)),
                    float(row.get("generation", 0)),
                    float(row.get("qvodgen", 0)),
                    float(row.get("qvodavg", 0)),
                    float(row.get("qvodmax", 0)),
                    float(row.get("qvodmin", 0)),
                ))
            
            execute_values(
                cursor,
                """INSERT INTO nambu_generation 
                   (datetime, gencd, plant_name, hogi, generation, daily_total, daily_avg, daily_max, daily_min) 
                   VALUES %s""",
                records
            )
            conn.commit()
            total_rows += len(records)
        
        print(f"    {file_path.name} 완료")
    
    print(f"남부발전 총 {total_rows:,}건 로딩 완료")


# =============================================================================
# 발전소 좌표 추가
# =============================================================================
async def add_plant_coordinates(conn):
    """발전소 좌표 정보 추가"""
    cursor = conn.cursor()
    
    async with aiohttp.ClientSession() as session:
        # 1. 남동발전 발전소
        cursor.execute("SELECT DISTINCT plant_name FROM namdong_generation")
        namdong_plants = [row[0] for row in cursor.fetchall()]
        print(f"\n남동발전 발전소 {len(namdong_plants)}개 좌표 추가")
        
        for plant in namdong_plants:
            base_name = normalize_plant_name(plant)
            lat, lng = await geocode_address(session, plant, plant)
            
            cursor.execute(
                """INSERT INTO namdong_plants (plant_name, base_name, latitude, longitude)
                   VALUES (%s, %s, %s, %s)
                   ON CONFLICT (plant_name) DO UPDATE SET latitude = %s, longitude = %s""",
                (plant, base_name, lat, lng, lat, lng)
            )
        conn.commit()
        
        # 2. 남부발전 발전소 (CSV 정보 활용)
        if NAMBU_INFO_CSV.exists():
            print(f"\n남부발전 발전소 정보 로딩: {NAMBU_INFO_CSV}")
            
            # CP949 인코딩으로 읽기
            try:
                info_df = pd.read_csv(NAMBU_INFO_CSV, encoding="cp949")
            except:
                info_df = pd.read_csv(NAMBU_INFO_CSV, encoding="utf-8")
            
            print(f"  발전소 정보 {len(info_df)}개")
            
            for _, row in info_df.iterrows():
                plant_name = row.get("발전소명", "")
                address = row.get("발전소 주소지", "")
                capacity = row.get("설치용량", "")
                angle = row.get("설치각", "")
                
                if not plant_name or pd.isna(plant_name):
                    continue
                
                lat, lng = await geocode_address(session, address, plant_name)
                
                cursor.execute(
                    """INSERT INTO nambu_plants (plant_name, address, capacity, install_angle, latitude, longitude)
                       VALUES (%s, %s, %s, %s, %s, %s)
                       ON CONFLICT (plant_name) DO UPDATE SET 
                           address = %s, capacity = %s, install_angle = %s, latitude = %s, longitude = %s""",
                    (plant_name, address, capacity, angle, lat, lng, 
                     address, capacity, angle, lat, lng)
                )
            conn.commit()
            print(f"남부발전 발전소 정보 추가 완료")
        
        # 3. 발전량 데이터에 있지만 정보 CSV에 없는 발전소 추가
        cursor.execute("""
            SELECT DISTINCT plant_name FROM nambu_generation 
            WHERE plant_name NOT IN (SELECT plant_name FROM nambu_plants)
        """)
        missing_plants = [row[0] for row in cursor.fetchall()]
        
        if missing_plants:
            print(f"\n누락된 남부발전 발전소 {len(missing_plants)}개 좌표 추가")
            for plant in missing_plants:
                lat, lng = await geocode_address(session, plant, plant)
                cursor.execute(
                    """INSERT INTO nambu_plants (plant_name, latitude, longitude)
                       VALUES (%s, %s, %s)
                       ON CONFLICT (plant_name) DO NOTHING""",
                    (plant, lat, lng)
                )
            conn.commit()


# =============================================================================
# 메인 실행
# =============================================================================
def main():
    print("=" * 60)
    print("PV 데이터 초기화 시작 (PostgreSQL)")
    print("=" * 60)
    
    # DB 연결 대기
    max_retries = 10
    for i in range(max_retries):
        try:
            conn = get_connection()
            print("PostgreSQL 연결 성공")
            break
        except Exception as e:
            print(f"PostgreSQL 연결 대기 중... ({i+1}/{max_retries})")
            time.sleep(3)
    else:
        raise Exception("PostgreSQL 연결 실패")
    
    try:
        # 테이블 초기화
        init_tables(conn)
        clear_tables(conn)
        
        # 데이터 로딩 (청크 단위)
        load_namdong_chunks(conn)
        load_nambu_chunks(conn)
        
        # 좌표 추가 (비동기)
        asyncio.run(add_plant_coordinates(conn))
        
        # 결과 확인
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM namdong_generation")
        namdong_count = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM nambu_generation")
        nambu_count = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM namdong_plants")
        namdong_plants = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM nambu_plants")
        nambu_plants = cursor.fetchone()[0]
        
        print("\n" + "=" * 60)
        print("완료!")
        print(f"  - 남동발전 발전량: {namdong_count:,}건")
        print(f"  - 남부발전 발전량: {nambu_count:,}건")
        print(f"  - 남동발전 발전소: {namdong_plants}개")
        print(f"  - 남부발전 발전소: {nambu_plants}개")
        print("=" * 60)
        
    finally:
        conn.close()


if __name__ == "__main__":
    main()
