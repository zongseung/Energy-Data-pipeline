"""
CSV -> SQLite 변환 및 발전소 좌표 추가

지오코딩 API 우선순위:
1. VWorld API (국토교통부, 무료 40,000건/일)
2. Kakao API (무료)
3. Nominatim (OpenStreetMap, 무료, 키 불필요)
"""

import os
import re
import sqlite3
import time
from pathlib import Path
from typing import Optional, Tuple

import pandas as pd
import requests

# 경로 설정
GENERATION_CSV = Path("/app/input/south_pv_all_long.csv")
PLANT_INFO_CSV = Path("/app/input/한국남동발전㈜_태양광 및 연계ESS 발전소 정보_20230810.csv")
DB_PATH = Path("/app/data/pv.db")

# API 키
VWORLD_API_KEY = os.getenv("VWORLD_API_KEY", "")
KAKAO_API_KEY = os.getenv("KAKAO_REST_API_KEY", "")

# API URLs
VWORLD_GEOCODE_URL = "https://api.vworld.kr/req/address"
KAKAO_SEARCH_URL = "https://dapi.kakao.com/v2/local/search/keyword.json"
NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"

# 발전소 좌표 하드코딩 (Nominatim 실패 대비)
PLANT_COORDS = {
    # 영흥태양광 계열 (인천 영흥도)
    "영흥태양광": (37.2594, 126.4281),
    "영흥태양광#5": (37.2594, 126.4281),
    "영흥태양광 #3": (37.2594, 126.4281),
    "영흥태양광_1": (37.2594, 126.4281),
    "영흥태양광_2": (37.2594, 126.4281),
    "영흥태양광 #3_1": (37.2594, 126.4281),
    "영흥태양광 #3_2": (37.2594, 126.4281),
    "영흥태양광 #3_3": (37.2594, 126.4281),
    # 삼천포태양광 계열 (경남 고성)
    "삼천포태양광": (34.9454, 128.0680),
    "삼천포태양광#5": (34.9454, 128.0680),
    "삼천포태양광#6": (34.9454, 128.0680),
    "삼천포태양광_1": (34.9454, 128.0680),
    "삼천포태양광_2": (34.9454, 128.0680),
    "삼천포태양광_3": (34.9454, 128.0680),
    "삼천포태양광_4": (34.9454, 128.0680),
    "삼천포태양광#5_1": (34.9454, 128.0680),
    "삼천포태양광#5_2": (34.9454, 128.0680),
    # 기타 발전소
    "영동태양광": (37.7519, 128.8761),      # 강원 강릉
    "여수태양광": (34.7604, 127.6622),      # 전남 여수
    "예천태양광": (36.6469, 128.4375),      # 경북 예천
    "구미태양광": (36.1195, 128.3446),      # 경북 구미
    "광양항세방태양광": (34.9156, 127.6800), # 전남 광양
    "두산엔진MG태양광": (35.2100, 128.5800), # 경남 창원
    "탑선태양광": (35.2733, 126.7297),      # 전남 장성
    "탑선태양광_1": (35.2733, 126.7297),
    "탑선태양광_3": (35.2733, 126.7297),
    "경상대태양광": (35.1582, 128.1002),    # 경남 진주
    "고흥만 수상태양광": (34.5500, 127.2800), # 전남 고흥
}


def normalize_plant_name(name: str) -> str:
    """발전소명 정규화 (공백, 특수문자 제거)"""
    name = re.sub(r"\s+", "", name)  # 공백 제거
    name = re.sub(r"[#＃]", "", name)  # # 제거
    name = re.sub(r"[-－]", "", name)  # - 제거
    name = re.sub(r"단지$", "", name)  # '단지' 제거
    return name.lower()


def extract_base_plant_name(name: str) -> str:
    """기본 발전소명 추출 (번호 제거)"""
    # "영흥 태양광 1단지" -> "영흥태양광"
    # "삼천포태양광#5" -> "삼천포태양광"
    name = normalize_plant_name(name)
    # 숫자와 단지 제거
    name = re.sub(r"\d+", "", name)
    return name


def load_plant_info() -> dict:
    """발전소 정보 CSV 로드 (주소 포함)"""
    if not PLANT_INFO_CSV.exists():
        print(f"[WARN] 발전소 정보 CSV 없음: {PLANT_INFO_CSV}")
        return {}

    df = pd.read_csv(PLANT_INFO_CSV, encoding="cp949")
    print(f"발전소 정보 CSV 로드: {len(df)}개")

    # 기본 발전소명으로 그룹화 (첫 번째 주소 사용)
    plant_addresses = {}
    for _, row in df.iterrows():
        plant_name = row["발전소명"]
        address = row["소재지주소"]
        base_name = extract_base_plant_name(plant_name)

        if base_name not in plant_addresses:
            plant_addresses[base_name] = {
                "address": address,
                "capacity": row["설비용량"],
                "original_name": plant_name,
            }

    print(f"고유 발전소 (기준): {len(plant_addresses)}개")
    return plant_addresses


def geocode_vworld(address: str) -> Optional[Tuple[float, float]]:
    """VWorld API로 주소 -> 좌표 변환"""
    if not VWORLD_API_KEY:
        return None

    params = {
        "service": "address",
        "request": "getcoord",
        "version": "2.0",
        "crs": "epsg:4326",
        "address": address,
        "refine": "true",
        "simple": "false",
        "format": "json",
        "type": "road",
        "key": VWORLD_API_KEY,
    }

    try:
        resp = requests.get(VWORLD_GEOCODE_URL, params=params, timeout=5)
        resp.raise_for_status()
        data = resp.json()

        if data.get("response", {}).get("status") == "OK":
            result = data["response"]["result"]
            point = result["point"]
            lat = float(point["y"])
            lng = float(point["x"])
            print(f"[VWorld] {address} -> ({lat}, {lng})")
            return (lat, lng)

    except Exception as e:
        print(f"[VWorld ERROR] {address}: {e}")

    return None


def geocode_kakao(address: str) -> Optional[Tuple[float, float]]:
    """카카오 API로 주소 -> 좌표 변환"""
    if not KAKAO_API_KEY:
        return None

    headers = {"Authorization": f"KakaoAK {KAKAO_API_KEY}"}
    params = {"query": address, "size": 1}

    try:
        resp = requests.get(KAKAO_SEARCH_URL, headers=headers, params=params, timeout=5)
        resp.raise_for_status()
        data = resp.json()

        if data.get("documents"):
            doc = data["documents"][0]
            lat = float(doc["y"])
            lng = float(doc["x"])
            print(f"[Kakao] {address} -> ({lat}, {lng})")
            return (lat, lng)

    except Exception as e:
        print(f"[Kakao ERROR] {address}: {e}")

    return None


def geocode_nominatim(address: str) -> Optional[Tuple[float, float]]:
    """Nominatim (OpenStreetMap) API로 주소 -> 좌표 변환 (무료, 키 불필요)"""
    headers = {"User-Agent": "PV-Dashboard/1.0"}
    params = {
        "q": address,
        "format": "json",
        "limit": 1,
        "countrycodes": "kr",
    }

    try:
        resp = requests.get(NOMINATIM_URL, headers=headers, params=params, timeout=5)
        resp.raise_for_status()
        data = resp.json()

        if data:
            lat = float(data[0]["lat"])
            lng = float(data[0]["lon"])
            print(f"[Nominatim] {address} -> ({lat}, {lng})")
            return (lat, lng)

    except Exception as e:
        print(f"[Nominatim ERROR] {address}: {e}")

    return None


def geocode(address: str, plant_name: str = "") -> Tuple[float, float]:
    """주소 -> 좌표 변환 (하드코딩 -> VWorld -> Kakao -> Nominatim 순서로 시도)"""

    # 1. 하드코딩된 좌표 확인
    if plant_name in PLANT_COORDS:
        coords = PLANT_COORDS[plant_name]
        print(f"[하드코딩] {plant_name} -> ({coords[0]}, {coords[1]})")
        return coords

    # 2. VWorld 시도
    coords = geocode_vworld(address)
    if coords:
        return coords

    time.sleep(0.2)

    # 3. Kakao 시도
    coords = geocode_kakao(address)
    if coords:
        return coords

    time.sleep(0.2)

    # 4. Nominatim 시도
    coords = geocode_nominatim(address)
    if coords:
        return coords

    # 기본값 (한국 중심)
    print(f"[WARN] 좌표 변환 실패, 기본값 사용: {plant_name} / {address}")
    return (35.9078, 127.7669)


def init_database() -> sqlite3.Connection:
    """SQLite DB 초기화"""
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # 테이블 생성
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS pv_generation (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            datetime DATETIME,
            plant_name TEXT,
            hour INTEGER,
            generation REAL
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS pv_plants (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            plant_name TEXT UNIQUE,
            base_name TEXT,
            address TEXT,
            capacity REAL,
            latitude REAL,
            longitude REAL
        )
    """)

    # 인덱스 생성
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_datetime ON pv_generation(datetime)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_plant ON pv_generation(plant_name)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_base_name ON pv_plants(base_name)")

    conn.commit()
    return conn


def load_generation_csv(conn: sqlite3.Connection):
    """발전량 CSV를 DB에 적재"""
    print(f"\n발전량 CSV 읽기: {GENERATION_CSV}")
    df = pd.read_csv(GENERATION_CSV, encoding="utf-8-sig")

    print(f"컬럼: {df.columns.tolist()}")
    print(f"행 수: {len(df):,}")

    # 중복 제거
    df = df.drop_duplicates()
    print(f"중복 제거 후: {len(df):,}")

    # DB에 저장
    df.to_sql(
        "pv_generation",
        conn,
        if_exists="replace",
        index=False,
        dtype={
            "일자": "DATETIME",
            "발전소명": "TEXT",
            "시간": "INTEGER",
            "발전량": "REAL",
        },
    )

    # 컬럼명 영문 변환
    conn.execute("ALTER TABLE pv_generation RENAME COLUMN 일자 TO datetime")
    conn.execute("ALTER TABLE pv_generation RENAME COLUMN 발전소명 TO plant_name")
    conn.execute("ALTER TABLE pv_generation RENAME COLUMN 시간 TO hour")
    conn.execute("ALTER TABLE pv_generation RENAME COLUMN 발전량 TO generation")
    conn.commit()

    # 인덱스 생성 (pandas to_sql 이후에 수동으로 생성)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_datetime ON pv_generation(datetime)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_plant ON pv_generation(plant_name)")
    conn.commit()

    print("발전량 CSV -> DB 완료")


def add_plant_coordinates(conn: sqlite3.Connection, plant_info: dict):
    """발전소 좌표 추가"""
    cursor = conn.cursor()

    # 발전량 데이터에서 고유 발전소명 조회
    cursor.execute("SELECT DISTINCT plant_name FROM pv_generation")
    gen_plants = [row[0] for row in cursor.fetchall()]
    print(f"\n발전량 데이터 발전소 수: {len(gen_plants)}")

    for plant in gen_plants:
        # 이미 존재하면 스킵
        cursor.execute("SELECT id FROM pv_plants WHERE plant_name = ?", (plant,))
        if cursor.fetchone():
            continue

        base_name = extract_base_plant_name(plant)

        # 발전소 정보에서 주소 찾기
        if base_name in plant_info:
            info = plant_info[base_name]
            address = info["address"]
            capacity = info["capacity"]
        else:
            # 매칭 실패 시 발전소명으로 검색
            address = plant
            capacity = None
            print(f"[WARN] 주소 매핑 실패: {plant} (base: {base_name})")

        # 좌표 변환 (plant_name으로 하드코딩된 좌표 먼저 확인)
        lat, lng = geocode(address, plant)
        time.sleep(0.1)  # API 호출 제한 방지

        cursor.execute(
            """INSERT INTO pv_plants
               (plant_name, base_name, address, capacity, latitude, longitude)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (plant, base_name, address, capacity, lat, lng),
        )

    conn.commit()

    # 결과 출력
    cursor.execute("SELECT plant_name, address, latitude, longitude FROM pv_plants")
    print("\n발전소 좌표:")
    for row in cursor.fetchall():
        print(f"  {row[0]}: ({row[2]:.4f}, {row[3]:.4f}) - {row[1][:30]}...")


def main():
    print("=" * 60)
    print("PV 데이터 초기화 시작")
    print("=" * 60)

    # API 키 상태 출력
    print(f"\nAPI 키 상태:")
    print(f"  VWorld: {'설정됨' if VWORLD_API_KEY else '없음'}")
    print(f"  Kakao: {'설정됨' if KAKAO_API_KEY else '없음'}")
    print(f"  Nominatim: 항상 사용 가능 (무료)")

    # 발전소 정보 로드
    plant_info = load_plant_info()

    # DB 초기화
    conn = init_database()

    # 발전량 데이터 로드
    load_generation_csv(conn)

    # 발전소 좌표 추가
    add_plant_coordinates(conn, plant_info)

    # 검증
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM pv_generation")
    gen_count = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM pv_plants")
    plant_count = cursor.fetchone()[0]

    print("\n" + "=" * 60)
    print(f"완료!")
    print(f"  - pv_generation: {gen_count:,}건")
    print(f"  - pv_plants: {plant_count}개")
    print(f"  - DB 경로: {DB_PATH}")
    print("=" * 60)

    conn.close()


if __name__ == "__main__":
    main()
