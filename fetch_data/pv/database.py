"""
PV 데이터 전용 Database 모듈

Tables:
1. pv_nambu: 남부발전 태양광 발전 데이터
2. pv_namdong: 남동발전 태양광 발전 데이터
3. plant_info_nambu: 남부발전 발전소 정보 (위경도 포함)
4. plant_info_namdong: 남동발전 발전소 정보 (위경도 포함)
"""

from pathlib import Path
from datetime import datetime
from typing import Optional

from sqlalchemy import (
    Column,
    Integer,
    Float,
    String,
    DateTime,
    Text,
    Index,
    create_engine,
    text,
)
from sqlalchemy.orm import declarative_base, sessionmaker

from fetch_data.common.config import get_db_url

# Database URL (환경 변수 중앙 관리 모듈에서 로드)
DB_URL = get_db_url()

Base = declarative_base()

# ========================================
# 남부발전 테이블
# ========================================

class PVNambu(Base):
    """
    남부발전 태양광 발전 데이터 테이블
    """
    __tablename__ = "pv_nambu"

    id = Column(Integer, primary_key=True, autoincrement=True)
    timestamp = Column(DateTime, nullable=False, index=True)
    plant_id = Column(String(50), nullable=False, comment="발전소 코드 (gencd)")
    plant_name = Column(String(200), nullable=False, comment="발전소명")
    hogi = Column(String(10), nullable=False, default="1", comment="호기")
    generation = Column(Float, nullable=True, comment="발전량 (kWh)")
    lat = Column(Float, nullable=True, comment="위도")
    lon = Column(Float, nullable=True, comment="경도")

    __table_args__ = (
        Index("ix_pv_nambu_ts_plant_hogi", "timestamp", "plant_id", "hogi", unique=True),
    )

    def __repr__(self):
        return f"<PVNambu(timestamp={self.timestamp}, plant={self.plant_name}, gen={self.generation})>"


class PlantInfoNambu(Base):
    """
    남부발전 발전소 마스터 정보
    """
    __tablename__ = "plant_info_nambu"

    id = Column(Integer, primary_key=True, autoincrement=True)
    plant_id = Column(String(50), nullable=False, comment="발전소 코드 (gencd)")
    plant_name = Column(String(200), nullable=False, comment="발전소명")
    hogi = Column(String(10), nullable=False, default="1", comment="호기")
    address = Column(Text, nullable=True, comment="주소")
    capacity_kw = Column(Float, nullable=True, comment="설치용량 (kW)")
    angle_deg = Column(Float, nullable=True, comment="설치각도")
    lat = Column(Float, nullable=True, comment="위도")
    lon = Column(Float, nullable=True, comment="경도")
    module_spec = Column(Text, nullable=True, comment="모듈 사양")
    inverter_spec = Column(Text, nullable=True, comment="인버터 사양")

    __table_args__ = (
        Index("ix_plant_info_nambu_id_hogi", "plant_id", "hogi", unique=True),
    )


# ========================================
# 남동발전 테이블
# ========================================

class PVNamdong(Base):
    """
    남동발전 태양광 발전 데이터 테이블
    """
    __tablename__ = "pv_namdong"

    id = Column(Integer, primary_key=True, autoincrement=True)
    timestamp = Column(DateTime, nullable=False, index=True)
    plant_name = Column(String(200), nullable=False, comment="발전소명")
    generation = Column(Float, nullable=True, comment="발전량 (kWh)")
    lat = Column(Float, nullable=True, comment="위도")
    lon = Column(Float, nullable=True, comment="경도")

    __table_args__ = (
        Index("ix_pv_namdong_ts_plant", "timestamp", "plant_name", unique=True),
    )

    def __repr__(self):
        return f"<PVNamdong(timestamp={self.timestamp}, plant={self.plant_name}, gen={self.generation})>"


class PlantInfoNamdong(Base):
    """
    남동발전 발전소 마스터 정보
    """
    __tablename__ = "plant_info_namdong"

    id = Column(Integer, primary_key=True, autoincrement=True)
    plant_name = Column(String(200), nullable=False, unique=True, comment="발전소명")
    lat = Column(Float, nullable=True, comment="위도")
    lon = Column(Float, nullable=True, comment="경도")
    capacity_kw = Column(Float, nullable=True, comment="설치용량 (kW)")
    address = Column(Text, nullable=True, comment="주소")


# ========================================
# Engine & Session
# ========================================

_engine = None

def get_engine():
    """Get or create SQLAlchemy engine."""
    global _engine
    if _engine is None:
        _engine = create_engine(DB_URL, echo=False)
    return _engine


def get_session():
    """Get database session."""
    engine = get_engine()
    Session = sessionmaker(bind=engine)
    return Session()


def init_db():
    """Initialize database tables."""
    engine = get_engine()
    Base.metadata.create_all(engine)
    print("[DB] PV 테이블 생성 완료")


def drop_all_tables():
    """Drop all PV tables (use with caution)."""
    engine = get_engine()
    Base.metadata.drop_all(engine)
    print("[DB] PV 테이블 삭제 완료")


# ========================================
# 남동발전 발전소 위경도 매핑
# ========================================

NAMDONG_PLANT_LOCATIONS = {
    # 영흥 지역 (인천 영흥도)
    "영흥태양광_1": {"lat": 37.2542, "lon": 126.4278},
    "영흥태양광_2": {"lat": 37.2542, "lon": 126.4278},
    "영흥태양광 #3_1": {"lat": 37.2542, "lon": 126.4278},
    "영흥태양광 #3_2": {"lat": 37.2542, "lon": 126.4278},
    "영흥태양광 #3_3": {"lat": 37.2542, "lon": 126.4278},
    "영흥태양광#5": {"lat": 37.2542, "lon": 126.4278},

    # 영동 지역 (강원 영동)
    "영동태양광": {"lat": 37.1837, "lon": 128.9437},

    # 삼천포 지역 (경남 고성)
    "삼천포태양광_1": {"lat": 34.9294, "lon": 128.0656},
    "삼천포태양광_2": {"lat": 34.9294, "lon": 128.0656},
    "삼천포태양광_3": {"lat": 34.9294, "lon": 128.0656},
    "삼천포태양광_4": {"lat": 34.9294, "lon": 128.0656},
    "삼천포태양광#5_1": {"lat": 34.9294, "lon": 128.0656},
    "삼천포태양광#5_2": {"lat": 34.9294, "lon": 128.0656},
    "삼천포태양광#6": {"lat": 34.9294, "lon": 128.0656},

    # 광양 지역 (전남 광양)
    "광양항세방태양광": {"lat": 34.9126, "lon": 127.6958},

    # 여수 지역 (전남 여수)
    "여수태양광": {"lat": 34.7604, "lon": 127.6622},

    # 고흥 지역 (전남 고흥)
    "고흥만 수상태양광": {"lat": 34.6047, "lon": 127.2850},

    # 구미 지역 (경북 구미)
    "구미태양광": {"lat": 36.1195, "lon": 128.3446},

    # 예천 지역 (경북 예천)
    "예천태양광": {"lat": 36.6579, "lon": 128.4526},

    # 경상대 지역 (경남 진주)
    "경상대태양광": {"lat": 35.1535, "lon": 128.0986},

    # 두산엔진 (경남 창원)
    "두산엔진MG태양광": {"lat": 35.2270, "lon": 128.6811},

    # 탑선 지역
    "탑선태양광_1": {"lat": 35.9078, "lon": 128.8097},
    "탑선태양광_3": {"lat": 35.9078, "lon": 128.8097},
}


def get_namdong_location(plant_name: str) -> dict:
    """남동발전 발전소명으로 위경도 조회"""
    # 정확히 매칭
    if plant_name in NAMDONG_PLANT_LOCATIONS:
        return NAMDONG_PLANT_LOCATIONS[plant_name]

    # 부분 매칭 (영흥, 삼천포 등 키워드로)
    for key, loc in NAMDONG_PLANT_LOCATIONS.items():
        if key in plant_name or plant_name in key:
            return loc

    # 매칭 실패시 기본값 (서울)
    return {"lat": 37.5665, "lon": 126.9780}


if __name__ == "__main__":
    print("PV Database 초기화")
    init_db()
