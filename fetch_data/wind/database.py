"""
풍력 데이터 전용 Database 모듈

Tables:
1. wind_namdong: 남동발전 풍력 발전 데이터
2. wind_seobu: 서부발전 풍력 발전 데이터
3. wind_hangyoung: 한경풍력 발전 데이터
"""

from pathlib import Path

from sqlalchemy import (
    Column,
    Integer,
    Float,
    String,
    DateTime,
    Index,
)
from sqlalchemy.orm import declarative_base
from dotenv import load_dotenv

from fetch_data.common.logger import get_logger
from fetch_data.common.db_base import get_engine, get_session

logger = get_logger(__name__)

# 환경 변수 로드
PROJECT_ROOT = Path(__file__).parent.parent.parent
load_dotenv(PROJECT_ROOT / ".env")

Base = declarative_base()


# ========================================
# 남동발전 풍력 테이블
# ========================================

class WindNamdong(Base):
    """남동발전 풍력 발전 데이터 테이블"""
    __tablename__ = "wind_namdong"

    id = Column(Integer, primary_key=True, autoincrement=True)
    timestamp = Column(DateTime, nullable=False, index=True)
    plant_name = Column(String(200), nullable=False, comment="발전소명")
    generation = Column(Float, nullable=True, comment="발전량 (kWh)")

    __table_args__ = (
        Index("ix_wind_namdong_ts_plant", "timestamp", "plant_name", unique=True),
    )

    def __repr__(self):
        return f"<WindNamdong(timestamp={self.timestamp}, plant={self.plant_name}, gen={self.generation})>"


# ========================================
# 서부발전 풍력 테이블
# ========================================

class WindSeobu(Base):
    """서부발전 풍력 발전 데이터 테이블"""
    __tablename__ = "wind_seobu"

    id = Column(Integer, primary_key=True, autoincrement=True)
    timestamp = Column(DateTime, nullable=False, index=True)
    plant_name = Column(String(200), nullable=False, comment="발전기명")
    capacity_mw = Column(Float, nullable=True, comment="설비용량 (MW)")
    generation = Column(Float, nullable=True, comment="발전량 (kWh)")

    __table_args__ = (
        Index("ix_wind_seobu_ts_plant", "timestamp", "plant_name", unique=True),
    )

    def __repr__(self):
        return f"<WindSeobu(timestamp={self.timestamp}, plant={self.plant_name}, gen={self.generation})>"


# ========================================
# 한경풍력 테이블
# ========================================

class WindHangyoung(Base):
    """한경풍력 발전 데이터 테이블"""
    __tablename__ = "wind_hangyoung"

    id = Column(Integer, primary_key=True, autoincrement=True)
    timestamp = Column(DateTime, nullable=False, index=True)
    plant_name = Column(String(200), nullable=False, default="Hangyoung", comment="발전소명")
    generation = Column(Float, nullable=True, comment="발전량 (kWh)")

    def __repr__(self):
        return f"<WindHangyoung(timestamp={self.timestamp}, gen={self.generation})>"


# ========================================
# Engine & Session
# ========================================

# 엔진/세션은 fetch_data.common.db_base 사용 (get_engine, get_session)


def init_db():
    """Initialize wind database tables."""
    engine = get_engine()
    Base.metadata.create_all(engine)
    logger.info("풍력 테이블 생성 완료")


def drop_all_tables():
    """Drop all wind tables (use with caution)."""
    engine = get_engine()
    Base.metadata.drop_all(engine)
    logger.info("풍력 테이블 삭제 완료")


if __name__ == "__main__":
    logger.info("Wind Database 초기화")
    init_db()
