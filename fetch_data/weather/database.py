"""
ASOS 기상 데이터 전용 Database 모듈

Tables:
1. weather_asos: ASOS 시간자료(기온/습도/일사량) — data/asos_all_merged.csv의 DB 반영본

시간 규약(중요):
- timestamp는 ASOS API의 tm 필드(KST) 값을 그대로 저장한다. tm은 KPX SMP처럼
  "1~24" 구간라벨이 아니라 이미 실제 시각(예: 2025-11-01 14:00:00)이므로
  적재 시 별도의 시(hour) 보정을 하지 않는다.
- 다만 물리적 의미는 컬럼마다 다르다. 기상청 지상기상관측지침(2024.10) 2.4.2/2.4.3에
  따르면 기온·습도는 "정시(00분) 순간값"이고, 2.4.6~2.4.8에 따르면 강수량·일조시간·
  일사량 계열은 "정시까지 누적된 1시간 합"이다(예: 09~09강수량, 00시~24시 일조시간처럼
  종료시각으로 라벨링). 즉 같은 행의 timestamp라도 temperature/humidity는 그 순간의
  스냅샷, solar_radiation은 그 직전 1시간의 누적값으로 해석해야 한다. 코드에서 이 차이를
  보정하지는 않는다(원본 그대로 보존) — research 뷰(Task 3)에서 조인 시 참고할 것.
"""

from typing import Optional

import pandas as pd
from sqlalchemy import (
    Column,
    Float,
    Integer,
    String,
    DateTime,
    Index,
    func,
)
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.engine import Engine
from sqlalchemy.orm import declarative_base

from fetch_data.common.db_base import get_engine, get_session
from fetch_data.common.logger import get_logger

logger = get_logger(__name__)

Base = declarative_base()

# upsert 배치 크기 (demand_weather_1h와 동일 규모의 시간별 관측 데이터)
BATCH_SIZE = 3000


# ========================================
# ASOS 기상 테이블
# ========================================

class WeatherASOS(Base):
    """ASOS 시간자료(기온/습도/일사량) 테이블"""

    __tablename__ = "weather_asos"

    id = Column(Integer, primary_key=True, autoincrement=True)
    timestamp = Column(DateTime, nullable=False, index=True, comment="관측 시각 (KST, ASOS tm 원본값)")
    station_name = Column(String(50), nullable=False, comment="관측지점명")
    temperature = Column(Float, nullable=True, comment="기온 (℃, 정시 순간값)")
    humidity = Column(Float, nullable=True, comment="상대습도 (%, 정시 순간값)")
    solar_radiation = Column(Float, nullable=True, comment="일사량 (MJ/m^2, 정시까지의 1시간 누적값)")

    __table_args__ = (
        Index("ix_weather_asos_ts_station", "timestamp", "station_name", unique=True),
    )

    def __repr__(self):
        return (
            f"<WeatherASOS(timestamp={self.timestamp}, station={self.station_name}, "
            f"temp={self.temperature}, humidity={self.humidity}, solar={self.solar_radiation})>"
        )


# ========================================
# Engine & Session
# ========================================

# 엔진/세션은 fetch_data.common.db_base 사용 (get_engine, get_session)


def init_db():
    """Initialize weather database tables."""
    engine = get_engine()
    Base.metadata.create_all(engine)
    logger.info("기상(ASOS) 테이블 생성 완료")


def drop_all_tables():
    """Drop all weather tables (use with caution)."""
    engine = get_engine()
    Base.metadata.drop_all(engine)
    logger.info("기상(ASOS) 테이블 삭제 완료")


# ========================================
# 적재 (UPSERT)
# ========================================

_VALUE_COLUMNS = ("temperature", "humidity", "solar_radiation")


def load_asos_df(df: pd.DataFrame, engine: Optional[Engine] = None) -> int:
    """ASOS DataFrame을 weather_asos에 UPSERT한다.

    입력 컬럼은 원본 CSV(`date`, `humidity`, `temperature`, `station_name`,
    `solar radiation`)나 정규화된 수집 결과(`normalize_weather_data` 출력,
    `solar radiation` 없음) 어느 쪽이든 받는다. 없는 값 컬럼은 NULL로 채운다.

    (timestamp, station_name) 충돌 시 COALESCE로 갱신한다 — 들어온 값이 NULL이면
    기존 값을 유지한다(예: 일일 수집은 일사량을 안 보내므로, 매일 적재해도
    백필로 채워둔 일사량이 지워지지 않는다). 기존 upsert_demand_5min과 동일한 패턴.

    Returns:
        upsert된 행 수 (timestamp/station_name이 없는 행은 제외하고 센다)
    """
    if df is None or df.empty:
        logger.info("[DB] 적재할 ASOS 데이터가 없습니다.")
        return 0

    frame = df.rename(columns={"date": "timestamp", "solar radiation": "solar_radiation"}).copy()

    if "timestamp" not in frame.columns or "station_name" not in frame.columns:
        raise ValueError(
            f"필수 컬럼(timestamp/date, station_name)이 없습니다: {list(frame.columns)}"
        )

    frame["timestamp"] = pd.to_datetime(frame["timestamp"], errors="coerce")
    for col in _VALUE_COLUMNS:
        if col not in frame.columns:
            frame[col] = None
        frame[col] = pd.to_numeric(frame[col], errors="coerce")

    # station_name의 결측(NaN)은 문자열로 바꾸기 전에 걸러야 한다. astype(str)을
    # 먼저 하면 NaN이 "nan"이라는 멀쩡해 보이는 문자열이 되어 dropna를 통과해버린다.
    frame = frame.dropna(subset=["timestamp", "station_name"])
    frame["station_name"] = frame["station_name"].astype(str).str.strip()
    frame = frame[frame["station_name"] != ""]
    if frame.empty:
        logger.info("[DB] 유효한 ASOS 행이 없습니다(timestamp/station_name 결측).")
        return 0

    columns = ["timestamp", "station_name", *_VALUE_COLUMNS]
    subset = frame[columns]
    # NaN -> None 명시 변환. object로 먼저 캐스팅해야 float 컬럼에서 None이
    # 다시 NaN으로 되돌아가지 않는다(NaN을 그대로 보내면 DB에 SQL NULL이 아니라
    # 부동소수점 NaN 리터럴이 저장되어 집계 함수가 오염된다).
    records = subset.astype(object).where(pd.notnull(subset), None).to_dict("records")

    engine = engine or get_engine()
    total = 0
    for offset in range(0, len(records), BATCH_SIZE):
        batch = records[offset:offset + BATCH_SIZE]
        statement = insert(WeatherASOS).values(batch)
        statement = statement.on_conflict_do_update(
            index_elements=["timestamp", "station_name"],
            set_={
                column: func.coalesce(getattr(statement.excluded, column), getattr(WeatherASOS, column))
                for column in _VALUE_COLUMNS
            },
        )
        with engine.begin() as connection:
            connection.execute(statement)
        total += len(batch)
        logger.info(f"[DB] weather_asos upsert 진행: {total}/{len(records)}행")

    logger.info(f"[DB] weather_asos upsert 완료: {total}행")
    return total


if __name__ == "__main__":
    logger.info("ASOS 기상 Database 초기화")
    init_db()
