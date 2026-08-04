"""Synchronous persistence boundary for nationwide KPX demand data."""

import os
from datetime import datetime

from sqlalchemy import Boolean, Column, DateTime, Float, Index, Integer, String, func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.engine import Engine
from sqlalchemy.orm import declarative_base

DEFAULT_DEMAND_DB_URL = "postgresql+psycopg2://demand:demand@demand-postgres:5432/demand"
BATCH_SIZE = 1000
HOURLY_BATCH_SIZE = 3000

Base = declarative_base()


class Demand5Min(Base):
    __tablename__ = "demand_5min"

    id = Column(Integer, primary_key=True, autoincrement=True)
    timestamp = Column(DateTime, nullable=False, index=True)
    current_demand = Column(Float, nullable=True)
    current_supply = Column(Float, nullable=True)
    supply_capacity = Column(Float, nullable=True)
    supply_reserve = Column(Float, nullable=True)
    reserve_rate = Column(Float, nullable=True)
    operation_reserve = Column(Float, nullable=True)
    is_holiday = Column(Boolean, default=False)
    day_type = Column(Integer, default=0)
    created_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (Index("ix_demand_5min_timestamp_unique", "timestamp", unique=True),)


class DemandWeather1H(Base):
    __tablename__ = "demand_weather_1h"

    id = Column(Integer, primary_key=True, autoincrement=True)
    timestamp = Column(DateTime, nullable=False, index=True)
    station_name = Column(String(50), nullable=False)
    temperature = Column(Float, nullable=True)
    humidity = Column(Float, nullable=True)
    demand_avg = Column(Float, nullable=True)
    is_holiday = Column(Boolean, default=False)
    day_type = Column(Integer, default=0)
    created_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        Index("ix_demand_weather_1h_timestamp_station", "timestamp", "station_name", unique=True),
    )


def get_demand_engine(db_url: str | None = None) -> Engine:
    """Create a synchronous engine for the dedicated demand database."""
    from sqlalchemy import create_engine

    return create_engine(db_url or os.getenv("DEMAND_DB_URL", DEFAULT_DEMAND_DB_URL), echo=False)


def get_last_5min_timestamp(engine: Engine) -> datetime | None:
    """Return the newest nationwide five-minute demand timestamp."""
    with engine.connect() as connection:
        return connection.execute(select(func.max(Demand5Min.timestamp))).scalar_one()


def get_last_demand_weather_timestamp(engine: Engine) -> datetime | None:
    """Return the newest persisted hourly demand-weather timestamp."""
    with engine.connect() as connection:
        return connection.execute(select(func.max(DemandWeather1H.timestamp))).scalar_one()


def get_first_unknown_timestamp(engine: Engine) -> datetime | None:
    """Return the first legacy hourly placeholder timestamp, if present."""
    with engine.connect() as connection:
        return connection.execute(
            select(func.min(DemandWeather1H.timestamp)).where(
                DemandWeather1H.station_name == "UNKNOWN"
            )
        ).scalar_one()


def upsert_demand_5min(engine: Engine, records: list[dict]) -> int:
    """Insert or update KPX demand records by timestamp in bounded batches."""
    for offset in range(0, len(records), BATCH_SIZE):
        statement = insert(Demand5Min).values(records[offset:offset + BATCH_SIZE])
        statement = statement.on_conflict_do_update(
            index_elements=["timestamp"],
            set_={
                column: func.coalesce(
                    getattr(statement.excluded, column), getattr(Demand5Min, column)
                )
                for column in (
                    "current_demand",
                    "current_supply",
                    "supply_capacity",
                    "supply_reserve",
                    "reserve_rate",
                    "operation_reserve",
                    "is_holiday",
                    "day_type",
                )
            },
        )
        with engine.begin() as connection:
            connection.execute(statement)
    return len(records)


def upsert_demand_weather(engine: Engine, records: list[dict]) -> int:
    """Insert or update real hourly weather rows by timestamp and station."""
    for offset in range(0, len(records), HOURLY_BATCH_SIZE):
        statement = insert(DemandWeather1H).values(records[offset:offset + HOURLY_BATCH_SIZE])
        statement = statement.on_conflict_do_update(
            index_elements=["timestamp", "station_name"],
            set_={
                column: getattr(statement.excluded, column)
                for column in (
                    "temperature",
                    "humidity",
                    "demand_avg",
                    "is_holiday",
                    "day_type",
                )
            },
        )
        with engine.begin() as connection:
            connection.execute(statement)
    return len(records)
