"""
Database connection and ORM models for demand data.

Tables:
1. demand_5min: 5분 단위 전력수급 원본 데이터
2. demand_weather_1h: 1시간 단위 기상+전력수요 통합 데이터
"""

import asyncio
import os
from contextlib import asynccontextmanager
from datetime import datetime
from typing import AsyncGenerator, Optional

from sqlalchemy import (
    Column,
    Integer,
    Float,
    String,
    DateTime,
    Boolean,
    Index,
    text,
)
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
    AsyncEngine,
)
from sqlalchemy.orm import declarative_base
from sqlalchemy.pool import NullPool

# Database URL from environment variable
DATABASE_URL = os.getenv(
    "DEMAND_DATABASE_URL",
    "postgresql+asyncpg://demand:demand@demand-db:5432/demand"
)

Base = declarative_base()

# Track current event loop for engine
_engine: Optional[AsyncEngine] = None
_engine_loop_id: Optional[int] = None


def _get_current_loop_id() -> int:
    """Get current event loop id."""
    try:
        loop = asyncio.get_running_loop()
        return id(loop)
    except RuntimeError:
        return 0


def get_engine() -> AsyncEngine:
    """
    Get or create async engine.
    
    Creates a new engine if:
    - No engine exists
    - Current event loop is different from the one engine was created in
    
    Uses NullPool to avoid connection pool issues across event loops.
    """
    global _engine, _engine_loop_id
    
    current_loop_id = _get_current_loop_id()
    
    # Check if we need to create a new engine
    if _engine is None or _engine_loop_id != current_loop_id:
        # Dispose old engine if exists (ignore errors)
        if _engine is not None:
            try:
                # Can't await here, but NullPool doesn't need disposal
                pass
            except Exception:
                pass
        
        # Create new engine with NullPool to avoid cross-loop issues
        _engine = create_async_engine(
            DATABASE_URL,
            echo=False,
            poolclass=NullPool,  # No connection pooling - safest for Prefect
        )
        _engine_loop_id = current_loop_id
    
    return _engine


def get_session_factory() -> async_sessionmaker:
    """Get session factory for current engine."""
    return async_sessionmaker(
        get_engine(),
        class_=AsyncSession,
        expire_on_commit=False,
    )


@asynccontextmanager
async def get_async_session() -> AsyncGenerator[AsyncSession, None]:
    """
    Context manager for async database session.
    
    Usage:
        async with get_async_session() as session:
            result = await session.execute(...)
    """
    factory = get_session_factory()
    session = factory()
    try:
        yield session
    finally:
        await session.close()


# Alias for backward compatibility
async_session_factory = get_session_factory


class Demand5Min(Base):
    """
    5분 단위 전력수급 데이터 테이블

    KPX API에서 수집한 원본 데이터를 저장
    """
    __tablename__ = "demand_5min"

    id = Column(Integer, primary_key=True, autoincrement=True)
    timestamp = Column(DateTime, nullable=False, index=True)

    # 수요/공급 데이터
    current_demand = Column(Float, nullable=True, comment="현재수요(MW)")
    current_supply = Column(Float, nullable=True, comment="현재공급(MW)")
    supply_capacity = Column(Float, nullable=True, comment="공급가능용량(MW)")
    supply_reserve = Column(Float, nullable=True, comment="공급예비력(MW)")
    reserve_rate = Column(Float, nullable=True, comment="공급예비율(%)")
    operation_reserve = Column(Float, nullable=True, comment="운영예비력(MW)")

    # 메타데이터
    is_holiday = Column(Boolean, default=False, comment="공휴일 여부")
    created_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        Index("ix_demand_5min_timestamp_unique", "timestamp", unique=True),
    )

    def __repr__(self):
        return f"<Demand5Min(timestamp={self.timestamp}, demand={self.current_demand})>"


class DemandWeather1H(Base):
    """
    1시간 단위 기상+전력수요 통합 데이터 테이블

    기상 데이터(온도, 습도)와 1시간 평균 전력수요를 조인하여 저장
    """
    __tablename__ = "demand_weather_1h"

    id = Column(Integer, primary_key=True, autoincrement=True)
    timestamp = Column(DateTime, nullable=False, index=True)

    # 기상 데이터
    station_name = Column(String(50), nullable=False, comment="관측소명")
    temperature = Column(Float, nullable=True, comment="기온(°C)")
    humidity = Column(Float, nullable=True, comment="습도(%)")

    # 전력수요 (1시간 평균)
    demand_avg = Column(Float, nullable=True, comment="1시간 평균 수요(MW)")

    # 메타데이터
    is_holiday = Column(Boolean, default=False, comment="공휴일 여부")
    created_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        Index(
            "ix_demand_weather_1h_timestamp_station",
            "timestamp",
            "station_name",
            unique=True
        ),
    )

    def __repr__(self):
        return (
            f"<DemandWeather1H(timestamp={self.timestamp}, "
            f"station={self.station_name}, temp={self.temperature})>"
        )


async def init_db():
    """Initialize database tables."""
    engine = get_engine()
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    print("[DB] Tables created successfully")


async def get_session() -> AsyncGenerator[AsyncSession, None]:
    """Get async database session (generator version)."""
    async with get_async_session() as session:
        yield session


async def get_last_timestamp_5min() -> datetime | None:
    """Get the last timestamp from demand_5min table for backfill."""
    async with get_async_session() as session:
        result = await session.execute(
            text("SELECT MAX(timestamp) FROM demand_5min")
        )
        row = result.scalar()
        return row


async def get_last_timestamp_1h(station_name: str | None = None) -> datetime | None:
    """Get the last timestamp from demand_weather_1h table for backfill."""
    async with get_async_session() as session:
        if station_name:
            result = await session.execute(
                text(
                    "SELECT MAX(timestamp) FROM demand_weather_1h "
                    "WHERE station_name = :station"
                ),
                {"station": station_name}
            )
        else:
            result = await session.execute(
                text("SELECT MAX(timestamp) FROM demand_weather_1h")
            )
        row = result.scalar()
        return row


# Export engine for backward compatibility
engine = property(lambda self: get_engine())
