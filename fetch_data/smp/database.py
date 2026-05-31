"""
SMP(계통한계가격) 데이터 전용 Database 모듈

Tables:
1. smp_hourly        : 하루전시장 시간별 SMP (육지/제주)
2. smp_weighted_avg  : 공식 거래량가중평균 SMP (일/월/연, 육지/제주/통합)
3. smp_realtime_jeju : 제주 실시간시장 SMP (15분 단위)

설계 원칙:
- 기존 PV/Wind/Weather/Gen 테이블과 충돌하지 않도록 독립 Base 사용.
- init_db()는 이 모듈의 3개 테이블만 생성한다(기존 테이블 무영향).
- DB URL은 공통 헬퍼(get_db_url)로 해석한다.

시간 표기 규약(중요):
- KPX는 1~24시 hour-ending(구간 종료) 표기를 쓴다. 적재 시 구간시작으로 변환:
  시간별  라벨 N -> (N-1)시          (1시->00:00 ... 24시->23:00)
  실시간  Nh K구간 -> (N-1)시 + (K-1)*15분 (1h1구간->00:00 ... 24h4구간->23:45)
"""

from typing import Optional

from sqlalchemy import (
    Boolean,
    Column,
    Date,
    DateTime,
    Float,
    Index,
    Integer,
    String,
    create_engine,
)
from sqlalchemy.orm import declarative_base, sessionmaker

from fetch_data.common.db_utils import resolve_db_url

Base = declarative_base()


# ========================================
# 1. 하루전시장 시간별 SMP
# ========================================

class SMPHourly(Base):
    """하루전시장 시간별 SMP 테이블 (육지/제주)."""

    __tablename__ = "smp_hourly"

    id = Column(Integer, primary_key=True, autoincrement=True)
    timestamp = Column(DateTime, nullable=False, index=True, comment="구간시작 시각(KST)")
    region = Column(String(10), nullable=False, comment="land | jeju")
    price = Column(Float, nullable=True, comment="SMP (원/kWh)")

    __table_args__ = (
        Index("ix_smp_hourly_ts_region", "timestamp", "region", unique=True),
    )

    def __repr__(self) -> str:
        return f"<SMPHourly(ts={self.timestamp}, region={self.region}, price={self.price})>"


# ========================================
# 2. 공식 가중평균 SMP (일/월/연)
# ========================================

class SMPWeightedAvg(Base):
    """KPX/EPSIS 공식 가중평균 가격 테이블 (일/월/연 × 육지/제주/통합 × SMP/BLMP).

    가격 종류(price_type):
      - smp  : 계통한계가격 가중평균
      - blmp : 밸런싱 한계가격 가중평균 (제주 실시간시장, 2024~)
    region: land | jeju | unified(통합)
    """

    __tablename__ = "smp_weighted_avg"

    id = Column(Integer, primary_key=True, autoincrement=True)
    period_type = Column(String(10), nullable=False, comment="daily | monthly | yearly")
    period = Column(Date, nullable=False, comment="대표일(일=당일, 월=1일, 연=1/1)")
    region = Column(String(10), nullable=False, comment="land | jeju | unified")
    price_type = Column(String(10), nullable=False, default="smp", comment="smp | blmp")
    weighted_avg = Column(Float, nullable=True, comment="가중평균 가격 (원/kWh)")

    __table_args__ = (
        Index(
            "ix_smp_wavg_type_period_region_price",
            "period_type",
            "period",
            "region",
            "price_type",
            unique=True,
        ),
    )

    def __repr__(self) -> str:
        return (
            f"<SMPWeightedAvg(type={self.period_type}, period={self.period}, "
            f"region={self.region}, price_type={self.price_type}, wavg={self.weighted_avg})>"
        )


# ========================================
# 3. 제주 실시간시장 SMP (15분)
# ========================================

class SMPRealtimeJeju(Base):
    """제주 실시간시장 SMP 테이블 (15분 단위, 96구간/일)."""

    __tablename__ = "smp_realtime_jeju"

    id = Column(Integer, primary_key=True, autoincrement=True)
    timestamp = Column(DateTime, nullable=False, index=True, comment="15분 구간시작 시각(KST)")
    region = Column(String(10), nullable=False, default="jeju", comment="jeju 고정")
    price = Column(Float, nullable=True, comment="실시간 SMP (원/kWh, 음수 가능)")
    is_confirmed = Column(
        Boolean, nullable=False, default=False, comment="D+1 18시 공표 확정값 여부"
    )

    __table_args__ = (
        Index("ix_smp_rt_jeju_ts_region", "timestamp", "region", unique=True),
    )

    def __repr__(self) -> str:
        return (
            f"<SMPRealtimeJeju(ts={self.timestamp}, price={self.price}, "
            f"confirmed={self.is_confirmed})>"
        )


# ========================================
# Engine & Session
# ========================================

_engine = None


def get_engine(db_url: Optional[str] = None):
    """SQLAlchemy 엔진을 생성/반환합니다(싱글턴).

    적재 모듈(_common)과 동일하게 resolve_db_url을 사용해 도커/호스트 환경을
    자동 전환한다(호스트에서 pv-db DNS 실패 방지).
    """
    global _engine
    if db_url is not None:
        return create_engine(db_url, echo=False)
    if _engine is None:
        _engine = create_engine(resolve_db_url(), echo=False)
    return _engine


def get_session():
    """DB 세션을 반환합니다."""
    Session = sessionmaker(bind=get_engine())
    return Session()


def init_db() -> None:
    """SMP 테이블(3개)을 생성합니다. 기존 테이블에는 영향이 없습니다."""
    engine = get_engine()
    Base.metadata.create_all(engine)
    print("[DB] SMP 테이블 생성 완료: smp_hourly, smp_weighted_avg, smp_realtime_jeju")


def drop_all_tables() -> None:
    """SMP 테이블을 삭제합니다(주의해서 사용)."""
    engine = get_engine()
    Base.metadata.drop_all(engine)
    print("[DB] SMP 테이블 삭제 완료")


if __name__ == "__main__":
    print("SMP Database 초기화")
    init_db()
