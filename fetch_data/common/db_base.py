"""
공통 DB 엔진/세션 팩토리.

pv/wind/smp 등 모든 모듈이 동일한 DB(pv-data-postgres)에 접속하므로,
엔진/세션 생성 보일러플레이트를 여기 한 곳으로 통일한다.
URL은 resolve_db_url로 해소(컨테이너/호스트 자동 전환, 호스트 기본 localhost:5436=메인).
모델/Base는 각 소스 모듈(pv/wind/smp의 database.py)에 그대로 둔다.
"""
from typing import Optional

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker

from fetch_data.common.db_utils import resolve_db_url

_engine: Optional[Engine] = None


def get_engine(db_url: Optional[str] = None) -> Engine:
    """공유 SQLAlchemy 엔진(싱글톤). db_url 지정 시 그 URL로 별도 엔진 생성."""
    global _engine
    if db_url:
        return create_engine(db_url, echo=False)
    if _engine is None:
        _engine = create_engine(resolve_db_url(), echo=False)
    return _engine


def get_session(db_url: Optional[str] = None):
    """DB 세션을 반환한다."""
    return sessionmaker(bind=get_engine(db_url))()
