"""
DB 접속 URL 해석 유틸리티

실행 환경(Docker / 호스트)에 따라 적절한 DB URL을 결정합니다.
"""

import os
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse, urlunparse


def running_in_docker() -> bool:
    """Docker 컨테이너 내부에서 실행 중인지 판별합니다."""
    return Path("/.dockerenv").exists() or os.getenv("RUNNING_IN_DOCKER") == "1"


def resolve_db_url(cli_db_url: Optional[str] = None) -> str:
    """
    실행 환경에 따라 DB URL을 결정합니다.

    우선순위:
      1) cli_db_url (CLI --db-url 등)
      2) DB_URL 환경변수
      3) (호스트 실행) PV_DATABASE_URL이 pv-db(도커 DNS)이면 LOCAL_DB_URL 우선
      4) (호스트 실행) 그래도 pv-db면 localhost:5435로 자동 치환 시도
      5) PV_DATABASE_URL
      6) LOCAL_DB_URL
    """
    if cli_db_url:
        return cli_db_url

    db_url = os.getenv("DB_URL")
    if db_url:
        return db_url

    pv_db_url = os.getenv("PV_DATABASE_URL") or ""
    local_db_url = os.getenv("LOCAL_DB_URL") or ""

    if not running_in_docker() and pv_db_url:
        try:
            u = urlparse(pv_db_url)
            if u.hostname == "pv-db" and local_db_url:
                return local_db_url
            if u.hostname == "pv-db":
                host_port = int(os.getenv("PV_DB_PORT_FORWARD", "5435"))
                if u.username and u.password:
                    netloc = f"{u.username}:{u.password}@localhost:{host_port}"
                elif u.username:
                    netloc = f"{u.username}@localhost:{host_port}"
                else:
                    netloc = f"localhost:{host_port}"
                return urlunparse(u._replace(netloc=netloc))
        except Exception:
            pass

    return pv_db_url or local_db_url


def redact_db_url(db_url: str) -> str:
    """DB URL에서 비밀번호를 마스킹하여 안전한 로깅용 문자열을 반환합니다."""
    try:
        u = urlparse(db_url)
        if not u.password:
            return db_url
        netloc = u.netloc.replace(f":{u.password}@", ":****@")
        return urlunparse(u._replace(netloc=netloc))
    except Exception:
        return "<unparseable db url>"
