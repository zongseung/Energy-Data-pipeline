"""
환경변수 중앙 관리 모듈.
모든 파일에서 os.getenv() 직접 호출 대신 이 모듈을 사용합니다.
"""
import os
from pathlib import Path
from dotenv import load_dotenv

# 프로젝트 루트에서 .env 로딩 (중복 호출 안전)
load_dotenv(Path(__file__).parents[2] / ".env", override=False)


def get_db_url() -> str:
    """실행 환경(Docker/로컬)에 관계없이 올바른 PostgreSQL DB URL 반환."""
    for key in ("PV_DATABASE_URL", "DB_URL", "LOCAL_DB_URL"):
        url = os.getenv(key)
        if url:
            return url
    # Docker 내부 기본값
    if Path("/.dockerenv").exists():
        return "postgresql+psycopg2://pv:pv@pv-db:5432/pv"
    # 로컬 기본값
    return "postgresql+psycopg2://pv:pv@localhost:5436/pv"


def get_prefect_api_url() -> str:
    return os.getenv("PREFECT_API_URL", "http://pv-prefect-server:4200/api")


def get_nambu_api_key() -> str:
    return os.getenv("NAMBU_API_KEY", "")


def get_slack_webhook_url() -> str:
    return os.getenv("SLACK_WEBHOOK_URL", "")


def get_service_key() -> str:
    return os.getenv("SERVICE_KEY", "")
