"""
환경변수 중앙 관리 모듈.
모든 파일에서 os.getenv() 직접 호출 대신 이 모듈을 사용합니다.
"""
import os
from pathlib import Path
from dotenv import load_dotenv

# 프로젝트 루트에서 .env 로딩 (중복 호출 안전)
load_dotenv(Path(__file__).parents[2] / ".env", override=False)


def get_nambu_api_key() -> str:
    return os.getenv("NAMBU_API_KEY", "")


def get_service_key() -> str:
    return os.getenv("SERVICE_KEY") or os.getenv("NAMDONG_WIND_KEY", "")
