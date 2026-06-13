"""공통 유틸리티 함수."""
import re
from datetime import datetime, timezone, timedelta

KST = timezone(timedelta(hours=9))


def now_kst() -> datetime:
    """KST 기준 현재 시각을 반환합니다."""
    return datetime.now(KST)


def today_kst():
    """KST 기준 오늘 날짜를 반환합니다."""
    return now_kst().date()


def parse_hour_column(col: str) -> int:
    """
    시간 컬럼명에서 0-based 시간 인덱스 추출.
    예: qhorgen01 -> 0, qhorgen24 -> 23, H01 -> 0
    """
    match = re.search(r'\d+', col)
    return int(match.group()) - 1 if match else -1


def decode_bytes(body: bytes) -> str:
    """바이트 응답을 utf-8 → cp949 → euc-kr 순으로 안전하게 디코딩 (한글 깨짐 방지)."""
    for enc in ("utf-8", "cp949", "euc-kr"):
        try:
            return body.decode(enc)
        except UnicodeDecodeError:
            continue
    return body.decode("cp949", errors="replace")
