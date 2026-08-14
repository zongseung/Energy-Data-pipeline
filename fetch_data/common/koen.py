"""koenergy.kr 공용 헬퍼 — SSL 보충, CSV 응답 판별, 월 분할, 유연 CSV 리더.

koenergy.kr 는 TLS 핸드셰이크에서 중간 인증서를 누락한다. 여기의
get_koen_ssl_context() 가 AIA URL 에서 중간 인증서를 받아 보충한다.
남동 태양광(pv)·비태양광(gen) 수집기가 공유한다.
"""
from __future__ import annotations

import os
import ssl
from datetime import date, timedelta
from pathlib import Path
from typing import List, Optional, Tuple, Union
from urllib.request import urlopen

import pandas as pd

from fetch_data.common.date_utils import month_end, to_yyyymmdd
from fetch_data.common.logger import get_logger

logger = get_logger(__name__)

# koenergy.kr 는 TLS 핸드셰이크에서 leaf 인증서만 보내고 중간 인증서를 누락한다.
# (issuer: TuringSign RSA Secure CA 2 -> root: OISTE WISeKey Global Root GB CA)
# leaf 인증서 AIA 확장의 CA Issuers URL 에서 중간 인증서를 받아 보충하면
# TLS 검증을 끄지 않고도 정상 연결된다. 받은 인증서는 모듈 옆에 캐시한다.
KOEN_INTERMEDIATE_AIA_URL = "http://public.wisekey.com/crt/tsrsasecureca2.cer"
_INTERMEDIATE_CACHE = Path(__file__).resolve().parent / "_koen_intermediate.pem"
_SSL_CTX: Optional[ssl.SSLContext] = None


def _ensure_intermediate_pem() -> Optional[Path]:
    """서버가 누락한 중간 인증서를 AIA URL에서 받아 PEM으로 캐시."""
    if _INTERMEDIATE_CACHE.exists():
        return _INTERMEDIATE_CACHE
    url = os.getenv("KOEN_INTERMEDIATE_AIA_URL", KOEN_INTERMEDIATE_AIA_URL)
    try:
        der = urlopen(url, timeout=30).read()  # noqa: S310 (http AIA endpoint)
        pem = ssl.DER_cert_to_PEM_cert(der)
        _INTERMEDIATE_CACHE.write_text(pem)
        logger.info(f"[SSL] 중간 인증서 캐시 저장: {_INTERMEDIATE_CACHE.name}")
        return _INTERMEDIATE_CACHE
    except Exception as e:
        logger.warning(f"[SSL] 중간 인증서 다운로드 실패: {e}")
        return None


def get_koen_ssl_context() -> Union[ssl.SSLContext, bool]:
    """koenergy.kr 연결용 SSL 컨텍스트.

    기본: certifi 루트 + 보충한 중간 인증서로 정상 검증.
    KOEN_SSL_NO_VERIFY=1 이면 검증을 끈다(비권장, 명시적 탈출구).
    """
    global _SSL_CTX
    if os.getenv("KOEN_SSL_NO_VERIFY", "").strip().lower() in ("1", "true", "yes"):
        logger.warning("[SSL] KOEN_SSL_NO_VERIFY 활성화 -> TLS 검증 비활성화(비권장)")
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        return ctx

    if _SSL_CTX is None:
        try:
            import certifi

            ctx = ssl.create_default_context(cafile=certifi.where())
        except Exception:
            ctx = ssl.create_default_context()
        pem = _ensure_intermediate_pem()
        if pem:
            try:
                ctx.load_verify_locations(str(pem))
            except Exception as e:
                logger.warning(f"[SSL] 중간 인증서 로드 실패: {e}")
        _SSL_CTX = ctx
    return _SSL_CTX


def is_probably_csv(body: bytes, min_len: int = 1000) -> bool:
    head = body.lstrip()[:80].lower()
    if head.startswith(b"<!doctype") or head.startswith(b"<html") or b"<head" in head:
        return False
    if len(body) < min_len:
        return False
    if body[:2000].count(b",") < 5:
        return False
    return True


def split_by_month(start: date, end: date) -> List[Tuple[str, str]]:
    """[start, end] 를 월 단위 (YYYYMMDD, YYYYMMDD) 구간으로 분할."""
    if end < start:
        raise ValueError("종료일이 시작일보다 빠릅니다.")
    ranges: List[Tuple[str, str]] = []
    cur = start
    while cur <= end:
        me = month_end(cur)
        chunk_end = me if me <= end else end
        ranges.append((to_yyyymmdd(cur), to_yyyymmdd(chunk_end)))
        cur = chunk_end + timedelta(days=1)
    return ranges


def normalize_columns(cols: List[str]) -> List[str]:
    s = pd.Index(cols).astype(str)
    s = (
        s.str.replace("\n", " ", regex=False)
         .str.replace("\r", " ", regex=False)
         .str.replace("\t", " ", regex=False)
         .str.strip()
         .str.replace(r"\s+", " ", regex=True)
    )
    return list(s)


def read_csv_flexible(fp: Path) -> pd.DataFrame:
    """
    - cp949/euc-kr/utf-8-sig/utf-8 순서로 시도
    - 콤마 뒤 공백 자동 제거(skipinitialspace=True) -> ' 호기' 같은 문제 해결
    - python engine 사용(깨진 행이 섞인 경우가 있어 C엔진보다 안전)
    """
    encodings = ["cp949", "euc-kr", "utf-8-sig", "utf-8"]

    last_err: Optional[Exception] = None
    for enc in encodings:
        try:
            df = pd.read_csv(
                fp,
                encoding=enc,
                sep=",",
                engine="python",
                index_col=False,
                skipinitialspace=True,   # ★ 핵심: 콤마 뒤 공백 제거
            )
            df.columns = normalize_columns(df.columns.tolist())
            return df
        except Exception as e:
            last_err = e

    raise RuntimeError(f"CSV 읽기 실패: {fp} / last_err={last_err}")
