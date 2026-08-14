"""fetch_data/common/koen.py — KOEN 사이트 공용 헬퍼 테스트 (네트워크 불필요)."""
import ssl
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


def test_is_probably_csv_accepts_csv_bytes():
    from fetch_data.common.koen import is_probably_csv
    # gen 기본 임계값(min_len=1000)을 넘기도록 실제 응답 크기에 맞춰 반복.
    body = ("날짜,발전구분,1시\n" + "20260101,삼천포,1.0\n" * 80).encode("cp949")
    assert is_probably_csv(body)


def test_is_probably_csv_rejects_html():
    from fetch_data.common.koen import is_probably_csv
    assert not is_probably_csv(b"<!DOCTYPE html><html><body>error</body></html>")


def test_is_probably_csv_min_len_threshold():
    """pv(2000)·gen(1000) 임계값 차이가 min_len 파라미터로 보존돼야 한다."""
    from fetch_data.common.koen import is_probably_csv
    body_1500 = ("컬럼A,컬럼B\n" + "20260101,1.0\n" * 120).encode("cp949")
    assert 1000 < len(body_1500) < 2000
    assert is_probably_csv(body_1500)                 # gen 기본값 통과
    assert not is_probably_csv(body_1500, min_len=2000)  # pv 임계값에선 거부


def test_ssl_context_no_verify_env(monkeypatch):
    from fetch_data.common import koen
    monkeypatch.setenv("KOEN_SSL_NO_VERIFY", "1")
    ctx = koen.get_koen_ssl_context()
    assert isinstance(ctx, ssl.SSLContext)
    assert ctx.verify_mode == ssl.CERT_NONE


def test_pv_collector_imports_from_common():
    """pv 수집기가 gen 이 아니라 common 에서 SSL 컨텍스트를 가져와야 한다."""
    src = (PROJECT_ROOT / "fetch_data/pv/namdong_collect.py").read_text(encoding="utf-8")
    assert "from fetch_data.gen.namdong_collect import" not in src
    assert "from fetch_data.common.koen import" in src
