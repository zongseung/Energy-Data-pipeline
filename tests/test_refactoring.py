"""
리팩토링 검증 테스트

DB / API / 환경변수 없이 실행 가능한 순수 함수 위주 테스트.
리팩토링 전후로 동작이 동일한지 확인하는 용도.

NOTE: prefect_flows, daily_pv_automation 등 모듈 레벨 부수효과가 있는
모듈은 직접 import하지 않고, 순수 함수만 별도로 로드하여 테스트한다.
"""

import importlib.util
import re
import sys
from pathlib import Path
from unittest.mock import patch, MagicMock

import numpy as np
import pandas as pd
import pytest

# 프로젝트 루트를 sys.path에 추가
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


# ============================================================
# Helper: 부수효과 없이 특정 함수만 소스에서 추출
# ============================================================
def _load_function_from_source(filepath: str, func_name: str):
    """모듈 전체를 import하지 않고, 소스 코드에서 순수 함수를 추출·실행한다."""
    source = (PROJECT_ROOT / filepath).read_text(encoding="utf-8")
    # 함수 정의 블록 추출
    pattern = rf"(^def {func_name}\(.*?\n(?:(?:    .+|)\n)*)"
    match = re.search(pattern, source, re.MULTILINE)
    if not match:
        raise RuntimeError(f"{func_name}을 {filepath}에서 찾을 수 없습니다")
    func_source = match.group(1)
    ns = {}
    exec(func_source, ns)
    return ns[func_name]


# normalize_date_format: prefect import 없이 소스에서 직접 로드
normalize_date_format = _load_function_from_source(
    "prefect_flows/prefect_pipeline.py", "normalize_date_format"
)


# ============================================================
# 1. normalize_date_format
# ============================================================
class TestNormalizeDateFormat:
    def test_yyyymmdd_passthrough(self):
        assert normalize_date_format("20260101") == "20260101"

    def test_dash_separated(self):
        assert normalize_date_format("2026-01-01") == "20260101"

    def test_slash_separated(self):
        assert normalize_date_format("2026/01/01") == "20260101"

    def test_invalid_length_raises(self):
        with pytest.raises(ValueError, match="날짜 형식"):
            normalize_date_format("202601")

    def test_non_digit_raises(self):
        with pytest.raises(ValueError, match="날짜 형식"):
            normalize_date_format("abcdefgh")

    def test_empty_string_raises(self):
        with pytest.raises(ValueError, match="날짜 형식"):
            normalize_date_format("")


# ============================================================
# 5. send_slack_message (notify/slack_notifier.py)
# ============================================================
from notify.slack_notifier import send_slack_message


class TestSendSlackMessage:
    @patch("notify.slack_notifier.requests.post")
    def test_sends_post_request(self, mock_post):
        mock_post.return_value = MagicMock(status_code=200)
        send_slack_message("테스트 메시지", webhook_url="https://hooks.slack.com/test")
        mock_post.assert_called_once()
        call_args = mock_post.call_args
        assert call_args[0][0] == "https://hooks.slack.com/test"

    @patch("notify.slack_notifier.requests.post")
    def test_skips_when_no_url(self, mock_post, capsys):
        with patch.dict("os.environ", {}, clear=True):
            send_slack_message("테스트", webhook_url=None)
        mock_post.assert_not_called()
        captured = capsys.readouterr()
        assert "스킵" in captured.out or "SLACK_WEBHOOK_URL" in captured.out

    @patch("notify.slack_notifier.requests.post")
    def test_handles_http_error(self, mock_post, capsys):
        mock_post.return_value = MagicMock(status_code=500, text="Internal Error")
        send_slack_message("테스트", webhook_url="https://hooks.slack.com/test")
        captured = capsys.readouterr()
        assert "실패" in captured.out

    @patch("notify.slack_notifier.requests.post", side_effect=ConnectionError("timeout"))
    def test_handles_exception(self, mock_post, capsys):
        send_slack_message("테스트", webhook_url="https://hooks.slack.com/test")
        captured = capsys.readouterr()
        assert "예외" in captured.out
