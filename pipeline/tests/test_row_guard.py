"""적재 행수 검증(assert_rows_loaded) 회귀 테스트.

수집기가 0행을 적재해도 flow 가 성공으로 끝나던 침묵 실패를 막는 가드다.
실제로 남동 풍력이 2026-05~08 그렇게 멈췄고 아무 알림도 나가지 않았다.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from pipeline.prefect_flows.notify_tasks import assert_rows_loaded  # noqa: E402

guard = assert_rows_loaded.fn  # Prefect task 래핑을 벗긴 순수 함수


def test_행수가_있으면_통과하고_그대로_반환():
    assert guard("X", 744) == 744


def test_목록도_길이로_판정():
    assert guard("X", ["a.csv", "b.csv"]) == 2


@pytest.mark.parametrize("rows", [0, [], None])
def test_0행이면_실패시킨다(rows):
    with pytest.raises(ValueError, match="수집 실패로 처리"):
        guard("X", rows)


def test_최소치를_올리면_부족분도_실패():
    assert guard("X", 10, minimum=10) == 10
    with pytest.raises(ValueError):
        guard("X", 9, minimum=10)
