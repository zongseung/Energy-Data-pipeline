"""
Prefect Slack 알림 공통 Task

모든 Prefect Flow에서 공유하는 Slack 알림 task.
"""

from prefect import task

from pipeline.fetch_data.common.notify import send_slack_message, send_slack_rich_message


@task(name="Slack 성공 알림", retries=0)
def notify_slack_success(flow_name: str, details: str):
    send_slack_message(f"[{flow_name} 완료]\n{details}")


@task(name="Slack 실패 알림", retries=0)
def notify_slack_failure(flow_name: str, error_msg: str):
    send_slack_message(f"[{flow_name} 실패]\n- 에러: {error_msg}")


@task(name="Slack 리치 알림", retries=0)
def notify_slack_rich(title: str, status: str, details: dict):
    send_slack_rich_message(title, status, details)


@task(name="적재 행수 검증", retries=0)
def assert_rows_loaded(flow_name: str, rows, minimum: int = 1) -> int:
    """0행(또는 기대 이하) 적재를 성공으로 넘기지 않는다.

    수집기가 원천 오류로 아무것도 못 받아도 flow 는 COMPLETED + Slack 성공으로
    끝난다. 남동 풍력이 그렇게 3개월(2026-05~08) 조용히 멈췄고, KOEN 화력도
    2026-07 에 14호기가 같은 방식으로 빠졌다. 그래서 적재 결과를 flow 성공의
    조건으로 만든다.

    rows 는 행수(int) 또는 산출물 목록(list) 둘 다 받는다.

    ※ 0행이 정상인 경로(제주 실시간 SMP·시간별 유가)에는 붙이지 않는다 —
      그쪽은 이미 경고 문구를 따로 보낸다.
    """
    n = len(rows) if hasattr(rows, "__len__") else rows
    if n is None or n < minimum:
        raise ValueError(
            f"{flow_name}: 적재 {n}행 < 최소 {minimum}행 — 수집 실패로 처리한다"
        )
    return n
