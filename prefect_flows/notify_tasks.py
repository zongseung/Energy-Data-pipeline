"""
Prefect Slack 알림 공통 Task

모든 Prefect Flow에서 공유하는 Slack 알림 task.
"""

from prefect import task

from notify.slack_notifier import send_slack_message, send_slack_rich_message


@task(name="Slack 성공 알림", retries=0)
def notify_slack_success(flow_name: str, details: str):
    send_slack_message(f"[{flow_name} 완료]\n{details}")


@task(name="Slack 실패 알림", retries=0)
def notify_slack_failure(flow_name: str, error_msg: str):
    send_slack_message(f"[{flow_name} 실패]\n- 에러: {error_msg}")


@task(name="Slack 리치 알림", retries=0)
def notify_slack_rich(title: str, status: str, details: dict):
    send_slack_rich_message(title, status, details)
