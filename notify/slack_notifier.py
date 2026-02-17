"""
Slack 알림 유틸리티

- send_slack_message: 단순 텍스트 메시지 전송
- send_slack_rich_message: Block Kit 리치 메시지 전송
"""

import os
from datetime import datetime

import requests
from dotenv import load_dotenv

load_dotenv()


def send_slack_message(text: str, webhook_url: str | None = None):
    """
    Slack Incoming Webhook으로 단순 텍스트 메시지 전송
    """
    if webhook_url is None:
        webhook_url = os.getenv("SLACK_WEBHOOK_URL")

    if not webhook_url:
        print("SLACK_WEBHOOK_URL이 설정되어 있지 않습니다. Slack 전송 스킵.")
        return

    try:
        resp = requests.post(webhook_url, json={"text": text}, timeout=5)
        if resp.status_code != 200:
            print(f"Slack 전송 실패: {resp.status_code}, {resp.text}")
    except Exception as e:
        print(f"Slack 전송 중 예외 발생: {e}")


def send_slack_rich_message(
    title: str,
    status: str,
    details: dict,
    webhook_url: str | None = None,
):
    """
    Slack Block Kit 형식의 리치 메시지 전송

    Args:
        title: 메시지 제목
        status: "success", "warning", "error", "info"
        details: 상세 정보 dict (key-value 쌍)
        webhook_url: Webhook URL (미지정 시 환경변수 사용)
    """
    if webhook_url is None:
        webhook_url = os.getenv("SLACK_WEBHOOK_URL")

    if not webhook_url:
        print("SLACK_WEBHOOK_URL이 설정되어 있지 않습니다.")
        return

    emoji_map = {
        "success": ":white_check_mark:",
        "warning": ":warning:",
        "error": ":x:",
        "info": ":information_source:",
    }
    emoji = emoji_map.get(status, ":bell:")

    detail_lines = [f"• *{k}*: {v}" for k, v in details.items()]
    detail_text = "\n".join(detail_lines)

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    payload = {
        "blocks": [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": f"{emoji} {title}",
                    "emoji": True,
                },
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": detail_text,
                },
            },
            {
                "type": "context",
                "elements": [
                    {
                        "type": "mrkdwn",
                        "text": f":clock1: {timestamp} KST",
                    }
                ],
            },
            {"type": "divider"},
        ]
    }

    try:
        resp = requests.post(webhook_url, json=payload, timeout=5)
        if resp.status_code != 200:
            print(f"Slack 전송 실패: {resp.status_code}")
    except Exception as e:
        print(f"Slack 전송 예외: {e}")
