import os
import json
import requests
from dotenv import load_dotenv
import os

load_dotenv()  # 환경변수 불러오기

def send_slack_message(text: str, webhook_url: str | None = None):
    """
    Slack Incoming Webhook으로 단순 텍스트 메시지 전송
    """
    if webhook_url is None:
        webhook_url = os.getenv("SLACK_WEBHOOK_URL")

    if not webhook_url:
        print("SLACK_WEBHOOK_URL이 설정되어 있지 않습니다. Slack 전송 스킵.")
        return

    payload = {"text": text}

    try:
        resp = requests.post(
            webhook_url,
            data=json.dumps(payload),
            headers={"Content-Type": "application/json"},
            timeout=5,
        )
        if resp.status_code != 200:
            print(f"Slack 전송 실패: {resp.status_code}, {resp.text}")
    except Exception as e:
        print(f"Slack 전송 중 예외 발생: {e}")
