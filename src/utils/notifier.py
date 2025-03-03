import os
import requests
from prefect import task, flow, context


from dotenv import load_dotenv
load_dotenv()

# LINE TOKEN 跟 發送群組設定
LINE_ACCESS_TOKEN = os.getenv("LINE_ACCESS_TOKEN", "").strip().replace("\"", "")
GROUP_ID = os.getenv("GROUP_ID", "").strip().replace("\"", "")
LINE_NOTIFY_URL = "https://api.line.me/v2/bot/message/push"

# 發送 LINE 訊息函式

def send_line_notification(task_name: str, error_msg: str):
    """當 Task 失敗時發送 LINE 訊息"""
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {LINE_ACCESS_TOKEN}"
    }
    payload = {
        "to": GROUP_ID,
        "messages": [{"type": "text", "text": f"⚠️ Task 執行失敗：{task_name}\n錯誤訊息：{error_msg}"}]
    }
    response = requests.post(LINE_NOTIFY_URL, json=payload, headers=headers)
    if response.status_code != 200:
        print(f"❌ LINE 通知失敗: {response.status_code}, {response.json()}")


# Prefect 任務（失敗）
@task(retries=0)
def failing_task():
    raise ValueError("這是一個模擬錯誤！")

# Prefect Flow（失敗時觸發 LINE 通知）
@flow(name="Error Notification Flow")
def error_notification_flow():
    try:
        failing_task()  # 這裡的 failing_task 會拋出錯誤
    except Exception as e:
        send_line_notification(failing_task.name, str(e))

# 執行 Flow 測試
if __name__ == "__main__":
    error_notification_flow()