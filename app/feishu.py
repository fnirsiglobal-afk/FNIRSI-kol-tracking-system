import httpx
import os
from datetime import datetime

FEISHU_APP_TOKEN = os.getenv("FEISHU_APP_TOKEN")
FEISHU_TABLE_ID = os.getenv("FEISHU_TABLE_ID")
FEISHU_APP_TOKEN = os.getenv("FEISHU_APP_TOKEN")
FEISHU_APP_SECRET = os.getenv("FEISHU_APP_SECRET")

async def get_tenant_access_token():
    url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal/"
    async with httpx.AsyncClient() as client:
        res = await client.post(url, json={
            "app_id": FEISHU_APP_TOKEN,
            "app_secret": FEISHU_APP_SECRET
        })
        return res.json()["tenant_access_token"]


async def update_feishu(event):
    token = await get_tenant_access_token()

    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{FEISHU_APP_TOKEN}/tables/{FEISHU_TABLE_ID}/records"

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    payload = {
        "fields": {
            "物流单号": event["tracking_number"],
            "运输商": event["carrier"],
            "物流子状态": event["sub_status"],
            "子状态描述": event["sub_status_desc"],
            "最新事件时间": event["event_time"],
            "更新时间": datetime.utcnow().isoformat()
        }
    }

    async with httpx.AsyncClient() as client:
        await client.post(url, json=payload, headers=headers)
