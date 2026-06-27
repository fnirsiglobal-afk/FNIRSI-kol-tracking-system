"""
Polling 引擎（兜底机制）
当 Webhook 漏推时，主动调用 17TRACK /gettrackinfo 查询状态
"""

import os
import logging

import httpx

from .feishu import update_record, to_ms, now_ms
from .tracker import process_single_tracking

log = logging.getLogger(__name__)

TRACK17_KEY  = os.environ["TRACK17_KEY"]
TRACK17_BASE = "https://api.17track.net/track/v2.2"


async def run_polling(tracking_numbers: list[str], records: list[dict]):
    """
    批量查询 17TRACK，最多 40 条每次
    records: [{"record_id": ..., "tracking_number": ...}]
    """
    # 构建 number → record_id 映射
    num_to_rec = {r["tracking_number"]: r["record_id"] for r in records}

    # 分批，每批最多 40 条（API 限制）
    batch_size = 40
    for i in range(0, len(tracking_numbers), batch_size):
        batch = tracking_numbers[i: i + batch_size]
        await _poll_batch(batch, num_to_rec)


async def _poll_batch(numbers: list[str], num_to_rec: dict):
    """单批次轮询"""
    payload = [{"number": n} for n in numbers]

    try:
        async with httpx.AsyncClient(timeout=20) as client:
            resp = await client.post(
                f"{TRACK17_BASE}/gettrackinfo",
                headers={
                    "17token": TRACK17_KEY,
                    "Content-Type": "application/json",
                },
                json=payload,
            )
            resp.raise_for_status()
            data = resp.json()
    except Exception as e:
        log.error(f"[Polling] 17TRACK API error: {e}")
        return

    if data.get("code") != 0:
        log.error(f"[Polling] API returned error: {data}")
        return

    accepted = data.get("data", {}).get("accepted", [])
    log.info(f"[Polling] Got {len(accepted)} results")

    for item in accepted:
        number = item.get("number", "")
        # 注入 record_id 到 tag，让 process_single_tracking 直接定位
        item["tag"] = num_to_rec.get(number, "")
        try:
            await process_single_tracking(item)
        except Exception as e:
            log.error(f"[Polling] Error processing {number}: {e}")


async def register_tracking_number(
    tracking_number: str,
    record_id: str,
    carrier: int = 0,
    kol_email: str = "",
    remark: str = "",
):
    """
    注册快递单号到 17TRACK
    - tag 存飞书 record_id，Webhook 回调时直接定位记录
    - email 填 KOL 邮箱，17TRACK 会直接发状态通知给 KOL
    """
    body = {
        "number": tracking_number,
        "tag": record_id,        # 关键：关联飞书行
        "email": kol_email,      # KOL 收件通知
        "remark": remark,
        "auto_detection": True,
    }
    if carrier:
        body["carrier"] = carrier

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.post(
                f"{TRACK17_BASE}/register",
                headers={
                    "17token": TRACK17_KEY,
                    "Content-Type": "application/json",
                },
                json=[body],
            )
            resp.raise_for_status()
            data = resp.json()
    except Exception as e:
        log.error(f"register {tracking_number} failed: {e}")
        return False

    accepted = data.get("data", {}).get("accepted", [])
    rejected = data.get("data", {}).get("rejected", [])

    if accepted:
        log.info(f"Registered {tracking_number} → carrier={accepted[0].get('carrier')}")
        return True
    else:
        log.error(f"Register rejected {tracking_number}: {rejected}")
        return False
