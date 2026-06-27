"""
KOL Package Tracker
17TRACK v2.4 Webhook + Polling → Feishu Bitable
"""

import os
import hmac
import hashlib
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, HTTPException, BackgroundTasks
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from .tracker import process_webhook_payload
from .polling import run_polling
from .feishu import search_records_needing_poll

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

TRACK17_KEY = os.environ["TRACK17_KEY"]          # 17TRACK API Key
WEBHOOK_SECRET = os.environ.get("WEBHOOK_SECRET", "")  # 可选签名验证

scheduler = AsyncIOScheduler()


@asynccontextmanager
async def lifespan(app: FastAPI):
    # 每6小时轮询一次，兜底 Webhook 漏推
    scheduler.add_job(polling_job, "interval", hours=6, id="polling")
    scheduler.start()
    log.info("Scheduler started")
    yield
    scheduler.shutdown()


app = FastAPI(title="KOL Tracker", lifespan=lifespan)


async def polling_job():
    """定时轮询：查找超过 48h 未收到 Webhook 推送的在途包裹"""
    log.info("[Polling] Job started")
    try:
        stale_records = await search_records_needing_poll()
        if stale_records:
            numbers = [r["tracking_number"] for r in stale_records]
            log.info(f"[Polling] Found {len(numbers)} stale records")
            await run_polling(numbers, stale_records)
        else:
            log.info("[Polling] All records up-to-date")
    except Exception as e:
        log.error(f"[Polling] Job failed: {e}")


@app.get("/")
async def health():
    return {"status": "ok", "service": "KOL Tracker"}


@app.post("/webhook/17track")
async def receive_webhook(request: Request, background_tasks: BackgroundTasks):
    """
    接收 17TRACK Webhook 推送
    立即返回 200，后台处理写入飞书（避免超时导致 17TRACK 重推）
    """
    body = await request.body()

    # 签名验证（如果配置了 WEBHOOK_SECRET）
    if WEBHOOK_SECRET:
        sig = request.headers.get("sign", "")
        expected = hmac.new(
            WEBHOOK_SECRET.encode(), body, hashlib.sha256
        ).hexdigest()
        if not hmac.compare_digest(sig, expected):
            raise HTTPException(status_code=401, detail="Invalid signature")

    try:
        payload = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON")

    # 立即返回 200，后台写入飞书
    background_tasks.add_task(process_webhook_payload, payload)
    return {"code": 0}


@app.post("/admin/poll-now")
async def manual_poll(background_tasks: BackgroundTasks):
    """手动触发轮询（调试用）"""
    background_tasks.add_task(polling_job)
    return {"status": "polling started"}


@app.get("/admin/health-detail")
async def health_detail():
    """详细健康检查"""
    jobs = [{"id": j.id, "next_run": str(j.next_run_time)} for j in scheduler.get_jobs()]
    return {"status": "ok", "scheduler_jobs": jobs}
