"""GET /ingestions/{id}/events - SSE stream via Redis pub/sub (no polling)"""
import asyncio
import json
import logging
import os
import uuid

import redis.asyncio as aioredis
from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import StreamingResponse
from sqlalchemy import text
from sqlalchemy.orm import Session

from api.dependencies import get_db

router = APIRouter()
logger = logging.getLogger(__name__)


def _get_redis_url() -> str:
    """Build Redis URL from env."""
    url = os.getenv("REDIS_URL")
    if url:
        return url
    host = os.getenv("REDIS_HOST", "localhost")
    port = os.getenv("REDIS_PORT", "6379")
    return f"redis://{host}:{port}/0"


async def event_generator(ingestion_id: str, redis_url: str):
    """
    Subscribe to Redis channel and yield SSE events.
    No polling - events pushed as DAG tasks publish to Redis.
    """
    channel = f"ingestion:{ingestion_id}:events"
    logger.info("Subscribing to Redis channel: %s", channel)

    r = aioredis.from_url(redis_url)
    pubsub = r.pubsub()

    try:
        await pubsub.subscribe(channel)

        while True:
            try:
                msg = await asyncio.wait_for(pubsub.get_message(ignore_subscribe_messages=True), timeout=30.0)
            except asyncio.TimeoutError:
                yield f": heartbeat\n\n"
                continue

            if msg and msg["type"] == "message":
                data = msg["data"]
                if isinstance(data, bytes):
                    data = data.decode()
                logger.info("Sending SSE message: %s", data[:100] if len(data) > 100 else data)
                yield f"data: {data}\n\n"

                try:
                    obj = json.loads(data)
                    if obj.get("status") in ("completed", "failed"):
                        logger.info("Terminal status received: %s", obj.get("status"))
                        break
                except json.JSONDecodeError:
                    pass
    except asyncio.CancelledError:
        logger.info("SSE client disconnected")

    finally:
        await pubsub.unsubscribe(channel)
        await pubsub.close()
        await r.close()


@router.get("/{ingestion_id}/events")
async def stream_ingestion_events(
    ingestion_id: uuid.UUID,
    db: Session = Depends(get_db),
):
    """
    SSE stream of ingestion status events via Redis pub/sub.
    No polling - events pushed as DAG tasks publish to Redis.
    """
    row = db.execute(
        text("SELECT id FROM control.ingestions WHERE id = :id"),
        {"id": ingestion_id},
    ).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Ingestion not found")

    redis_url = _get_redis_url()

    return StreamingResponse(
        event_generator(str(ingestion_id), redis_url),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )
