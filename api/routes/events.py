"""GET /ingestions/{id}/events - SSE stream via Redis pub/sub (no polling)"""
import asyncio
import json
import uuid

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import StreamingResponse
from sqlalchemy import text
from sqlalchemy.orm import Session
import redis.asyncio as aioredis

from api.dependencies import get_db

router = APIRouter()


async def event_generator(ingestion_id: str, redis_url: str):
    """Subscribe to Redis channel and yield SSE events."""
    r = aioredis.from_url(redis_url)
    pubsub = r.pubsub()
    channel = f"ingestion:{ingestion_id}:events"
    await pubsub.subscribe(channel)

    try:
        while True:
            msg = await pubsub.get_message(ignore_subscribe_messages=True)
            if msg and msg["type"] == "message":
                data = msg["data"]
                if isinstance(data, bytes):
                    data = data.decode()
                yield f"data: {data}\n\n"
                try:
                    obj = json.loads(data)
                    if obj.get("status") in ("completed", "failed"):
                        break
                except json.JSONDecodeError:
                    pass
            await asyncio.sleep(0.1)
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

    import os
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")

    return StreamingResponse(
        event_generator(str(ingestion_id), redis_url),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )
