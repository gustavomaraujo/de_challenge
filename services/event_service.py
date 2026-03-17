"""
Redis publisher utility for ingestion status events.
Used by Airflow DAG tasks and available for API logging.
Publishes to channel: ingestion:{ingestion_id}:events
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from typing import Any

import redis

logger = logging.getLogger(__name__)


def _get_redis_url() -> str:
    """Build Redis URL from env vars (REDIS_HOST, REDIS_PORT) or REDIS_URL."""
    redis_url = os.getenv("REDIS_URL")
    if redis_url:
        return redis_url
    host = os.getenv("REDIS_HOST", "localhost")
    port = os.getenv("REDIS_PORT", "6379")
    return f"redis://{host}:{port}/0"


def get_redis_client() -> redis.Redis:
    """Create Redis client with optional retry on connection."""
    url = _get_redis_url()
    try:
        client = redis.from_url(url, decode_responses=False)
        client.ping()
        return client
    except redis.ConnectionError as e:
        logger.warning("Redis connection failed, retrying: %s", e)
        raise


def publish_event(ingestion_id: str, payload: dict[str, Any]) -> None:
    """
    Publish an event to the ingestion Redis channel.
    Channel: ingestion:{ingestion_id}:events
    Payload is serialized as JSON.
    """
    channel = f"ingestion:{ingestion_id}:events"
    payload.setdefault("ingestion_id", ingestion_id)
    payload.setdefault("timestamp", datetime.now(timezone.utc).isoformat())
    data = json.dumps(payload)

    for attempt in range(3):
        try:
            client = get_redis_client()
            client.publish(channel, data)
            client.close()
            logger.info("Published event to %s: status=%s step=%s", channel, payload.get("status"), payload.get("step"))
            return
        except redis.ConnectionError as e:
            logger.warning("Redis publish attempt %d failed: %s", attempt + 1, e)
            if attempt == 2:
                raise
            import time
            time.sleep(0.5 * (attempt + 1))
