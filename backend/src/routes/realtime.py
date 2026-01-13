import os
import uuid
import json
import logging
import time
from typing import Optional, List, Dict, Any
from duckdb import DuckDBPyConnection
from fastapi import APIRouter, Depends, FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.responses import JSONResponse
from aiokafka import AIOKafkaConsumer

from src.dependencies import get_db_connection

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("RealTimeAPI")

# Simple in-memory cache for realtime data
_cache: Dict[str, Dict[str, Any]] = {}
CACHE_TTL_SECONDS = 3  # Cache data for 3 seconds to avoid overwhelming MinIO


KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")


# Gold Table Paths (matching batch naming convention)
PATH_GOLD_ANOMALY = "s3://lakehouse/gold/processed_data"

PATH_GOLD_DEVICES = "s3://lakehouse/gold/device_stats"

PATH_GOLD_TRENDING = "s3://lakehouse/gold/trending_pages"

PATH_GOLD_SESSIONS = "s3://lakehouse/gold/session_stats"

router = APIRouter(prefix="/realtime", tags=["realtime"])


def get_cached_data(cache_key: str) -> Optional[List[Dict[str, Any]]]:
    """Return cached data if still valid, otherwise None."""
    if cache_key in _cache:
        cached = _cache[cache_key]
        if time.time() - cached["timestamp"] < CACHE_TTL_SECONDS:
            logger.info(f"Cache hit for {cache_key}")
            return cached["data"]
    return None


def set_cached_data(cache_key: str, data: List[Dict[str, Any]]) -> None:
    """Store data in cache with current timestamp."""
    _cache[cache_key] = {"data": data, "timestamp": time.time()}


def safe_query(conn: DuckDBPyConnection, query: str, cache_key: str) -> List[Dict[str, Any]]:
    """Execute a query with caching and error handling."""
    # Check cache first
    cached = get_cached_data(cache_key)
    if cached is not None:
        return cached
    
    try:
        df = conn.execute(query).fetchdf()
        result = df.to_dict(orient="records")
        set_cached_data(cache_key, result)
        return result
    except Exception as e:
        logger.error(f"Query error for {cache_key}: {e}")
        # Return cached data even if expired, as fallback
        if cache_key in _cache:
            logger.info(f"Returning stale cache for {cache_key}")
            return _cache[cache_key]["data"]
        raise HTTPException(status_code=503, detail=f"Data temporarily unavailable: {str(e)}")


async def consume_stream(websocket: WebSocket, topic: str):
    """
    Generic handler that connects a WebSocket to a specific Kafka topic.
    """
    await websocket.accept()

    consumer_group = f"dashboard-{uuid.uuid4()}"

    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=consumer_group,
        auto_offset_reset="latest",  # Only listen for new messages starting NOW
    )

    try:
        await consumer.start()
        logger.info(
            f"Client connected to topic '{topic}' with group '{consumer_group}'"
        )

        async for msg in consumer:
            try:
                payload = json.loads(msg.value.decode("utf-8"))

                await websocket.send_json(payload)
            except json.JSONDecodeError:
                logger.error(f"Failed to decode message from {topic}")

    except WebSocketDisconnect:
        logger.info("Client disconnected")
    except Exception as e:
        logger.error(f"Error in stream {topic}: {e}")
        await websocket.close()
    finally:
        await consumer.stop()


@router.websocket("/ws/anomalies")
async def ws_anomalies(websocket: WebSocket):
    await consume_stream(websocket, "stats_anomalies")


@router.websocket("/ws/devices")
async def ws_devices(websocket: WebSocket):
    await consume_stream(websocket, "stats_devices")


@router.websocket("/ws/trending")
async def ws_trending(websocket: WebSocket):
    await consume_stream(websocket, "stats_trending")


@router.websocket("/ws/sessions")
async def ws_sessions(websocket: WebSocket):
    await consume_stream(websocket, "stats_sessions")


@router.get("/anomalies")
def get_anomalies(conn: DuckDBPyConnection = Depends(get_db_connection)):
    query = f"SELECT * FROM delta_scan('{PATH_GOLD_ANOMALY}')"
    return safe_query(conn, query, "anomalies")


@router.get("/devices")
def get_devices(conn: DuckDBPyConnection = Depends(get_db_connection)):
    query = f"SELECT * FROM delta_scan('{PATH_GOLD_DEVICES}')"
    return safe_query(conn, query, "devices")


@router.get("/trending")
def get_trending(conn: DuckDBPyConnection = Depends(get_db_connection)):
    query = f"SELECT * FROM delta_scan('{PATH_GOLD_TRENDING}')"
    return safe_query(conn, query, "trending")


@router.get("/sessions")
def get_sessions(conn: DuckDBPyConnection = Depends(get_db_connection)):
    query = f"SELECT * FROM delta_scan('{PATH_GOLD_SESSIONS}')"
    return safe_query(conn, query, "sessions")
