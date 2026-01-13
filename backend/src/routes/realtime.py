import os
import uuid
import json
import logging
from duckdb import DuckDBPyConnection
from fastapi import APIRouter, Depends, FastAPI, WebSocket, WebSocketDisconnect
from aiokafka import AIOKafkaConsumer

from src.dependencies import get_db_connection

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("RealTimeAPI")


KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")


# Gold Table Paths (matching batch naming convention)
PATH_GOLD_ANOMALY = "s3://lakehouse/gold/processed_data"

PATH_GOLD_DEVICES = "s3://lakehouse/gold/device_stats"

PATH_GOLD_TRENDING = "s3://lakehouse/gold/trending_pages"

PATH_GOLD_SESSIONS = "s3://lakehouse/gold/session_stats"

router = APIRouter(prefix="/realtime", tags=["realtime"])


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
    df = conn.execute(query).fetchdf()
    return df.to_dict(orient="records")


@router.get("/devices")
def get_devices(conn: DuckDBPyConnection = Depends(get_db_connection)):

    query = f"SELECT * FROM delta_scan('{PATH_GOLD_DEVICES}')"
    df = conn.execute(query).fetchdf()
    return df.to_dict(orient="records")


@router.get("/trending")
def get_trending(conn: DuckDBPyConnection = Depends(get_db_connection)):
    query = f"SELECT * FROM delta_scan('{PATH_GOLD_TRENDING}')"
    df = conn.execute(query).fetchdf()
    return df.to_dict(orient="records")


@router.get("/sessions")
def get_sessions(conn: DuckDBPyConnection = Depends(get_db_connection)):
    query = f"SELECT * FROM delta_scan('{PATH_GOLD_SESSIONS}')"
    df = conn.execute(query).fetchdf()
    return df.to_dict(orient="records")