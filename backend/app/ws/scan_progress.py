import asyncio
import json
import logging

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from app.services.scanner import scanner

logger = logging.getLogger(__name__)
router = APIRouter()

_KEEPALIVE_INTERVAL = 25.0  # seconds between pings when queue is idle


@router.websocket("/ws/scan")
async def scan_websocket(websocket: WebSocket):
    await websocket.accept()
    queue = scanner.subscribe()
    logger.info("Scan WebSocket connected")
    try:
        while True:
            try:
                data = await asyncio.wait_for(queue.get(), timeout=_KEEPALIVE_INTERVAL)
                await websocket.send_text(json.dumps(data))
            except asyncio.TimeoutError:
                await websocket.send_json({"type": "ping"})
    except WebSocketDisconnect:
        logger.info("Scan WebSocket disconnected by client")
    except asyncio.CancelledError:
        pass
    except Exception as e:
        logger.error("Scan WebSocket error: %s", e, exc_info=True)
    finally:
        scanner.unsubscribe(queue)


@router.websocket("/ws/scan/detail")
async def scan_detail_websocket(websocket: WebSocket):
    await websocket.accept()
    queue = scanner.subscribe_detail()
    logger.info("Detail WebSocket connected")
    try:
        while True:
            try:
                data = await asyncio.wait_for(queue.get(), timeout=_KEEPALIVE_INTERVAL)
                await websocket.send_text(json.dumps(data))
            except asyncio.TimeoutError:
                await websocket.send_json({"type": "ping"})
    except WebSocketDisconnect:
        logger.info("Detail WebSocket disconnected by client")
    except asyncio.CancelledError:
        pass
    except Exception as e:
        logger.error("Detail WebSocket error: %s", e, exc_info=True)
    finally:
        scanner.unsubscribe_detail(queue)
