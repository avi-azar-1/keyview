import asyncio
import json

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from app.services.scanner import scanner

router = APIRouter()


@router.websocket("/ws/scan")
async def scan_websocket(websocket: WebSocket):
    await websocket.accept()
    queue = scanner.subscribe()
    try:
        while True:
            data = await queue.get()
            await websocket.send_text(json.dumps(data))
    except WebSocketDisconnect:
        pass
    except asyncio.CancelledError:
        pass
    finally:
        scanner.unsubscribe(queue)


@router.websocket("/ws/scan/detail")
async def scan_detail_websocket(websocket: WebSocket):
    await websocket.accept()
    queue = scanner.subscribe_detail()
    try:
        while True:
            data = await queue.get()
            await websocket.send_text(json.dumps(data))
    except WebSocketDisconnect:
        pass
    except asyncio.CancelledError:
        pass
    finally:
        scanner.unsubscribe_detail(queue)
