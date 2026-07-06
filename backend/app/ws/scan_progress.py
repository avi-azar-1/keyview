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
            if data.get("status") == "completed":
                break
    except WebSocketDisconnect:
        pass
    except asyncio.CancelledError:
        pass
    finally:
        scanner.unsubscribe(queue)
