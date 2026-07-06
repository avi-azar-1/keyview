from fastapi import APIRouter, HTTPException

from app.models.connection import ConnectionRequest, ConnectionInfo
from app.services.redis_client import redis_client

router = APIRouter(prefix="/api", tags=["connection"])


@router.post("/connect", response_model=ConnectionInfo)
async def connect(params: ConnectionRequest):
    try:
        info = await redis_client.connect(params)
        return info
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.delete("/disconnect")
async def disconnect():
    await redis_client.disconnect()
    return {"status": "disconnected"}


@router.get("/connection/info", response_model=ConnectionInfo)
async def get_connection_info():
    if not redis_client.connected:
        raise HTTPException(status_code=400, detail="Not connected to Redis")
    try:
        return await redis_client.get_info()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
