from fastapi import APIRouter, HTTPException

from app.models.scan_result import ScanProgress, ScanResult
from app.services.redis_client import redis_client
from app.services.scanner import scanner

router = APIRouter(prefix="/api/scan", tags=["scan"])


@router.post("/start")
async def start_scan():
    if not redis_client.connected:
        raise HTTPException(status_code=400, detail="Not connected to Redis")
    await scanner.start_scan()
    return {"status": "started"}


@router.get("/status", response_model=ScanProgress)
async def scan_status():
    return scanner.progress


@router.get("/results", response_model=ScanResult)
async def scan_results():
    if scanner.result is None:
        raise HTTPException(status_code=404, detail="No scan results available")
    return scanner.result
