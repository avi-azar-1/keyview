from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from app.models.scan_result import ScanProgress, ScanResult
from app.services.redis_client import redis_client
from app.services.scanner import scanner

router = APIRouter(prefix="/api/scan", tags=["scan"])


class ScanRequest(BaseModel):
    scan_count: int | None = None


@router.post("/start")
async def start_scan(body: ScanRequest = ScanRequest()):
    if not redis_client.connected:
        raise HTTPException(status_code=400, detail="Not connected to Redis")
    await scanner.start_scan(scan_count=body.scan_count)
    return {"status": "started"}


@router.get("/status", response_model=ScanProgress)
async def scan_status():
    return scanner.progress


@router.get("/results", response_model=ScanResult)
async def scan_results():
    if scanner.result is None:
        raise HTTPException(status_code=404, detail="No scan results available")
    return scanner.result
