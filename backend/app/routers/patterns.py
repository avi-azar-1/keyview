import uuid

from fastapi import APIRouter, HTTPException

from app.models.pattern import Pattern, PatternCreate
from app.services.scanner import scanner
from app.services.grouper import regroup_keys
from app.services.redis_client import redis_client

router = APIRouter(prefix="/api/patterns", tags=["patterns"])

_patterns: dict[str, Pattern] = {}


@router.get("", response_model=list[Pattern])
async def list_patterns():
    return list(_patterns.values())


@router.post("", response_model=Pattern)
async def add_pattern(body: PatternCreate):
    pattern = Pattern(id=str(uuid.uuid4()), pattern=body.pattern)
    _patterns[pattern.id] = pattern
    scanner.patterns = [p.pattern for p in _patterns.values()]
    return pattern


@router.delete("/{pattern_id}")
async def delete_pattern(pattern_id: str):
    if pattern_id not in _patterns:
        raise HTTPException(status_code=404, detail="Pattern not found")
    del _patterns[pattern_id]
    scanner.patterns = [p.pattern for p in _patterns.values()]
    return {"status": "deleted"}


@router.post("/apply")
async def apply_patterns():
    if not redis_client.connected:
        raise HTTPException(status_code=400, detail="Not connected to Redis")
    patterns = [p.pattern for p in _patterns.values()]
    if not patterns:
        return {"pattern_counts": {}}
    counts = await regroup_keys(patterns)
    return {"pattern_counts": counts}
