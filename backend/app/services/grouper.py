import asyncio
import fnmatch
from typing import Callable, Awaitable

from app.services.redis_client import redis_client
from app.config import settings


async def regroup_keys(
    patterns: list[str],
    on_progress: Callable[[int, int], Awaitable[None]] | None = None,
) -> dict[str, int]:
    """Re-scan keys and apply pattern grouping. Used when patterns change after a scan."""
    nodes = await redis_client.get_primary_nodes()
    counts: dict[str, int] = {p: 0 for p in patterns}
    counts["(unmatched)"] = 0

    total_estimate = 0
    if on_progress:
        dbsizes = await asyncio.gather(
            *[node.dbsize() for node in nodes], return_exceptions=True
        )
        total_estimate = sum(s for s in dbsizes if isinstance(s, int))
        await on_progress(0, total_estimate)

    scanned = 0

    async def scan_node(node):
        nonlocal scanned
        cursor = 0
        while True:
            cursor, keys = await node.scan(cursor, count=settings.redis_scan_count)
            for key in keys:
                matched = False
                for pat in patterns:
                    if fnmatch.fnmatch(key, pat):
                        counts[pat] += 1
                        matched = True
                        break
                if not matched:
                    counts["(unmatched)"] += 1
            scanned += len(keys)
            if on_progress:
                await on_progress(scanned, total_estimate)
            if cursor == 0:
                break

    await asyncio.gather(*[scan_node(node) for node in nodes])
    return counts
