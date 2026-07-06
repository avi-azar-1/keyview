import fnmatch

from app.services.redis_client import redis_client
from app.config import settings


async def regroup_keys(patterns: list[str]) -> dict[str, int]:
    """Re-scan keys and apply pattern grouping. Used when patterns change after a scan."""
    nodes = await redis_client.get_primary_nodes()
    counts: dict[str, int] = {p: 0 for p in patterns}
    counts["(unmatched)"] = 0

    for node in nodes:
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
            if cursor == 0:
                break

    return counts
