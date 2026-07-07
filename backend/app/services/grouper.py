import asyncio
import fnmatch
import multiprocessing
from concurrent.futures import ProcessPoolExecutor
from typing import Callable, Awaitable

from app.services.redis_client import redis_client
from app.services.scan_worker import regroup_worker
from app.config import settings


async def regroup_keys(
    patterns: list[str],
    on_progress: Callable[[int, int], Awaitable[None]] | None = None,
) -> dict[str, int]:
    """Re-scan keys and apply pattern grouping. Uses multiprocessing for cluster."""
    nodes = await redis_client.get_primary_nodes()
    is_cluster = redis_client.is_cluster and len(nodes) > 1

    total_estimate = 0
    if on_progress:
        dbsizes = await asyncio.gather(
            *[node.dbsize() for node in nodes], return_exceptions=True
        )
        total_estimate = sum(s for s in dbsizes if isinstance(s, int))
        await on_progress(0, total_estimate)

    if is_cluster:
        return await _regroup_parallel(patterns, nodes, total_estimate, on_progress)
    else:
        return await _regroup_single(patterns, nodes[0], total_estimate, on_progress)


async def _regroup_parallel(
    patterns: list[str],
    nodes,
    total_estimate: int,
    on_progress: Callable[[int, int], Awaitable[None]] | None,
) -> dict[str, int]:
    """Regroup with multiprocessing — one process per node."""
    node_params = []
    for node in redis_client._node_connections:
        pool = node.connection_pool
        kwargs = pool.connection_kwargs
        node_params.append({
            "host": kwargs.get("host", "localhost"),
            "port": kwargs.get("port", 6379),
            "username": kwargs.get("username"),
            "password": kwargs.get("password"),
            "db": kwargs.get("db", 0),
        })

    manager = multiprocessing.Manager()
    progress_queue = manager.Queue()
    loop = asyncio.get_running_loop()

    with ProcessPoolExecutor(max_workers=len(node_params)) as pool:
        futures = []
        for i, params in enumerate(node_params):
            fut = loop.run_in_executor(
                pool, regroup_worker,
                params["host"], params["port"],
                params["username"], params["password"],
                params["db"], settings.redis_scan_count,
                patterns, progress_queue, i,
            )
            futures.append(fut)

        worker_progress = [0] * len(node_params)

        async def poll_progress():
            while True:
                drained = False
                while not drained:
                    try:
                        worker_id, count = progress_queue.get_nowait()
                        worker_progress[worker_id] = count
                    except Exception:
                        drained = True
                if on_progress:
                    total = sum(worker_progress)
                    await on_progress(total, total_estimate)
                await asyncio.sleep(0.1)

        progress_task = asyncio.create_task(poll_progress())
        results = await asyncio.gather(*futures)
        progress_task.cancel()

    manager.shutdown()

    # Merge counts
    counts: dict[str, int] = {p: 0 for p in patterns}
    counts["(unmatched)"] = 0
    for r in results:
        for pat, count in r["counts"].items():
            counts[pat] = counts.get(pat, 0) + count

    return counts


async def _regroup_single(
    patterns: list[str],
    node,
    total_estimate: int,
    on_progress: Callable[[int, int], Awaitable[None]] | None,
) -> dict[str, int]:
    """Regroup in-process for standalone mode."""
    counts: dict[str, int] = {p: 0 for p in patterns}
    counts["(unmatched)"] = 0
    scanned = 0
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

    return counts
