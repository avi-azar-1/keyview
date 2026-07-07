"""
Multiprocessing worker functions for parallel Redis scanning.
Each function runs in a separate OS process with its own sync Redis connection.
"""

import fnmatch

import redis

from app.services.prefix_trie import PrefixTree

TTL_BUCKET_RANGES = [
    ("no TTL", -1, -1),
    ("<1 min", 0, 60),
    ("1-10 min", 60, 600),
    ("10 min - 1 hr", 600, 3600),
    ("1-24 hr", 3600, 86400),
    (">24 hr", 86400, float("inf")),
]


def _classify_ttl(ttl: int) -> str:
    if ttl <= -1:
        return "no TTL"
    for label, low, high in TTL_BUCKET_RANGES:
        if label == "no TTL":
            continue
        if low <= ttl < high:
            return label
    return ">24 hr"


def scan_worker_phase1(
    host: str,
    port: int,
    username: str | None,
    password: str | None,
    db: int,
    scan_count: int,
    patterns: list[str],
    progress_queue,
    worker_id: int,
    max_depth: int,
    min_count: int,
) -> dict:
    """Phase 1: SCAN + namespace + patterns + prefix tree. One node."""
    r = redis.Redis(
        host=host, port=port, username=username, password=password,
        db=db, decode_responses=True,
    )
    namespace_counts: dict[str, int] = {}
    pattern_counts: dict[str, int] = {p: 0 for p in patterns}
    tree = PrefixTree(max_depth=max_depth, min_count=min_count)
    scanned = 0
    cursor = 0

    while True:
        cursor, keys = r.scan(cursor=cursor, count=scan_count)
        for key in keys:
            ns = key.split(":")[0] if ":" in key else "(root)"
            namespace_counts[ns] = namespace_counts.get(ns, 0) + 1
            tree.insert(key)
            for pat in patterns:
                if fnmatch.fnmatch(key, pat):
                    pattern_counts[pat] = pattern_counts.get(pat, 0) + 1
                    break
        scanned += len(keys)
        progress_queue.put((worker_id, scanned))
        if cursor == 0:
            break

    r.close()
    return {
        "namespace_counts": namespace_counts,
        "pattern_counts": pattern_counts,
        "prefix_tree": tree,
        "scanned": scanned,
    }


def scan_worker_phase2(
    host: str,
    port: int,
    username: str | None,
    password: str | None,
    db: int,
    scan_count: int,
    pipeline_batch: int,
    progress_queue,
    worker_id: int,
) -> dict:
    """Phase 2: SCAN + TYPE/TTL pipelines. One node."""
    r = redis.Redis(
        host=host, port=port, username=username, password=password,
        db=db, decode_responses=True,
    )
    type_counts: dict[str, int] = {}
    ttl_counts: dict[str, int] = {}
    scanned = 0
    cursor = 0

    while True:
        cursor, keys = r.scan(cursor=cursor, count=scan_count)
        for i in range(0, len(keys), pipeline_batch):
            batch = keys[i : i + pipeline_batch]
            pipe = r.pipeline(transaction=False)
            for key in batch:
                pipe.type(key)
                pipe.ttl(key)
            results = pipe.execute()
            for j in range(0, len(results), 2):
                key_type = results[j]
                key_ttl = results[j + 1]
                type_counts[key_type] = type_counts.get(key_type, 0) + 1
                bucket = _classify_ttl(key_ttl)
                ttl_counts[bucket] = ttl_counts.get(bucket, 0) + 1
            scanned += len(batch)
        progress_queue.put((worker_id, scanned))
        if cursor == 0:
            break

    r.close()
    return {"type_counts": type_counts, "ttl_counts": ttl_counts, "scanned": scanned}


def regroup_worker(
    host: str,
    port: int,
    username: str | None,
    password: str | None,
    db: int,
    scan_count: int,
    patterns: list[str],
    progress_queue,
    worker_id: int,
) -> dict:
    """Regroup keys by patterns for one node (used by pattern apply)."""
    r = redis.Redis(
        host=host, port=port, username=username, password=password,
        db=db, decode_responses=True,
    )
    counts: dict[str, int] = {p: 0 for p in patterns}
    counts["(unmatched)"] = 0
    scanned = 0
    cursor = 0

    while True:
        cursor, keys = r.scan(cursor=cursor, count=scan_count)
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
        progress_queue.put((worker_id, scanned))
        if cursor == 0:
            break

    r.close()
    return {"counts": counts, "scanned": scanned}
