"""
Multiprocessing worker functions for parallel Redis scanning.
Each function runs in a separate OS process with its own sync Redis connection.
"""

import fnmatch
import sys
import time

import redis

from app.services.prefix_trie import PrefixTree

# Socket timeouts for workers — prevents infinite hang if Redis is slow/unresponsive
_CONNECT_TIMEOUT = 15   # seconds to establish TCP connection
_SOCKET_TIMEOUT = 120   # seconds per Redis operation

TTL_BUCKET_RANGES = [
    ("no TTL",        -1,        -1),
    ("< 10s",          0,        10),
    ("10s – 1m",        10,        60),
    ("1 – 5m",         60,       300),
    ("5 – 30m",        300,      1800),
    ("30m – 2h",      1800,      7200),
    ("2 – 12h",       7200,     43200),
    ("12h – 2d",     43200,    172800),
    ("2 – 7d",      172800,    604800),
    ("1 – 4w",      604800,   2419200),
    ("1 – 6mo",    2419200,  15552000),
    ("6mo – 2y",  15552000,  63072000),
    ("> 2y",          63072000, float("inf")),
]


def _classify_ttl(ttl: int) -> str:
    if ttl <= -1:
        return "no TTL"
    for label, low, high in TTL_BUCKET_RANGES:
        if label == "no TTL":
            continue
        if low <= ttl < high:
            return label
    return "> 2y"


def _log(worker_id: int, msg: str):
    print(f"[worker-{worker_id}] {time.monotonic():.2f} {msg}", flush=True, file=sys.stderr)


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
    t0 = time.monotonic()
    _log(worker_id, f"phase1 start {host}:{port}")
    try:
        r = redis.Redis(
            host=host, port=port, username=username, password=password,
            db=db, decode_responses=True,
            socket_connect_timeout=_CONNECT_TIMEOUT,
            socket_timeout=_SOCKET_TIMEOUT,
        )
        _log(worker_id, "redis.Redis() created, pinging")
        r.ping()
        _log(worker_id, "ping ok")
    except Exception as e:
        _log(worker_id, f"connection failed: {e}")
        raise

    namespace_counts: dict[str, int] = {}
    pattern_counts: dict[str, int] = {p: 0 for p in patterns}
    tree = PrefixTree(max_depth=max_depth, min_count=min_count)
    scanned = 0
    cursor = 0
    batch_num = 0

    while True:
        try:
            cursor, keys = r.scan(cursor=cursor, count=scan_count)
        except Exception as e:
            _log(worker_id, f"SCAN failed at cursor={cursor} batch={batch_num}: {e}")
            raise
        batch_num += 1
        for key in keys:
            ns = key.split(":")[0] if ":" in key else "(root)"
            namespace_counts[ns] = namespace_counts.get(ns, 0) + 1
            tree.insert(key)
            for pat in patterns:
                if fnmatch.fnmatch(key, pat):
                    pattern_counts[pat] = pattern_counts.get(pat, 0) + 1
                    break
        scanned += len(keys)
        if batch_num % 10 == 0:
            _log(worker_id, f"batch={batch_num} scanned={scanned} cursor={cursor} elapsed={time.monotonic()-t0:.1f}s")
        progress_queue.put((worker_id, scanned))
        if cursor == 0:
            break

    r.close()
    _log(worker_id, f"phase1 scan done scanned={scanned}, pruning trie (nodes before prune: will walk)")
    # Prune before pickling — removes leaf nodes below min_count so the serialized
    # payload is a small list of significant paths rather than millions of raw nodes.
    prune_threshold = max(int(scanned * 0.001), min_count)
    tree.prune(prune_threshold)
    path_counts = tree.to_path_counts()
    _log(worker_id, f"phase1 done scanned={scanned} trie_paths={len(path_counts)} in {time.monotonic()-t0:.1f}s")
    return {
        "namespace_counts": namespace_counts,
        "pattern_counts": pattern_counts,
        "prefix_tree_paths": path_counts,
        "prefix_tree_key_count": scanned,
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
    tracked_namespaces: list[str],
) -> dict:
    """Phase 2: SCAN + TYPE/TTL pipelines. One node.

    Tracks aggregate type/ttl counts ("all") plus per-namespace breakdowns
    for the whitelisted `tracked_namespaces` (top-N from phase 1).
    """
    t0 = time.monotonic()
    _log(worker_id, f"phase2 start {host}:{port}")
    try:
        r = redis.Redis(
            host=host, port=port, username=username, password=password,
            db=db, decode_responses=True,
            socket_connect_timeout=_CONNECT_TIMEOUT,
            socket_timeout=_SOCKET_TIMEOUT,
        )
        _log(worker_id, "redis.Redis() created, pinging")
        r.ping()
        _log(worker_id, "ping ok")
    except Exception as e:
        _log(worker_id, f"connection failed: {e}")
        raise

    type_counts: dict[str, int] = {}
    ttl_counts: dict[str, int] = {}
    tracked = set(tracked_namespaces)
    ns_type: dict[str, dict[str, int]] = {ns: {} for ns in tracked}
    ns_ttl: dict[str, dict[str, int]] = {ns: {} for ns in tracked}
    scanned = 0
    cursor = 0
    batch_num = 0

    while True:
        try:
            cursor, keys = r.scan(cursor=cursor, count=scan_count)
        except Exception as e:
            _log(worker_id, f"SCAN failed at cursor={cursor} batch={batch_num}: {e}")
            raise
        batch_num += 1
        for i in range(0, len(keys), pipeline_batch):
            batch = keys[i : i + pipeline_batch]
            pipe = r.pipeline(transaction=False)
            for key in batch:
                pipe.type(key)
                pipe.ttl(key)
            try:
                pipe_results = pipe.execute()
            except Exception as e:
                _log(worker_id, f"pipeline.execute() failed at batch={batch_num}: {e}")
                raise
            for j in range(0, len(pipe_results), 2):
                key = batch[j // 2]
                key_type = pipe_results[j]
                key_ttl = pipe_results[j + 1]
                type_counts[key_type] = type_counts.get(key_type, 0) + 1
                bucket = _classify_ttl(key_ttl)
                ttl_counts[bucket] = ttl_counts.get(bucket, 0) + 1
                ns = key.split(":")[0] if ":" in key else "(root)"
                if ns in tracked:
                    nt = ns_type[ns]
                    nt[key_type] = nt.get(key_type, 0) + 1
                    nl = ns_ttl[ns]
                    nl[bucket] = nl.get(bucket, 0) + 1
            scanned += len(batch)
        if batch_num % 10 == 0:
            _log(worker_id, f"batch={batch_num} scanned={scanned} cursor={cursor} elapsed={time.monotonic()-t0:.1f}s")
        progress_queue.put((worker_id, scanned))
        if cursor == 0:
            break

    r.close()
    _log(worker_id, f"phase2 done scanned={scanned} in {time.monotonic()-t0:.1f}s")
    return {
        "type_counts": type_counts,
        "ttl_counts": ttl_counts,
        "ns_type_counts": ns_type,
        "ns_ttl_counts": ns_ttl,
        "scanned": scanned,
    }


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
    t0 = time.monotonic()
    _log(worker_id, f"regroup start {host}:{port}")
    try:
        r = redis.Redis(
            host=host, port=port, username=username, password=password,
            db=db, decode_responses=True,
            socket_connect_timeout=_CONNECT_TIMEOUT,
            socket_timeout=_SOCKET_TIMEOUT,
        )
        r.ping()
        _log(worker_id, "ping ok")
    except Exception as e:
        _log(worker_id, f"connection failed: {e}")
        raise

    counts: dict[str, int] = {p: 0 for p in patterns}
    counts["(unmatched)"] = 0
    scanned = 0
    cursor = 0
    batch_num = 0

    while True:
        cursor, keys = r.scan(cursor=cursor, count=scan_count)
        batch_num += 1
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
        if batch_num % 10 == 0:
            _log(worker_id, f"batch={batch_num} scanned={scanned} cursor={cursor} elapsed={time.monotonic()-t0:.1f}s")
        progress_queue.put((worker_id, scanned))
        if cursor == 0:
            break

    r.close()
    _log(worker_id, f"regroup done scanned={scanned} in {time.monotonic()-t0:.1f}s")
    return {"counts": counts, "scanned": scanned}
