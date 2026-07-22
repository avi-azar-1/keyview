import asyncio
import fnmatch
import logging
import math
import multiprocessing
import os
import time
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor

from app.config import settings
from app.models.scan_result import (
    NamespaceBreakdown,
    PrefixSuggestion,
    ScanProgress,
    ScanResult,
    TTLBucket,
)
from app.services.prefix_trie import PrefixTree
from app.services.redis_client import redis_client
from app.services.scan_worker import scan_worker_phase1, scan_worker_phase2

logger = logging.getLogger(__name__)

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


def classify_ttl(ttl: int) -> str:
    if ttl == -1:
        return "no TTL"
    if ttl == -2:
        return "no TTL"
    for label, low, high in TTL_BUCKET_RANGES:
        if label == "no TTL":
            continue
        if low <= ttl < high:
            return label
    return "> 2y"


def _format_ttl_secs(s: int | float) -> str:
    s = int(s)
    if s < 60:
        return f"{s}s"
    if s < 3600:
        return f"{s // 60}m"
    if s < 86400:
        return f"{s // 3600}h"
    if s < 604800:
        return f"{s // 86400}d"
    if s < 2592000:
        return f"{s // 604800}w"
    if s < 31536000:
        return f"{s // 2592000}mo"
    return f"{s // 31536000}y"


def _ttl_range_label(low: int | float, high: int | float) -> str:
    if low == 0:
        return f"< {_format_ttl_secs(high)}"
    if high == float("inf"):
        return f"> {_format_ttl_secs(low)}"
    return f"{_format_ttl_secs(low)} – {_format_ttl_secs(high)}"


def _merge_ttl_buckets(ttl_counts: dict[str, int], max_buckets: int = 6) -> list[TTLBucket]:
    no_ttl_count = ttl_counts.get("no TTL", 0)
    timed = [
        [label, low, high, ttl_counts.get(label, 0)]
        for label, low, high in TTL_BUCKET_RANGES
        if label != "no TTL" and ttl_counts.get(label, 0) > 0
    ]
    max_timed = max_buckets - 1
    while len(timed) > max_timed:
        best_i = 0
        best_combined = timed[0][3] + timed[1][3]
        for i in range(1, len(timed) - 1):
            combined = timed[i][3] + timed[i + 1][3]
            if combined < best_combined:
                best_combined = combined
                best_i = i
        left = timed[best_i]
        right = timed[best_i + 1]
        merged = [_ttl_range_label(left[1], right[2]), left[1], right[2], left[3] + right[3]]
        timed[best_i : best_i + 2] = [merged]
    result = []
    if no_ttl_count > 0:
        result.append(TTLBucket(label="no TTL", count=no_ttl_count))
    for label, _low, _high, count in timed:
        result.append(TTLBucket(label=label, count=count))
    return result


def extract_namespace(key: str, delimiter: str = ":") -> str:
    parts = key.split(delimiter)
    if len(parts) > 1:
        return parts[0]
    return "(root)"


def _node_scan_limit(dbsize: int, percent: int) -> int:
    """Per-node key cap for sampling mode. 0 means unlimited (full scan)."""
    if percent >= 100:
        return 0
    return max(1, math.ceil(dbsize * percent / 100))


def _node_scale(dbsize: int, scanned: int, percent: int) -> float:
    """Reverse-normalization factor: dbsize / actual_scanned (self-corrects overshoot)."""
    if percent >= 100 or scanned <= 0:
        return 1.0
    return dbsize / scanned


class Scanner:
    def __init__(self):
        self._progress = ScanProgress(status="idle")
        self._detail_progress = ScanProgress(status="idle")
        self._result: ScanResult | None = None
        self._task: asyncio.Task | None = None
        self._detail_task: asyncio.Task | None = None
        self._listeners: list[asyncio.Queue] = []
        self._detail_listeners: list[asyncio.Queue] = []
        self._patterns: list[str] = []
        self._scan_count: int = settings.redis_scan_count
        self._estimate_percent: int = 100

    @property
    def progress(self) -> ScanProgress:
        return self._progress

    @property
    def detail_progress(self) -> ScanProgress:
        return self._detail_progress

    @property
    def result(self) -> ScanResult | None:
        return self._result

    @property
    def patterns(self) -> list[str]:
        return self._patterns

    @patterns.setter
    def patterns(self, value: list[str]):
        self._patterns = value

    def subscribe(self) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue()
        self._listeners.append(q)
        return q

    def unsubscribe(self, q: asyncio.Queue):
        if q in self._listeners:
            self._listeners.remove(q)

    def subscribe_detail(self) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue()
        self._detail_listeners.append(q)
        return q

    def unsubscribe_detail(self, q: asyncio.Queue):
        if q in self._detail_listeners:
            self._detail_listeners.remove(q)

    async def _notify(self):
        for q in self._listeners:
            await q.put(self._progress.model_dump())

    async def _notify_detail(self):
        for q in self._detail_listeners:
            await q.put(self._detail_progress.model_dump())

    async def start_scan(self, scan_count: int | None = None, estimate_percent: int = 100):
        if self._task and not self._task.done():
            logger.info("start_scan called but scan already running — ignoring")
            return
        self._scan_count = scan_count or settings.redis_scan_count
        self._estimate_percent = max(1, min(100, estimate_percent))
        logger.info("start_scan: scan_count=%d estimate_percent=%d",
                    self._scan_count, self._estimate_percent)
        self._task = asyncio.create_task(self._run_scan())
        self._task.add_done_callback(self._on_task_done)

    def _on_task_done(self, task: asyncio.Task):
        if task.cancelled():
            logger.warning("Scan task was cancelled")
        elif task.exception():
            logger.error("Scan task raised an exception", exc_info=task.exception())
            self._progress = ScanProgress(status="error", scanned=0, total_estimate=0, percent=0.0)
            asyncio.get_event_loop().create_task(self._notify())
        else:
            logger.info("Scan task completed normally")

    def _get_node_params(self) -> list[dict]:
        nodes = redis_client._node_connections
        params_list = []
        for node in nodes:
            pool = node.connection_pool
            kwargs = pool.connection_kwargs
            params_list.append({
                "host": kwargs.get("host", "localhost"),
                "port": kwargs.get("port", 6379),
                "username": kwargs.get("username"),
                "password": kwargs.get("password"),
                "db": kwargs.get("db", 0),
            })
        logger.info("_get_node_params: %d nodes: %s",
                    len(params_list),
                    [(p["host"], p["port"]) for p in params_list])
        return params_list

    async def _run_scan(self):
        """Phase 1: fast scan-only pass."""
        t0 = time.monotonic()
        logger.info("_run_scan: starting")
        nodes = await redis_client.get_primary_nodes()
        is_cluster = redis_client.is_cluster and len(nodes) > 1
        logger.info("_run_scan: is_cluster=%s, node_count=%d", is_cluster, len(nodes))

        dbsizes = await asyncio.gather(
            *[node.dbsize() for node in nodes], return_exceptions=True
        )
        logger.info("_run_scan: dbsizes per node: %s", dbsizes)
        node_dbsizes = [s if isinstance(s, int) else 0 for s in dbsizes]
        total_estimate = sum(node_dbsizes)
        node_limits = [_node_scan_limit(s, self._estimate_percent) for s in node_dbsizes]
        # Progress denominator: the sampled target (sum of per-node caps), or full
        # keyspace when sampling is off (limit 0 → count that node's full dbsize).
        scan_target = sum(
            lim if lim else db for lim, db in zip(node_limits, node_dbsizes)
        )
        logger.info("_run_scan: total_estimate=%d scan_target=%d percent=%d",
                    total_estimate, scan_target, self._estimate_percent)

        self._progress = ScanProgress(
            status="scanning", scanned=0, total_estimate=scan_target, percent=0.0
        )
        await self._notify()

        if is_cluster:
            await self._run_scan_parallel(nodes, node_dbsizes, node_limits, scan_target)
        else:
            await self._run_scan_single(nodes[0], node_dbsizes[0], node_limits[0], scan_target)

        logger.info("_run_scan: phase 1 done in %.1fs, notifying and starting detail scan",
                    time.monotonic() - t0)
        await self._notify()
        self._start_detail_scan()

    async def _run_scan_parallel(self, nodes, node_dbsizes: list[int],
                                 node_limits: list[int], scan_target: int):
        """Phase 1 with multiprocessing — one process per node."""
        t0 = time.monotonic()
        node_params = self._get_node_params()
        max_workers = min(len(node_params), os.cpu_count() or 4)
        logger.info("_run_scan_parallel: %d nodes, %d workers, target ~%d keys, cpu_count=%s",
                    len(node_params), max_workers, scan_target, os.cpu_count())

        logger.info("_run_scan_parallel: creating Manager()")
        manager = multiprocessing.Manager()
        logger.info("_run_scan_parallel: Manager() ready, creating queue")
        progress_queue = manager.Queue()
        loop = asyncio.get_running_loop()

        logger.info("_run_scan_parallel: submitting %d workers to ProcessPoolExecutor", len(node_params))
        with ProcessPoolExecutor(max_workers=max_workers) as pool:
            futures = []
            for i, params in enumerate(node_params):
                logger.info("_run_scan_parallel: submitting worker %d -> %s:%d",
                            i, params["host"], params["port"])
                fut = loop.run_in_executor(
                    pool, scan_worker_phase1,
                    params["host"], params["port"],
                    params["username"], params["password"],
                    params["db"], self._scan_count,
                    self._patterns, progress_queue, i,
                    settings.prefix_tree_max_depth, 50,
                    node_limits[i],
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
                    total = sum(worker_progress)
                    pct = min((total / scan_target) * 100, 100.0) if scan_target > 0 else 100.0
                    self._progress = ScanProgress(
                        status="scanning", scanned=total,
                        total_estimate=scan_target, percent=round(pct, 1),
                    )
                    await self._notify()
                    await asyncio.sleep(0.1)

            logger.info("_run_scan_parallel: starting poll_progress task, awaiting all workers")
            progress_task = asyncio.create_task(poll_progress())
            results = await asyncio.gather(*futures, return_exceptions=True)
            progress_task.cancel()
            logger.info("_run_scan_parallel: all workers returned in %.1fs", time.monotonic() - t0)

            # Pair each result with its node dbsize (for reverse-normalization),
            # then drop failures — keeps dbsize aligned across the filter.
            paired = []
            for i, r in enumerate(results):
                if isinstance(r, Exception):
                    logger.error("Phase 1 worker %d failed: %s", i, r, exc_info=r)
                else:
                    logger.info("Phase 1 worker %d ok: scanned=%d namespaces=%d",
                                i, r.get("scanned", 0), len(r.get("namespace_counts", {})))
                    paired.append((r, node_dbsizes[i]))
            results = [r for r, _ in paired]
            result_dbsizes = [db for _, db in paired]
            logger.info("_run_scan_parallel: %d/%d workers succeeded",
                        len(results), len(node_params))

        logger.info("_run_scan_parallel: ProcessPoolExecutor exited (%.1fs)", time.monotonic() - t0)

        try:
            while True:
                worker_id, count = progress_queue.get_nowait()
                worker_progress[worker_id] = count
        except Exception:
            pass

        logger.info("_run_scan_parallel: starting merge in thread (%.1fs)", time.monotonic() - t0)

        percent = self._estimate_percent

        def _merge_phase1(results, dbsizes, patterns):
            mt0 = time.monotonic()
            logger.info("_merge_phase1: starting manager.shutdown()")
            manager.shutdown()
            logger.info("_merge_phase1: manager.shutdown() done in %.1fs", time.monotonic() - mt0)
            namespace_counts: dict[str, int] = {}
            pattern_counts: dict[str, int] = {p: 0 for p in patterns}
            merged_tree = PrefixTree(max_depth=settings.prefix_tree_max_depth, min_count=50)
            total_scanned = 0
            for idx, r in enumerate(results):
                scale = _node_scale(dbsizes[idx], r["scanned"], percent)
                for ns, count in r["namespace_counts"].items():
                    namespace_counts[ns] = namespace_counts.get(ns, 0) + round(count * scale)
                for pat, count in r["pattern_counts"].items():
                    if pat in pattern_counts:
                        pattern_counts[pat] += round(count * scale)
                paths = r["prefix_tree_paths"]
                key_count = r["prefix_tree_key_count"]
                if scale != 1.0:
                    paths = [(p, round(c * scale)) for p, c in paths]
                    key_count = round(key_count * scale)
                logger.info("_merge_phase1: merging worker %d paths=%d keys=%d scale=%.3f",
                            idx, len(paths), key_count, scale)
                merged_tree.merge_path_counts(paths, key_count)
                logger.info("_merge_phase1: worker %d merged (%.1fs)", idx, time.monotonic() - mt0)
                total_scanned += round(r["scanned"] * scale)
            logger.info("_merge_phase1: all merges done, total_scanned=%d (%.1fs)",
                        total_scanned, time.monotonic() - mt0)
            return namespace_counts, pattern_counts, merged_tree, total_scanned

        namespace_counts, pattern_counts, merged_tree, total_scanned = await loop.run_in_executor(
            None, _merge_phase1, results, result_dbsizes, list(self._patterns)
        )
        logger.info("_run_scan_parallel: merge done, calling _finalize_phase1 (%.1fs total)",
                    time.monotonic() - t0)
        self._finalize_phase1(namespace_counts, pattern_counts, merged_tree, total_scanned, scan_target)
        logger.info("_run_scan_parallel: complete (%.1fs total)", time.monotonic() - t0)

    async def _run_scan_single(self, node, dbsize: int, scan_limit: int, scan_target: int):
        """Phase 1 in-process for standalone mode."""
        namespace_counts: dict[str, int] = defaultdict(int)
        pattern_counts: dict[str, int] = defaultdict(int)
        prefix_tree = PrefixTree(max_depth=settings.prefix_tree_max_depth, min_count=50)
        scanned = 0

        cursor = 0
        while True:
            cursor, keys = await node.scan(cursor, count=self._scan_count)
            for key in keys:
                namespace_counts[extract_namespace(key)] += 1
                prefix_tree.insert(key)
                for pat in self._patterns:
                    if fnmatch.fnmatch(key, pat):
                        pattern_counts[pat] += 1
                        break
            scanned += len(keys)
            pct = min((scanned / scan_target) * 100, 100.0) if scan_target > 0 else 100.0
            self._progress = ScanProgress(
                status="scanning", scanned=scanned,
                total_estimate=scan_target, percent=round(pct, 1),
            )
            await self._notify()
            if cursor == 0 or (scan_limit and scanned >= scan_limit):
                break

        # Reverse-normalize the sampled counts up to the full-keyspace estimate.
        scale = _node_scale(dbsize, scanned, self._estimate_percent)
        if scale != 1.0:
            namespace_counts = {ns: round(c * scale) for ns, c in namespace_counts.items()}
            pattern_counts = {p: round(c * scale) for p, c in pattern_counts.items()}
            # Rebuild the prefix tree at scaled counts so suggestions/coverage match.
            scaled_paths = [(p, round(c * scale)) for p, c in prefix_tree.to_path_counts()]
            scaled_tree = PrefixTree(max_depth=settings.prefix_tree_max_depth, min_count=50)
            scaled_tree.merge_path_counts(scaled_paths, round(scanned * scale))
            prefix_tree = scaled_tree
        estimated_total = round(scanned * scale)

        self._finalize_phase1(dict(namespace_counts), dict(pattern_counts), prefix_tree, estimated_total, scan_target)

    def _finalize_phase1(self, namespace_counts, pattern_counts, prefix_tree, total_scanned, total_estimate):
        t0 = time.monotonic()
        logger.info("_finalize_phase1: total_scanned=%d, namespaces=%d, pruning tree",
                    total_scanned, len(namespace_counts))
        prune_threshold = max(int(prefix_tree.total_keys * 0.001), 50)
        prefix_tree.prune(prune_threshold)
        logger.info("_finalize_phase1: prune done, suggesting prefixes")
        raw_suggestions = prefix_tree.suggest(top_n=settings.prefix_suggestion_count)
        logger.info("_finalize_phase1: got %d suggestions (%.2fs)", len(raw_suggestions), time.monotonic() - t0)
        suggested_prefixes = [
            PrefixSuggestion(
                prefix=s["prefix"],
                key_count=s["key_count"],
                depth=s["depth"],
                child_count=s["child_count"],
                coverage_pct=s["coverage_pct"],
            )
            for s in raw_suggestions
        ]

        self._result = ScanResult(
            total_keys=total_scanned,
            type_counts={},
            ttl_buckets=[],
            namespace_counts=namespace_counts,
            pattern_counts=pattern_counts,
            suggested_prefixes=suggested_prefixes,
            estimate_percent=self._estimate_percent,
        )

        self._progress = ScanProgress(
            status="completed", scanned=total_scanned,
            total_estimate=total_estimate, percent=100.0,
        )
        logger.info("_finalize_phase1: done, progress set to completed")

    def _start_detail_scan(self):
        if self._detail_task and not self._detail_task.done():
            logger.info("_start_detail_scan: detail task already running, skipping")
            return
        logger.info("_start_detail_scan: creating detail task")
        self._detail_task = asyncio.create_task(self._run_detail_scan())
        self._detail_task.add_done_callback(self._on_detail_task_done)

    def _on_detail_task_done(self, task: asyncio.Task):
        if task.cancelled():
            logger.warning("Detail scan task was cancelled")
        elif task.exception():
            logger.error("Detail scan task raised an exception", exc_info=task.exception())
        else:
            logger.info("Detail scan task completed normally")

    async def _run_detail_scan(self):
        """Phase 2: full scan with TYPE + TTL pipelines."""
        t0 = time.monotonic()
        logger.info("_run_detail_scan: starting")
        nodes = await redis_client.get_primary_nodes()
        is_cluster = redis_client.is_cluster and len(nodes) > 1
        logger.info("_run_detail_scan: is_cluster=%s, node_count=%d", is_cluster, len(nodes))

        dbsizes = await asyncio.gather(
            *[node.dbsize() for node in nodes], return_exceptions=True
        )
        node_dbsizes = [s if isinstance(s, int) else 0 for s in dbsizes]
        total_estimate = sum(node_dbsizes)
        node_limits = [_node_scan_limit(s, self._estimate_percent) for s in node_dbsizes]
        scan_target = sum(
            lim if lim else db for lim, db in zip(node_limits, node_dbsizes)
        )
        logger.info("_run_detail_scan: total_estimate=%d scan_target=%d percent=%d",
                    total_estimate, scan_target, self._estimate_percent)

        self._detail_progress = ScanProgress(
            status="scanning", scanned=0, total_estimate=scan_target, percent=0.0
        )
        await self._notify_detail()

        # Phase 1 already computed complete namespace_counts. Pick the top 10 by
        # count — these are the only namespaces phase 2 builds per-ns breakdowns for.
        tracked_namespaces: list[str] = []
        if self._result and self._result.namespace_counts:
            tracked_namespaces = [
                ns for ns, _ in sorted(
                    self._result.namespace_counts.items(),
                    key=lambda kv: kv[1], reverse=True,
                )[:10]
            ]
        logger.info("_run_detail_scan: tracking %d namespaces: %s",
                    len(tracked_namespaces), tracked_namespaces)

        if is_cluster:
            await self._run_detail_parallel(nodes, node_dbsizes, node_limits,
                                            scan_target, tracked_namespaces)
        else:
            await self._run_detail_single(nodes[0], node_dbsizes[0], node_limits[0],
                                          scan_target, tracked_namespaces)

        logger.info("_run_detail_scan: done in %.1fs, sending final notify", time.monotonic() - t0)
        await self._notify_detail()

    async def _run_detail_parallel(self, nodes, node_dbsizes: list[int], node_limits: list[int],
                                   scan_target: int, tracked_namespaces: list[str]):
        """Phase 2 with multiprocessing — one process per node."""
        t0 = time.monotonic()
        node_params = self._get_node_params()
        max_workers = min(len(node_params), os.cpu_count() or 4)
        logger.info("_run_detail_parallel: %d nodes, %d workers, target ~%d keys",
                    len(node_params), max_workers, scan_target)

        logger.info("_run_detail_parallel: creating Manager()")
        manager = multiprocessing.Manager()
        logger.info("_run_detail_parallel: Manager() ready")
        progress_queue = manager.Queue()
        loop = asyncio.get_running_loop()

        logger.info("_run_detail_parallel: submitting workers")
        with ProcessPoolExecutor(max_workers=max_workers) as pool:
            futures = []
            for i, params in enumerate(node_params):
                logger.info("_run_detail_parallel: submitting worker %d -> %s:%d",
                            i, params["host"], params["port"])
                fut = loop.run_in_executor(
                    pool, scan_worker_phase2,
                    params["host"], params["port"],
                    params["username"], params["password"],
                    params["db"], self._scan_count,
                    settings.redis_pipeline_batch,
                    progress_queue, i,
                    tracked_namespaces,
                    node_limits[i],
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
                    total = sum(worker_progress)
                    pct = min((total / scan_target) * 100, 100.0) if scan_target > 0 else 100.0
                    self._detail_progress = ScanProgress(
                        status="scanning", scanned=total,
                        total_estimate=scan_target, percent=round(pct, 1),
                    )
                    await self._notify_detail()
                    await asyncio.sleep(0.1)

            logger.info("_run_detail_parallel: awaiting all workers")
            progress_task = asyncio.create_task(poll_progress())
            results = await asyncio.gather(*futures, return_exceptions=True)
            progress_task.cancel()
            logger.info("_run_detail_parallel: all workers returned in %.1fs", time.monotonic() - t0)

            # Pair each result with its node dbsize (for reverse-normalization),
            # then drop failures — keeps dbsize aligned across the filter.
            paired = []
            for i, r in enumerate(results):
                if isinstance(r, Exception):
                    logger.error("Phase 2 worker %d failed: %s", i, r, exc_info=r)
                else:
                    logger.info("Phase 2 worker %d ok: scanned=%d types=%s",
                                i, r.get("scanned", 0), dict(r.get("type_counts", {})))
                    paired.append((r, node_dbsizes[i]))
            results = [r for r, _ in paired]
            result_dbsizes = [db for _, db in paired]
            logger.info("_run_detail_parallel: %d/%d workers succeeded",
                        len(results), len(node_params))

        logger.info("_run_detail_parallel: ProcessPoolExecutor exited (%.1fs)", time.monotonic() - t0)
        logger.info("_run_detail_parallel: starting merge in thread")

        percent = self._estimate_percent

        def _merge_phase2(results, dbsizes):
            mt0 = time.monotonic()
            logger.info("_merge_phase2: starting manager.shutdown()")
            manager.shutdown()
            logger.info("_merge_phase2: manager.shutdown() done in %.1fs", time.monotonic() - mt0)
            type_counts: dict[str, int] = {}
            ttl_counts: dict[str, int] = {}
            ns_type: dict[str, dict[str, int]] = {}
            ns_ttl: dict[str, dict[str, int]] = {}
            detail_scanned = 0
            for idx, r in enumerate(results):
                scale = _node_scale(dbsizes[idx], r["scanned"], percent)
                for t, count in r["type_counts"].items():
                    type_counts[t] = type_counts.get(t, 0) + round(count * scale)
                for t, count in r["ttl_counts"].items():
                    ttl_counts[t] = ttl_counts.get(t, 0) + round(count * scale)
                for ns, inner in r.get("ns_type_counts", {}).items():
                    dst = ns_type.setdefault(ns, {})
                    for t, count in inner.items():
                        dst[t] = dst.get(t, 0) + round(count * scale)
                for ns, inner in r.get("ns_ttl_counts", {}).items():
                    dst = ns_ttl.setdefault(ns, {})
                    for t, count in inner.items():
                        dst[t] = dst.get(t, 0) + round(count * scale)
                detail_scanned += round(r["scanned"] * scale)
            logger.info("_merge_phase2: done, detail_scanned=%d (%.1fs)",
                        detail_scanned, time.monotonic() - mt0)
            return type_counts, ttl_counts, ns_type, ns_ttl, detail_scanned

        type_counts, ttl_counts, ns_type, ns_ttl, detail_scanned = await loop.run_in_executor(
            None, _merge_phase2, results, result_dbsizes
        )
        logger.info("_run_detail_parallel: merge complete, calling _finalize_phase2 (%.1fs total)",
                    time.monotonic() - t0)
        self._finalize_phase2(type_counts, ttl_counts, ns_type, ns_ttl, detail_scanned, scan_target)
        logger.info("_run_detail_parallel: complete (%.1fs total)", time.monotonic() - t0)

    async def _run_detail_single(self, node, dbsize: int, scan_limit: int,
                                 scan_target: int, tracked_namespaces: list[str]):
        """Phase 2 in-process for standalone mode."""
        type_counts: dict[str, int] = defaultdict(int)
        ttl_counts: dict[str, int] = defaultdict(int)
        tracked = set(tracked_namespaces)
        ns_type: dict[str, dict[str, int]] = {ns: defaultdict(int) for ns in tracked}
        ns_ttl: dict[str, dict[str, int]] = {ns: defaultdict(int) for ns in tracked}
        detail_scanned = 0

        cursor = 0
        while True:
            cursor, keys = await node.scan(cursor, count=self._scan_count)
            for i in range(0, len(keys), settings.redis_pipeline_batch):
                batch = keys[i : i + settings.redis_pipeline_batch]
                pipe = node.pipeline(transaction=False)
                for key in batch:
                    pipe.type(key)
                    pipe.ttl(key)
                results = await pipe.execute()
                for j in range(0, len(results), 2):
                    key = batch[j // 2]
                    key_type = results[j]
                    bucket = classify_ttl(results[j + 1])
                    type_counts[key_type] += 1
                    ttl_counts[bucket] += 1
                    ns = key.split(":")[0] if ":" in key else "(root)"
                    if ns in tracked:
                        ns_type[ns][key_type] += 1
                        ns_ttl[ns][bucket] += 1
                detail_scanned += len(batch)

            pct = min((detail_scanned / scan_target) * 100, 100.0) if scan_target > 0 else 100.0
            self._detail_progress = ScanProgress(
                status="scanning", scanned=detail_scanned,
                total_estimate=scan_target, percent=round(pct, 1),
            )
            await self._notify_detail()
            if cursor == 0 or (scan_limit and detail_scanned >= scan_limit):
                break

        # Reverse-normalize the sampled counts up to the full-keyspace estimate.
        scale = _node_scale(dbsize, detail_scanned, self._estimate_percent)

        def _scaled(d):
            return {k: round(v * scale) for k, v in d.items()}

        type_counts_out = _scaled(type_counts)
        ttl_counts_out = _scaled(ttl_counts)
        ns_type_plain = {ns: _scaled(inner) for ns, inner in ns_type.items()}
        ns_ttl_plain = {ns: _scaled(inner) for ns, inner in ns_ttl.items()}
        estimated_scanned = round(detail_scanned * scale)
        self._finalize_phase2(
            type_counts_out, ttl_counts_out,
            ns_type_plain, ns_ttl_plain, estimated_scanned, scan_target,
        )

    def _finalize_phase2(self, type_counts, ttl_counts, ns_type, ns_ttl, detail_scanned, total_estimate):
        logger.info("_finalize_phase2: detail_scanned=%d types=%s", detail_scanned, type_counts)
        ttl_buckets = _merge_ttl_buckets(ttl_counts)

        breakdowns = []
        for ns, ns_types in ns_type.items():
            total = sum(ns_types.values())
            if total == 0:
                continue
            breakdowns.append(NamespaceBreakdown(
                namespace=ns,
                total=total,
                type_counts=ns_types,
                ttl_buckets=_merge_ttl_buckets(ns_ttl.get(ns, {})),
            ))
        breakdowns.sort(key=lambda b: b.total, reverse=True)

        if self._result:
            self._result.type_counts = type_counts
            self._result.ttl_buckets = ttl_buckets
            self._result.namespace_breakdowns = breakdowns

        self._detail_progress = ScanProgress(
            status="completed", scanned=detail_scanned,
            total_estimate=total_estimate, percent=100.0,
        )
        logger.info("_finalize_phase2: done, detail progress set to completed")


scanner = Scanner()
