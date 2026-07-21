import asyncio
import fnmatch
import logging
import multiprocessing
import os
import time
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor

from app.config import settings
from app.models.scan_result import PrefixSuggestion, ScanProgress, ScanResult, TTLBucket
from app.services.prefix_trie import PrefixTree
from app.services.redis_client import redis_client
from app.services.scan_worker import scan_worker_phase1, scan_worker_phase2

logger = logging.getLogger(__name__)

TTL_BUCKET_RANGES = [
    ("no TTL", -1, -1),
    ("<1 min", 0, 60),
    ("1-10 min", 60, 600),
    ("10 min - 1 hr", 600, 3600),
    ("1-24 hr", 3600, 86400),
    (">24 hr", 86400, float("inf")),
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
    return ">24 hr"


def extract_namespace(key: str, delimiter: str = ":") -> str:
    parts = key.split(delimiter)
    if len(parts) > 1:
        return parts[0]
    return "(root)"


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

    async def start_scan(self, scan_count: int | None = None):
        if self._task and not self._task.done():
            logger.info("start_scan called but scan already running — ignoring")
            return
        self._scan_count = scan_count or settings.redis_scan_count
        logger.info("start_scan: scan_count=%d", self._scan_count)
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
        total_estimate = sum(s for s in dbsizes if isinstance(s, int))
        logger.info("_run_scan: total_estimate=%d", total_estimate)

        self._progress = ScanProgress(
            status="scanning", scanned=0, total_estimate=total_estimate, percent=0.0
        )
        await self._notify()

        if is_cluster:
            await self._run_scan_parallel(nodes, total_estimate)
        else:
            await self._run_scan_single(nodes[0], total_estimate)

        logger.info("_run_scan: phase 1 done in %.1fs, notifying and starting detail scan",
                    time.monotonic() - t0)
        await self._notify()
        self._start_detail_scan()

    async def _run_scan_parallel(self, nodes, total_estimate: int):
        """Phase 1 with multiprocessing — one process per node."""
        t0 = time.monotonic()
        node_params = self._get_node_params()
        max_workers = min(len(node_params), os.cpu_count() or 4)
        logger.info("_run_scan_parallel: %d nodes, %d workers, ~%d keys, cpu_count=%s",
                    len(node_params), max_workers, total_estimate, os.cpu_count())

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
                    pct = min((total / total_estimate) * 100, 100.0) if total_estimate > 0 else 100.0
                    self._progress = ScanProgress(
                        status="scanning", scanned=total,
                        total_estimate=total_estimate, percent=round(pct, 1),
                    )
                    await self._notify()
                    await asyncio.sleep(0.1)

            logger.info("_run_scan_parallel: starting poll_progress task, awaiting all workers")
            progress_task = asyncio.create_task(poll_progress())
            results = await asyncio.gather(*futures, return_exceptions=True)
            progress_task.cancel()
            logger.info("_run_scan_parallel: all workers returned in %.1fs", time.monotonic() - t0)

            for i, r in enumerate(results):
                if isinstance(r, Exception):
                    logger.error("Phase 1 worker %d failed: %s", i, r, exc_info=r)
                else:
                    logger.info("Phase 1 worker %d ok: scanned=%d namespaces=%d",
                                i, r.get("scanned", 0), len(r.get("namespace_counts", {})))
            results = [r for r in results if not isinstance(r, Exception)]
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

        def _merge_phase1(results, patterns):
            mt0 = time.monotonic()
            logger.info("_merge_phase1: starting manager.shutdown()")
            manager.shutdown()
            logger.info("_merge_phase1: manager.shutdown() done in %.1fs", time.monotonic() - mt0)
            namespace_counts: dict[str, int] = {}
            pattern_counts: dict[str, int] = {p: 0 for p in patterns}
            merged_tree = PrefixTree(max_depth=settings.prefix_tree_max_depth, min_count=50)
            total_scanned = 0
            for idx, r in enumerate(results):
                for ns, count in r["namespace_counts"].items():
                    namespace_counts[ns] = namespace_counts.get(ns, 0) + count
                for pat, count in r["pattern_counts"].items():
                    if pat in pattern_counts:
                        pattern_counts[pat] += count
                paths = r["prefix_tree_paths"]
                key_count = r["prefix_tree_key_count"]
                logger.info("_merge_phase1: merging worker %d paths=%d keys=%d", idx, len(paths), key_count)
                merged_tree.merge_path_counts(paths, key_count)
                logger.info("_merge_phase1: worker %d merged (%.1fs)", idx, time.monotonic() - mt0)
                total_scanned += r["scanned"]
            logger.info("_merge_phase1: all merges done, total_scanned=%d (%.1fs)",
                        total_scanned, time.monotonic() - mt0)
            return namespace_counts, pattern_counts, merged_tree, total_scanned

        namespace_counts, pattern_counts, merged_tree, total_scanned = await loop.run_in_executor(
            None, _merge_phase1, results, list(self._patterns)
        )
        logger.info("_run_scan_parallel: merge done, calling _finalize_phase1 (%.1fs total)",
                    time.monotonic() - t0)
        self._finalize_phase1(namespace_counts, pattern_counts, merged_tree, total_scanned, total_estimate)
        logger.info("_run_scan_parallel: complete (%.1fs total)", time.monotonic() - t0)

    async def _run_scan_single(self, node, total_estimate: int):
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
            pct = min((scanned / total_estimate) * 100, 100.0) if total_estimate > 0 else 100.0
            self._progress = ScanProgress(
                status="scanning", scanned=scanned,
                total_estimate=total_estimate, percent=round(pct, 1),
            )
            await self._notify()
            if cursor == 0:
                break

        self._finalize_phase1(dict(namespace_counts), dict(pattern_counts), prefix_tree, scanned, total_estimate)

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
        total_estimate = sum(s for s in dbsizes if isinstance(s, int))
        logger.info("_run_detail_scan: total_estimate=%d", total_estimate)

        self._detail_progress = ScanProgress(
            status="scanning", scanned=0, total_estimate=total_estimate, percent=0.0
        )
        await self._notify_detail()

        if is_cluster:
            await self._run_detail_parallel(nodes, total_estimate)
        else:
            await self._run_detail_single(nodes[0], total_estimate)

        logger.info("_run_detail_scan: done in %.1fs, sending final notify", time.monotonic() - t0)
        await self._notify_detail()

    async def _run_detail_parallel(self, nodes, total_estimate: int):
        """Phase 2 with multiprocessing — one process per node."""
        t0 = time.monotonic()
        node_params = self._get_node_params()
        max_workers = min(len(node_params), os.cpu_count() or 4)
        logger.info("_run_detail_parallel: %d nodes, %d workers, ~%d keys",
                    len(node_params), max_workers, total_estimate)

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
                    pct = min((total / total_estimate) * 100, 100.0) if total_estimate > 0 else 100.0
                    self._detail_progress = ScanProgress(
                        status="scanning", scanned=total,
                        total_estimate=total_estimate, percent=round(pct, 1),
                    )
                    await self._notify_detail()
                    await asyncio.sleep(0.1)

            logger.info("_run_detail_parallel: awaiting all workers")
            progress_task = asyncio.create_task(poll_progress())
            results = await asyncio.gather(*futures, return_exceptions=True)
            progress_task.cancel()
            logger.info("_run_detail_parallel: all workers returned in %.1fs", time.monotonic() - t0)

            for i, r in enumerate(results):
                if isinstance(r, Exception):
                    logger.error("Phase 2 worker %d failed: %s", i, r, exc_info=r)
                else:
                    logger.info("Phase 2 worker %d ok: scanned=%d types=%s",
                                i, r.get("scanned", 0), dict(r.get("type_counts", {})))
            results = [r for r in results if not isinstance(r, Exception)]
            logger.info("_run_detail_parallel: %d/%d workers succeeded",
                        len(results), len(node_params))

        logger.info("_run_detail_parallel: ProcessPoolExecutor exited (%.1fs)", time.monotonic() - t0)
        logger.info("_run_detail_parallel: starting merge in thread")

        def _merge_phase2(results):
            mt0 = time.monotonic()
            logger.info("_merge_phase2: starting manager.shutdown()")
            manager.shutdown()
            logger.info("_merge_phase2: manager.shutdown() done in %.1fs", time.monotonic() - mt0)
            type_counts: dict[str, int] = {}
            ttl_counts: dict[str, int] = {}
            detail_scanned = 0
            for r in results:
                for t, count in r["type_counts"].items():
                    type_counts[t] = type_counts.get(t, 0) + count
                for t, count in r["ttl_counts"].items():
                    ttl_counts[t] = ttl_counts.get(t, 0) + count
                detail_scanned += r["scanned"]
            logger.info("_merge_phase2: done, detail_scanned=%d (%.1fs)",
                        detail_scanned, time.monotonic() - mt0)
            return type_counts, ttl_counts, detail_scanned

        type_counts, ttl_counts, detail_scanned = await loop.run_in_executor(
            None, _merge_phase2, results
        )
        logger.info("_run_detail_parallel: merge complete, calling _finalize_phase2 (%.1fs total)",
                    time.monotonic() - t0)
        self._finalize_phase2(type_counts, ttl_counts, detail_scanned, total_estimate)
        logger.info("_run_detail_parallel: complete (%.1fs total)", time.monotonic() - t0)

    async def _run_detail_single(self, node, total_estimate: int):
        """Phase 2 in-process for standalone mode."""
        type_counts: dict[str, int] = defaultdict(int)
        ttl_counts: dict[str, int] = defaultdict(int)
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
                    key_type = results[j]
                    key_ttl = results[j + 1]
                    type_counts[key_type] += 1
                    ttl_counts[classify_ttl(key_ttl)] += 1
                detail_scanned += len(batch)

            pct = min((detail_scanned / total_estimate) * 100, 100.0) if total_estimate > 0 else 100.0
            self._detail_progress = ScanProgress(
                status="scanning", scanned=detail_scanned,
                total_estimate=total_estimate, percent=round(pct, 1),
            )
            await self._notify_detail()
            if cursor == 0:
                break

        self._finalize_phase2(dict(type_counts), dict(ttl_counts), detail_scanned, total_estimate)

    def _finalize_phase2(self, type_counts, ttl_counts, detail_scanned, total_estimate):
        logger.info("_finalize_phase2: detail_scanned=%d types=%s", detail_scanned, type_counts)
        ttl_buckets = [
            TTLBucket(label=label, count=ttl_counts.get(label, 0))
            for label, _, _ in TTL_BUCKET_RANGES
        ]

        if self._result:
            self._result.type_counts = type_counts
            self._result.ttl_buckets = ttl_buckets

        self._detail_progress = ScanProgress(
            status="completed", scanned=detail_scanned,
            total_estimate=total_estimate, percent=100.0,
        )
        logger.info("_finalize_phase2: done, detail progress set to completed")


scanner = Scanner()
