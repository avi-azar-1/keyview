import asyncio
import fnmatch
from collections import defaultdict

from app.config import settings
from app.models.scan_result import ScanProgress, ScanResult, TTLBucket
from app.services.redis_client import redis_client

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
        self._result: ScanResult | None = None
        self._task: asyncio.Task | None = None
        self._listeners: list[asyncio.Queue] = []
        self._patterns: list[str] = []

    @property
    def progress(self) -> ScanProgress:
        return self._progress

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

    async def _notify(self):
        for q in self._listeners:
            await q.put(self._progress.model_dump())

    async def start_scan(self, scan_count: int | None = None):
        if self._task and not self._task.done():
            return
        self._scan_count = scan_count or settings.redis_scan_count
        self._task = asyncio.create_task(self._run_scan())

    async def _run_scan(self):
        nodes = await redis_client.get_primary_nodes()

        total_estimate = 0
        for node in nodes:
            try:
                total_estimate += await node.dbsize()
            except Exception:
                pass

        self._progress = ScanProgress(
            status="scanning", scanned=0, total_estimate=total_estimate, percent=0.0
        )
        await self._notify()

        type_counts: dict[str, int] = defaultdict(int)
        ttl_counts: dict[str, int] = defaultdict(int)
        namespace_counts: dict[str, int] = defaultdict(int)
        pattern_counts: dict[str, int] = defaultdict(int)
        scanned = 0

        for node in nodes:
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

                    for j, key in enumerate(batch):
                        key_type = results[j * 2]
                        key_ttl = results[j * 2 + 1]

                        type_counts[key_type] += 1
                        ttl_counts[classify_ttl(key_ttl)] += 1
                        namespace_counts[extract_namespace(key)] += 1

                        for pat in self._patterns:
                            if fnmatch.fnmatch(key, pat):
                                pattern_counts[pat] += 1
                                break

                    scanned += len(batch)
                    pct = min((scanned / total_estimate) * 100, 100.0) if total_estimate > 0 else 100.0
                    self._progress = ScanProgress(
                        status="scanning",
                        scanned=scanned,
                        total_estimate=total_estimate,
                        percent=round(pct, 1),
                    )
                    await self._notify()

                if cursor == 0:
                    break

        ttl_buckets = [
            TTLBucket(label=label, count=ttl_counts.get(label, 0))
            for label, _, _ in TTL_BUCKET_RANGES
        ]

        self._result = ScanResult(
            total_keys=scanned,
            type_counts=dict(type_counts),
            ttl_buckets=ttl_buckets,
            namespace_counts=dict(namespace_counts),
            pattern_counts=dict(pattern_counts),
        )

        self._progress = ScanProgress(
            status="completed",
            scanned=scanned,
            total_estimate=total_estimate,
            percent=100.0,
        )
        await self._notify()


scanner = Scanner()
