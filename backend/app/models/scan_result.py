from pydantic import BaseModel


class ScanProgress(BaseModel):
    status: str  # "scanning", "completed", "idle"
    scanned: int = 0
    total_estimate: int = 0
    percent: float = 0.0


class TTLBucket(BaseModel):
    label: str
    count: int


class PrefixSuggestion(BaseModel):
    prefix: str
    key_count: int
    depth: int
    child_count: int
    coverage_pct: float


class NamespaceBreakdown(BaseModel):
    namespace: str
    total: int
    type_counts: dict[str, int]
    ttl_buckets: list[TTLBucket]


class ScanResult(BaseModel):
    total_keys: int
    type_counts: dict[str, int]
    ttl_buckets: list[TTLBucket]
    namespace_counts: dict[str, int]
    pattern_counts: dict[str, int]
    suggested_prefixes: list[PrefixSuggestion] = []
    namespace_breakdowns: list[NamespaceBreakdown] = []
