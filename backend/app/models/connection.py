from pydantic import BaseModel, field_validator

from app.config import AUTO_DOMAIN


class ConnectionRequest(BaseModel):
    host: str = "localhost"
    port: int = 6379
    username: str | None = None
    password: str | None = None
    db: int = 0

    @field_validator("host")
    @classmethod
    def append_domain(cls, v: str) -> str:
        if not v.endswith(AUTO_DOMAIN):
            return v + AUTO_DOMAIN
        return v


class ConnectionInfo(BaseModel):
    redis_version: str
    connected_clients: int
    used_memory_human: str
    total_keys: int
    uptime_in_seconds: int
    cluster_mode: bool = False
    node_count: int = 1
