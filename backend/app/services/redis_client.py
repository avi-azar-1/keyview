import redis.asyncio as aioredis

from app.models.connection import ConnectionRequest, ConnectionInfo


class RedisClient:
    def __init__(self):
        self._pool: aioredis.Redis | None = None
        self._connection_params: ConnectionRequest | None = None

    @property
    def connected(self) -> bool:
        return self._pool is not None

    @property
    def pool(self) -> aioredis.Redis:
        if self._pool is None:
            raise RuntimeError("Not connected to Redis")
        return self._pool

    async def connect(self, params: ConnectionRequest) -> ConnectionInfo:
        self._pool = aioredis.Redis(
            host=params.host,
            port=params.port,
            username=params.username,
            password=params.password,
            db=params.db,
            decode_responses=True,
        )
        await self._pool.ping()
        self._connection_params = params
        return await self.get_info()

    async def disconnect(self):
        if self._pool:
            await self._pool.aclose()
            self._pool = None
            self._connection_params = None

    async def get_info(self) -> ConnectionInfo:
        info = await self.pool.info()
        dbsize = await self.pool.dbsize()
        return ConnectionInfo(
            redis_version=info.get("redis_version", "unknown"),
            connected_clients=info.get("connected_clients", 0),
            used_memory_human=info.get("used_memory_human", "0B"),
            total_keys=dbsize,
            uptime_in_seconds=info.get("uptime_in_seconds", 0),
        )


redis_client = RedisClient()
