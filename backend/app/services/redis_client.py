import redis.asyncio as aioredis

from app.models.connection import ConnectionRequest, ConnectionInfo


class RedisClient:
    def __init__(self):
        self._client: aioredis.Redis | aioredis.RedisCluster | None = None
        self._connection_params: ConnectionRequest | None = None
        self._is_cluster: bool = False
        self._node_connections: list[aioredis.Redis] = []

    @property
    def connected(self) -> bool:
        return self._client is not None

    @property
    def is_cluster(self) -> bool:
        return self._is_cluster

    @property
    def client(self) -> aioredis.Redis | aioredis.RedisCluster:
        if self._client is None:
            raise RuntimeError("Not connected to Redis")
        return self._client

    @property
    def pool(self) -> aioredis.Redis | aioredis.RedisCluster:
        return self.client

    async def connect(self, params: ConnectionRequest) -> ConnectionInfo:
        standalone = aioredis.Redis(
            host=params.host,
            port=params.port,
            username=params.username,
            password=params.password,
            db=params.db,
            decode_responses=True,
        )

        if params.cluster_mode is not None:
            is_cluster = params.cluster_mode
            if not is_cluster:
                await standalone.ping()
        else:
            try:
                await standalone.ping()
                cluster_info = await standalone.execute_command("CLUSTER", "INFO")
                is_cluster = "cluster_enabled:1" in cluster_info if isinstance(cluster_info, str) else False
            except Exception:
                is_cluster = False

        if is_cluster:
            await standalone.aclose()
            self._client = aioredis.RedisCluster(
                host=params.host,
                port=params.port,
                username=params.username,
                password=params.password,
                decode_responses=True,
            )
            await self._client.ping()
            self._is_cluster = True
        else:
            self._client = standalone
            self._is_cluster = False

        self._connection_params = params
        return await self.get_info()

    async def disconnect(self):
        for node in self._node_connections:
            await node.aclose()
        self._node_connections = []
        if self._client:
            await self._client.aclose()
            self._client = None
            self._connection_params = None
            self._is_cluster = False

    async def get_primary_nodes(self) -> list[aioredis.Redis]:
        """Return cached connections to each primary node in the cluster."""
        if not self._is_cluster or not isinstance(self._client, aioredis.RedisCluster):
            return [self._client]

        if self._node_connections:
            return self._node_connections

        nodes = []
        for node in self._client.get_primaries():
            r = aioredis.Redis(
                host=node.host,
                port=node.port,
                username=self._connection_params.username,
                password=self._connection_params.password,
                decode_responses=True,
            )
            nodes.append(r)
        self._node_connections = nodes
        return nodes

    async def get_info(self) -> ConnectionInfo:
        if self._is_cluster and isinstance(self._client, aioredis.RedisCluster):
            return await self._get_cluster_info()
        return await self._get_standalone_info()

    async def _get_standalone_info(self) -> ConnectionInfo:
        info = await self._client.info()
        dbsize = await self._client.dbsize()
        return ConnectionInfo(
            redis_version=info.get("redis_version", "unknown"),
            connected_clients=info.get("connected_clients", 0),
            used_memory_human=info.get("used_memory_human", "0B"),
            total_keys=dbsize,
            uptime_in_seconds=info.get("uptime_in_seconds", 0),
            cluster_mode=False,
            node_count=1,
        )

    async def _get_cluster_info(self) -> ConnectionInfo:
        import asyncio

        primary_nodes = await self.get_primary_nodes()
        total_keys = 0
        total_clients = 0
        total_memory = 0
        version = "unknown"
        uptime = 0

        async def fetch_node(node):
            info = await node.info()
            size = await node.dbsize()
            return info, size

        results = await asyncio.gather(
            *[fetch_node(node) for node in primary_nodes], return_exceptions=True
        )

        for r in results:
            if isinstance(r, Exception):
                continue
            node_info, node_dbsize = r
            version = node_info.get("redis_version", version)
            total_clients += node_info.get("connected_clients", 0)
            total_memory += node_info.get("used_memory", 0)
            uptime = max(uptime, node_info.get("uptime_in_seconds", 0))
            total_keys += node_dbsize

        if total_memory >= 1073741824:
            mem_human = f"{total_memory / 1073741824:.2f}G"
        elif total_memory >= 1048576:
            mem_human = f"{total_memory / 1048576:.2f}M"
        elif total_memory >= 1024:
            mem_human = f"{total_memory / 1024:.2f}K"
        else:
            mem_human = f"{total_memory}B"

        return ConnectionInfo(
            redis_version=version,
            connected_clients=total_clients,
            used_memory_human=mem_human,
            total_keys=total_keys,
            uptime_in_seconds=uptime,
            cluster_mode=True,
            node_count=len(primary_nodes),
        )


redis_client = RedisClient()
