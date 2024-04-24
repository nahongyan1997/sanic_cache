import typing as _t

from redis.asyncio import Redis as AIORedis

from .base import BaseCache
from .serializers import RedisSerializer


class AsyncRedisCache(BaseCache):
    """Uses the Redis key-value store as a cache backend.

    The first argument can be either a string denoting address of the Redis
    server or an object resembling an instance of a redis.Redis class.

    Note: Python Redis API already takes care of encoding unicode strings on
    the fly.

    :param host: address of the Redis server or an object which API is
                 compatible with the official Python Redis client (redis-py).
    :param port: port number on which Redis server listens for connections.
    :param password: password authentication for the Redis server.
    :param db: db (zero-based numeric index) on Redis Server to connect.
    :param default_timeout: the default timeout that is used if no timeout is
                            specified on :meth:`~BaseCache.set`. A timeout of
                            0 indicates that the cache never expires.
    :param key_prefix: A prefix that should be added to all keys.

    Any additional keyword arguments will be passed to ``redis.Redis``.
    """

    app: AIORedis = None
    serializer = RedisSerializer()

    def __init__(
        self,
        host: _t.Any = "localhost",
        port: int = 6379,
        password: _t.Optional[str] = None,
        db: int = 0,
        default_timeout: int = 300,
        key_prefix: _t.Optional[str] = None,
        **kwargs: _t.Any
    ):
        BaseCache.__init__(self, default_timeout)
        if host is None:
            raise ValueError("RedisCache host parameter may not be None")

        if isinstance(host, str):
            self.async_cache: AIORedis = AIORedis(
                host=host, port=port, password=password, db=db, **kwargs
            )
        else:
            self.async_cache: AIORedis = host
        self.key_prefix = key_prefix or ""

    def _normalize_timeout(self, timeout: _t.Optional[int]) -> int:
        """Normalize timeout by setting it to default of 300 if
        not defined (None) or -1 if explicitly set to zero.

        :param timeout: timeout to normalize.
        """
        timeout = BaseCache._normalize_timeout(self, timeout)
        if timeout == 0:
            timeout = -1
        return timeout

    def __getattr__(self, item):
        return getattr(self.async_cache, item)

    async def get(self, key: str) -> _t.Any:
        return await self.serializer.loads(
            await self.async_cache.get(self.key_prefix + key)
        )

    async def get_many(self, *keys: str) -> _t.List[_t.Any]:
        if self.key_prefix:
            prefixed_keys = [self.key_prefix + key for key in keys]
        else:
            prefixed_keys = list(keys)
        return [
            await self.serializer.loads(x)
            for x in await self.async_cache.mget(prefixed_keys)
        ]

    async def set(
        self, key: str, value: _t.Any, timeout: _t.Optional[int] = None
    ) -> _t.Any:
        timeout = self._normalize_timeout(timeout)
        dump = await self.serializer.dumps(value)
        if timeout == -1:
            result = await self.async_cache.set(name=self.key_prefix + key, value=dump)
        else:
            result = await self.async_cache.setex(
                name=self.key_prefix + key, value=dump, time=timeout
            )
        return result

    async def add(
        self, key: str, value: _t.Any, timeout: _t.Optional[int] = None
    ) -> _t.Any:
        timeout = self._normalize_timeout(timeout)
        dump = await self.serializer.dumps(value)
        created = await self.async_cache.setnx(name=self.key_prefix + key, value=dump)
        # handle case where timeout is explicitly set to zero
        if created and timeout != -1:
            await self.async_cache.expire(name=self.key_prefix + key, time=timeout)
        return created

    async def set_many(
        self, mapping: _t.Dict[str, _t.Any], timeout: _t.Optional[int] = None
    ) -> _t.List[_t.Any]:
        timeout = self._normalize_timeout(timeout)
        # Use transaction=False to batch without calling redis MULTI
        # which is not supported by twemproxy
        pipe = await self.async_cache.pipeline(transaction=False)

        for key, value in mapping.items():
            dump = await self.serializer.dumps(value)
            if timeout == -1:
                await pipe.set(name=self.key_prefix + key, value=dump)
            else:
                await pipe.setex(name=self.key_prefix + key, value=dump, time=timeout)
        results = await pipe.execute()
        res = zip(mapping.keys(), results)  # noqa: B905
        return [k for k, was_set in res if was_set]

    async def delete(self, key: str) -> bool:
        return bool(await self.async_cache.delete(self.key_prefix + key))

    async def delete_many(self, *keys: str) -> _t.List[_t.Any]:
        if not keys:
            return []
        if self.key_prefix:
            prefixed_keys = [self.key_prefix + key for key in keys]
        else:
            prefixed_keys = [k for k in keys]
        await self.async_cache.delete(*prefixed_keys)
        return [k for k in prefixed_keys if not await self.has(k)]

    async def has(self, key: str) -> bool:
        return bool(await self.async_cache.exists(self.key_prefix + key))

    async def clear(self) -> bool:
        status = 0
        if self.key_prefix:
            keys = await self.async_cache.keys(self.key_prefix + "*")
            if keys:
                status = await self.async_cache.delete(*keys)
        else:
            status = await self.async_cache.flushdb()
        return bool(status)

    async def inc(self, key: str, delta: int = 1) -> _t.Any:
        return await self.async_cache.incr(name=self.key_prefix + key, amount=delta)

    async def dec(self, key: str, delta: int = 1) -> _t.Any:
        return await self.async_cache.incr(name=self.key_prefix + key, amount=-delta)
