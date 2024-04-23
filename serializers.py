import pickle
import typing as _t


class RedisSerializer:
    """Default serializer for RedisCache."""

    async def dumps(
        self, value: _t.Any, protocol: int = pickle.HIGHEST_PROTOCOL
    ) -> bytes:
        """Dumps an object into a string for redis. By default it serializes
        integers as regular string and pickle dumps everything else.
        """
        return b"!" + pickle.dumps(value, protocol)

    async def loads(self, value: _t.Optional[bytes]) -> _t.Any:
        """The reversal of :meth:`dump_object`. This might be called with
        None.
        """
        if value is None:
            return None
        if value.startswith(b"!"):
            try:
                return pickle.loads(value[1:])
            except pickle.PickleError:
                return None
        try:
            return int(value)
        except ValueError:
            # before 0.8 we did not have serialization. Still support that.
            return value
