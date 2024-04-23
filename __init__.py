# -*- coding: utf-8 -*-
"""
    sanic.ctx.cache
    ~~~~~~~~~~~~~~

    Adds cache support to your application.

    :copyright: (c) 2024 by Na Haha.
    :license: BSD, see LICENSE for more details
"""

__version__ = "0.1.0"
__versionfull__ = __version__

import base64
import functools
import hashlib
import inspect
import pickle
import string
import traceback
import uuid
import warnings
from typing import Literal, Optional, Union

from redis.asyncio import Redis as AIORedis, Redis
from sanic import json as s_json, Request, HTTPResponse, Sanic
from sanic_ext.utils.extraction import extract_request

from .redis import AsyncRedisCache

# Used to remove control characters and whitespace from cache keys.
valid_chars = set(string.ascii_letters + string.digits + "_.")
delchars = "".join(c for c in map(chr, range(256)) if c not in valid_chars)

null_control = (dict((k, None) for k in delchars),)


current_app: Sanic = Sanic.get_app()


async def _memvname(funcname):
    return funcname + "_memver"


async def _memoize_make_version_hash():
    return base64.b64encode(uuid.uuid4().bytes)[:6].decode("utf-8")


async def memoize_kwargs_to_args(f, *args, **kwargs):
    #: Inspect the arguments to the function
    #: This allows the memoization to be the same
    #: whether the function was called with
    #: 1, b=2 is equivilant to a=1, b=2, etc.
    new_args = []
    arg_num = 0
    argspec = inspect.getfullargspec(f)

    args_len = len(argspec.args)
    for i in range(args_len):
        if i == 0 and argspec.args[i] in ("self", "cls"):
            #: use the repr of the class instance
            #: this supports instance methods for
            #: the memoized functions, giving more
            #: flexibility to developers
            arg = repr(args[0])
            arg_num += 1
        elif argspec.args[i] in kwargs:
            arg = kwargs[argspec.args[i]]
        elif arg_num < len(args):
            arg = args[arg_num]
            arg_num += 1
        elif argspec.defaults is not None and abs(i - args_len) <= len(
            argspec.defaults
        ):
            arg = argspec.defaults[i - args_len]
            arg_num += 1
        else:
            arg = None
            arg_num += 1

        #: Attempt to convert all arguments to a
        #: hash/id or a representation?
        #: Not sure if this is necessary, since
        #: using objects as keys gets tricky quickly.
        # if hasattr(arg, '__class__'):
        #     try:
        #         arg = hash(arg)
        #     except:
        #         arg = repr(arg)

        #: Or what about a special __cacherepr__ function
        #: on an object, this allows objects to act normal
        #: upon inspection, yet they can define a representation
        #: that can be used to make the object unique in the
        #: cache key. Given that a case comes across that
        #: an object "must" be used as a cache key
        # if hasattr(arg, '__cacherepr__'):
        #     arg = arg.__cacherepr__

        new_args.append(arg)

    return tuple(new_args), {}


async def function_namespace(f, args=None):
    """
    Attempts to returns unique namespace for function
    """
    m_args = inspect.getfullargspec(f)[0]
    instance_token = None

    instance_self = getattr(f, "__self__", None)

    if instance_self and not inspect.isclass(instance_self):
        instance_token = repr(f.__self__)
    elif m_args and m_args[0] == "self" and args:
        instance_token = repr(args[0])

    module = f.__module__

    if hasattr(f, "__qualname__"):
        name = f.__qualname__
    else:
        klass = getattr(f, "__self__", None)

        if klass and not inspect.isclass(klass):
            klass = klass.__class__

        if not klass:
            klass = getattr(f, "im_class", None)

        if not klass:
            if m_args and args:
                if m_args[0] == "self":
                    klass = args[0].__class__
                elif m_args[0] == "cls":
                    klass = args[0]

        if klass:
            name = klass.__name__ + "." + f.__name__
        else:
            name = f.__name__

    ns = ".".join((module, name))
    ns = ns.translate(*null_control)

    if instance_token:
        ins = ".".join((module, name, instance_token))
        ins = ins.translate(*null_control)
    else:
        ins = None

    return ns, ins


def e_make_cache_key(f: callable, *args, **kwargs):
    key = ".".join((f.__module__, f.__qualname__))

    for arg in args:
        key += base64.b64encode(pickle.dumps(arg)).decode()

    for kwarg in kwargs.items():
        key += base64.b64encode(pickle.dumps(kwarg)).decode()

    return key


#: Cache Object
################


class Cache(AsyncRedisCache):
    """
    This class is used to control the cache objects.
    """

    retry_time_prefix = "retry-times-limit:"
    locker_prefix = "redis-locker:"
    async_cache: AIORedis

    def __init__(self, app=None, config=None):
        if not (config is None or isinstance(config, dict)):
            raise ValueError("`config` must be an instance of dict or None")

        self.config = config

        self.app = app
        if app is not None:
            self.init_app(app)

    def init_app(self, app):
        "This is used to initialize cache with your app object"

        if "extensions" not in app.__dir__():
            app.ctx.extensions = {}

        app.ctx.extensions["cache"]: Cache = self

        config = app.config.copy()

        if self.config:
            config.update(self.config)

        config.setdefault(
            "CACHE_DEFAULT_TIMEOUT", getattr(app.config, "CACHE_DEFAULT_TIMEOUT", 300)
        )
        config.setdefault(
            "CACHE_THRESHOLD", getattr(app.config, "CACHE_THRESHOLD", 500)
        )
        config.setdefault(
            "CACHE_KEY_PREFIX", getattr(app.config, "CACHE_KEY_PREFIX", "sanic_cache_")
        )
        config.setdefault(
            "CACHE_MEMCACHED_SERVERS",
            getattr(app.config, "CACHE_MEMCACHED_SERVERS", None),
        )
        config.setdefault("CACHE_DIR", getattr(app.config, "CACHE_DIR", None))
        config.setdefault("CACHE_OPTIONS", getattr(app.config, "CACHE_OPTIONS", None))
        config.setdefault("CACHE_ARGS", getattr(app.config, "CACHE_ARGS", []))
        config.setdefault("CACHE_TYPE", getattr(app.config, "CACHE_TYPE", None))
        config.setdefault(
            "CACHE_NO_NULL_WARNING", getattr(app.config, "CACHE_NO_NULL_WARNING", False)
        )

        self.config = config

        if config["CACHE_TYPE"] is None and not config["CACHE_NO_NULL_WARNING"]:
            warnings.warn(
                "Sanic-Cache: CACHE_TYPE is set to null, "
                "caching is effectively disabled."
            )
            return

        cache_options = {"default_timeout": config["CACHE_DEFAULT_TIMEOUT"]}

        if config["CACHE_OPTIONS"]:
            cache_options.update(config["CACHE_OPTIONS"])

        config.update(
            dict(
                host=getattr(app.config, "CACHE_REDIS_HOST", "localhost"),
                port=getattr(app.config, "CACHE_REDIS_PORT", 6379),
            )
        )
        password = getattr(app.config, "CACHE_REDIS_PASSWORD", None)
        if password:
            config["password"] = password

        key_prefix = config["CACHE_KEY_PREFIX"]
        if key_prefix:
            config["key_prefix"] = key_prefix

        db_number = getattr(app.config, "CACHE_REDIS_DB", None)
        if db_number:
            config["db"] = db_number

        redis_url = getattr(app.config, "CACHE_REDIS_URL", None)
        if redis_url:
            config["host"] = Redis.from_url(
                redis_url,
                db=config.pop("db", None),
            )

        super().__init__(
            host=config.get("CACHE_REDIS_HOST") or config.get("host"),
            port=config.get("CACHE_REDIS_PORT"),
            password=config.get("CACHE_REDIS_PASSWORD") or config.get("password"),
            db=config.get("CACHE_REDIS_DB") or config.get("db"),
            key_prefix=config.get("CACHE_KEY_PREFIX") or config.get("key_prefix"),
            **cache_options,
        )

    # 免序列化的读写方法
    async def eget(self, key: str):
        return await self.async_cache.get(self.key_prefix + key)

    async def eset(self, key: str, value, timeout=None):
        if timeout is None:
            result = await self.async_cache.set(name=self.key_prefix + key, value=value)
        else:
            result = await self.async_cache.setex(
                name=self.key_prefix + key, value=value, time=timeout
            )
        return result

    async def expire(self, key, *args, **kwargs):
        return await self.async_cache.expire(self.key_prefix + key, *args, **kwargs)

    async def ttl(self, key):
        return await self.async_cache.ttl(self.key_prefix + key)

    async def delete_prefix(self, prefix):
        cursor = None
        while cursor != 0:
            try:
                cursor, keys = await self.async_cache.scan(
                    cursor, match=f"{self.key_prefix}{prefix}*"
                )
                for key in keys:
                    # 删除以特定前缀开头的键
                    key = key.decode()

                    await self.async_cache.delete(key)
            except Exception as err:
                raise err

    def cached(
        self, timeout=None, key_prefix: [str, callable] = "view/%s", unless=None
    ):
        """
        Decorator. Use this to cache a function. By default the cache key
        is `view/request.path`. You are able to use this decorator with any
        function by changing the `key_prefix`. If the token `%s` is located
        within the `key_prefix` then it will replace that with `request.path`

        Example::

            # An example view function
            @cache.cached(timeout=50)
            def big_foo():
                return big_bar_calc()

            # An example misc function to cache.
            @cache.cached(key_prefix='MyCachedList')
            def get_list():
                return [random.randrange(0, 1) for i in range(50000)]

            my_list = get_list()

        .. note::

            You MUST have a request context to actually called any functions
            that are cached.

        .. versionadded:: 0.4
            The returned decorated function now has three function attributes
            assigned to it. These attributes are readable/writable.

                **uncached**
                    The original undecorated function

                **cache_timeout**
                    The cache timeout value for this function. For a custom value
                    to take affect, this must be set before the function is called.

                **make_cache_key**
                    A function used in generating the cache_key used.

        :param timeout: Default None. If set to an integer, will cache for that
                        amount of time. Unit of time is in seconds.
        :param key_prefix: Default 'view/%(request.path)s'. Beginning key to .
                           use for the cache key.

                           .. versionadded:: 0.3.4
                               Can optionally be a callable which takes no arguments
                               but returns a string that will be used as the cache_key.

        :param unless: Default None. Cache will *always* execute the caching
                       facilities unless this callable is true.
                       This will bypass the caching entirely.
        """

        def decorator(f):
            @functools.wraps(f)
            async def decorated_function(*args, **kwargs):
                if callable(unless) and unless() is True:
                    return await f(*args, **kwargs)

                try:
                    cache_key = await decorated_function.make_cache_key(*args, **kwargs)
                    rv = await self.get(cache_key)
                except Exception:
                    if current_app.debug:
                        raise
                    current_app.ctx.logger.exception(
                        "Exception possibly due to cache backend."
                    )
                    return await f(*args, **kwargs)

                if rv is None:
                    rv = await f(*args, **kwargs)
                    try:
                        await self.set(
                            cache_key, rv, timeout=decorated_function.cache_timeout
                        )
                    except Exception:
                        if current_app.debug:
                            raise
                        current_app.ctx.logger.exception(
                            "Exception possibly due to cache backend."
                        )
                return rv

            async def make_cache_key(*args, **kwargs):
                if callable(key_prefix):
                    cache_key = key_prefix(*args, **kwargs)
                elif "%s" in key_prefix:
                    cache_key = key_prefix % Request.get_current().path
                else:
                    cache_key = key_prefix

                return cache_key

            decorated_function.uncached = f
            decorated_function.cache_timeout = timeout
            decorated_function.make_cache_key = make_cache_key

            return decorated_function

        return decorator

    async def memoize_version(
        self, f, args=None, reset=False, delete=False, timeout=None
    ):
        """
        Updates the hash version associated with a memoized function or method.
        """
        fname, instance_fname = await function_namespace(f, args=args)
        version_key = await _memvname(fname)
        fetch_keys = [version_key]

        if instance_fname:
            instance_version_key = await _memvname(instance_fname)
            fetch_keys.append(instance_version_key)

        # Only delete the per-instance version key or per-function version
        # key but not both.
        if delete:
            await self.delete_many(fetch_keys[-1])
            return fname, None

        version_data_list = list(await self.get_many(*fetch_keys))
        dirty = False

        if version_data_list[0] is None:
            version_data_list[0] = await _memoize_make_version_hash()
            dirty = True

        if instance_fname and version_data_list[1] is None:
            version_data_list[1] = await _memoize_make_version_hash()
            dirty = True

        # Only reset the per-instance version or the per-function version
        # but not both.
        if reset:
            fetch_keys = fetch_keys[-1:]
            version_data_list = [await _memoize_make_version_hash()]
            dirty = True

        if dirty:
            await self.set_many(
                dict(zip(fetch_keys, version_data_list)), timeout=timeout
            )

        return fname, "".join(version_data_list)

    def _memoize_make_cache_key(self, make_name=None, timeout=None):
        """
        Function used to create the cache_key for memoized functions.
        """

        async def make_cache_key(f, *args, **kwargs):
            _timeout = getattr(timeout, "cache_timeout", timeout)
            fname, version_data = await self.memoize_version(
                f, args=args, timeout=_timeout
            )

            #: this should have to be after version_data, so that it
            #: does not break the delete_memoized functionality.
            if callable(make_name):
                altfname = make_name(fname)
            else:
                altfname = fname

            if callable(f):
                keyargs, keykwargs = await memoize_kwargs_to_args(f, *args, **kwargs)
            else:
                keyargs, keykwargs = args, kwargs

            try:
                updated = "{0}{1}{2}".format(altfname, keyargs, keykwargs)
            except AttributeError:
                updated = "%s%s%s" % (altfname, keyargs, keykwargs)

            cache_key = hashlib.md5()
            cache_key.update(updated.encode("utf-8"))
            cache_key = base64.b64encode(cache_key.digest())[:16]
            cache_key = cache_key.decode("utf-8")
            cache_key += version_data

            return cache_key

        return make_cache_key

    def immediately_delete_memoized(self, f):
        async def wrapper():
            await self.delete_memoized(f)

        return wrapper

    def memoize(self, timeout=None, make_name=None, unless=None):
        """
        Use this to cache the result of a function, taking its arguments into
        account in the cache key.

        Information on
        `Memoization <http://en.wikipedia.org/wiki/Memoization>`_.

        Example::

            @cache.memoize(timeout=50)
            def big_foo(a, b):
                return a + b + random.randrange(0, 1000)

        .. code-block:: pycon

            >>> big_foo(5, 2)
            753
            >>> big_foo(5, 3)
            234
            >>> big_foo(5, 2)
            753

        .. versionadded:: 0.4
            The returned decorated function now has three function attributes
            assigned to it.

                **uncached**
                    The original undecorated function. readable only

                **cache_timeout**
                    The cache timeout value for this function. For a custom value
                    to take affect, this must be set before the function is called.

                    readable and writable

                **make_cache_key**
                    A function used in generating the cache_key used.

                    readable and writable


        :param timeout: Default None. If set to an integer, will cache for that
                        amount of time. Unit of time is in seconds.
        :param make_name: Default None. If set this is a function that accepts
                          a single argument, the function name, and returns a
                          new string to be used as the function name. If not set
                          then the function name is used.
        :param unless: Default None. Cache will *always* execute the caching
                       facilities unelss this callable is true.
                       This will bypass the caching entirely.

        .. versionadded:: 0.5
            params ``make_name``, ``unless``
        """

        def memoize(f):
            @functools.wraps(f)
            async def decorated_function(*args, **kwargs):
                if callable(unless) and unless() is True:
                    return await f(*args, **kwargs)

                try:
                    cache_key = await decorated_function.make_cache_key(
                        f, *args, **kwargs
                    )
                    rv = await self.get(cache_key)
                except Exception:
                    if current_app.debug:
                        raise
                    await current_app.ctx.logger.critical(
                        "Exception possibly due to cache backend."
                    )
                    return await f(*args, **kwargs)

                if rv is None:
                    rv = await f(*args, **kwargs)
                    try:
                        await self.set(
                            cache_key, rv, timeout=decorated_function.cache_timeout
                        )
                    except Exception:
                        if current_app.debug:
                            raise
                        current_app.ctx.logger.exception(
                            "Exception possibly due to cache backend."
                        )
                return rv

            decorated_function.uncached = f
            decorated_function.cache_timeout = timeout
            decorated_function.make_cache_key = self._memoize_make_cache_key(
                make_name, decorated_function
            )
            decorated_function.delete_memoized = self.immediately_delete_memoized

            return decorated_function

        return memoize

    async def delete_memoized(self, f, *args, **kwargs):
        """
        Deletes the specified functions caches, based by given parameters.
        If parameters are given, only the functions that were memoized with them
        will be erased. Otherwise all versions of the caches will be forgotten.

        Example::

            @cache.memoize(50)
            def random_func():
                return random.randrange(1, 50)

            @cache.memoize()
            def param_func(a, b):
                return a+b+random.randrange(1, 50)

        .. code-block:: pycon

            >>> random_func()
            43
            >>> random_func()
            43
            >>> cache.delete_memoized('random_func')
            >>> random_func()
            16
            >>> param_func(1, 2)
            32
            >>> param_func(1, 2)
            47
            >>> cache.delete_memoized('param_func', 1, 2)
            >>> param_func(1, 2)
            13
            >>> param_func(2, 2)
            47

        Delete memoized is also smart about instance methods vs class methods.

        When passing a instancemethod, it will only clear the cache related
        to that instance of that object. (object uniqueness can be overridden
            by defining the __repr__ method, such as user id).

        When passing a classmethod, it will clear all caches related across
        all instances of that class.

        Example::

            class Adder(object):
                @cache.memoize()
                def add(self, b):
                    return b + random.random()

        .. code-block:: pycon

            >>> adder1 = Adder()
            >>> adder2 = Adder()
            >>> adder1.add(3)
            3.23214234
            >>> adder2.add(3)
            3.60898509
            >>> cache.delete_memoized(adder.add)
            >>> adder1.add(3)
            3.01348673
            >>> adder2.add(3)
            3.60898509
            >>> cache.delete_memoized(Adder.add)
            >>> adder1.add(3)
            3.53235667
            >>> adder2.add(3)
            3.72341788

        :param fname: Name of the memoized function, or a reference to the function.
        :param \*args: A list of positional parameters used with memoized function.
        :param \**kwargs: A dict of named parameters used with memoized function.

        .. note::

            Sanic-Cache uses inspect to order kwargs into positional args when
            the function is memoized. If you pass a function reference into ``fname``
            instead of the function name, Sanic-Cache will be able to place
            the args/kwargs in the proper order, and delete the positional cache.

            However, if ``delete_memoized`` is just called with the name of the
            function, be sure to pass in potential arguments in the same order
            as defined in your function as args only, otherwise Sanic-Cache
            will not be able to compute the same cache key.

        .. note::

            Sanic-Cache maintains an internal random version hash for the function.
            Using delete_memoized will only swap out the version hash, causing
            the memoize function to recompute results and put them into another key.

            This leaves any computed caches for this memoized function within the
            caching backend.

            It is recommended to use a very high timeout with memoize if using
            this function, so that when the version has is swapped, the old cached
            results would eventually be reclaimed by the caching backend.
        """
        if not callable(f):
            raise DeprecationWarning(
                "Deleting messages by relative name is no longer"
                " reliable, please switch to a function reference"
            )

        try:
            if not args and not kwargs:
                await self.memoize_version(f, reset=True)
            else:
                cache_key = await f.make_cache_key(f.uncached, *args, **kwargs)
                await self.delete(cache_key)
        except Exception:
            if current_app.debug:
                raise
            current_app.ctx.logger.exception("Exception possibly due to cache backend.")

    async def delete_memoized_verhash(self, f, *args):
        """
        Delete the version hash associated with the function.

        ..warning::

            Performing this operation could leave keys behind that have
            been created with this version hash. It is up to the application
            to make sure that all keys that may have been created with this
            version hash at least have timeouts so they will not sit orphaned
            in the cache backend.
        """
        if not callable(f):
            raise DeprecationWarning(
                "Deleting messages by relative name is no longer"
                " reliable, please use a function reference"
            )

        try:
            await self.memoize_version(f, delete=True)
        except Exception:
            if current_app.debug:
                raise
            current_app.ctx.logger.exception("Exception possibly due to cache backend.")

    def ememoize(self, timeout=None):
        def memoize(f: callable):
            @functools.wraps(f)
            async def decorated_function(*args, **kwargs):
                try:
                    cache_key = e_make_cache_key(f, *args, **kwargs)
                    rv = await self.get(cache_key)
                except:
                    return await f(*args, **kwargs)

                if rv is None:
                    rv = await f(*args, **kwargs)
                    try:
                        await self.set(cache_key, rv, timeout=timeout)
                    except Exception as err:
                        await current_app.ctx.logger.error(traceback.format_exc())
                        raise err
                return rv

            return decorated_function

        return memoize

    async def edelete_memoized(self, f: callable, *args, **kwargs):
        try:
            cache_key = e_make_cache_key(f, *args, **kwargs)
            await self.delete_prefix(cache_key)
        except Exception as err:
            await current_app.ctx.logger.error(traceback.format_exc())
            raise err

    def retry_limiter(
        self,
        args=None,
        form=None,
        json=None,
        make_name: Union[callable, functools.partial, str] = "",
        key_prefix="",
        max_retry_times: int = 1,
        timeout: int = 30,
        mode: Literal["ert", "frt"] = "frt",
        message: str = "The maximum number of times has been reached",
        maximum_message: str = "Please wait %d second and retry",
        unless: callable = None,
    ):
        """
        Retry limit decorator
        If the view function returns an HTTPResponse object, the limiter is cleared;
        If the view function returns a None object, it limits the count to +1 and returns a preset error message;
        If you want to return a custom error message, return a `str` object.
        You can also manually clear the limiter using `await f.delete_limiter()`

        请将此装饰器放在离函数最进的位置, 否则将无法正常工作

        @param args=None,
        @param form=None,
        @param json=None,
        @param make_name 二级缓存键,
        @param key_prefix 缓存键前缀 ,
        @param timeout: 达到次数后的等待时间
        @param max_retry_times: 最大重试次数
        @param mode: "ert" 每次重试都重置等待时间 | "frt" 第一次输入时重置等待时间
        @param message: 访问失败时的返回信息
        @param maximum_message: 达到最大访问次数限制时返回的信息, `%d` is ttl of cache
        @param unless: 强制不使用缓存
        """

        if max_retry_times < 1:
            raise AttributeError("`max_retry_times` must be greater than 0")

        async def _max_retry_ert(cache_key):
            """
            @return {ttl | None} True 表示超过最大重试次数, None 表示没有超过最大重试次数
            """
            retry_time = await self.inc(cache_key)

            await self.expire(cache_key, timeout)

            if retry_time > max_retry_times:
                return await self.ttl(cache_key)
            else:
                return

        async def _max_retry_frt(cache_key):
            """
            同 __max_retry_ert
            """

            retry_time = await self.inc(cache_key)

            if retry_time == 1:
                await self.expire(cache_key, timeout)

            if retry_time > max_retry_times:
                return await self.ttl(cache_key)
            else:
                return

        async def make_cache_key(f, *args_, **kwargs):
            request = extract_request(*args_)

            cache_key_kwargs = {}

            if make_name is not None:
                if callable(make_name):
                    second_key = make_name()
                else:
                    try:
                        second_key = str(make_name)
                    except:
                        raise "`make_name` must be a callable or string"
            else:
                second_key = ""

            if args:
                for each in args:
                    cache_key_kwargs[each] = request.args.get(each)

            if form:
                for each in form:
                    cache_key_kwargs[each] = request.form.get(each)

            if json and request.json:
                for each in json:
                    cache_key_kwargs[each] = request.json.get(each)

            return f"{self.retry_time_prefix}{key_prefix}:{second_key}:{e_make_cache_key(f, **cache_key_kwargs)}"

        def wrapper(f):
            @functools.wraps(f)
            async def decorator(*args, **kwargs):
                if callable(unless) and unless() is True:
                    return await f(*args, **kwargs)

                cache_key = await make_cache_key(f, *args, **kwargs)

                if mode == "ert":
                    result: Optional[int] = await _max_retry_ert(cache_key)
                elif mode == "frt":
                    result: Optional[int] = await _max_retry_frt(cache_key)
                else:
                    raise AttributeError("`mode` argument must be `ert` or `frt`")

                if result:
                    return s_json(
                        {
                            "message": (
                                maximum_message % result
                                if "%d" in maximum_message
                                else maximum_message
                            )
                        },
                        400,
                    )

                if "retry_clear" in kwargs:
                    kwargs["retry_clear"].append(
                        functools.partial(self.delete, cache_key)
                    )
                else:
                    kwargs["retry_clear"] = [functools.partial(self.delete, cache_key)]

                retval = await f(
                    *args,
                    **kwargs,
                )

                if retval:
                    if isinstance(retval, HTTPResponse) and retval.status == 200:
                        await self.delete(cache_key)
                    elif isinstance(retval, str):
                        return s_json({"message": retval}, 400)

                    return retval
                else:
                    return s_json({"message": message}, 400)

            return decorator

        return wrapper

    def locker(
        self,
        args=None,
        form=None,
        json=None,
        make_name: Union[callable, functools.partial, str] = "",
        timeout: int = 0,
        sleep: int = 0.1,
        blocking_timeout: int = 5,
    ):
        if timeout:
            blocking = True
        else:
            blocking = False

        async def make_cache_key(f, *args_, **kwargs):
            request = extract_request(*args_)

            cache_key_kwargs = {}

            if make_name is not None:
                if callable(make_name):
                    second_key = make_name()
                else:
                    try:
                        second_key = str(make_name)
                    except:
                        raise "`make_name` must be a callable or string"
            else:
                second_key = ""

            if args:
                for each in args:
                    cache_key_kwargs[each] = request.args.get(each)

            if form:
                for each in form:
                    cache_key_kwargs[each] = request.form.get(each)

            if json and request.json:
                for each in json:
                    cache_key_kwargs[each] = request.json.get(each)

            return f"{self.locker_prefix}:{second_key}:{e_make_cache_key(f, **cache_key_kwargs)}"

        def wrapper(f):
            @functools.wraps(f)
            async def decorator_function(*args, **kwargs):
                cache_key = await make_cache_key(f, *args, **kwargs)

                lock = self.async_cache.lock(
                    cache_key, timeout, sleep, blocking, blocking_timeout
                )

                if await lock.acquire():
                    try:
                        rv = await f(*args, **kwargs)
                        return rv
                    finally:
                        await lock.release()

            return decorator_function

        return wrapper
