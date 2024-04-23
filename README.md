# Sanic-Cache
## 从flask-cache魔改的sanic缓存扩展(仅支持redis缓存)

这代表您完全可以使用与flask-cache相同的语法和用法。
当然, 他是异步的, 所以你必须在原语句前加上 ```await```

比如 ```await cache.set(...)```

## 新增功能

### 增加了重试次数限制器装饰器
```
@cache.retry_limiter(make_name=partial(f, ...))
def index(request: Request, retry_clear: typing:List[callable]):
    ...
```
### 增加了分布式锁
```
@cache.locker(make_name=partial(f, ...))
def index(request: Request):
    ...
```
### 增加了免序列化缓存
```
@cache.ememoize(make_name=partial(f, ...))
def index(request: Request):
    ...
    
cache.edelete_memoized(index)
```
### 增加了免序列化的set与get
```cache.eget() 与 cache.eset()```



