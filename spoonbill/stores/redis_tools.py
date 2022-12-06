import gc
import json
from collections import defaultdict
from copy import deepcopy
from uuid import uuid4

import redis
import contextlib
import logging

logger = logging.getLogger(__name__)
REDIS_HOST = "localhost"
REDIS_PORT = 6379
REDIS_FEATURES_DB = 0

def to_int(i):
    with contextlib.suppress(ValueError):
        return int(i)
    return False


def to_float(f):
    with contextlib.suppress(ValueError):
        return float(f)
    return False


def build_set(name=None, host=None, port=None, db=-1, clean=True):
    if host and port:
        return RedisSet(name=name, host=host, db=db, port=port, clean=clean)
    return set([])


def build_list(name=None, host=None, port=None, db=-1, clean=True):
    if host and port:
        return RedisList(name=name, host=host, db=db, port=port, clean=clean)
    return []


def build_dict(host=None, port=None, db=-1, clean=True, value_type=dict):
    if host and port and db != -1:
        if value_type == set:
            return RedisDictOfSets(host=host, port=port, db=db, clean=clean)
        elif value_type == list:
            return RedisDictOfLists(host=host, port=port, db=db, clean=clean)
        return RedisDict(host=host, port=port, db=db, clean=clean)
    return StringKeysDict()


class StringKeysDict(object):

    def __init__(self):
        self._r = {}
        self.sizes = defaultdict(int)

    def increment(self, name=None, i=1):
        self.sizes[name] += i
        return self.sizes[name]

    def __repr__(self):
        return self._r.__repr__()

    def copy(self):
        ret = StringKeysDict()
        ret._r = deepcopy(self._r)
        return ret

    @property
    def count(self, name=None):
        if name is not None:
            return self.sizes.get(name, 0)
        return len(self)

    def increment(self, name, i=1):
        self._r[name] = self.get(name, 0) + i
        return self._r[name]

    def __setitem__(self, key, value):
        self._r[str(key)] = value
        return value

    def __getitem__(self, item):
        return self.get(item)

    def get(self, item, default=None):
        return self._r.get(str(item), default)

    def pop(self, item, default=None):
        return self._r.pop(str(item), default)

    def keys(self):
        return list(self._r.keys())

    def update(self, m):
        self._r.update({str(k): v for k, v in m.items()})
        return self

    def values(self):
        return self._r.values()

    def items(self):
        return self._r.items()

    def restart(self):
        count = self.count
        self._r = {}
        gc.collect()
        return count

    def __len__(self):
        return len(self._r)

    def __contains__(self, item):
        return str(item) in self._r

    def to_dict(self):
        return {k: v for k, v in self.items()}

    def insert(self, key, values):
        ret = False
        if key not in self:
            self[key] = set()
            ret = True
        for value in values:
            self[key].add(value)
        return ret


class RedisBase(object):
    def __init__(self, host, port, db, clean=True):
        super().__init__()
        self.host = host
        self.port = port
        self.db = db
        self.name = 'redis'
        self._r = redis.Redis(host=host, port=port, db=self.db, decode_responses=True)
        self.validate_connection()
        logger.debug(f"Redis connected")
        if clean:
            self.restart()

    @staticmethod
    def decode(value, default=None):
        if value is None:
            return default
        value_int = to_int(value)
        if value_int:
            return value_int
        value_float = to_float(value)
        if value_float:
            return value_float
        return value

    def copy(self, host=None, port=None, db=None, clean=None):
        host = host or self.host
        port = port or self.port
        db = db or self.db
        clean = clean or self.clean
        return self.__class__(host=host, port=port, db=db, clean=False)

    def validate_connection(self):
        rand_str = str(uuid4())
        self._r.set(rand_str, rand_str)
        self._r.delete(rand_str)
        return True

    def restart(self):
        count = len(self)
        logger.info(f"Redis db:{self.db} flush")
        self._r.flushdb()
        return count


class RedisDict(RedisBase):
    """
    A Redis implementation of a python dict

    Notes
    ______

    * All keys are saved as strings.
    * Not all dict functions a are implemented

    """

    def copy(self, host=None, port=None, db=None):
        host = host or self.host
        port = port or self.port
        db = db or self.db
        return RedisDict(host=host, port=port, db=db, clean=False)

    @property
    def count(self, name=None):
        if name is not None:
            return int(self._r.get(name) or 0)
        return len(self)

    def increment(self, name, i=1):
        return self._r.incr(name, i)

    def __setitem__(self, key, value):
        self._r[str(key)] = value
        return value

    def __getitem__(self, item):
        return self._r.get(str(item))

    def get(self, item, default=None):
        return self._r.get(item) or default

    def pop(self, item, default=None):
        item = str(item)
        value = self.decode(self._r.get(item), default)
        self._r.delete(item)
        return value

    def keys(self):
        return list(self._r.keys())

    def update(self, m):
        self._r.mset(m)
        return self

    def values(self):
        data = []

        keys = self._r.keys()
        if len(keys) == 0:
            return data
        values = self._r.mget(*keys)
        values = [self.decode(value) for value in values if value is not None]
        data.extend(values)
        return data

    def items(self):
        data = {}
        keys = self._r.keys()
        if len(keys) == 0:
            return []
        values = self._r.mget(*keys)
        values = [self.decode(value) for value in values if value is not None]
        data.update(dict(zip(keys, values)))
        return data.items()

    def restart(self):
        count = self.count
        logger.debug(f"Redis db:{self.db} flush")
        self._r.flushdb()
        return count

    def __len__(self):
        return self._r.dbsize()

    def __iter__(self):
        return self

    def __next__(self):
        raise RuntimeError(f"RedisDict iteration is not implemented")

    def __contains__(self, item):
        return item in self._r

    def to_dict(self):
        return {k: v for k, v in self.items()}


class RedisDictOfLists(RedisDict):
    def bytes_to_str(self, b):
        return b.hex()

    def str_to_bytes(self, s):
        return bytes.fromhex(s)

    def bytelist_to_str(self, l):
        return '|'.join([self.bytes_to_str(value) for value in l])

    def str_to_bytelist(self, s):
        return [self.str_to_bytes(value) for value in s.split('|')]

    def list_to_str(self, l):
        return '|'.join(l)

    def str_to_list(self, s):
        return s.split('|')

    def get(self, item, default=None):
        ret = self._r.get(str(item))
        if isinstance(ret, str):
            ret = ret.split("|")
        try:
            ret = [bytes.fromhex(value) for value in ret]
        except:
            pass
        return ret or default

    def pop(self, item, default=None):
        item = str(item)
        value = self.decode(self.get(item))
        self._r.delete(item)
        return value

    def insert(self, key, value):
        self._r.rpush(key, value)
        return True

    def items(self):
        for key in self.keys():
            yield key, set(self._r.lrange(key, 0, -1))


class RedisDictOfSets(RedisDict):
    def bytes_to_str(self, b):
        return b.hex()

    def str_to_bytes(self, s):
        return bytes.fromhex(s)

    def bytelist_to_str(self, l):
        return '|'.join([self.bytes_to_str(value) for value in l])

    def str_to_bytelist(self, s):
        return [self.str_to_bytes(value) for value in s.split('|')]

    def list_to_str(self, l):
        return '|'.join(l)

    def str_to_list(self, s):
        return s.split('|')

    def get(self, item, default=None):
        ret = self._r.get(str(item))
        if isinstance(ret, str):
            ret = ret.split("|")
        try:
            ret = [bytes.fromhex(value) for value in ret]
        except:
            pass
        return ret or default

    def pop(self, item, default=None):
        item = str(item)
        value = self.decode(self.get(item))
        self._r.delete(item)
        return value

    def insert(self, key, value):
        self._r.sadd(key, value)
        return True

    def items(self):
        for key in self.keys():
            yield key, set(self._r.smembers(key))


class RedisList(RedisBase):
    def __init__(self, name, db, host=REDIS_HOST, port=REDIS_PORT, clean=True):
        super().__init__(host=host, port=port, db=db, clean=clean)
        self.name = name

    def add(self, *values):
        return self.append(*values)

    def append(self, *values):
        return self._r.rpush(self.name, *values)

    def pop(self, default=None):
        return self._r.rpop(self.name) or default

    def lpop(self, default=None):
        return self._r.lpop(self.name) or default

    def rpop(self, default=None):
        return self._r.rpop(self.name) or default

    def insert(self, value):
        return self.add(value)

    def __len__(self):
        if self.empty:
            return 0
        return self._r.llen(self.name)

    def to_list(self):
        return self._r.lrange(self.name, 0, -1)

    def __getitem__(self, item):
        return self._r.lindex(self.name, item)

    def __setitem__(self, key, value):
        return self._r.linsert(self.name, key, value)

    @property
    def empty(self):
        return self.name not in self._r

    def __repr__(self):
        return str(self.to_list())


class RedisSet(RedisBase):

    def __init__(self, name, db, host=REDIS_HOST, port=REDIS_PORT, clean=True):
        super().__init__(host=host, port=port, db=db, clean=clean)
        self.name = name

    @property
    def count(self):
        return len(self)

    def pop(self, item, default=None):
        return self._r.spop(self.name, item)

    def keys(self):
        return list(self._r.keys())

    def update(self, m):
        return self.add(m)

    @property
    def empty(self):
        return self.name not in self._r

    def __len__(self):
        if self.empty:
            return 0
        return self._r[self.name].scard()

    def __iter__(self):
        return self

    def __next__(self):
        for item in self._r.scan_iter(self.name):
            yield item
        raise RuntimeError(f"RedisDict iteration is not implemented")

    def __contains__(self, item):
        return self._r.sismember(self.name, item)

    def to_set(self):
        if self.empty:
            return set([])
        return self._r.smembers(self.name)

    def add(self, *value):
        return self._r.sadd(self.name, *value)

    def insert(self, *values):
        return self.add(values)

    def __repr__(self):
        return str(self.to_set())

    def intersection(self, other):
        if isinstance(other, RedisSet) and other.db == self.db:
            return self._r.sinter(self.name, other.name)
        local_set = self.to_set()
        return local_set.intersection(other)

    def difference(self, other):
        if isinstance(other, RedisSet) and other.db == self.db:
            return self._r.sdiff(self.name, other.name)
        local_set = self.to_set()
        return local_set.difference(other)

    def difference_update(self, other):
        # TODO test
        if isinstance(other, RedisSet) and other.db == self.db:
            return self._r.sdiffstore(self.name, other.name)
        local_set = self.to_set()
        return local_set.difference_update(other)

    def union(self, other):
        if isinstance(other, RedisSet) and other.db == self.db:
            return self._r.sunion(self.name, other.name)
        local_set = self.to_set()
        return local_set.union(other)


class RedisQueue(RedisBase):
    LOCK = 'LOCK'

    def __init__(self, host=REDIS_HOST, port=REDIS_PORT, db=REDIS_PUBSUB, clean=True):
        super().__init__(host=host, port=port, db=db, clean=False)
        self.name = 'queue'
        logger.debug(f"RedisQueue created")
        if clean:
            self._r.delete(self.name)
            self.unlock()

    def lock(self):
        return self._r.setnx(RedisQueue.LOCK, 1)

    def unlock(self):
        self._r.delete(RedisQueue.LOCK)
        return True

    def __len__(self):
        return self._r.llen(self.name)

    def push(self, events):
        for event in events:
            self._r.rpush(self.name, json.dumps(event))
        return len(events)

    def get_messages(self, max_messages=MAX_MESSAGES):
        while 0 < max_messages:
            item = self._r.lpop(self.name)
            if item is None:
                return
            yield json.loads(item)
            max_messages -= 1


class PubSub(object):

    def __init__(self, host=REDIS_HOST, port=REDIS_PORT, name=PUBSUB_NAME, db=REDIS_PUBSUB):
        self.name = name
        self.host = host
        self.port = port
        self.db = db
        self.pubsub = None
        self._r = redis.Redis(host=host, port=port, db=db, decode_responses=True)
        self.redis = host is not None and port is not None and self.validate_connection()
        if self.redis:
            self.validate_connection()
            self.pubsub = self._r.pubsub()
            self.pubsub.subscribe(self.name)
        else:
            self._r = None

    def validate_connection(self):
        rand_str = str(uuid4())
        try:
            self._r.set(rand_str, rand_str)
            self._r.delete(rand_str)
        except:
            return False
        return True

    def publish(self, value):
        if self.redis:
            self._r.publish(self.name, value)
            return self.name
        return None

    def get_message(self):
        message = None
        if self.redis:
            ret = {'data': None}
            while ret is not None:
                message = ret.get('data')
                ret = self.pubsub.get_message()

        return message


def is_redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_FEATURES_DB):
    base = RedisBase(host=host, port=port, db=db)
    try:
        base.validate_connection()
        return True
    except:
        return False
