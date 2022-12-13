import contextlib

from spoonbill.stores.base import InMemoryDict

with contextlib.suppress(ImportError):
    from spoonbill.stores.redis import RedisDict

with contextlib.suppress(ImportError):
    from spoonbill.stores.lmdb import LmdbDict