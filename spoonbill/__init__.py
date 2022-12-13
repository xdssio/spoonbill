import contextlib

from spoonbill.stores.base import Dict, StringDict

with contextlib.suppress(ImportError):
    from spoonbill.stores.redis import RedisDict, RedisStringDict

with contextlib.suppress(ImportError):
    from spoonbill.stores.lmdb import LmdbDict