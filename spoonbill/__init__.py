import contextlib

from spoonbill.stores.inmemory import InMemoryDict
from spoonbill.stores.shelve import ShelveStore

with contextlib.suppress(ImportError):
    from spoonbill.stores.redis import RedisDict

with contextlib.suppress(ImportError):
    from spoonbill.stores.lmdb import LmdbDict

with contextlib.suppress(ImportError):
    from spoonbill.stores.pysos import PysosDict