import contextlib
import os

import shelve
from spoonbill.datastores import ContextStore, Strict


class ShelveStore(ContextStore, Strict):
    """
    A shelve key-value store based on [shelve](https://docs.python.org/3/library/shelve.html).
    """
    manager = shelve

    def __init__(self, path, strict=True):
        self.store_path = path
        self.strict = strict
        self.as_string = True

    @property
    def context(self):
        return shelve.open(self.store_path, flag="c", writeback=True)

    @classmethod
    def from_db(cls, path, strict: bool = False):
        return ShelveStore(path=path, strict=strict)

    def _flush(self):
        count = len(self)
        with contextlib.suppress(FileNotFoundError):
            os.remove(self.store_path)
        return count

    def encode_key(self, key):
        if self.strict:
            return str(key)
        return self.encode(key)

    open = from_db
