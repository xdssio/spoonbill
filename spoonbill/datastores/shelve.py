import contextlib
import os

import shelve
from spoonbill.datastores import ContextStore, Strict


class ShelveStore(ContextStore, Strict):
    manager = shelve

    def __init__(self, path, strict=False):
        self.store_path = path
        self.strict = strict

    @classmethod
    def from_db(cls, path, strict: bool = False):
        return ShelveStore(path=path, strict=strict)

    def _flush(self):
        count = len(self)
        with contextlib.suppress(FileNotFoundError):
            os.remove(self.store_path)
        return count

    open = from_db
