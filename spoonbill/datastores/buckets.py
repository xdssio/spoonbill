import pathlib
import re
import shutil
from spoonbill.datastores import KeyValueStore, VALUE


def is_cloud_url(path):
    return re.match("^s3:\/\/|^az:\/\/|^gs:\/\/", str(path)) is not None


class BucketStore(KeyValueStore):
    """
    A dictionary implemented as a bucket of files.
    Pros: cloud persistent, cheap.
    Cons: slow.
    """
    COUNT_KEY = f"count__count__count"

    def __init__(self, path):
        self.path = path
        self.bucket = self.get_pathlib(path)
        self.strict = False
        self._create_table()

    @property
    def _size_file(self):
        return self._to_key(self.COUNT_KEY)

    def _create_table(self):
        self.bucket.mkdir(parents=True, exist_ok=True)
        if not self._size_file.is_file():
            self._to_key(self.COUNT_KEY).write_text('0')

    @classmethod
    def open(self, path: str):
        return BucketStore(path)

    @staticmethod
    def get_pathlib(path):
        if is_cloud_url(path):
            import cloudpathlib
            return cloudpathlib.CloudPath(path)
        return pathlib.Path(path)

    def _to_key(self, key):
        return self.bucket.joinpath(str(key))

    def __len__(self):
        if self._size_file.is_file():
            return int(self._size_file.read_text())
        return 0

    def _get_item(self, key):
        file = self._to_key(key)
        if file.is_file():
            return self.decode_value(file.read_text())
        return None

    def _increment(self):
        file = self._size_file
        count = int(file.read_text()) if file.is_file() else 0
        file.write_text(str(count + 1))

    def _decrement(self):
        file = self._size_file
        count = int(file.read_text()) if file.is_file() else 0
        file.write_text(str(max(count - 1, 0)))

    def _put_item(self, key, value):
        file = self._to_key(key)
        if not file.is_file():
            self._increment()
        self._to_key(key).write_text(self.encode_value(value))

    def __getitem__(self, key):
        value = self._get_item(key)
        if value is None:
            raise KeyError(key)
        return value

    def __setitem__(self, key, value):
        self._put_item(key, value)

    def __delitem__(self, key):
        self.delete(key)

    def delete(self, key):
        file = self._to_key(key)
        if not file.is_file():
            raise KeyError(key)
        file.unlink()
        self._decrement()

    def get(self, key, default=None):
        file = self._to_key(key)
        if file.is_file():
            return self.decode_value(file.read_text())
        return default

    def set(self, key, value):
        self._put_item(key, value)

    def _iter_keys(self, pattern: str = None, limit: int = None):
        iterator = self.bucket.glob(pattern) if pattern is not None else self.bucket.iterdir()
        for i, key in enumerate(iterator):
            if i == limit:
                break
            if key.name == self.COUNT_KEY:
                continue
            if key.is_file():
                yield key

    def _to_key_value(self, key):
        return self.decode_key(key.name), self.decode_value(key.read_text())

    def keys(self, pattern: str = None, limit: int = None):
        for key in self._iter_keys(pattern, limit):
            yield self.decode_key(key.name)

    def items(self, conditions: dict = None, limit: int = None):
        if conditions is not None and not hasattr(conditions, 'items'):
            conditions = {VALUE: conditions}

        def iterator():
            for key in self._iter_keys(pattern=None, limit=limit):
                yield key

        for key, value in self._scan_match(iterator(), conditions=conditions,
                                           limit=limit):
            yield key, value

    def values(self, keys: list = None, limit: int = None):
        if keys is None:
            keys = self._iter_keys(limit=limit)
        for key in keys:
            yield self.decode_value(self._to_key(key).read_text())

    def __contains__(self, item):
        return self._to_key(item).is_file()

    def _flush(self):
        count = len(self)
        if isinstance(self.bucket, pathlib.Path):
            for file in self.bucket.iterdir():
                if file.is_file():
                    file.unlink()
            shutil.rmtree(self.bucket.name, ignore_errors=True)
        else:
            self.bucket.rmtree()
        return count

    def pop(self, key, default=None):
        file = self._to_key(key)
        if file.is_file():
            value = self.decode_value(file.read_text())
            file.unlink()
            self._size_file.write_text(str(max(len(self) - 1, 0)))
            return value
        return default

    def popitem(self):
        key = next(self.keys())
        value = self[key]
        self.__delitem__(key)
        return key, value

    def update(self, d):
        for key, value in d.items():
            self._put_item(key, value)
        return self
