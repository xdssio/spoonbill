from fsspec import AbstractFileSystem, get_filesystem_class
from fsspec.implementations.cached import SimpleCacheFileSystem
import typing
import fsspec


class FileSystem:

    def __init__(self, path, **kwargs):
        self.path = path
        self.fs = self.get_filesystem_from_path(path, **kwargs)
        self.kwargs = kwargs

    @classmethod
    def get_filesystem(cls, protocol: str, **kwargs) -> AbstractFileSystem:
        klass = get_filesystem_class(protocol)
        fs = klass(**kwargs)
        return fs

    @classmethod
    def get_protocol_from_path(cls, path: str, **kwargs) -> str:
        split = path.split(":")
        assert len(split) <= 2, f"too many colons found in {path}"
        protocol = split[0] if len(split) == 2 else "file"
        return protocol

    @classmethod
    def get_filesystem_from_path(cls, path: str, cache_location=None, **kwargs) -> AbstractFileSystem:
        protocol = cls.get_protocol_from_path(path)
        fs = cls.get_filesystem(protocol, **kwargs)

        # If a cache location is set and the protocol is not a local file, use as a simple disk-based cache
        if protocol != "file" and cache_location is not None:
            fs = SimpleCacheFileSystem(fs=fs, cache_storage=str(cache_location / "simple-cache-file-system"))
        return fs

    def write_bytes(self, data: typing.Union[bytes, str]):
        self.fs.write_bytes(self.path, data)

    def read_bytes(self) -> bytes:
        return self.fs.read_bytes(self.path)

    @staticmethod
    def copy_dir(source: str, target: str, **kwargs):
        source_path = fsspec.get_mapper(source, **kwargs)
        target_path = fsspec.get_mapper(target, **kwargs)
        for key, value in source_path.items():
            target_path[key] = value
        return True

    def get_mapper(self):
        return self.fs.get_mapper(self.path)
