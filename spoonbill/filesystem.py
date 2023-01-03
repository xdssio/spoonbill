from fsspec import AbstractFileSystem, get_filesystem_class
from fsspec.implementations.cached import SimpleCacheFileSystem
import typing


def get_filesystem(protocol: str, **kwargs) -> AbstractFileSystem:
    klass = get_filesystem_class(protocol)
    fs = klass(**kwargs)
    return fs


def get_protocol_from_path(path: str, **kwargs) -> str:
    split = path.split(":")
    assert len(split) <= 2, f"too many colons found in {path}"
    protocol = split[0] if len(split) == 2 else "file"
    return protocol


def get_filesystem_from_path(path: str, cache_location=None, **kwargs) -> AbstractFileSystem:
    protocol = get_protocol_from_path(path)
    fs = get_filesystem(protocol, **kwargs)

    # If a cache location is set and the protocol is not a local file, use as a simple disk-based cache
    if protocol != "file" and cache_location is not None:
        fs = SimpleCacheFileSystem(fs=fs, cache_storage=str(cache_location / "simple-cache-file-system"))
    return fs


def save_bytes(path: str, data: typing.Union[bytes, str], **kwargs):
    fs = get_filesystem_from_path(path, **kwargs)
    fs.write_bytes(path, data)


def load_bytes(path: str, **kwargs) -> bytes:
    fs = get_filesystem_from_path(path, **kwargs)
    return fs.read_bytes(path)
