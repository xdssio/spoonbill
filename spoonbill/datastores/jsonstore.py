from spoonbill.datastores.base import ContextStore
from typing import Any, Dict, Optional
from spoonbill.datastores.utils import get_pathlib
import json


class JSONContextManager:
    def __init__(self, path: str,
                 use_jsonpickle: bool = False,
                 lookfile_path: Optional[str] = None):
        self.path = get_pathlib(path)
        self.json = json
        self.lock = None
        if use_jsonpickle:
            import jsonpickle
            self.json = jsonpickle
        if lookfile_path:
            from filelock import FileLock
            self.lock = FileLock(lookfile_path)
        if not self.path.exists() or self.path.stat().st_size == 0:
            self.path.write_text(json.dumps({}))

    def _read(self) -> Dict[Any, Any]:
        self.data: Dict[Any, Any] = self.json.loads(self.path.read_text())
        return self.data

    def _write(self):
        self.path.write_text(self.json.dumps(self.data, indent=4))

    def __enter__(self):  # type: ignore
        if self.lock is not None:
            with self.lock:
                return self._read()
        return self._read()

    def __exit__(self, exc_type, exc_val, exc_tb):  # type: ignore
        if self.lock is not None:
            with self.lock:
                return self._write()
        return self._write()


class JsonStore(ContextStore):
    """Naive implenetation using json files."""

    def __init__(self, path: str,
                 strict: bool = False,
                 use_jsonpickle: bool = False,
                 lockfile_path: Optional[str] = None):
        self.path = path
        self.strict = strict
        self.use_jsonpickle = use_jsonpickle
        self.lockfile_path = lockfile_path

    def _flush(self):
        with self.context as data:
            data.clear()

    @classmethod
    def open(cls, path: str, strict: bool = False,
             use_jsonpickle: bool = False,
             lockfile_path: Optional[str] = None):
        return JsonStore(path, strict=strict,
                         use_jsonpickle=use_jsonpickle,
                         lockfile_path=lockfile_path)

    @property
    def context(self):
        return JSONContextManager(self.path,
                                  use_jsonpickle=self.use_jsonpickle,
                                  lookfile_path=self.lockfile_path)
