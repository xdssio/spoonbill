import contextlib

from spoonbill.datastores import ContextStore


class SafetensorsStore(ContextStore):
    """
    SafetensorsStore is a wrapper around the safetensors library.
    """
    NUMPY = 'np'
    TORCH = 'pt'
    TENSORFLOW = 'tf'
    FLAX = 'flax'

    def __init__(self, path: str, framework='pt', device='cpu'):
        self.store_path = path
        self.framework = framework
        self.device = device
        self.strict = True
        self.as_string = False

    @property
    def context(self):
        from safetensors import safe_open
        return safe_open(self.store_path, framework=self.framework, device=self.device)

    @classmethod
    def export_safetensors(cls, d: dict, path: str, framework: str, device='cpu'):
        if framework == SafetensorsStore.TORCH:
            from safetensors.torch import save_file
        elif framework == SafetensorsStore.TENSORFLOW:
            from safetensors.tensorflow import save_file
        elif framework == SafetensorsStore.FLAX:
            from safetensors.flax import save_file
        elif framework == SafetensorsStore.NUMPY:
            from safetensors.numpy import save_file
        else:
            raise ValueError(f'Framework {framework} not supported')
        save_file(d, path)
        return cls(path, framework, device)

    @classmethod
    def from_dict(cls, d, path, framework=NUMPY, device='cpu'):
        cls.export_safetensors(d, path, framework, device)
        return SafetensorsStore(path, framework=framework, device=device)

    def to_dict(self):
        with self.context as store:
            return {key: store.get_tensor(key) for key in store.keys()}

    @classmethod
    def open(cls, db_path, framework=NUMPY, device='cpu'):
        return SafetensorsStore(
            store=db_path,
            framework=framework,
            device=device
        )

    def __len__(self):
        with self.context as store:
            return len(store.keys())

    def items(self, *args, **kwargs):
        with self.context as store:
            for key in store.keys():
                yield key, store.get_tensor(key)

    def values(self, *args, **kwargs):
        with self.context as store:
            for key in store.keys():
                yield store.get_tensor(key)

    def __getitem__(self, item):
        try:
            with self.context as store:
                return store.get_tensor(item)
        except Exception:
            raise KeyError(f'Key {item} not found')

    def __setitem__(self, key, value):
        raise NotImplementedError("SafetensorsStore does not support set")

    def __contains__(self, item):
        return self.get(item) is not None

    def get(self, key, default=None):
        with contextlib.suppress(KeyError):
            return self[key]
        return default

    def set(self, key, value):
        raise NotImplementedError("SafetensorsStore does not support set")

    def pop(self, key, default=None):
        raise NotImplementedError("SafetensorsStore does not support pop")

    def popitem(self, key, default=None):
        raise NotImplementedError("SafetensorsStore does not support pop")

    def get_slice(self, key, slice):
        with self.context as store:
            return store.get_slice(key)[slice]

    def save(self, path):
        self._get_path(path).write_bytes(self._get_path(self.store_path).read_bytes())

    def load(self, path):
        self.store_path = path
