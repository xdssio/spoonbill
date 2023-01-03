import contextlib

from spoonbill.datastores import ContextStore, InMemoryStore, VALUE


def get_framework_save_file(framework):
    if framework == SafetensorsStore.TORCH:
        from safetensors.torch import save_file
    elif framework == SafetensorsStore.TENSORFLOW:
        from safetensors.tensorflow import save_file
    elif framework == SafetensorsStore.FLAX:
        from safetensors.flax import save_file
    elif framework == SafetensorsStore.NUMPY:
        from safetensors.numpy import save_file
    else:
        raise ValueError('Unknown framework: {}'.format(framework))
    return save_file


def get_framework_serialize(framework):
    if framework == SafetensorsStore.TORCH:
        from safetensors.torch import load
    elif framework == SafetensorsStore.TENSORFLOW:
        from safetensors.tensorflow import save
    elif framework == SafetensorsStore.FLAX:
        from safetensors.flax import save
    elif framework == SafetensorsStore.NUMPY:
        from safetensors.numpy import save
    else:
        raise ValueError('Unknown framework: {}'.format(framework))
    return save


def get_framework_deserialize(framework):
    if framework == SafetensorsStore.TORCH:
        from safetensors.torch import load
    elif framework == SafetensorsStore.TENSORFLOW:
        from safetensors.tensorflow import load
    elif framework == SafetensorsStore.FLAX:
        from safetensors.flax import load
    elif framework == SafetensorsStore.NUMPY:
        from safetensors.numpy import load
    else:
        raise ValueError('Unknown framework: {}'.format(framework))
    return load


def serialize(data, framework='pt'):
    return get_framework_serialize(framework)(data)


def deserialize(data, framework='pt'):
    return get_framework_deserialize(framework)(data)


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
        get_framework_save_file(framework)(d, path)
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


class SafetensorsInMemoryStore(InMemoryStore):

    def __init__(self, store: dict = None, framework='pt', device='cpu'):
        """
        :param store: a dictionary to use as the store
        :param strict: if False, encode and decode keys and values with cloudpickle
        """
        super().__init__(store=store, strict=False, as_string=True)
        self.framework = framework
        self.device = device

    def encode_key(self, key):
        return str(key)

    def encode_value(self, value):
        if not isinstance(value, dict):
            value = {VALUE: value}
        return serialize(value, self.framework)

    def decode_value(self, value):
        result = deserialize(value, self.framework)
        if VALUE in result:
            result = result[VALUE]
        return result

    def export_safetensors(self, path: str):
        return SafetensorsStore.from_dict({key: value for key, value in self.items()}, path, self.framework,
                                          self.device)


with contextlib.suppress(ImportError):
    from spoonbill.datastores import LmdbStore


    class SafetensorsLmdbStore(LmdbStore):

        def __init__(self, path: str, flag: str = "c", mode: int = 0o755, map_size: int = 2 ** 20,
                     autogrow: bool = True,
                     framework='pt', device='cpu'):
            self.store_path = path
            self.strict = False
            self.as_string = True
            self.open_params = {"flag": flag, "mode": mode, "map_size": map_size, "autogrow": autogrow}
            self.framework = framework
            self.device = device

        def encode_key(self, key):
            return str(key)

        def encode_value(self, value):
            if not isinstance(value, dict):
                value = {VALUE: value}
            return serialize(value, self.framework)

        def decode_value(self, value):
            result = deserialize(value, self.framework)
            if VALUE in result:
                result = result[VALUE]
            return result

        def export_safetensors(self, path: str):
            return SafetensorsStore.from_dict({key: value for key, value in self.items()}, path, self.framework,
                                              self.device)
