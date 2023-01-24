import abc


class DocumentBase(object):
    pass


class DocumentStore(abc.ABC):

    def __repr__(self):
        return f"{self.__class__.__name__}({self.model.__name__})"

    def __getitem__(self, item):
        self.get(item)

    def __delitem__(self, key):
        self.delete(key)

    def __iter__(self):
        return iter(self.keys())

    def __next__(self):
        for key in self.keys():
            yield key

    def values(self):
        for key in self.keys():
            yield self.get(key)

    def items(self):
        for key in self.keys():
            yield key, self.get(key)

    @abc.abstractmethod
    def __len__(self):
        return len(self.keys())

    @abc.abstractmethod
    def __contains__(self, item):
        pass

    @abc.abstractmethod
    def get(self, key, default=None):
        pass

    @abc.abstractmethod
    def set(self, document):
        pass

    @abc.abstractmethod
    def delete(self, document):
        pass

    @abc.abstractmethod
    def keys(self):
        pass

    @abc.abstractmethod
    def find(self, conditions: dict = None, limit: int = None):
        pass

    @classmethod
    def open(cls, *args, **kwargs):
        return cls(*args, **kwargs)

    def reload(self, store):
        for value in store.values():
            self.set(value)
