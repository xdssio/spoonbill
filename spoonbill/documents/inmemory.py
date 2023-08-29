from spoonbill.documents import DocumentStore, DocumentBase
import dataclasses
import abc


class Document(DocumentBase):

    def __new__(cls, *args, **kwargs):
        return dataclasses.dataclass(type(cls.__name__, (cls,DocumentBase), {}),frozen=True)

    @property
    @abc.abstractmethod
    def id(self):
        raise NotImplementedError

def document(cls):
    print(1)
    return dataclasses.dataclass(type(cls.__name__, (cls,DocumentBase), {}),frozen=True)

@document
class Address():
    id: str
    city: str
    country: str

cls = Address

Address(country='NL', city='Groningen', id='test').id


class InmemoryDocumentStore(DocumentStore):

    def __init__(self, model: Document, store: dict = None):
        """
        :param store: a dictionary to use as the store
        :param strict: if False, encode and decode keys and values with cloudpickle
        """
        self._store = {}
        self.model = model
        if store:
            if isinstance(store, dict):
                self._store = store
            elif isinstance(store, (DocumentStore,)):
                self.reload(store)

    def _to_key(self, item):
        if isinstance(item, Document):
            return item.id
        return item

    def __len__(self):
        return len(self._store)

    def __contains__(self, item):
        return self._to_key(item) in self._store

    def get(self, key, default=None):
        return self._store.get(self._to_key(key), default)

    def set(self, document):
        key = self._to_key(document)
        self._store[key] = document

    def delete(self, document):
        self._store.pop(self._to_key(document))

    def keys(self):
        return self._store.keys()

    def find(self, conditions: dict = None, limit: int = None):
        pass

    @classmethod
    def open(cls, model: Document, store: dict = None, *args, **kwargs):
        return InmemoryDocumentStore(model=model, store=store, *args, **kwargs)

    def _flush(self):
        self._store = {}
