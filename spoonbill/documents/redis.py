import contextlib
from spoonbill.documents import DocumentStore, DocumentBase
import typing
import warnings
import redis_om
from redis_om import Field
import redis


class Document(DocumentBase, redis_om.EmbeddedJsonModel):

    @classmethod
    def _propegate_database(cls, client):
        cls._meta.database = client
        for field in cls.__fields__.values():
            if issubclass(field.type_, Document):
                field.type_._propegate_database(client)

    @property
    def _exists(self):
        with contextlib.suppress(redis_om.model.model.NotFoundError):
            self.__class__.get(self.pk)
            return True
        return False

    @property
    def _count(self):
        return int(self.db().hget('counts', self.__class__.__name__) or 0)

    def _increment(self):
        self.db().hincrby('counts', self.__class__.__name__, 1)

    def _decrement(self):
        self.db().hincrby('counts', self.__class__.__name__, -1)

    def _set(self):
        if not self._exists:
            self._increment()
        self.save()
        for field in self.__dict__.values():
            if isinstance(field, Document):
                field._set()

    def _remove(self, pipeline=None):
        if not self._exists:
            raise ValueError(f"{self} does not exist")
        if self._count > 0:
            self._decrement()
        self.__class__.delete(self.pk, pipeline=pipeline)

    @property
    def id(self):
        return self.pk


class RedisDocumentStore(DocumentStore):
    model: Document

    def __init__(self, model, client: redis.Redis, **kwargs):
        if 'db' in kwargs:
            warnings.warn("db must be '0' in redis_om: https://github.com/redis/redis-om-python")

        self.model = model
        self.model._propegate_database(client)
        self.client = client
        self.client.hset('counts', self.model.__name__, self.client.hget('counts', self.model.__name__) or 0)

    @classmethod
    def index(self):
        redis_om.Migrator().run()

    @property
    def counts(self):
        return self.client.hgetall('counts')

    def _increment(self):
        self.client.hincrby('counts', self.model.__name__, 1)

    def _decrement(self):
        if self._model_count() > 0:
            self.client.hincrby('counts', self.model.__name__, -1)

    @classmethod
    def open(cls, model: Document, url: str, **kwargs):
        kwargs['decode_responses'] = kwargs.get('decode_responses', True)
        client = redis.Redis.from_url(url, **kwargs)
        if client.connection_pool.make_connection().db != 0:
            raise ValueError(
                f"db must be '0' in redis_om - db provided is {client.connection_pool.make_connection().db}")
        return RedisDocumentStore(model=model, client=client)

    def _flush_db(self):
        self.client.flushdb()
        self.client.hset('counts', self.model.__name__, self.client.hget('counts', self.model.__name__) or 0)

    def _flush(self):
        for key in self.keys():
            self.delete(key)
        self.client.hset('counts', self.model.__name__, 0)

    def keys(self):
        for key in self.model.all_pks():
            yield key

    def values(self):
        for key in self.keys():
            yield self.model.get(key)

    def items(self):
        for key in self.keys():
            yield key, self.get(key)

    @staticmethod
    def _get_id(key):
        if hasattr(key, 'id'):
            key = key.id
        return key

    @classmethod
    def _to_document(cls, document):
        if isinstance(document, redis_om.RedisModel) and not isinstance(document, Document):
            document = document.__class__(**document.__dict__)
            new_dict = {key: cls._to_document(value) for key, value in document.__dict__.items()}
            for key, value in new_dict.items():
                setattr(document, key, value)

        return document

    def get(self, key, default=None):
        key = self._get_id(key)
        with contextlib.suppress(redis_om.model.model.NotFoundError):
            return self._to_document(self.model.get(key))
        return default

    def set(self, document: typing.Union[Document, dict, list]):
        if isinstance(document, list):
            for item in document:
                self.set(item)
            return
        elif isinstance(document, dict):
            document = self.model(**document)
        document._set()

    def delete(self, key, pipeline=None):
        document = self.get(key)
        if document is None:
            raise KeyError(f"{key} not in {self.model.__name__}")
        document._remove(pipeline=pipeline)

    def find(self, conditions=redis_om.model.model.Expression):
        self.index()
        for document in self.model.find(conditions).all():
            yield self._to_document(document)

    @property
    def schema(self):
        return self.model.schema()

    def __contains__(self, item):
        return self.get(item) is not None

    def __len__(self):
        return self._model_count()

    def _model_count(self):
        if not self.client.hexists('counts', self.model.__name__):
            self.client.hset('counts', self.model.__name__, 0)
        return int(self.client.hget('counts', self.model.__name__))
