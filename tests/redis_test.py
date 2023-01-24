import contextlib
from dataclasses import dataclass

import redis_om.model.model
from dataclasses_json import dataclass_json
from tempfile import TemporaryDirectory

import redis
import pytest
import pathlib
from spoonbill.keyvalues import RedisDict


def test_redis_from_connection():
    store = RedisDict.from_connection('localhost', 6379, 1, strict=False)
    store[1] = 1
    assert store[1] == 1
    store._flush()


def test_redis_open():
    store = RedisDict.open('redis://localhost:6379/1')

    store[1] = 1
    assert store[1] == 1
    store['test'] = {'1': 1}
    assert store['test'] == {'1': 1}
    store._flush()


def test_redis_dict():
    store = RedisDict.open('redis://localhost:6379/1', strict=False)
    store._flush()
    store['test'] = 'test'
    assert len(store) == 1
    assert store['test'] == store.get('test') == 'test'
    store.set('another', 'another')
    assert 'test' in store  # test contains
    store.set(1, 1)
    assert set(store.keys()) == set(['test', 'another', 1])
    assert set(store.values()) == set(['test', 'another', 1])
    assert set(store.items()) == set([('test', 'test'), ('another', 'another'), (1, 1)])

    assert list(store.keys('test')) == ['test']
    assert list(store.items('test')) == [('test', 'test')]

    assert store.pop('another') == 'another'
    assert len(store) == 2

    assert store.update({'test': 'test2', 'another': 'another2'})
    assert len(store) == 3
    assert store == {'test': 'test2', 'another': 'another2', 1: 1}

    store._flush()
    store.update({i: i for i in range(10)})
    assert len(store) == 10
    assert list(store.values(range(10))) == list(range(10))
    assert len([1 for _ in store]) == 10  # test iterator

    store['function'] = lambda x: x
    assert store['function'](1) == 1

    store._flush()
    store.update({'1': 1, '11': 11, '2': 2, '3': 3, 1: -1, 11: -11})
    assert set(store.scan('1*')) == set([('1', 1), ('11', 11)])  # scan only works with string keys
    assert set(store.keys('1*')) == set(['1', '11'])
    store._flush()


def test_redis_strict():
    store = RedisDict.open('redis://localhost:6379/1', strict=True)
    store._flush()
    store[1] = 1
    assert '1' in store
    assert store[1] == store.get(1) == store.get('1') == store._store[1] == '1'
    store[11] = 11
    store[2] = 2
    assert set(store.scan('1*')) == set([('1', '1'), ('11', '11')])
    assert set(store.keys('1*')) == set(['1', '11'])

    with pytest.raises(redis.exceptions.DataError):
        store['1'] = {'1': 1}


@pytest.mark.skip("Experimental")
def test_redis_save_load():
    tmpdir = TemporaryDirectory()
    store = RedisDict.open('redis://localhost:6379/1', strict=True)
    store.update({str(i): i for i in range(10000)})
    other_path = tmpdir.name + '/cloud.db'
    store.save(other_path)
    store._flush()
    assert pathlib.Path(other_path).is_file()


# def test_redis_document_store():


# def test_redis_json():
#     @dataclass_json
#     @dataclass
#     class User:
#         id: int
#         name: str
#         age: int
#     import redis
#     import json
#
#     User(1,'1',1).to_json()
#     store = RedisDict(store=redis.StrictRedis.from_url('redis://localhost:6379/1'))
#     store._flush()
#
#     data = {
#         'foo': 'bar',
#         'ans': 42
#     }
#
#     r = redis.StrictRedis()
#     r.execute_command('JSON.SET', 'object', '.', json.dumps(data))
#     r.delete('object')
#     reply = json.loads(r.execute_command('JSON.GET', 'object'))
#
#
#
#     store._store.json().set(name='user', path='$', obj='"user"')
#     doc = r.json().get('doc', '$')
#     dog = r.json().get('doc', '$.dog')
#     scientific_name = r.json().get('doc', '$..scientific-name')
