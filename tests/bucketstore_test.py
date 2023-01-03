from tempfile import TemporaryDirectory
from spoonbill.datastores import BucketStore
import pytest


def test_bucket_dict():
    tmpdir = TemporaryDirectory()
    store = BucketStore.open(tmpdir.name)
    store._flush()
    store['test'] = 'test'
    assert len(store) == 1
    assert store['test'] == store.get('test') == 'test'
    store.set('another', 'another')
    assert 'test' in store  # test contains

    assert set(store.keys()) == set(['test', 'another'])
    assert set(store.values()) == set(['test', 'another'])
    assert set(store.items()) == set([('test', 'test'), ('another', 'another')])

    assert list(store.keys(pattern='test')) == ['test']
    assert list(store.items(conditions='test')) == [('test', 'test')]

    assert store.pop('another') == 'another'
    assert len(store) == 1

    assert store.update({'test': 'test2', 'another': 'another2'})
    assert len(store) == 2
    assert store == {'test': 'test2', 'another': 'another2'}
    assert store.popitem()

    store._flush()
    N = 1000
    store.update({i: i for i in range(N)})
    assert len(store) == N
    assert list(store.values(range(10))) == list(range(10))
    assert len([1 for _ in store]) == N  # test iterator


@pytest.mark.skip("Run manually")
def buclketdict_s3():
    path = 's3://xdss-tmp/tmp.db/'
    store = BucketStore.open(path)
    store._flush()
    store['test'] = 'test'
    assert len(store) == 1
    assert store['test'] == store.get('test') == 'test'
    store.set('another', 'another')
    assert 'test' in store  # test contains

    assert set(store.keys()) == set(['test', 'another'])
    assert set(store.values()) == set(['test', 'another'])
    assert set(store.items()) == set([('test', 'test'), ('another', 'another')])

    assert list(store.keys(pattern='test')) == ['test']
    assert list(store.values(keys=['test'])) == ['test']
    assert list(store.items(conditions='test')) == [('test', 'test')]

    assert store.pop('another') == 'another'
    assert len(store) == 1

    assert store.update({'test': 'test2', 'another': 'another2'})
    assert len(store) == 2
    assert store == {'test': 'test2', 'another': 'another2'}

    store['test'] = {'a': 1, 'b': 2}
    assert list(store.items(conditions={'a': 1})) == [('test', {'a': 1, 'b': 2})]
    store['function'] = lambda x: x + 1
    assert store['function'](1) == 2

    store._flush()
    store.update({i: i for i in range(11)})
    assert len(store) == 11
    assert list(store.values(range(10))) == list(range(10))
    assert len([1 for _ in store]) == 11  # test iterator
    store._flush()
