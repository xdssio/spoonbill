from tempfile import TemporaryDirectory
from spoonbill.datastores import BucketStore
import pytest


def test_dict():
    tmpdir = TemporaryDirectory()
    self = store = BucketStore.open(tmpdir.name)
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
    assert list(store.items(patterns='test')) == [('test', 'test')]

    assert store.pop('another') == 'another'
    assert len(store) == 1

    assert store.update({'test': 'test2', 'another': 'another2'})
    assert len(store) == 2
    assert store == {'test': 'test2', 'another': 'another2'}

    store._flush()
    N = 1000
    store.set_batch(range(N), range(N))
    assert len(store) == N
    assert list(store.get_batch(range(10))) == list(range(10))
    assert len([1 for _ in store]) == N  # test iterator


@pytest.mark.skip("Run manually")
def buclketdict_s3():
    path = 's3://xdss-tmp/tmp.db'
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

    assert list(store.keys(patterns='test')) == ['test']
    assert list(store.items(patterns='test')) == [('test', 'test')]

    assert store.pop('another') == 'another'
    assert len(store) == 1

    assert store.update({'test': 'test2', 'another': 'another2'})
    assert len(store) == 2
    assert store == {'test': 'test2', 'another': 'another2'}

    store._flush()
    store.set_batch(range(11), range(11))
    assert len(store) == 11
    assert list(store.get_batch(range(10))) == list(range(10))
    assert len([1 for _ in store]) == 11  # test iterator
    store._flush()
