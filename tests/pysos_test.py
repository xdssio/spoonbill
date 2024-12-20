from spoonbill.datastores.pysos import PysosStore
from tempfile import TemporaryDirectory


def test_pysos_strict():
    tmpdir = TemporaryDirectory()
    path = tmpdir.name + '/tmp.db'
    store = PysosStore.open(path)
    store._flush()
    assert len(store) == 0
    store['test'] = 'test'
    assert len(store) == 1
    assert store['test'] == store.get('test') == 'test'
    # test set and get
    store.set('another', 'another')
    assert store.get('another') == 'another'
    assert store.get('nope', 'nope') == 'nope'  # test default value
    assert 'test' in store  # test contains

    assert set(store.keys()) == set(['test', 'another'])
    assert set(store.values()) == set(['test', 'another'])
    assert set(store.items()) == set(
        [('test', 'test'), ('another', 'another')])

    assert store.pop('another') == 'another'
    assert len(store) == 1

    assert store.update({'test': 'test2', 'another': 'another2'})
    assert len(store) == 2
    assert store == {'test': 'test2', 'another': 'another2'}
    store._flush()
    store.update({i: i for i in range(10)})
    assert len(store) == 10
    assert list(store.values(range(10))) == list(range(10))
    assert len([1 for _ in store]) == 10  # test iterator


def test_pysos_non_strict():
    tmpdir = TemporaryDirectory()
    path = tmpdir.name + '/tmp.db'
    store = PysosStore.open(path, strict=False)
    store['function'] = lambda x: x + 1
    assert store['function'](1) == 2


def test_shelve_save_load():
    tmpdir = TemporaryDirectory()
    local_path = tmpdir.name + '/local.db'
    store = PysosStore.open(local_path)
    store['test'] = 'test'
    other_path = tmpdir.name + '/cloud.db'
    store.save(other_path)
    store._flush()
    assert len(store) == 0
    store = PysosStore(local_path).load(other_path)
    assert len(store) == 1


def test_pysos_search():
    tmpdir = TemporaryDirectory()
    path = tmpdir.name + '/tmp.db'
    self = store = PysosStore.open(path)
    store.update({str(i): {'a': i, 'b': str(i)} for i in range(22)})
    store.update({1: 10, 2: 20})
    assert list(store.items(conditions={'b': '1', 'a': 1})) == [
        ('1', {'a': 1, 'b': '1'})]
    assert set(store.keys(pattern='1+')
               ) == {1, '13', '10', '14', '1', '11', '16', '17', '12', '15', '18', '19'}
    assert list(store.keys(pattern=1)) == [1]
    assert list(store.values(keys=['10', '13'])) == [
        {'a': 10, 'b': '10'}, {'a': 13, 'b': '13'}]
