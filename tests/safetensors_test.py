from tempfile import TemporaryDirectory
from spoonbill.keyvalues.safetensors import SafetensorsStore, serialize, deserialize, SafetensorsInMemoryStore, \
    SafetensorsLmdbStore

import pytest
import numpy as np


def test_safetensors_strict():
    tmpdir = TemporaryDirectory()
    path = tmpdir.name + '/tmp.db'
    data = {'weight1': np.array([1, 2, 3]), 'weight2': np.array([4, 5, 6])}
    store = SafetensorsStore.from_dict(data, path, framework=SafetensorsStore.NUMPY)

    assert store['weight1'].tolist() == store.get('weight1').tolist() == data['weight1'].tolist()
    assert len(store) == 2
    assert 'weight1' in store  # contain
    assert len([_ for _ in store]) == 2  # iter
    assert list(store.keys()) == ['weight1', 'weight2']
    for v1, v2 in zip(store.values(), data.values()):  # values
        assert v1.tolist() == v2.tolist()
    for key, value in store.items():  # items
        assert key in data
        assert np.all(value == data[key])

    with pytest.raises(NotImplementedError):
        store['weight3'] = np.array([1, 2, 3])

    d = store.to_dict()
    assert d['weight1'].tolist() == data['weight1'].tolist()


def test_safetensors_inmemory_store():
    store = SafetensorsInMemoryStore(framework=SafetensorsStore.NUMPY)
    store['1'] = np.array([1, 2, 3])
    tmpdir = TemporaryDirectory()
    path = tmpdir.name + '/tmp.db'
    new_store = store.export_safetensors(path)
    assert len(new_store) == 1


def test_safetensors_lmbd_store():
    tmpdir = TemporaryDirectory()
    path = tmpdir.name + '/tmp.db'
    store = SafetensorsLmdbStore(path=path, framework=SafetensorsStore.NUMPY)
    store['1'] = np.array([1, 2, 3])

    new_path = tmpdir.name + '/tmp2.db'
    new_store = store.export_safetensors(new_path)
    assert len(new_store) == 1


def test_safetensors_serialized():
    data = {"weight1": np.array([1, 2, 3]), "weight2": np.array([4, 5, 6])}
    serialized = serialize(data, framework='np')
    results = deserialize(serialized, framework='np')
    assert results['weight1'].tolist() == data['weight1'].tolist()
    assert results['weight2'].tolist() == data['weight2'].tolist()
