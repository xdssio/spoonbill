from tempfile import TemporaryDirectory

from spoonbill.datastores import SafetensorsStore
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
