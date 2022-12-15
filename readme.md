# Spoonbill

What is Spoonbill? Inspired by [ibis](https://ibis-project.org/docs/3.2.0/)
Spoonbill is a Python library that provides a lightweight, universal interface for Key-Value data stores. Write once,
run anywhere.

## Installation

```bash
pip install spoonbill
```

## Classes

For each key-value store, there are two classes:

1. **Dict** is the one that implements as python dict, as the keys and values are encoded to maintain the same behaviour
   as a python dict. Best for writing algorithms as you can save and load almost any python object.

2. **StringDict** is the counterpart, where the keys and values are strings. This allows for efficient scans of keys and
   values, but you can only store strings.

## Key Value operations

* For scan operations, you need to use a keys as strings.

| operation   | InMemoryDict | RedisDict  | LmdbDict | PysosDict | DynamoDB | Datastore | MongoDB |
|-------------|--------------|------------|----------|-----------|----------|-----------|---------|
| set         | √            | √          | √        | √         | √         |           |         | 
| get         | √            | √          | √        | √         | √         |           |         |
| pop         | √            | √          | √        | √         | √         |           |         |
| delete      | √            | √          | √        | √         | √         |           |         |
| len         | √            | √          | √        | √         | √         |           |         |
| eq          | √            | √          | √        | √         | √         |           |         |
| keys        | √            | √          | √        | √         | √         |           |         |
| values      | √            | √          | √        | √         | √         |           |         |
| items       | √            | √          | √        | √         | √         |           |         |
| iter        | √            | √          | √        | √         | √         |           |         |
| contains    | √            | √          | √        | √         | √         |           |         |
| update      | √            | √          | √        | √         | √         |           |         |
| get_batch   | √            | √          | √        | √         | √         |           |         |
| set_batch   | √            | √          | √        | √         | √         |           |         |
| scan        | √            | √          | √        | √         | √         |           |         |
| persistence | X            | √          | √        | √         | √         |           |         |
| key type    | Any          | Any/String |          |           |           |           |         |

## Usage

All the classes have the same interface, so you can use them interchangeably.

* The *strict* argument is used to control if to encode the keys and values with cloudpickle or keep original behavior.
  if strict is False, any key and value can be used, otherwise it depands on the backend.

### InMemoryDict

This object is to have a common interface for all the key-value stores. It is great for testing and for the average use
case, to have a common interface which includes the scan operation.

* When using scan, the keys are evaluated as strings to match with the pattern.
* Save/load are implemented to save/load the whole dict to/from a file, locally or on the cloud
  using [cloudpathlib](https://cloudpathlib.drivendata.org/stable/).

```python
from spoonbill import InMemoryDict

store = InMemoryDict.open()  # or InMemoryDict()
store["key"] = "value"
assert store["key"] == "value"
assert list(store.scan("key*")) == ["key"]
``` 

### LmdbDict

LmdbDict is a wrapper around the [lmdb-python-dbm](lmdb-python-dbm) library. It is a fast key-value with memory-mapped
persistence
```pip install lmdbm```

```python
from spoonbill import LmdbDict

store = LmdbDict.open('tmp.db')
store["key"] = "value"
assert store["key"] == "value"
assert list(store.scan("key*")) == ["key"]

```

### PysosDict pySOS: Simple Objects Storage

A wrapper around the [pysos](https://github.com/dagnelies/pysos) library. This is ideal for lists or dictionaries which
either need persistence, are too big to fit in memory or both.

```pip install pysos```

```python
from spoonbill import PysosDict

store = PysosDict.open('tmp.db')
store["key"] = "value"
assert store["key"] == "value"

```

### RedisDict

As default, the RedisDict act as a python dict, encode every key and value to bytes using cloudpickle. This allows to
store any python object, but it is not efficient for scan operations. It also make the scan a bit unstable as only
string keys are supported.

The *as_string* parameter allows to store the keys and values as strings (default redis), which allows for efficient
scan operations.

```python
from spoonbill import RedisDict

store = RedisDict.from_url("redis://localhost:6379/1")
store[1] = lambda x: x + 1  # anything goes using cloudpickle
assert store[1](1) == 2

store.update({'1': 1, '11': 1, 1: -1, 11: -1})
assert list(store.scan('1*')) == ['1', '11']  # because scan apply only on string keys

# set as_string to True to use redis with its default behaviour which turns keys and values to strings
store = RedisDict.from_url("redis://localhost:6379/1", as_strings=True)
store[1] = 1
assert store[1] == store["1"] == "1"

store.update({'1': 1, '11': 1, 111: -111})
assert list(store.scan('1*')) == ['111', '1', '11']  # redis turn every key to string
```

### DynamoDBDict

```bash
pip install boto3 cerealbox
```

```python 
```

