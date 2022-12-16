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

## Dictionaries operations

* For scan operations, you need to use a keys as strings.

| Operation                    | InMemoryDict | RedisDict                                  | LmdbDict                                            | PysosDict                                   | DynamoDBDict                                     | FireStoreDict                                               |
|------------------------------|--------------|--------------------------------------------|-----------------------------------------------------|---------------------------------------------|--------------------------------------------------|-------------------------------------------------------------|
| backend                      | python dict  | [Redis](https://github.com/redis/redis-py) | [Lmdb](https://github.com/Dobatymo/lmdb-python-dbm) | [Pysos](https://github.com/dagnelies/pysos) | [AWS DynamoDB](https://aws.amazon.com/dynamodb/) | [GCP Firestore](https://firebase.google.com/docs/firestore) |
| set                          | √            | √                                          | √                                                   | √                                           | √                                                | √                                                           | 
| get                          | √            | √                                          | √                                                   | √                                           | √                                                | √                                                           |
| pop                          | √            | √                                          | √                                                   | √                                           | √                                                | √                                                           |
| delete                       | √            | √                                          | √                                                   | √                                           | √                                                | √                                                           |
| len                          | √            | √                                          | √                                                   | √                                           | √                                                | √                                                           |
| eq                           | √            | √                                          | √                                                   | √                                           | √                                                | √                                                           |
| keys                         | √            | √                                          | √                                                   | √                                           | √                                                | √                                                           |
| values                       | √            | √                                          | √                                                   | √                                           | √                                                | √                                                           |
| items                        | √            | √                                          | √                                                   | √                                           | √                                                | √                                                           |
| iter                         | √            | √                                          | √                                                   | √                                           | √                                                | √                                                           |
| contains                     | √            | √                                          | √                                                   | √                                           | √                                                | √                                                           |
| update                       | √            | √                                          | √                                                   | √                                           | √                                                | √                                                           |
| get_batch                    | √            | √                                          | √                                                   | √                                           | √                                                | √                                                           |
| set_batch                    | √            | √                                          | √                                                   | √                                           | √                                                | √                                                           |
| persistence                  | X            | √                                          | √                                                   | √                                           | √                                                | √                                                           |
| save/load                    | √            | X                                          | X                                                   | X                                           | X                                                | X                                                           |
| key type (Not strict/strict) | Any          | Any/String                                 | Any                                                 | Any                                         | String                                           | Any/String                                                  |

## Usage

All the classes have the same interface, so you can use them interchangeably.

* The *strict* argument is used to control if to encode the keys and values with cloudpickle or keep original behavior.
  if strict is False, any key and value can be used, otherwise it depends on the backend.

## APIs

```python
from spoonbill.dictionaries import InMemoryDict
from spoonbill.dictionaries import InMemoryDict

store = InMemoryDict()
store["key"] = "value"
store["key"] == "value"
store.set("key", "value")
store.get("key", None)
store.keys(pattern="*", count=10)  # only correct when using string keys
store.items(pattern="*", count=10)  # only correct when using string keys
store.values()
'key' in store
del store['key']
store.update({'key': 'value'})
store.get_batch(['key'])
store.set_batch(['key'], ['value'])
len(store)
for key in store: pass
store.pop('key', None)
store.popitem()
```

### InMemoryDict

This object is to have a common interface for all the key-value stores. It is great for testing and for the average use
case, to have a common interface which includes the scan operation.

* Save/load are implemented to save/load the whole dict to/from a file, locally or on the cloud
  using [cloudpathlib](https://cloudpathlib.drivendata.org/stable/).

```python
from spoonbill.dictionaries import InMemoryDict

store = InMemoryDict()  # InMemoryDict.open() or InMemoryDict.open('path/to/file') from file
``` 

### LmdbDict

LmdbDict is a wrapper around the [lmdb-python-dbm](lmdb-python-dbm) library. It is a fast key-value with memory-mapped
persistence
```pip install lmdbm```

```python
from spoonbill.dictionaries import LmdbDict

store = LmdbDict.open('tmp.db')
```

### PysosDict pySOS: Simple Objects Storage

A wrapper around the [pysos](https://github.com/dagnelies/pysos) library. This is ideal for lists or dictionaries which
either need persistence, are too big to fit in memory or both.

```pip install pysos```

```python
from spoonbill.dictionaries import PysosDict

store = PysosDict.open('tmp.db')
```

### RedisDict

As default, the RedisDict act as a python dict, encode every key and value to bytes using cloudpickle. This allows to
store any python object, but it is not efficient for scan operations. It also make the scan a bit unstable as only
string keys are supported.

The *strict* parameter allows to store the keys and values as strings (default redis), which allows for efficient scan
operations.

```python
from spoonbill.dictionaries import RedisDict

store = RedisDict.from_url("redis://localhost:6379/1")
store["key"] = "value"
store["key"] == "value"

store[1] = lambda x: x + 1  # anything goes using cloudpickle
assert store[1](1) == 2

# set strict to True to use redis with its default behaviour which turns keys and values to strings
store = RedisDict.from_url("redis://localhost:6379/1", strict=True)
store[1] = 1
assert store[1] == store["1"] == "1"

assert list(store.keys('1*')) == ['111', '1', '11']  # redis turn every key to string
assert list(store.scan('1*')) == ['111', '1', '11']
```

Question: What is the difference between *keys* and *scan*?     
Answer: *keys()* is faster and blocking, while scan is (slightly) slower and non-blocking.

### DynamoDBDict

```bash
pip install boto3 cerealbox
```

```python 
```

### FireStoreDict

Prerequisites:

1. Create a project in Google Cloud Platform
2. Eneble Firestore API
3. Create a service account and download the json file
4. Set the environment variable GOOGLE_APPLICATION_CREDENTIALS to the path of the json file
5. Create a database in Firestore
6. Create a collection in the database

```bash
pip install --upgrade google-cloud-firestore 
```

```python
from spoonbill.dictionaries import FireStoreDict

store = FireStoreDict.open(
    table_name="my-collection")  # this rest of the credentials are picked up from the environment variables
```

