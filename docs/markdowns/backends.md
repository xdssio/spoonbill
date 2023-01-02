# Backends

## InMemoryStore

This object is to have a common interface for all the key-value stores. It is great for testing and for the average use
case, to have a common interface which includes the scan operation.

* Save/load are implemented to save/load the whole dict to/from a file, locally or on the cloud
  using [cloudpathlib](https://cloudpathlib.drivendata.org/stable/).

```python
from spoonbill.datastores import InMemoryStore

store = InMemoryStore()  # InMemoryDict.open() or InMemoryDict.open('path/to/file') from file

# Also works with any dict-like object
from collections import defaultdict, OrderedDict, Counter

store = InMemoryStore(defaultdict)
store = InMemoryStore(OrderedDict)
store = InMemoryStore(Counter)
``` 

Dictionaries which are persisted to disk.

## [LmdbStore](https://github.com/Dobatymo/lmdb-python-dbm)

An LMDB key-value store based on [lmdb-python-dbm](https://github.com/Dobatymo/lmdb-python-dbm). This is ideal for lists
or datastores which either need persistence, are too big to fit in memory or both.   
This is a Python DBM interface style wrapper around [LMDB](http://www.lmdb.tech/doc/) (Lightning Memory-Mapped Database)
.     
It uses the existing lower level Python bindings [py-lmdb](https://lmdb.readthedocs.io/en/release/). This is especially
useful on Windows, where otherwise dbm.dumb is the default dbm database.

Requirements:   
```pip install lmdbm```

```python
from spoonbill.datastores import LmdbStore

store = LmdbStore.open('tmp.db')
```

## [PysosStore](https://github.com/dagnelies/pysos)

This is ideal for lists or dictionaries which either need persistence, are too big to fit in memory or both.

Requirements:   
```pip install pysos```

```python
from spoonbill.datastores import PysosStore

store = PysosStore.open('tmp.db')
```

## [Shelve](https://docs.python.org/3/library/shelve.html)

The difference with “dbm” databases is that the values (not the keys!) in a shelf can be essentially arbitrary Python
objects — anything that the pickle module can handle. This includes most class instances, recursive data types, and
objects containing lots of shared sub-objects. The keys are ordinary strings.

```python
from spoonbill.datastores import ShelveStore

store = ShelveStore.open('tmp.db')
```

## [Safetensors](https://github.com/huggingface/safetensors)

This is ideal whe you want to work with tensors from disc, but it is a frozen store - no set or update.

Requirements:   
```pip install safetensors```

* if you use tensorflow, torch, numpy or flax, youll need to install them too... duh.

```python
from spoonbill.datastores import SafetensorsStore
import numpy as np

data = {'weight1': np.array([1, 2, 3]), 'weight2': np.array([4, 5, 6])}
SafetensorsStore.export_safetensors(data, 'tmp.db', framework=SafetensorsStore.NUMPY)
store = SafetensorsStore.open('tmp.db', framework=SafetensorsStore.NUMPY, device='cpu')

store['weight1']  # returns a numpy array
store['weight1'] = 1  # raises an error
```

## BucketDict

Requirements:   
```pip install cloudpathlib```

This dict is implemented as key-value files locally or on a cloud provider (S3, GS, AZ). It is **very slow**, but good
for as a cheap persisted key-value store.

For faster applications with cloud persistence, you can use InMemoryStore/LmdbStore and save/load to the cloud after
updates.

## [Redis](https://github.com/redis/redis-py)

* When strict=False any key-value can be used, otherwise only string keys and values can be used.
* When using keys with patterns -> the pattern is passed to redis *keys* function, so the behaviour is what you would
  expect from redis.
* Redis doesn't have any search for values.

```python
from spoonbill.datastores import RedisStore

# set strict to True to use redis with its default behaviour which turns keys and values to strings
store = RedisStore.open("redis://localhost:6379/1")
store[1] = 1
assert store[1] == store["1"] == "1"

assert list(store.keys('1*')) == ['111', '1', '11']  # redis turn every key to string
assert list(store.scan('1*')) == ['111', '1', '11']  # slower but non-blocking

store = RedisStore.open("redis://localhost:6379/1", strict=False)
store[1] = lambda x: x + 1  # anything goes using cloudpickle
assert store[1](1) == 2
```

## Serverless stores

* Recommended to use values as dict values, as they are more efficient to scan.
    * Good Example: `store['key'] = {'a': 1, 'b': 2}`
    * Bad Example: `store['key'] = "a value which is not a dict"`

Recommended using with `strict=True` to enjoy all the benefits of backends including **searches**.

Searches API Example:

```python
from spoonbill.datastores import MongoDBStore

store = MongoDBStore()
store.keys(pattern="*", limit=10)  # scan keys to a pattern
store.values(keys=['key1', 'key2'])  # retrieve a batch of values efficiently 
store.items(conditions={'a': '1+', 'b': 1}, limit=10)  # filter based on match conditions
```

## [MongoDB]((https://www.mongodb.com/home))

* Save/load is only implemented for `strict=True`.

Requirements:

```pip install pymongo```

```python
from spoonbill.datastores import MongoDBStore

store = MongoDBStore.open(uri='mongodb://localhost:27017/')
```

## [DynamoDB]((https://aws.amazon.com/dynamodb/))

Notes:

* It is always recommended to set values which are a dict {attribute_name: value} to enjoy all the dynamodb features.
* Keys are defined per table as either strings ('S'), numbers ('N') or bytes ('B').
* If you set a primitive number value, it will return as float (:
* cerealbox is required for retrieving multiple values with *values(["key1", "key2"])*:
    * ```pip install cerealbox```

Requirements:

```bash
pip install boto3 
```

## [Firestore]((https://firebase.google.com/docs/firestore))

Notes:

* It is recommended use dict-values {attribute_name: value} + `strict=True` to enjoy all the firestore features.
    * Example: `store['key'] = {'feature': 'value'}`   
      Prerequisites:

1. Create a project in Google Cloud Platform
2. Enable Firestore API
3. Create a service account and download the json file
4. Set the environment variable GOOGLE_APPLICATION_CREDENTIALS to the path of the json file
5. Create a database in Firestore
6. Create a collection in the database
7. Install google-cloud-firestore with

```bash
pip install --upgrade google-cloud-firestore 
```

```python
from spoonbill.datastores import Firestore

# this rest of the credentials are picked up from the file in the GOOGLE_APPLICATION_CREDENTIALS environment variable
store = Firestore.open(table_name="my-collection")
```

## [Azure CosmosDB]((https://www.google.com/search?client=safari&rls=en&q=Azure+Cosmos&ie=UTF-8&oe=UTF-8))

Notes:

* It is recommended use dict-values {attribute_name: value} + `strict=True` to enjoy all the CosmosDB features.
    * Example: `store['key'] = {'feature': 'value'}`
* The scans are implemented with SQL and  `LIKE` (Regex is not implemented on Cosmos). So it is not possible to do
  `store.keys('a*')` but `store.keys('a%')` works.

Prerequisites: [Quickstart](https://learn.microsoft.com/en-us/azure/cosmos-db/nosql/quickstart-python?tabs=azure-portal%2Clinux)

Requirements:

```pip install azure-cosmos```

```python
from spoonbill.datastores import CosmosDBStore

store = CosmosDBStore.open(database='db',
                           container='container',
                           endpoint='endpoint',
                           credential='credential')
```
