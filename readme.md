# Spoonbill

What is Spoonbill? Inspired by [ibis](https://ibis-project.org/docs/3.2.0/)
Spoonbill is a Python library that provides a lightweight, universal interface for Key-Values data stores. Write once,
run anywhere.

## Feature

1. A `strict=False` mode is available to allow for more flexible data types - anything which is cloudpickle-able will work
including classes and functions.
2. A unified interface for all data stores.


## Installation

```bash
pip install spoonbill
```

## Operations map

* For scan operations, you need to use a keys as strings.

| Operation                    | InMemoryDict | BucketDict                 | RedisDict                                  | LmdbDict                                            | PysosDict                                   | ShelveDict                                              | DynamoDBDict                                     | FireStoreDict                                               | CosmosDB                                                                                               | MongoDbDict                             |
|------------------------------|--------------|----------------------------|--------------------------------------------|-----------------------------------------------------|---------------------------------------------|---------------------------------------------------------|--------------------------------------------------|-------------------------------------------------------------|--------------------------------------------------------------------------------------------------------|-----------------------------------------|
| backend                      | python dict  | S3/GS/AZ buckets           | [Redis](https://github.com/redis/redis-py) | [Lmdb](https://github.com/Dobatymo/lmdb-python-dbm) | [Pysos](https://github.com/dagnelies/pysos) | [Shelve](https://docs.python.org/3/library/shelve.html) | [AWS DynamoDB](https://aws.amazon.com/dynamodb/) | [GCP Firestore](https://firebase.google.com/docs/firestore) | [Azure Cosmos DB](https://www.google.com/search?client=safari&rls=en&q=Azure+Cosmos&ie=UTF-8&oe=UTF-8) | [MongoDB](https://www.mongodb.com/home) |
| set                          | √            | √                          | √                                          | √                                                   | √                                           | √                                                       | √                                                | √                                                           | √                                                                                                      | √                                       | 
| get                          | √            | √                          | √                                          | √                                                   | √                                           | √                                                       | √                                                | √                                                           | √                                                                                                      | √                                       |
| pop                          | √            | √                          | √                                          | √                                                   | √                                           | √                                                       | √                                                | √                                                           | √                                                                                                      | √                                       |
| delete                       | √            | √                          | √                                          | √                                                   | √                                           | √                                                       | √                                                | √                                                           | √                                                                                                      | √                                       |
| len                          | √            | √                          | √                                          | √                                                   | √                                           | √                                                       | √                                                | √                                                           | √                                                                                                      | √                                       |
| eq                           | √            | √                          | √                                          | √                                                   | √                                           | √                                                       | √                                                | √                                                           | √                                                                                                      | √                                       |
| keys                         | √            | √                          | √                                          | √                                                   | √                                           | √                                                       | √                                                | √                                                           | √                                                                                                      | √                                       |
| values                       | √            | √                          | √                                          | √                                                   | √                                           | √                                                       | √                                                | √                                                           | √                                                                                                      | √                                       |
| items                        | √            | √                          | √                                          | √                                                   | √                                           | √                                                       | √                                                | √                                                           | √                                                                                                      | √                                       |
| iter                         | √            | √                          | √                                          | √                                                   | √                                           | √                                                       | √                                                | √                                                           | √                                                                                                      | √                                       |
| contains                     | √            | √                          | √                                          | √                                                   | √                                           | √                                                       | √                                                | √                                                           | √                                                                                                      | √                                       |
| update                       | √            | √                          | √                                          | √                                                   | √                                           | √                                                       | √                                                | √                                                           | √                                                                                                      | √                                       |
| get_batch                    | √            | √                          | √                                          | √                                                   | √                                           | √                                                       | √                                                | √                                                           | √                                                                                                      | √                                       |
| set_batch                    | √            | √                          | √                                          | √                                                   | √                                           | √                                                       | √                                                | √                                                           | √                                                                                                      | √                                       |
| persistence                  | X            | √                          | √                                          | √                                                   | √                                           | √                                                       | √                                                | √                                                           | √                                                                                                      | √                                       |
| save/load                    | √            | Auto                       | X                                          | √                                                   | √                                           | √                                                       | X                                                | X                                                           | X                                                                                                      | X                                       |
| key type (Not strict/strict) | Any          | Any(local) / String(cloud) | Any/String                                 | Any                                                 | Any                                         | Any                                                     | String                                           | Any/String                                                  | String                                                                                                 | String                                  |
| value_type                   | Any          | Any(local) / String(cloud) | Any/String                                 | Any                                                 | Any                                         | Any                                                     | Jsonable                                         | Any                                                         | Any                                                                                                    | Any                                     |

## Usage

All the classes have the same interface, so you can use them interchangeably.

* The *strict* argument is used to control if to encode the keys and values with cloudpickle or keep original behavior.
  if strict is False, any key and value can be used, otherwise it depends on the backend.

## APIs

```python
from spoonbill.datastores import InMemoryDict
from spoonbill.datastores import InMemoryDict

store = InMemoryDict()
store["key"] = "value"
store["key"] == "value"
store.set("key", "value")
store.get("key", None)
store.keys(pattern="*", limit=10)  # only correct when using string keys
store.items(pattern="*", limit=10)  # only correct when using string keys
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
from spoonbill.datastores import InMemoryDict

store = InMemoryDict()  # InMemoryDict.open() or InMemoryDict.open('path/to/file') from file
``` 

### BucketDict

Requirements:   
```pip install cloudpathlib```

This dict is bucket by a file locally or on a cloud provider (S3, GS, AZ). It is **very slow**, but good for as a cheap
persisted key-value store.

For faster applications with cloud persistence, use InMemoryDict and save/load to the cloud.

### LmdbDict

LmdbDict is a wrapper around the [lmdb-python-dbm](lmdb-python-dbm) library. It is a fast key-value with memory-mapped
persistence

Requirements:   
```pip install lmdbm```

```python
from spoonbill.datastores import LmdbDict

store = LmdbDict.open('tmp.db')
```

### PysosDict pySOS: Simple Objects Storage

A wrapper around the [pysos](https://github.com/dagnelies/pysos) library. This is ideal for lists or dictionaries which
either need persistence, are too big to fit in memory or both.

Requirements:   
```pip install pysos```

```python
from spoonbill.datastores import PysosDict

store = PysosDict.open('tmp.db')
```

### RedisDict

As default, the RedisDict act as a python dict, encode every key and value to bytes using cloudpickle. This allows to
store any python object, but it is not efficient for scan operations. It also make the scan a bit unstable as only
string keys are supported.

The *strict* parameter allows to store the keys and values as strings (default redis), which allows for efficient scan
operations.

```python
from spoonbill.datastores import RedisDict

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

Notes:

* It is always recommended to set values which are a dict {attribute_name: value} to enjoy all the dynamodb features.
* Keys are always strings ('S'), numbers ('N') or bytes ('B').
* If you set a primitive number value, it will return as float (:
* cerealbox is required for *get_batch*: ```pip install cerealbox```

Requirements:

```bash
pip install boto3 
```

### FireStoreDict

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
from spoonbill.datastores import FireStoreDict

# this rest of the credentials are picked up from the file in the GOOGLE_APPLICATION_CREDENTIALS environment variable
store = FireStoreDict.open(table_name="my-collection")
```

### CosmosDBDict

Azure Cosmos DB Table Notes:

* It is recommended use dict-values {attribute_name: value} + `strict=True` to enjoy all the CosmosDB features.
    * Example: `store['key'] = {'feature': 'value'}`
      Prerequisites:
      [Quickstart](https://learn.microsoft.com/en-us/azure/cosmos-db/nosql/quickstart-python?tabs=azure-portal%2Clinux)

Requirements:

```pip install azure-cosmos```

```python
from spoonbill.datastores import CosmosDBDict

store = CosmosDBDict.open(database='db',
                          container='container',
                          endpoint='endpoint',
                          credential='credential')
```

### MongoDB

Requirements:

```pip install pymongo```

```python
from spoonbill.datastores import MongoDBDict

store = MongoDBDict.open(uri='mongodb://localhost:27017/')
```



