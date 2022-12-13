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

| operation | MemoryDict | MemoryStringDict | RedisDict | RedisStringDict | DynamoDB | Datastore | MongoDB |
|-----------|------------|------------------|-----------|-----------------|----------|-----------|---------|
| set       | √          | √                | √         | √               |          |           |         | 
| get       | √          | √                | √         | √               |          |           |         |
| pop       | √          | √                | √         | √               |          |           |         |
| delete    | √          | √                | √         | √               |          |           |         |
| len       | √          | √                | √         | √               |          |           |         |
| eq        | √          | √                | √         | √               |          |           |         |
| keys      | √          | √                | √         | √               |          |           |         |
| values    | √          | √                | √         | √               |          |           |         |
| items     | √          | √                | √         | √               |          |           |         |
| iter      | √          | √                | √         | √               |          |           |         |
| contains  | √          | √                | √         | √               |          |           |         |
| update    | √          | √                | √         | √               |          |           |         |
| get_batch | √          | √                | √         | √               |          |           |         |
| set_batch | √          | √                | √         | √               |          |           |         |
| scan      | √          | √                | X         | √               |          |           |         |

