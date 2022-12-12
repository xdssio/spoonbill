# Spoonbill

What is Spoonbill?

Spoonbill is a Python library that provides a lightweight, universal interface for ML data stores. It helps Python users
explore and transform data of any size, stored anywhere.

## Key Value operations

| operation | Dict | RedisDict | RedisStringDict | DynamoDB | Datastore | MongoDB |
|-----------|------|-----------|-----------------|----------|-----------|---------|
| set       |      | √         | √               |          |           |         | 
| get       |      | √         | √               |          |           |         |
| pop       |      | √         | √               |          |           |         |
| delete    |      | √         | √               |          |           |         |
| len       |      | √         | √               |          |           |         |
| eq        |      | √         | √               |          |           |         |
| keys      |      | √         | √               |          |           |         |
| values    |      | √         | √               |          |           |         |
| items     |      | √         | √               |          |           |         |
| iter      |      | √         | √               |          |           |         |
| contains  |      | √         | √               |          |           |         |
| update    |      | √         | √               |          |           |         |
| get_batch |      | √         | √               |          |           |         |
| set_batch |      | √         | √               |          |           |         |
| scan      |      | X         | √               |          |           |         |