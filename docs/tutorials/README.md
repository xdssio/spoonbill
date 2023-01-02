<p align="center">
   <img src="https://github.com/xdssio/spoonbill/blob/main/assests/logo.png" alt="logo" width="200" />
</p>

# Spoonbill

What is Spoonbill? Inspired by [ibis](https://ibis-project.org/docs/3.2.0/)
Spoonbill is a Python library that provides a lightweight, universal interface for Key-Values data stores. Write once,
run anywhere.

For fast prototyping, testing, and simplification of data pipelines.


## Usage

All the classes have the same interface, so you can use them interchangeably.

* The *strict* argument is used to control if to encode the keys and values with cloudpickle or keep original behavior.
  if strict is False, any key and value can be used, otherwise it depends on the backend.

## How to choose a backend?

For fastest performance, use the InMemoryStore. It is a simple dict that is not persisted to disk. If you need local
persistence, use Lmdb store or ShelveStore. They are both fast and efficient.

If speed is not important, but you want cheap persistence in the cloud, use BucketStore.

If you are using it to load tensors for embedding or deep learning weights, use SafetensorsStore

If you need persistence in the cloud with realtime search, use one of the Providers key-values store:

* CosmosDB (Azure)
* Firestore (GCP)
* DynamoDB (AWS)
* MongoDB (Wherever it is deployed)

If you need very fast realtime, then the RedisStore is the best choice.




