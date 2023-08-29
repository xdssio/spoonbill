from spoonbill.documents.inmemory import Document, InmemoryDocumentStore
import dataclasses

def test_dict_find():


    class Address(Document):
        city: str
        country: str

    Address(country='NL', city='Groningen').to_dict()


    class Customer(Document):
        first_name: str
        last_name: str
        email: str
        age: int
        bio: str
        address: Address

    self = customers = InmemoryDocumentStore.open(model=Customer)
    customers._flush()

    andrew = Customer(
        first_name="Andrew",
        last_name="Brookins",
        email="andrew.brookins@example.com",
        age=38,
        bio="Python developer, works at Redis, Inc.",
        address=Address(country='NL', city='Groningen'),
    )

    yohai = Customer(
        first_name="Yohai",
        last_name="Ararat",
        email="yohai.brookins@example.com",
        age=38,
        bio="Python developer, works at Redis, Inc.",
        address=Address(country='NL', city='Groningen'),
    )

    customers.set([andrew, yohai])
    assert len(customers) == 2

    assert list(customers.find(Customer.first_name == "Andrew"))[0].first_name == 'Andrew'
    assert list(customers.find(Customer.first_name == "Yohai"))[0].first_name == 'Yohai'
    assert list(customers.find((Customer.first_name == "Yohai") &
                               (Customer.age > 15)))[0].first_name == 'Yohai'
    assert list(customers.find((Customer.first_name == "Yohai") &
                               (Customer.age < 15))) == []


def test_redis_document_store():
    class Address(Document):
        city: str = Field(index=True)
        country: str

    class Customer(Document):
        first_name: str = Field(index=True)
        last_name: str
        email: str = Field(index=True)
        age: int = Field(index=True)
        bio: str = Field(index=True, full_text_search=True, default="")
        address: Address

    customers = RedisDocumentStore.open(model=Customer, url='redis://localhost:6379/0')
    customers._flush_db()
    assert len(customers) == 0

    andrew = Customer(
        first_name="Andrew",
        last_name="Brookins",
        email="andrew.brookins@example.com",
        age=38,
        bio="Python developer, works at Redis, Inc.",
        address=Address(country='NL', city='Groningen'),
    )

    customers.set(andrew)
    assert len(customers) == 1
    assert customers.get(andrew) == andrew
    assert customers.get(andrew.id) == andrew
    customers.delete(andrew)
    assert len(customers) == 0
    assert list(customers.find(Customer.first_name == "Andrew")) == []

    customers.set(andrew)

    assert list(customers.keys()) == [andrew.id]
    assert list(customers.values()) == [andrew]
    assert list(customers.items()) == [(andrew.id, andrew)]
    assert list(customers.find(Customer.first_name == "Andrew"))[0].first_name == 'Andrew'
    assert list(customers.find(Customer.first_name == "Andrew"))[0].first_name == 'Andrew'

    addresses = RedisDocumentStore.open(model=Address, url='redis://localhost:6379/0')
    assert len(addresses) == 1  # andrew.address

    addresses.set(Address(country='NL', city="Amsterdam"))
    assert len(addresses) == 2
    addresses.delete(andrew.address)
    assert len(addresses) == 1
    assert len(list(addresses.find(Address.city == "Amsterdam"))) == 1
