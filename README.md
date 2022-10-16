[![ubuntu 20.04](https://github.com/duckstax/RocketJoe/actions/workflows/ubuntu_20_04.yaml/badge.svg)](https://github.com/duckstax/RocketJoe/actions/workflows/ubuntu_20_04.yaml)


Ottergon is a combined storage/engine/development platform for building high-performance analytical systems and data warehouses. 
It is designed to be fast, reliable and easy to use.

Ottergon contains a set of technologies across different storage types, driving low-latency access to large datasets and enabling big data systems to process and move data fast. 
It also offers rich NoSQL/SQL dialects.

Ottergon combines transactions and analytics in a unified data engine, powering fast analytics and simplifying the development of state-of-the-art, high-performance applications.

Python example :

```python
from duck_charmer import Client

client = Client()
database = client["MyDatabase"]
collection = database["MyCollection"]

collection.insert_one({"object_name": "object value", "count": 1000})

collection.find({"object_name": "object value"})["count"] # 1000

```