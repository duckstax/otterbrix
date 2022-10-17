Ottergon is a development platform for building analytical systems and applications.

[![ubuntu 20.04](https://github.com/duckstax/RocketJoe/actions/workflows/ubuntu_20_04.yaml/badge.svg)](https://github.com/duckstax/RocketJoe/actions/workflows/ubuntu_20_04.yaml)

Ottergon is designed to combine supporting analytics and processing transactions. It is a unified data engine to power fast analytics and fast applications.

Ottergon contains a set of technologies across different storage types, driving low-latency access to large datasets and enabling big data systems to process and move data fast. It also offers rich NoSQL/SQL dialects. It is designed to be fast, reliable and easy to use.


## Enjoy easy programming with Ottergon!
We offer help and support for С++, Python.

Python example :

```python
from duck_charmer import Client

client = Client()
database = client["MyDatabase"]
collection = database["MyCollection"]

collection.insert_one({"object_name": "object value", "count": 1000})

collection.find({"object_name": "object value"})["count"] # 1000

```

## Major components of the project include:
* In-process
* serverless
* Persistence index and storage
* wal

### Coming soon
* APIs for rust/go/R/Java/….
* Vectorized engine
* Optimized for analytics
* Parallel query processing
* Transactions
* SQL support
* Parquet/CSVjson
* Column storage