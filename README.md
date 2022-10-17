Ottergon is a development platform for building analytical systems and applications.

[![ubuntu 20.04](https://github.com/duckstax/RocketJoe/actions/workflows/ubuntu_20_04.yaml/badge.svg)](https://github.com/duckstax/RocketJoe/actions/workflows/ubuntu_20_04.yaml)

The platform is designed to combine supporting analytics and processing transactions. It is a unified data engine to power fast analytics and fast applications.

It contains a set of technologies across different storage types, driving low-latency access to large datasets and enabling big data systems to process and move data fast. It also offers rich NoSQL/SQL dialects. It is designed to be fast, reliable and easy to use.

# Enjoy easy programming with Ottergon!

Python example:

```python
    from ottergon import Client

    client = Client()
    database = client["MyDatabase"]
    collection = database["MyCollection"]
    collection.insert_one({"object_name": "object value", "count": 1000})
    collection.find({"object_name": "object value"})["count"] # 1000
```

C++ example:

```cpp
    static const auto database = "MyDatabase";
    static const auto collection = "MyCollection";
    auto config = create_config("/tmp/my_collection");
    spaces_t space(config);
    auto* dispatcher = space.dispatcher();
    dispatcher->insert_one(database_name, collection_name, {"object_name": "object value", "count": 1000});
    auto value = dispatcher->find(database_name, collection_name, {"object_name": "object value"});
```

# Major components of the project

* In-process
* serverless
* Persistence index and storage
* Write-ahead log

# Coming soon

* APIs for Rust, Go, R, Java, etc.
* Vectorized engine
* Optimized for analytics
* Parallel query processing
* Transactions
* SQL support
* Parquet / CSV / ORC
* Column storage
