Ottergon is a development platform for building analytical systems and applications.

[![ubuntu 20.04](https://github.com/duckstax/ottergon/actions/workflows/ubuntu_20_04.yaml/badge.svg)](https://github.com/duckstax/ottergon/actions/workflows/ubuntu_20_04.yaml)
[![manylinux2014](https://github.com/duckstax/ottergon/actions/workflows/manylinux2014.yml/badge.svg)](https://github.com/duckstax/ottergon/actions/workflows/manylinux2014.yml)

The platform is designed to combine supporting analytics and processing transactions. It is a unified data engine to power fast analytics and fast applications.

It contains a set of technologies across different storage types, driving low-latency access to large datasets and enabling big data systems to process and move data fast. It also offers rich NoSQL/SQL dialects. It is designed to be fast, reliable and easy to use.

## Enjoy easy programming with Ottergon!

## Installation

### Python example:

```bash
    pip install ottergon==0.4.0 
```

Python SQl example:

```python
    from ottergon import Client

    client = Client()
    database = client["MyDatabase"]
    collection = database["MyCollection"]
    collection.execute("INSERT INTO MyDatabase.MyCollection (object_name, count ) VALUES ('object value', 1000)")
    collection.execute("SELECT * FROM MyDatabase.MyCollection WHERE object_name = 'object value' ")
```


Python NoSQl example:

```python
    from ottergon import Client

    client = Client()
    database = client["MyDatabase"]
    collection = database["MyCollection"]
    collection.insert_one({"object_name": "object value", "count": 1000})
    collection.find_one({"object_name": "object value"})
```

C++ SQL example:
```cpp
    auto config = create_config("/tmp/my_collection");
    spaces_t space(config);
    auto* dispatcher = space.dispatcher();
    dispatcher->execute_sql("INSERT INTO MyDatabase.MyCollection (object_name, count ) VALUES ('object value', 1000)");
    auto value = dispatcher->execute_sql("SELECT * FROM MyDatabase.MyCollection WHERE object_name = 'object value' ");
```

C++ NoSQL example:

```cpp
    auto config = create_config("/tmp/my_collection");
    spaces_t space(config);
    auto* dispatcher = space.dispatcher();
    dispatcher->insert_one("MyDatabase", "MyCollection", {"object_name": "object value", "count": 1000});
    auto value = dispatcher->find_one("MyDatabase", "MyCollection", {"object_name": "object value"});
```

## Major futures of the project

* In-process
* serverless
* Persistence index
* Persistence storage
* Write-ahead log

## Coming soon

* APIs for Rust, Go, R, Java, etc.
* Vectorized engine
* Optimized for analytics
* Parallel query processing
* Transactions
* SQL support
* Parquet / CSV / ORC
* Column storage
 
## Troubleshooting
In case you've encountered any issues, please feel free to create them right here on GitHub!
