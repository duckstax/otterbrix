Otterbrix is an open-source framework for developing conventional and analytical applications.

[![ubuntu 20.04](https://github.com/duckstax/otterbrix/actions/workflows/ubuntu-20-04.yaml/badge.svg)](https://github.com/duckstax/otterbrix/actions/workflows/ubuntu-20-04.yaml)
[![ubuntu 22.04](https://github.com/duckstax/otterbrix/actions/workflows/ubuntu-22-04.yaml/badge.svg)](https://github.com/duckstax/otterbrix/actions/workflows/ubuntu-22-04.yaml)
[![manylinux2014](https://github.com/duckstax/otterbrix/actions/workflows/manylinux2014.yml/badge.svg)](https://github.com/duckstax/otterbrix/actions/workflows/manylinux2014.yml)


Otterbrix is an open-source framework for developing conventional and analytical applications. 
By adding the otterbrix module to their applications, developers unlock the ability to quickly process unstructured and loosely structured data.

otterbrix seamlessly integrates with column-oriented memory format and can represent both flat and hierarchical data for efficient analytical operations.

## Enjoy easy programming with otterbrix!

## Installation

### Python example:

```bash
    pip install otterbrix==1.0a7 
```

Python SQl example:

```python
    from otterbrix import Client

    client = Client()
    database = client["MyDatabase"]
    collection = database["MyCollection"]
    collection.execute("INSERT INTO MyDatabase.MyCollection (object_name, count ) VALUES ('object value', 1000)")
    collection.execute("SELECT * FROM MyDatabase.MyCollection WHERE object_name = 'object value' ")
```


Python NoSQl example:

```python
    from otterbrix import Client

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
