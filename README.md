Otterbrix is an open-source framework for developing conventional and analytical applications.

[![ubuntu 20.04](https://github.com/duckstax/otterbrix/actions/workflows/ubuntu-20-04.yaml/badge.svg)](https://github.com/duckstax/otterbrix/actions/workflows/ubuntu-20-04.yaml)
[![ubuntu 22.04](https://github.com/duckstax/otterbrix/actions/workflows/ubuntu-22-04.yaml/badge.svg)](https://github.com/duckstax/otterbrix/actions/workflows/ubuntu-22-04.yaml)


Otterbrix is an open-source framework for developing conventional and analytical applications. 
By adding the otterbrix module to their applications, developers unlock the ability to quickly process unstructured and loosely structured data.

otterbrix seamlessly integrates with column-oriented memory format and can represent both flat and hierarchical data for efficient analytical operations.

## Enjoy easy programming with otterbrix!

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

## Troubleshooting
In case you've encountered any issues, please feel free to create them right here on GitHub!
