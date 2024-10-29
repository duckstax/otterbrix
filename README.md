Otterbrix: computation framework for semi-structured data processing

[![ubuntu 20.04](https://github.com/duckstax/otterbrix/actions/workflows/ubuntu-20-04.yaml/badge.svg)](https://github.com/duckstax/otterbrix/actions/workflows/ubuntu-20-04.yaml)
[![ubuntu 22.04](https://github.com/duckstax/otterbrix/actions/workflows/ubuntu-22-04.yaml/badge.svg)](https://github.com/duckstax/otterbrix/actions/workflows/ubuntu-22-04.yaml)

## About Otterbrix

Otterbrix is an open-source framework for developing conventional and analytical applications.
By adding the Otterbrix module to their applications, developers unlock the ability to quickly process unstructured and loosely structured data.

Particularly, Otterbrix enables inserting data without schema creation, see the example below:

```cpp
    auto config = create_config("/tmp/my_collection");
    spaces_t space(config);
    auto* dispatcher = space.dispatcher();
    dispatcher->execute_sql("INSERT INTO MyDatabase.MyCollection (object_name, count ) VALUES ('object value', 1000)");
    auto value = dispatcher->execute_sql("SELECT * FROM MyDatabase.MyCollection WHERE object_name = 'object value' ");
```

Otterbrix seamlessly integrates with column-oriented memory format and can represent both flat and hierarchical data for efficient analytical operations.

## Security

See the project [security policy](.github/SECURITY.md) for
information about reporting vulnerabilities.

Build requirements
To correctly build Otterbrix, you will need the most current [version of Docker](https://docs.docker.com/reference/cli/docker/version/).
Building Otterbrix
The current version of Otterbrix can be built in Dockerfiles only. If you need assistance when building Otterbrix, please contact our t


## Troubleshooting
In case you've encountered any issues, please feel free to create them right here on GitHub!
