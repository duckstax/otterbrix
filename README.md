# Otterbrix: computation framework for semi-structured data processing

[![ubuntu 20.04](https://github.com/duckstax/otterbrix/actions/workflows/ubuntu-20-04.yaml/badge.svg)](https://github.com/duckstax/otterbrix/actions/workflows/ubuntu-20-04.yaml)
[![ubuntu 22.04](https://github.com/duckstax/otterbrix/actions/workflows/ubuntu-22-04.yaml/badge.svg)](https://github.com/duckstax/otterbrix/actions/workflows/ubuntu-22-04.yaml)

## About Otterbrix

Otterbrix is an open-source framework for developing conventional and analytical applications.
By adding the Otterbrix module to their applications, developers unlock the ability to quickly process unstructured and loosely structured data.

Particularly, Otterbrix enables inserting data without schema creation, see the example below:

```python
client = Client()
c = client.execute("SELECT * FROM schema.table WHERE count = 1000;")
c.close()
```

Otterbrix seamlessly integrates with column-oriented memory format and can represent both flat and hierarchical data for efficient analytical operations.

## Get Started
Get started with **Otterbrix** using our installation and usage example below:

## Installation

Otterbrix is available as a Python package on PyPI. You can install it using `pip`.

```bash
pip install "otterbrix==1.0.1a9"
```

## Development

The Otterbrix team aims at keeping the code readable and consistent with the surrounding code where possible. A detailed code style guide is work in progress.

## Contributing

See [CONTRIBUTING](CONTRIBUTING.md) for contribution requirements.

## Security

See the project [security policy](.github/SECURITY.md) for
information about reporting vulnerabilities.

## Build requirements
To correctly build Otterbrix, you will need the most current [version of Docker](https://docs.docker.com/reference/cli/docker/version/).

## Building Otterbrix
The current version of Otterbrix can be built in Dockerfiles only. If you need assistance when building Otterbrix, please contact our [team](team@otterbrix.com).

## Troubleshooting
In case you've encountered any issues, please feel free to create them right here on GitHub!
