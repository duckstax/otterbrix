# Otterbrix: computation framework for semi-structured data processing

[![ubuntu 20.04](https://github.com/duckstax/otterbrix/actions/workflows/ubuntu-20-04.yaml/badge.svg)](https://github.com/duckstax/otterbrix/actions/workflows/ubuntu-20-04.yaml)
[![ubuntu 22.04](https://github.com/duckstax/otterbrix/actions/workflows/ubuntu-22-04.yaml/badge.svg)](https://github.com/duckstax/otterbrix/actions/workflows/ubuntu-22-04.yaml)

## About Otterbrix

## Overview

OtterBrix is an open-source, embeddable framework designed to process semi-structured data seamlessly, without the need for predefined schemas. It merges the flexibility of document stores with the analytical capabilities of columnar databases, thanks to its innovative multi-dimensional document model. This allows developers to directly ingest JSON or other irregular formats and execute SQL-like queries while ensuring high performance.

Built with C++, this lightweight engine can be easily integrated into your applications, microservices, or data pipelines. It enables efficient in-memory analytics on nested data structures, right where your data resides.

## ✨ Key Features

- **Schema-Free Processing**: Ingest and query data without defining schemas first
- **Unified Data Model**: Handle both flat (tabular) and hierarchical (nested) data in one engine
- **High-Performance Analytics**: Column-oriented in-memory format for fast operations on semi-structured data
- **SQL Query Interface**: Familiar SQL syntax and DataFrame-like operations
- **Multi-Modal Storage**: Hybrid Structure-of-Arrays (SoA) and Array-of-Structures (AoS) approach 
- **Embeddability**: Lightweight design for integration into various environments
- **Python Integration**: Available as a pip-installable package
- **Data Interoperability**: Convert between internal formats and standards like Parquet or Arrow

## 🚀 Quick Start

### Basic Usage:

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
## 🔍 Use Cases

OtterBrix excels in the following scenarios:

### Data Pre-Processing and Filtering

Use OtterBrix in backend services to filter or aggregate streaming data before it reaches heavier data warehouses. Filter events in real-time and reduce load on downstream systems.

### Real-Time Analytics in Microservices

Embed OtterBrix in microservices for local analytic capabilities. Process logs or sensor data and perform dynamic queries without round-trips to central data stores.

### Hybrid Data Processing

Handle mixed structured and semi-structured data workloads. Join relational data with JSON documents for unified ETL jobs, data science notebooks, or lightweight data integration.

### Embedded Analytics in Applications

Add advanced querying and reporting capabilities to your applications. Enable features like in-app reports or offline analytics without external database dependencies.

### Accelerated Data Processing

Leverage OtterBrix's performance optimizations for heavy JSON processing or data transformations, with potential for GPU acceleration in future releases.

## 📊 Architecture

OtterBrix introduces a multi-dimensional document model that bridges document-oriented storage with analytical databases:

```
┌────────────────────────────────────────┐
│            Application Layer           │
└────────────────────┬───────────────────┘
                     │
┌────────────────────┴───────────────────┐
│           OtterBrix Engine             │
├─────────────────────────────────────────┤
│ ┌─────────────┐        ┌──────────────┐ │
│ │  SQL Query  │        │  DataFrame   │ │
│ │  Interface  │        │  Operations  │ │
│ └─────────────┘        └──────────────┘ │
├─────────────────────────────────────────┤
│ ┌─────────────────────────────────────┐ │
│ │    Multi-Dimensional Document Model │ │
│ └─────────────────────────────────────┘ │
├─────────────────────────────────────────┤
│ ┌───────────┐ ┌──────────┐ ┌─────────┐ │
│ │ Structure │ │ Array of │ │ Hybrid  │ │
│ │ of Arrays │ │ Struct   │ │ Storage │ │
│ └───────────┘ └──────────┘ └─────────┘ │
└────────────────────┬───────────────────┘
                     │
┌────────────────────┴───────────────────┐
│            Data Sources                │
│  (JSON, Parquet, CSV, Custom Formats)  │
└────────────────────────────────────────┘
```

## 🔄 Comparison with Other Solutions

| Feature | OtterBrix | Document DBs | Columnar DBs | SQLite | DuckDB |
|---------|-----------|--------------|--------------|--------|--------|
| Schema-free ingestion | ✅ | ✅ | ❌ | ❌ | ❌ |
| Nested data support | ✅ | ✅ | Limited | Limited | Limited |
| Fast analytics | ✅ | ❌ | ✅ | ❌ | ✅ |
| Embeddable | ✅ | ❌ | ❌ | ✅ | ✅ |
| SQL support | ✅ | Limited | ✅ | ✅ | ✅ |
| Memory-optimized | ✅ | Varies | ✅ | ❌ | ✅ |
| Python integration | ✅ | Varies | Varies | ✅ | ✅ |

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
