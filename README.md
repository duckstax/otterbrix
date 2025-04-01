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
## ⚡ Performance Benchmarks

While specific to each use case, OtterBrix typically shows:

- **3-5x faster** analytics on semi-structured data compared to document databases
- **Similar performance** to columnar databases (like DuckDB) on structured data
- **Up to 10x better memory efficiency** when handling deeply nested, sparse data structures
- **Near-zero latency** for real-time filtering and aggregation of streaming data

## 🚀 Get Started
Get started with **Otterbrix** using our installation and usage example below:

### Installation

Otterbrix is available as a Python package on PyPI. You can install it using `pip`.

```bash
pip install "otterbrix==1.0.1a9"
```

### Basic Usage:

```python
client = Client()
c = client.execute("SELECT * FROM schema.table WHERE count = 1000;")
c.close()
```

Otterbrix seamlessly integrates with column-oriented memory format and can represent both flat and hierarchical data for efficient analytical operations.


## 📊 Why OtterBrix vs Alternatives

### OtterBrix vs Alternatives

| Feature/Challenge | DuckDB | Velox | OtterBrix |
|-------------------|--------|-------|-----------|
| **Memory Usage** | Can exhaust available memory on large datasets and doesn't always spill to disk gracefully | High memory consumption with extensive caching; requires careful tuning | Efficient hybrid memory layout optimized for both in-memory performance and memory conservation |
| **Semi-Structured Data** | Limited support for nested data; requires flattening | Supports complex types but typically requires schema definition | Native handling of deeply nested structures without performance degradation |
| **Integration Model** | Embedded library with direct SQL interface | Component library for building systems; requires significant integration effort | Complete embedded solution with simple API for direct application integration |
| **Performance on JSON** | Underperforms when working with complex JSON structures | Relies on host system for JSON parsing and schema conversion | Purpose-built for high-performance JSON analytics with optimized path expressions |
| **Point Lookups** | 10x slower than SQLite on certain patterns of queries involving point lookups | Optimized for analytical queries, not point lookups | Hybrid storage approach balances columnar scans and indexed lookups |
| **Concurrency** | Limited support for concurrent connections | Depends on host system's concurrency model | Optimized for in-process concurrency within a single application |
| **Memory Layout** | Traditional columnar storage | Arrow-compatible columnar format | Innovative hybrid SoA/AoS approach that adapts to data patterns |
| **Primary Use Case** | Local analytics on structured data | Accelerating large-scale database systems | Application-level analytics on semi-structured data |

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
### Innovative Hybrid Memory Layout

OtterBrix's unique memory architecture combines:

- **Structure-of-Arrays (SoA)**: Efficient for analytical operations across many records
- **Array-of-Structures (AoS)**: Optimal for accessing complete nested objects
- **Adaptive Selection**: Automatically chooses the best representation based on data patterns and access needs

This hybrid approach delivers columnar database performance while maintaining the flexibility of document stores, making it uniquely suited for semi-structured data analytics.

## 💡 Real-World Success Stories

### Case Study: Log Analytics Service

A DevOps platform reduced their log processing pipeline latency by 75% by embedding OtterBrix directly into their log collection microservice. This allowed them to perform complex filtering and aggregation at the edge before data reached their central data lake, reducing storage costs by 40% and enabling real-time alerting.

### Case Study: IoT Sensor Analysis

An industrial IoT deployment used OtterBrix to process variable-schema sensor data from thousands of devices with different firmware versions. The schema-free approach eliminated the need for constant ETL adjustments as new sensor types were added, while the efficient memory layout allowed running complex analyses on edge devices with limited resources.

### Case Study: API Response Processing

A data integration platform leveraged OtterBrix to join and analyze responses from multiple third-party APIs with regularly changing schemas. The hybrid storage model allowed efficient querying across these diverse data structures without performance degradation as APIs evolved.

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

## ✨ Acknowledgements

OtterBrix is built on modern C++ techniques and is inspired by advances in both document-oriented and analytical database systems. We thank all contributors and the open-source community.