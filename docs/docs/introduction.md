---
id: introduction
slug: /
---

# Introduction

Firehose is a cloud-native service for delivering real-time streaming data to destinations such as service endpoints \(HTTP or GRPC\) & managed databases \(MongoDB, Prometheus, Postgres, InfluxDB, Redis, & ElasticSearch\). With Firehose, you don't need to write applications or manage resources. It automatically scales to match the throughput of your data and requires no ongoing administration. If your data is present in Kafka, Firehose delivers it to the destination\(SINK\) that you specified.

![](/assets/overview.svg)

## Key Features

Discover why users choose Firehose as their main Kafka Consumer

- **Sinks** Firehose supports sinking stream data to log console, HTTP, GRPC, PostgresDB\(JDBC\), InfluxDB, Elastic Search, Redis, Prometheus and MongoDB.
- **Scale** Firehose scales in an instant, both vertically and horizontally, for high-performance streaming sink and zero data drops.
- **Extensibility** Add your own sink to Firehose with a clearly defined interface or choose from already provided ones.
- **Runtime** Firehose can run inside containers or VMs in a fully managed runtime environment like Kubernetes.
- **Metrics** Always know what’s going on with your deployment with built-in monitoring of throughput, response times, errors, and more.

## Supported Incoming data types from kafka
- [Protobuf](https://developers.google.com/protocol-buffers)
- [JSON](https://www.json.org/json-en.html)
  - Supported limited to bigquery, elastic and mongo sink. In future support to other sinks will be added

## Supported Sinks:

Following sinks are supported in the Firehose

- [Log](https://en.wikipedia.org/wiki/Log_file) - Standard Output
- [HTTP](https://en.wikipedia.org/wiki/Hypertext_Transfer_Protocol) - HTTP services (deprecated in v0.10.0+)
- [HTTP V2](https://en.wikipedia.org/wiki/Hypertext_Transfer_Protocol) - HTTP services 
- [JDBC](https://en.wikipedia.org/wiki/Java_Database_Connectivity) - Postgres DB
- [InfluxDB](https://en.wikipedia.org/wiki/InfluxDB) - A time-series database
- [Redis](https://en.wikipedia.org/wiki/Redis) - An in-memory Key value store
- [ElasticSearch](https://en.wikipedia.org/wiki/Elasticsearch) - A search database
- [GRPC](https://en.wikipedia.org/wiki/GRPC) - GRPC based services
- [Prometheus](https://en.wikipedia.org/wiki/Prometheus_%28software) - A time-series database
- [MongoDB](https://en.wikipedia.org/wiki/MongoDB) - A NoSQL database
- [Bigquery](https://cloud.google.com/bigquery) - A data warehouse provided by Google Cloud
- [Bigtable](https://cloud.google.com/bigtable) - A fully managed, scalable NoSQL database service for large analytical and operational workloads.
- [Blob Storage](https://gocloud.dev/howto/blob/) - A data storage architecture for large stores of unstructured data like google cloud storage, amazon s3, apache hadoop distributed filesystem

## How can I get started?

Explore the following resources to get started with Firehose:

- [Guides](./guides/create_firehose.md) provide guidance on creating Firehose with different sinks.
- [Concepts](./concepts/overview.md) describe all important Firehose concepts.
- [FAQs](./reference/faq.md) lists down some common frequently asked questions about Firehose and related components.
- [Reference](./advance/generic/) contains details about configurations, metrics, FAQs, and other aspects of Firehose.
- [Contributing](./contribute/contribution.md) contains resources for anyone who wants to contribute to Firehose.
