# Overview

The following topics will describe key concepts of Firehose.

## [Architecture](architecture.md)

Firehose is a fully managed service that allows you to consume messages to any sink from kafka streams in realtime at
scale. This section explains the overall architecture of Firehose and describes its various components.

## [Monitoring Firehose with exposed metrics](monitoring.md)

Always know what’s going on with your deployment with
built-in [monitoring](https://github.com/goto/firehose/blob/main/docs/assets/firehose-grafana-dashboard.json) of
throughput, response times, errors and more. This section contains guides, best practices and advises related to
managing Firehose in production.

## [Filters](filters.md)

Use the Filter feature provided in Firehose which allows you to apply any filters on the fields present in the key or
message of the event data set and helps you narrow it down to your use case-specific data.

## [Templating](templating.md)

Firehose provides various templating features

## [Decorators](decorators.md)

Decorators are used for chained processing of messages.

* SinkWithFailHandler
* SinkWithRetry
* SinkWithDlq
* SinkFinal

## [FirehoseConsumer](consumer.md)
A firehose consumer read messages from kafka, pushes those messages to sink and commits offsets back to kafka based on certain strategies.

## [Offsets](offsets.md)

Offset manager is a data structure used to manage offsets asynchronously. An offset should only be committed when a
message is processed fully. Offset manager maintains a state of all the offsets of all topic-partitions, that can be
committed. It can also be used by sinks to manage its own offsets.


