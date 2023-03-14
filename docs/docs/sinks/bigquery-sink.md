# BigQuery

Bigquery Sink has several responsibilities, first creation of bigquery table and dataset when they are not exist, second update the bigquery table schema based on the latest schema defined in stencil or infer from incoming data, third translate incoming messages into bigquery records and insert them to bigquery tables.
Bigquery utilise Bigquery [Streaming API](https://cloud.google.com/bigquery/streaming-data-into-bigquery) to insert record into bigquery tables. For more info on the sink refer to [Depot Bigquery sink documentation](https://github.com/goto/depot/blob/main/docs/sinks/bigquery.md)

## Asynchronous consumer mode

Bigquery Streaming API limits size of payload sent for each insert operations. The limitation reduces the amount of message allowed to be inserted when the message size is big.
This will reduce the throughput of bigquery sink. To increase the throughput, firehose provide kafka consumer asynchronous mode.
In asynchronous mode sink operation is executed asynchronously, so multiple sink task can be scheduled and run concurrently.
Throughput can be increased by increasing the number of sink pool.

## At Least Once Guarantee

Because of asynchronous consumer mode and the possibility of retry on the insert operation. There is no guarantee of the message order that successfully sent to the sink.
That also happened with commit offset, the there is no order of the offset number of the processed messages.
Firehose collect all the offset sort them and only commit the latest continuous offset.
This will ensure all the offset being committed after messages successfully processed even when some messages are being re processed by retry handler or when the insert operation took a long time.

## Configurations
For Bigquery sink in Firehose we need to set first \(`SINK_TYPE`=`bigquery`\). There are some generic configs which are common across different sink types which need to be set example: kafka consumer configs, the generic ones are mentioned in [generic.md](../advance/generic.md). Bigquery sink specific configs are mentioned in depot [Depot-configuration/bigquery-sink.md section](https://github.com/goto/depot/blob/main/docs/reference/configuration/bigquery-sink.md)


## Bigquery table schema update
Refer to [Depot-bigquery.md#bigquery-table-schema-update section](https://github.com/goto/depot/blob/main/docs/sinks/bigquery.md#bigquery-table-schema-update)

## Protobuf and BigQuery table type mapping
For type conversion between protobuf to bigquery type. Please refer to
[Depot-bigquery.md#protobuf-bigquery-table-type-mapping section](https://github.com/goto/depot/blob/main/docs/sinks/bigquery.md#protobuf---bigquery-table-type-mapping)

## Partitioning
Bigquery Sink supports creation of table with partition configuration.
For more information refer to [Depot-bigquery.md#partitioning section](https://github.com/goto/depot/blob/main/docs/sinks/bigquery.md#partitioning)

## Clustering
Bigquery Sink supports for creating and modifying clustered or unclustered table with clustering configuration.
For more information refer to [Depot-bigquery.md#clustering section](https://github.com/goto/depot/blob/main/docs/sinks/bigquery.md#clustering)

## Kafka Metadata
For data quality checking purpose sometimes kafka metadata need to be added on the record. For more information refer to [Depot-bigquery.md#metadata sectionn](https://github.com/goto/depot/blob/main/docs/sinks/bigquery.md#metadata)

## Default columns for json data type
With dynamic schema for json we need to create table with some default columns, example like parition key needs to be set during creation of the table. Sample config `SINK_BIGQUERY_DEFAULT_COLUMNS =event_timestamp=timestamp`. For more information refer to [Depot-bigquery.md#default-columns-for-json-data-type section](https://github.com/goto/depot/blob/main/docs/sinks/bigquery.md#default-columns-for-json-data-type)

## Error handling
The response can contain multiple errors which will be sent to the firehose from depot. Please refer to [Depot-bigquery.md#errors-handling section](https://github.com/goto/depot/blob/main/docs/sinks/bigquery.md#errors-handling)


## Google Cloud Bigquery IAM Permission
Several IAM permission is required for bigquery sink to run properly. For more detail refer to [Depot-bigquery.md#google-cloud-bigquery-iam-permission section](https://github.com/goto/depot/blob/main/docs/sinks/bigquery.md#google-cloud-bigquery-iam-permission)


