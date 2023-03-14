# Metrics

Service-level Indicators \(SLIs\) are the measurements used to calculate the performance for the goal. It is a direct measurement of a service’s behaviour and helps us and the users to evaluate whether our system has been running within SLO. The metrics captured as part of SLI for Firehose are described below.

## Table of Contents

* [Type Details](metrics.md#type-details)
* [Overview](metrics.md#overview)
* [Pods Health](metrics.md#pods-health)
* [Kafka Consumer Details](metrics.md#kafka-consumer-details)
* [Error](metrics.md#error)
* [Memory](metrics.md#memory)
* [Error](metrics.md#error)
* [Garbage Collection](metrics.md#garbage-collection)
* [Retry](metrics.md#retry)
* [HTTP Sink](metrics.md#http-sink)
* [Filter](metrics.md#filter)
* [Blob Sink](metrics.md#blob-sink)
* [Bigquery Sink](https://github.com/goto/depot/blob/main/docs/reference/metrics.md#bigquery-sink)

## Type Details

Collection of all the generic configurations in a Firehose.

### `Sink`

* The type of sink of the Firehose. It could be 'log', 'HTTP', 'DB', 'redis', 'influx' or 'Elasticsearch'

### `Team`

* Team who has the ownership for the given Firehose.

### `Proto Schema`

* The proto class used for creating the Firehose

### `Stream`

* The stream where the input topic is read from

## Overview

Some of the most important metrics related to Firehose that gives you an overview of the current state of it.

### `ConsumerLag: MaxLag`

* The maximum lag in terms of number of records for any partition in this window. An increasing value over time is your best indication that the consumer group is not keeping up with the producers.

### `Total Message Received`

* Sum of all messages received from Kafka per pod.

### `Message Sent Successfully`

* Messages sent successfully to the sink per batch per pod.

### `Message Sent Failed`

* Messages failed to be pushed into the sink per batch per pod. In case of HTTP sink, if status code is not in retry codes configured, the records will be dropped.

### `Message Dropped`

* In case of HTTP sink, when status code is not in retry codes configured, the records are dropped. This metric captures the dropped messages count.

### `Batch size Distribution`

* 99p of batch size distribution for pulled and pushed messages per pod. 

### `Time spent in Firehose`

* Latency introduced by Firehose \(time before sending to sink - time when reading from Kafka\). Note: It could be high if the response time of the sink is higher as subsequent batches could be delayed. 

### `Time spent in pipeline`

* Time difference between Kafka ingestion and sending to sink \(Time before sending to sink - Time of Kafka ingestion\) 

### `Sink Response Time`

* Different percentile of the response time of the sink. 

## Pods Health

Since Firehose runs on Kube, this gives a nice health details of each pods.

### `JVM Lifetime`

* JVM Uptime of each pod. 

### `Cpu Load`

* Returns the "recent cpu usage" for the Java Virtual Machine process. This value is a double in the \[0.0,1.0\] interval. A value of 0.0 means that none of the CPUs were running threads from the JVM process during the recent period of time observed, while a value of 1.0 means that all CPUs were actively running threads from the JVM 100% of the time during the recent period being observed. Threads from the JVM include the application threads as well as the JVM internal threads. All values betweens 0.0 and 1.0 are possible depending of the activities going on in the JVM process and the whole system. If the Java Virtual Machine recent CPU usage is not available, the method returns a negative value. 

### `Cpu Time`

* Returns the CPU time used by the process on which the Java virtual machine is running. The returned value is of nanoseconds precision but not necessarily nanoseconds accuracy. 

## Kafka Consumer Details

Listing some of the Kafka consumer metrics here.

### `Assigned partitions`

* Consumer Group Metrics: The number of partitions currently assigned to this consumer \(per pod\). 

### `Consumer Number of Request/second`

* Global Request Metrics: The average number of requests sent per second per pod. 

### `Records Consumed Rate/second`

* Topic-level Fetch Metrics: The average number of records consumed per second for a specific topic per pod. 

### `Bytes Consumed Rate/second`

* Topic-level Fetch Metrics: The average number of bytes consumed per second per pod. 

### `Fetch Rate/second`

* Fetch Metrics: The number of fetch requests per second per pod. 

### `Max Fetch Latency`

* Fetch Metrics: The max time taken for a fetch request per pod. 

### `Average Fetch Latency`

* Fetch Metrics: The average time taken for a fetch request per pod. 

### `Average Fetch Size`

* Fetch Metrics: The average number of bytes fetched per request per pod.

### `Max Fetch Size`

* Fetch Metrics: The max number of bytes fetched per request per pod.

### `Commit Rate/second`

* Consumer Group Metrics: The number of commit calls per second per pod.

### `Consumer Active Connections Count`

* Global Connection Metrics: The current number of active connections per pod.

### `New Connections Creation Rate/second`

* Global Connection Metrics: New connections established per second in the window per pod. 

### `Connections Close Rate/second`

* Global Connection Metrics: Connections closed per second in the window per pod. 

### `Consumer Outgoing Byte Rate/Sec`

* Global Request Metrics: The average number of outgoing bytes sent per second to all servers per pod. 

### `Avg time between poll`

* Average time spent between poll per pod. 

### `Max time between poll`

* Max time spent between poll per pod. 

### `Sync rate`

* Consumer Group Metrics: The number of group syncs per second per pod. Group synchronization is the second and last phase of the rebalance protocol. Similar to join-rate, a large value indicates group instability. 

### `Consumer Network IO rate /second`

* The average number of network operations \(reads or writes\) on all connections per second per pod 

### `Rebalance Rate /hour`

* Rate of rebalance the consumer.  

### `Average Commit latency`

* Consumer Group Metrics: The average time taken for a commit request per pod 

### `Max Commit latency`

* Consumer Group Metrics: The max time taken for a commit request per pod. 

### `Avg Rebalance latency`

* Average Rebalance Latency for the consumer per pod. 

### `Max Rebalance latency`

* Max Rebalance Latency for the consumer per pod.

## Error

This gives you a nice insight about the critical and noncritical exceptions happened in the Firehose.

### `Fatal Error`

* Count of all the exception raised by the pods which can restart the Firehose.

### `Nonfatal Error`

* Count of all the exception raised by the Firehose which will not restart the Firehose and Firehose will keep retrying.

## Memory

Details on memory used by the Firehose for different tasks.

### `Heap Memory Usage`

* Details of heap memory usage:

  ```text
  Max: The amount of memory that can be used for memory management
  Used: The amount of memory currently in use
  ```

### `Non-Heap Memory Usage`

* Details of non-heap memory usage:

  ```text
  Max: The amount of memory that can be used for memory management
  Used: The amount of memory currently in use
  ```

### `GC: Memory Pool Collection Usage`

* For a garbage-collected memory pool, the amount of used memory includes the memory occupied by all objects in the pool including both reachable and unreachable objects. This is for all the names in the type: MemoryPool.

### `GC: Memory Pool Peak Usage`

* Peak usage of GC memory usage.

### `GC: Memory Pool Usage`

* Total usage of GC memory usage.

## Garbage Collection

All JVM Garbage Collection Details.

### `GC Collection Count`

* The total number of collections that have occurred per pod. Rather than showing the absolute value we are showing the difference to see the rate of change more easily.

### `GC Collection Time`

* The approximate accumulated collection elapsed time in milliseconds per pod. Rather than showing the absolute value we are showing the difference to see the rate of change more easily.

### `Thread Count`

* daemonThreadCount: Returns the current number of live daemon threads per pod peakThreadCount: Returns the peak live thread count since the Java virtual machine started or peak was reset per pod threadCount: Returns the current number of live threads including both daemon and non-daemon threads per pod.

### `Class Count`

* loadedClass: Displays number of classes that are currently loaded in the Java virtual machine per pod unloadedClass: Displays the total number of classes unloaded since the Java virtual machine has started execution.

### `Code Cache Memory after GC`

* The code cache memory usage in the memory pools at the end of a GC per pod.

### `Compressed Class Space after GC`

* The compressed class space memory usage in the memory pools at the end of a GC per pod.

### `Metaspace after GC`

* The metaspace memory usage in the memory pools at the end of a GC per pod.

### `Par Eden Space after GC`

* The eden space memory usage in the memory pools at the end of a GC per pod.

### `Par Survivor Space after GC`

* The survivor space memory usage in the memory pools at the end of a GC per pod.

### `Tenured Space after GC`

* The tenured space memory usage in the memory pools at the end of a GC per pod.

  **`File Descriptor`**

* Number of file descriptor per pod

  ```text
  Open: Current open file descriptors
  Max: Based on config max allowed
  ```

## Retry

If you have configured retries this will give you some insight about the retries.

### `Average Retry Requests`

* Request retries per min per pod.

### `Back Off time`

* Time spent per pod backing off.

## HTTP Sink

HTTP Sink response code details.

### `2XX Response Count`

* Total number of 2xx response received by Firehose from the HTTP service,

### `4XX Response Count`

* Total number of 4xx response received by Firehose from the HTTP service.

### `5XX Response Count`

* Total number of 5xx response received by Firehose from the HTTP service.

### `No Response Count`

* Total number of No response received by Firehose from the HTTP service.

## Filter

Since Firehose supports filtration based on some data, these metrics give some information related to that.

### `Filter Type`

* Type of filter in the Firehose. It will be one of the "none", "key", "message".

### `Total Messages filtered`

* Sum of all the messages filtered because of the filter condition per pod.

## Blob Sink

### `Local File Open Total`
A gauge, total number of local file that is currently being opened.

### `Local File Closed Total`

Total number of local file that being closed and ready to be uploaded, excluding local file that being closed prematurely due to consumer restart.

### `Local File Closing Time`

Duration of local file closing time.

### `Local File Records Total`

Total number of records that written to all files that have been closed and ready to be uploaded.

### `Local File Size Bytes`

Size of file in bytes.

### `File Uploaded Total`

Total number file that successfully being uploaded.

### `File Upload Time`

Duration of file upload.

### `File Upload Size Bytes`

Total Size of the uploaded file in bytes.

### `File Upload Records Total`

Total number records inside files that successfully being uploaded to blob storage.
