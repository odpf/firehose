# GRPC

[gRPC](https://grpc.io/) is a modern open source high performance Remote Procedure Call framework that can run in any environment.

A GRPC sink Firehose \(`SINK_TYPE`=`grpc`\) requires the following variables to be set along with Generic ones

### `SINK_GRPC_SERVICE_HOST`

Defines the host of the GRPC service.

- Example value: `http://grpc-service.sample.io`
- Type: `required`

### `SINK_GRPC_SERVICE_PORT`

Defines the port of the GRPC service.

- Example value: `8500`
- Type: `required`

### `SINK_GRPC_METHOD_URL`

Defines the URL of the GRPC method that needs to be called.

- Example value: `com.tests.SampleServer/SomeMethod`
- Type: `required`

### `SINK_GRPC_METADATA`

Defines the GRPC additional static Metadata that allows clients to provide information to server that is associated with an RPC.

Note - final metadata will be generated with merging static metadata and the kafka record header. 

- Example value: `authorization:token,dlq:true`
- Type: `optional`

### `SINK_GRPC_RESPONSE_SCHEMA_PROTO_CLASS`

Defines the Proto which would be the response of the GRPC Method.

- Example value: `com.tests.SampleGrpcResponse`
- Type: `required`

### `SINK_GRPC_ARG_KEEPALIVE_TIME_MS`

The keepalive ping is a way to check if a channel is currently working by sending HTTP2 pings over the transport. It is sent periodically, and if the ping is not acknowledged by the peer within a certain timeout period, the transport is disconnected. Other keepalive configurations are described [here](https://github.com/grpc/grpc/blob/master/doc/keepalive.md).

Defines the period (in milliseconds) after which a keepalive ping is sent on the transport. If smaller than 10000, 10000 will be used instead.

- Example value: `60000`
- Type: `optional`
- Default value: `infinite`

### `SINK_GRPC_ARG_KEEPALIVE_TIMEOUT_MS`

Defines the amount of time (in milliseconds) the sender of the keepalive ping waits for an acknowledgement. If it does not receive an acknowledgment within this time, it will close the connection.

- Example value: `5000`
- Type: `optional`
- Default value: `20000`

### `SINK_GRPC_ARG_DEADLINE_MS`

Defines the amount of time (in milliseconds) gRPC clients are willing to wait for an RPC to complete before the RPC is terminated with the error [DEADLINE_EXCEEDED](https://grpc.io/docs/guides/deadlines/#:~:text=By%20default%2C%20gRPC%20does%20not,realistic%20deadline%20in%20your%20clients.)

- Example value: `1000`
- Type: `optional`

### `SINK_GRPC_RESPONSE_RETRY_CEL_EXPRESSION`

Defines the CEL(Common Expression Language) expression used to evaluate whether gRPC sink call should be retried or not based on the gRPC response.
Currently, we support all standard CEL macro including: has, all, exists, exists_one, map, map_filter, filter
For more information about CEL please refer to this documentation : https://github.com/google/cel-spec/blob/master/doc/langdef.md

- Example value: `com.gotocompany.generic.GrpcResponse.success == false && com.gotocompany.generic.GenericResponse.errors.exists(e, int(e.code) >= 400 && int(e.code) <= 500)`
- Type: `optional`
- Default value: ``
- Use cases :
    Example response proto :
    ```
    syntax = "proto3";
    package com.gotocompany.generic;

      GenericResponse {
          bool success = 1;
          repeated Error errors = 2;
      }

      Error {
          string code = 1;
          string reason = 2;
      }
  ```

  Example retry rule : 
  - Retry on specific error code : `com.gotocompany.generic.GenericResponse.errors.exists(e, e.code == "400")`
  - Retry on specific error code range : `com.gotocompany.generic.GenericResponse.errors.exists(e, int(e.code) >= 400 && int(e.code) <= 500)`
  - Retry on error codes outside from specific error codes : `com.gotocompany.generic.GenericResponse.errors.exists(e, !(int(e.code) in [400, 500, 600]))`