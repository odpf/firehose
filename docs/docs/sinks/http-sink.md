# HTTP

Note: HTTP sink type is deprecated from Firehose version 0.10.0 onwards. Please consider using HTTPV2 sink type instead.

REST API stands for Representational State Transfer and is an architectural pattern for creating web services.

An Http sink Firehose \(`SINK_TYPE`=`http`\) requires the following variables to be set along with Generic ones.

### `SINK_HTTP_SERVICE_URL`

The HTTP endpoint of the service to which this consumer should PUT/POST/PATCH/DELETE data. This can be configured as per the requirement, a constant or a dynamic one \(which extract given field values from each message and use that as the endpoint\)
If service url is constant, messages will be sent as batches while in case of dynamic one each message will be sent as a separate request \(Since they’d be having different endpoints\).

- Example value: `http://http-service.test.io`
- Example value: `http://http-service.test.io/test-field/%%s,6` This will take the value with index 6 from proto and create the endpoint as per the template
- Type: `required`

### `SINK_HTTP_REQUEST_METHOD`

Defines the HTTP verb supported by the endpoint, Supports PUT, POST, PATCH and DELETE verbs as of now.

- Example value: `post`
- Type: `required`
- Default value: `put`

### `SINK_HTTP_REQUEST_TIMEOUT_MS`

Defines the connection timeout for the request in millis.

- Example value: `10000`
- Type: `required`
- Default value: `10000`

### `SINK_HTTP_MAX_CONNECTIONS`

Defines the maximum number of HTTP connections.

- Example value: `10`
- Type: `required`
- Default value: `10`

### `SINK_HTTP_RETRY_STATUS_CODE_RANGES`

Defines the range of HTTP status codes for which retry will be attempted. Please remove 404 from retry code range in case of HTTP DELETE otherwise it might try to retry to delete already deleted resources.

- Example value: `400-600`
- Type: `optional`
- Default value: `400-600`

### `SINK_HTTP_DATA_FORMAT`

If set to `proto`, the log message will be sent as Protobuf byte strings. Otherwise, the log message will be deserialized into readable JSON strings.

- Example value: `JSON`
- Type: `required`
- Default value: `proto`

### `SINK_HTTP_HEADERS`

Deifnes the HTTP headers required to push the data to the above URL.

- Example value: `Authorization:auth_token, Accept:text/plain`
- Type: `optional`

### `SINK_HTTP_JSON_BODY_TEMPLATE`

Deifnes a template for creating a custom request body from the fields of a protobuf message. This should be a valid JSON itself.

- Example value: `{"test":"$.routes[0]", "$.order_number" : "xxx"}`
- Type: `optional`

### `SINK_HTTP_PARAMETER_SOURCE`

Defines the source from which the fields should be parsed. This field should be present in order to use this feature.

- Example value: `Key`
- Example value: `Message`
- Type: `optional`
- Default value: `None`

### `SINK_HTTP_PARAMETER_PLACEMENT`

Deifnes the fields parsed can be passed in query parameters or in headers.

- Example value: `Header`
- Example value: `Query`
- Type: `optional`

### `SINK_HTTP_PARAMETER_SCHEMA_PROTO_CLASS`

Defines the fully qualified name of the proto class which is to be used for parametrised http sink.

- Example value: `com.tests.TestMessage`
- Type: `optional`

### `INPUT_SCHEMA_PROTO_TO_COLUMN_MAPPING`

Defines the mapping of the proto fields to header/query fields in JSON format.

- Example value: `{"1":"order_number","2":"event_timestamp","3":"driver_id"}`
- Type: `optional`

### `SINK_HTTP_OAUTH2_ENABLE`

Enable/Disable OAuth2 support for HTTP sink.

- Example value: `true`
- Type: `optional`
- Default value: `false`

### `SINK_HTTP_OAUTH2_ACCESS_TOKEN_URL`

Defines the OAuth2 Token Endpoint.

- Example value: `https://sample-oauth.my-api.com/oauth2/token`
- Type: `optional`

### `SINK_HTTP_OAUTH2_CLIENT_NAME`

Defines the OAuth2 identifier issued to the client.

- Example value: `client-name`
- Type: `optional`

### `SINK_HTTP_OAUTH2_CLIENT_SECRET`

Defines the OAuth2 secret issued for the client.

- Example value: `client-secret`
- Type: `optional`

### `SINK_HTTP_OAUTH2_SCOPE`

Space-delimited scope overrides. If scope override is not provided, no scopes will be granted to the token.

- Example value: `User:read, sys:info`
- Type: `optional`

### `SINK_HTTP_DELETE_BODY_ENABLE`

This config if set to true will allow body for the HTTP DELETE method, otherwise no payload will be sent with DELETE request.

- Example value: `false`
- Type: `optional`
- Default value: `true`

### `SINK_HTTP_SIMPLE_DATE_FORMAT_ENABLE`

This config if set to `true` will enable the simple date format (Eg. - `May 3, 2023 11:59:37 AM` ) for timestamps (both at root and nested level also) in case of JSON payload. If set to `false`, it will send the timestamps (both at root and nested level also) in the ISO format (Eg. - `2023-05-03T11:59:36.965Z`) . Note: This config is only applicable when the `SINK_HTTP_JSON_BODY_TEMPLATE` config is empty or not supplied.

- Example value: `false`
- Type: `optional`
- Default value: `true`

### `SINK_HTTP_SERIALIZER_JSON_TYPECAST`

Defines the mapping for field typecasting of the resulting JSON Serialization. This configuration could accept multiple mappings for multiple JSON Path
Currently supported typecasting target: DOUBLE, INTEGER, LONG, STRING

- Example value: `[{"jsonPath": "$.root.someIntegerField", "type": "INTEGER"}, {"jsonPath": "$..[*].doubleField", "type": "DOUBLE"}]`
- Type: `optional`
- Default Value: `[]`