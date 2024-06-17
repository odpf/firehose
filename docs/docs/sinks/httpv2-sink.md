# HttpV2 Sink

HttpV2 Sink is implemented in Firehose using the Http sink connector implementation in Depot library. For details on all the features supported by HttpV2 Sink, please refer the Depot documentation [here](https://github.com/goto/depot/blob/main/docs/sinks/http-sink.md).

### Configuration

For HttpV2 sink in Firehose we need to set first (`SINK_TYPE`=`httpv2`). There are some generic configs which are common across different sink types which need to be set which are mentioned in [generic.md](../advance/generic.md). Http sink specific configs are mentioned in Depot repository. You can check out the Http Sink configs [here](https://github.com/goto/depot/blob/main/docs/reference/configuration/http-sink.md)

## Changelogs

Here’s a detailed and descriptive changelog to help you understand the differences between HTTP V1 and HTTP V2:

1. **JSON Payload Template Format**:
   - **HTTP V1**: Uses JSONPath format for setting the JSON payload template. For example, you might have a configuration like `{"test":"$.order_number"}` to specify the field.
   - **HTTP V2**: Shifts to using printf format for JSON payload templates. The equivalent configuration would be `{"test":"%s,order_number"}`. This change makes template expressions more consistent with many programming environments.

2. **Parameter Placement**:
   - **HTTP V1**: Parameters are placed either in the header or query string according to the `SINK_HTTP_PARAMETER_PLACEMENT` setting.
   - **HTTP V2**: Parameters can now be placed in both the header and query string simultaneously, offering more flexibility in parameter placement.

3. **Parameter Source Configuration**:
   - **HTTP V1**: Uses a single setting, `SINK_HTTP_PARAMETER_SOURCE`, to determine the source for both header and query parameters, which could be set to either `key` or `message`.
   - **HTTP V2**: Separates these configurations with `SINK_HTTPV2_HEADERS_PARAMETER_SOURCE` for header parameters and `SINK_HTTPV2_QUERY_PARAMETER_SOURCE` for query parameters, providing more granular control.

4. **Template Configuration for Header/Query Parameters**:
   - **HTTP V1**: Uses `INPUT_SCHEMA_PROTO_TO_COLUMN_MAPPING` globally for both header and query parameter templates.
   - **HTTP V2**: Introduces `SINK_HTTPV2_HEADERS_TEMPLATE` for header parameters and `SINK_HTTPV2_QUERY_TEMPLATE` for query parameters, allowing distinct template settings for each.

5. **Field Indexes vs. Field Names**:
   - **HTTP V1**: Utilizes proto field indexes for setting header and query parameters. For example, `{"2":"Order"}`.
   - **HTTP V2**: Uses proto field names instead. The equivalent would be `{"Order":"%s,order_id"}`, which makes configurations easier to understand and maintain.

6. **Parameter Ordering**:
   - **HTTP V1**: Maintains the ordering of parameters in header or query based on the field indexes. For instance, a template like `{"2":"Order"}` will result in `?Order=xyz234`.
   - **HTTP V2**: Reverses the ordering, so the same template would translate to `?xyz234=Order`, creating a different query string structure.

7. **Request Mode Selection**:
   - **HTTP V1**: Automatically decides between single and batch mode.
   - **HTTP V2**: Explicitly requires you to set the request mode using `SINK_HTTPV2_REQUEST_MODE`, which can be either single or batch.

8. **Metadata Inclusion**:
   - **HTTP V1**: Automatically includes Kafka topic metadata in the requests.
   - **HTTP V2**: Requires additional settings like `SINK_ADD_METADATA_ENABLED=true` and `SINK_METADATA_COLUMNS_TYPES=message_topic=string` to include Kafka topic metadata.

9. **JSON Payload Configuration**:
   - **HTTP V1**: For JSON payloads, you set `SINK_HTTP_DATA_FORMAT=json` and leave `SINK_HTTP_JSON_BODY_TEMPLATE` empty.
   - **HTTP V2**: Uses `SINK_HTTPV2_REQUEST_BODY_MODE=json` to specify that the request body should be JSON formatted.

10. **Templatized JSON Payload**:
    - **HTTP V1**: Requires setting `SINK_HTTP_DATA_FORMAT=json` and defining `SINK_HTTP_JSON_BODY_TEMPLATE` for templatized JSON payloads.
    - **HTTP V2**: Utilizes `SINK_HTTPV2_REQUEST_BODY_MODE=templatized_json` for similar functionality, making the configuration more intuitive.

11. **Raw Proto Payload**:
    - **HTTP V1**: Configures raw proto payloads with `SINK_HTTP_DATA_FORMAT=proto`.
    - **HTTP V2**: Uses `SINK_HTTPV2_REQUEST_BODY_MODE=raw` to handle such payloads.

12. **Templatized Service URL**:
    - **HTTP V1**: Incorporates proto field indexes in URL templates, like `http://abc.com/e%%s,2`.
    - **HTTP V2**: Moves to proto field names in URLs, such as `http://abc.com/e%s,order_number`, improving readability.

13. **Nested Field Handling**:
    - **HTTP V1**: Automatically sends default values for non-empty fields even if nested fields are empty. For example, `driver_pickup_location`.
    - **HTTP V2**: Omits fields that are empty, providing cleaner and more accurate payloads.

14. **Empty Field Handling**:
    - **HTTP V1**: Throws exceptions if nested fields like `driver_pickup_location` are empty.
    - **HTTP V2**: Manages empty fields differently by sending default values instead of throwing exceptions.

15. **Timestamp Field Format**:
    - **HTTP V1**: Supports both simple date and ISO formats for timestamp fields.
    - **HTTP V2**: Supports only ISO format, simplifying and standardizing date handling.

16. **Template Parameter Use**:
    - **HTTP V1**: Allows template parameters to be used in URL, header, query, and body configurations.
    - **HTTP V2**: Restricts the use of template parameters in batch mode for URL, header, and query to ensure consistency and prevent errors.

17. **Template Parameter Mode**:
    - **HTTP V1**: Restricts templatized JSON body templates to single mode only.
    - **HTTP V2**: Supports the use of templatized JSON body in both batch and single modes, increasing flexibility.

18. **Field Level Usage**:
    - **HTTP V1**: Allows only top-level proto fields to be used in configurations.
    - **HTTP V2**: Supports nested level proto fields, offering more detailed data handling capabilities.

19. **Retryable Error Handling**:
    - **HTTP V1**: Automatically retries on certain response status codes by default.
    - **HTTP V2**: Requires adding `SINK_RETRYABLE_ERROR` to `ERROR_TYPES_FOR_RETRY` to specify which errors are retryable, providing more controlled error handling.

20. **JSON Support**:
    - **HTTP V1**: Does not support JSON as an input message data type.
    - **HTTP V2**: Adds support for JSON input message data type, broadening its usability.


