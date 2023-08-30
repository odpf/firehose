package com.gotocompany.firehose.sink.dlq.blobstorage;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class DlqMessage {
    @JsonProperty("key")
    private String key;
    @JsonProperty("value")
    private String value;
    @JsonProperty("topic")
    private String topic;
    @JsonProperty("partition")
    private int partition;
    @JsonProperty("offset")
    private long offset;
    @JsonProperty("timestamp")
    private long timestamp;
    @JsonProperty("error")
    private String error;
    @JsonProperty("error_type")
    private String errorType;
}
