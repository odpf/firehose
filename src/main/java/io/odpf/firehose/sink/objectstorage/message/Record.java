package io.odpf.firehose.sink.objectstorage.message;

import com.google.protobuf.DynamicMessage;
import lombok.AllArgsConstructor;
import lombok.Data;

@AllArgsConstructor
@Data
public class Record {
    private DynamicMessage message;
    private DynamicMessage metadata;
}
