package io.odpf.firehose.sink.objectstorage.writer.local;

import io.odpf.firehose.sink.objectstorage.Constants;
import lombok.AllArgsConstructor;
import lombok.Data;

@AllArgsConstructor
@Data
public class PartitionConfig {
    private String zone;
    private Constants.PartitioningType partitioningType;
    private String datePrefix;
    private String hourPrefix;
}
