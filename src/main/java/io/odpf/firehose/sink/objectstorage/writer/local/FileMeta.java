package io.odpf.firehose.sink.objectstorage.writer.local;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class FileMeta {
    private String fullPath;
    private Long recordCount;
    private Long fileSizeBytes;
    private Partition partition;
}
