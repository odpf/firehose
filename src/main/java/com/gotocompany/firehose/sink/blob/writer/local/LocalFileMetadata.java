package com.gotocompany.firehose.sink.blob.writer.local;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;

@AllArgsConstructor
@Getter
@EqualsAndHashCode
public class LocalFileMetadata {
    private final String basePath;
    private final String fullPath;
    private final long createdTimestampMillis;
    private final long recordCount;
    private final long size;
}
