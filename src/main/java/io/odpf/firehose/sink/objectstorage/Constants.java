package io.odpf.firehose.sink.objectstorage;

public class Constants {


    public enum WriterType {
        PARQUET,
    }

    public enum PartitioningType {
        NONE,
        DAY,
        HOUR
    }
}
