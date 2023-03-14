package com.gotocompany.firehose.config;

import com.gotocompany.firehose.config.converter.BlobStorageTypeConverter;
import com.gotocompany.firehose.config.converter.DlqWriterTypeConverter;
import com.gotocompany.firehose.sink.common.blobstorage.BlobStorageType;
import com.gotocompany.firehose.sink.dlq.DLQWriterType;

public interface DlqConfig extends AppConfig {

    @Key("DLQ_WRITER_TYPE")
    @ConverterClass(DlqWriterTypeConverter.class)
    @DefaultValue("LOG")
    DLQWriterType getDlqWriterType();

    @Key("DLQ_BLOB_STORAGE_TYPE")
    @DefaultValue("GCS")
    @ConverterClass(BlobStorageTypeConverter.class)
    BlobStorageType getBlobStorageType();

    @Key("DLQ_RETRY_MAX_ATTEMPTS")
    @DefaultValue("2147483647")
    Integer getDlqRetryMaxAttempts();

    @Key("DLQ_RETRY_FAIL_AFTER_MAX_ATTEMPT_ENABLE")
    @DefaultValue("false")
    boolean getDlqRetryFailAfterMaxAttemptEnable();

    @Key("DLQ_SINK_ENABLE")
    @DefaultValue("false")
    boolean getDlqSinkEnable();

}
