package com.gotocompany.firehose.sink.blob.writer.local;


import com.google.protobuf.Descriptors;
import com.gotocompany.firehose.config.BlobSinkConfig;
import com.gotocompany.firehose.metrics.FirehoseInstrumentation;
import com.gotocompany.firehose.sink.blob.Constants;
import com.gotocompany.firehose.sink.blob.writer.local.policy.WriterPolicy;
import org.junit.Test;
import org.mockito.Mockito;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class LocalStorageTest {

    @Test
    public void shouldDeleteFiles() throws Exception {
        BlobSinkConfig sinkConfig = Mockito.mock(BlobSinkConfig.class);
        List<Descriptors.FieldDescriptor> metadataFieldDescriptor = new ArrayList<>();
        List<WriterPolicy> policies = new ArrayList<>();
        FirehoseInstrumentation firehoseInstrumentation = Mockito.mock(FirehoseInstrumentation.class);
        LocalStorage storage = new LocalStorage(sinkConfig, null, metadataFieldDescriptor, policies, firehoseInstrumentation);
        LocalStorage spy = Mockito.spy(storage);
        Mockito.doNothing().when(spy).deleteLocalFile(Paths.get("/tmp/a"), Paths.get("/tmp/.a.crc"));
        Mockito.when(sinkConfig.getLocalFileWriterType()).thenReturn(Constants.WriterType.PARQUET);
        spy.deleteLocalFile("/tmp/a");
        Mockito.verify(spy, Mockito.times(1)).deleteLocalFile(Paths.get("/tmp/a"), Paths.get("/tmp/.a.crc"));
        Mockito.verify(firehoseInstrumentation, Mockito.times(1)).logInfo("Deleting Local File {}", Paths.get("/tmp/a"));
        Mockito.verify(firehoseInstrumentation, Mockito.times(1)).logInfo("Deleting Local File {}", Paths.get("/tmp/.a.crc"));
    }
}
