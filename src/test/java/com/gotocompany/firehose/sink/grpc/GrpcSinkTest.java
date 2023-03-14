package com.gotocompany.firehose.sink.grpc;

import com.gotocompany.firehose.exception.DeserializerException;
import com.gotocompany.firehose.message.Message;
import com.gotocompany.firehose.metrics.FirehoseInstrumentation;
import com.gotocompany.firehose.sink.grpc.client.GrpcClient;
import com.gotocompany.depot.error.ErrorInfo;
import com.gotocompany.depot.error.ErrorType;
import com.gotocompany.firehose.consumer.TestGrpcResponse;
import com.google.protobuf.DynamicMessage;
import com.gotocompany.stencil.client.StencilClient;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;


public class GrpcSinkTest {

    private GrpcSink sink;

    @Mock
    private Message message;

    @Mock
    private GrpcClient grpcClient;

    @Mock
    private StencilClient stencilClient;

    @Mock
    private FirehoseInstrumentation firehoseInstrumentation;

    @Before
    public void setUp() {
        initMocks(this);
        sink = new GrpcSink(firehoseInstrumentation, grpcClient, stencilClient);
    }

    @Test
    public void shouldWriteToSink() throws Exception {
        when(message.getLogMessage()).thenReturn(new byte[]{});
        RecordHeaders headers = new RecordHeaders();
        when(message.getHeaders()).thenReturn(headers);
        TestGrpcResponse build = TestGrpcResponse.newBuilder().setSuccess(true).build();
        DynamicMessage response = DynamicMessage.parseFrom(build.getDescriptorForType(), build.toByteArray());
        when(grpcClient.execute(any(byte[].class), any(RecordHeaders.class))).thenReturn(response);

        sink.pushMessage(Collections.singletonList(message));
        verify(grpcClient, times(1)).execute(any(byte[].class), eq(headers));

        verify(firehoseInstrumentation, times(1)).logInfo("Preparing {} messages", 1);
        verify(firehoseInstrumentation, times(1)).logDebug("Response: {}", response);
        verify(firehoseInstrumentation, times(0)).logWarn("Grpc Service returned error");
        verify(firehoseInstrumentation, times(1)).logDebug("Failed messages count: {}", 0);
    }

    @Test
    public void shouldReturnBackListOfFailedMessages() throws IOException, DeserializerException {
        when(message.getLogMessage()).thenReturn(new byte[]{});
        when(message.getHeaders()).thenReturn(new RecordHeaders());
        when(message.getErrorInfo()).thenReturn(new ErrorInfo(null, ErrorType.DESERIALIZATION_ERROR));
        TestGrpcResponse build = TestGrpcResponse.newBuilder().setSuccess(false).build();
        DynamicMessage response = DynamicMessage.parseFrom(build.getDescriptorForType(), build.toByteArray());
        when(grpcClient.execute(any(), any(RecordHeaders.class))).thenReturn(response);
        List<Message> failedMessages = sink.pushMessage(Collections.singletonList(message));

        assertFalse(failedMessages.isEmpty());
        assertEquals(1, failedMessages.size());

        verify(firehoseInstrumentation, times(1)).logInfo("Preparing {} messages", 1);
        verify(firehoseInstrumentation, times(1)).logDebug("Response: {}", response);
        verify(firehoseInstrumentation, times(1)).logWarn("Grpc Service returned error");
        verify(firehoseInstrumentation, times(1)).logDebug("Failed messages count: {}", 1);
    }

    @Test
    public void shouldCloseStencilClient() throws IOException {
        sink = new GrpcSink(firehoseInstrumentation, grpcClient, stencilClient);

        sink.close();
        verify(stencilClient, times(1)).close();
    }

    @Test
    public void shouldLogWhenClosingConnection() throws IOException {
        sink = new GrpcSink(firehoseInstrumentation, grpcClient, stencilClient);

        sink.close();
        verify(firehoseInstrumentation, times(1)).logInfo("GRPC connection closing");
    }
}
