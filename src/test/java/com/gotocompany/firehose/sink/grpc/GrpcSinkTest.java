package com.gotocompany.firehose.sink.grpc;

import com.google.protobuf.InvalidProtocolBufferException;
import com.gotocompany.firehose.config.GrpcSinkConfig;
import com.gotocompany.firehose.consumer.GenericError;
import com.gotocompany.firehose.consumer.GenericResponse;
import com.gotocompany.firehose.evaluator.DefaultGrpcResponsePayloadEvaluator;
import com.gotocompany.firehose.evaluator.GrpcResponseCelPayloadEvaluator;
import com.gotocompany.firehose.evaluator.PayloadEvaluator;
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
import org.junit.jupiter.api.Assertions;
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

    @Mock
    private GrpcSinkConfig grpcSinkConfig;

    private PayloadEvaluator<com.google.protobuf.Message> grpcResponsePayloadEvaluator;
    @Before
    public void setUp() {
        initMocks(this);
        this.grpcResponsePayloadEvaluator = new GrpcResponseCelPayloadEvaluator(
                GenericResponse.getDescriptor(),
                "GenericResponse.success == false && GenericResponse.errors.exists(e, e.code == \"4000\")"
        );
        sink = new GrpcSink(firehoseInstrumentation, grpcClient, stencilClient, new DefaultGrpcResponsePayloadEvaluator());
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
        verify(firehoseInstrumentation, times(1)).logDebug("Retrying grpc service");
    }

    @Test
    public void shouldCloseStencilClient() throws IOException {
        sink = new GrpcSink(firehoseInstrumentation, grpcClient, stencilClient, this.grpcResponsePayloadEvaluator);

        sink.close();
        verify(stencilClient, times(1)).close();
    }

    @Test
    public void shouldLogWhenClosingConnection() throws IOException {
        sink = new GrpcSink(firehoseInstrumentation, grpcClient, stencilClient, this.grpcResponsePayloadEvaluator);

        sink.close();
        verify(firehoseInstrumentation, times(1)).logInfo("GRPC connection closing");
    }

    @Test
    public void shouldReturnFailedMessagesWithRetryableErrorsWhenCELExpressionMatches() throws InvalidProtocolBufferException {
        sink = new GrpcSink(firehoseInstrumentation, grpcClient, stencilClient, this.grpcResponsePayloadEvaluator);
        Message payload = new Message(new byte[]{}, new byte[]{}, "topic", 0, 1);
        GenericResponse response = GenericResponse.newBuilder()
                .setSuccess(false)
                .setDetail("detail")
                .addErrors(GenericError.newBuilder()
                        .setCode("4000")
                        .setCause("cause")
                        .setEntity("gtf")
                        .build())
                .build();
        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(
                response.getDescriptorForType(),
                response.toByteArray()
        );
        when(grpcClient.execute(any(), any()))
                .thenReturn(dynamicMessage);
        sink = new GrpcSink(firehoseInstrumentation, grpcClient, stencilClient, grpcResponsePayloadEvaluator);

        List<Message> result = sink.pushMessage(Collections.singletonList(payload));

        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals(result.get(0).getErrorInfo().getErrorType(), ErrorType.SINK_RETRYABLE_ERROR);
    }

    @Test
    public void shouldReturnFailedMessagesWithNonRetryableErrorsWhenCELExpressionDoesntMatch() throws InvalidProtocolBufferException {
        sink = new GrpcSink(firehoseInstrumentation, grpcClient, stencilClient, this.grpcResponsePayloadEvaluator);
        Message payload = new Message(new byte[]{}, new byte[]{}, "topic", 0, 1);
        GenericResponse response = GenericResponse.newBuilder()
                .setSuccess(false)
                .setDetail("detail")
                .addErrors(GenericError.newBuilder()
                        .setCode("not-exist-code")
                        .setCause("cause")
                        .setEntity("gtf")
                        .build())
                .build();
        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(
                response.getDescriptorForType(),
                response.toByteArray()
        );
        when(grpcClient.execute(any(), any()))
                .thenReturn(dynamicMessage);
        sink = new GrpcSink(firehoseInstrumentation, grpcClient, stencilClient, grpcResponsePayloadEvaluator);

        List<Message> result = sink.pushMessage(Collections.singletonList(payload));

        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals(result.get(0).getErrorInfo().getErrorType(), ErrorType.SINK_NON_RETRYABLE_ERROR);
    }
}
