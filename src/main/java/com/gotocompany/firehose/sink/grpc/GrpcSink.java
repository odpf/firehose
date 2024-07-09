package com.gotocompany.firehose.sink.grpc;


import com.gotocompany.depot.error.ErrorInfo;
import com.gotocompany.depot.error.ErrorType;
import com.gotocompany.firehose.config.GrpcSinkConfig;
import com.gotocompany.firehose.evaluator.PayloadEvaluator;
import com.gotocompany.firehose.exception.DefaultException;
import com.gotocompany.firehose.exception.DeserializerException;
import com.gotocompany.firehose.message.Message;
import com.gotocompany.firehose.metrics.FirehoseInstrumentation;
import com.gotocompany.firehose.sink.AbstractSink;
import com.gotocompany.firehose.sink.grpc.client.GrpcClient;
import com.google.protobuf.DynamicMessage;
import com.gotocompany.stencil.client.StencilClient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * GrpcSink allows messages consumed from kafka to be relayed to a http service.
 * The related configurations for HTTPSink can be found here: {@see com.gotocompany.firehose.config.HTTPSinkConfig}
 */
public class GrpcSink extends AbstractSink {

    private final GrpcClient grpcClient;
    private final StencilClient stencilClient;
    private List<Message> messages;
    private PayloadEvaluator<com.google.protobuf.Message> retryEvaluator;

    public GrpcSink(FirehoseInstrumentation firehoseInstrumentation,
                    GrpcClient grpcClient,
                    StencilClient stencilClient,
                    PayloadEvaluator<com.google.protobuf.Message> retryEvaluator) {
        super(firehoseInstrumentation, "grpc");
        this.grpcClient = grpcClient;
        this.stencilClient = stencilClient;
        this.retryEvaluator = retryEvaluator;
    }

    @Override
    protected List<Message> execute() throws Exception {
        ArrayList<Message> failedMessages = new ArrayList<>();

        for (Message message : this.messages) {
            DynamicMessage response = grpcClient.execute(message.getLogMessage(), message.getHeaders());
            getFirehoseInstrumentation().logDebug("Response: {}", response);
            Object m = response.getField(response.getDescriptorForType().findFieldByName("success"));
            boolean success = (m != null) ? Boolean.valueOf(String.valueOf(m)) : false;

            if (!success) {
                getFirehoseInstrumentation().logWarn("Grpc Service returned error");
                failedMessages.add(message);
                setRetryableErrorInfo(message, response);
            }
        }
        getFirehoseInstrumentation().logDebug("Failed messages count: {}", failedMessages.size());
        return failedMessages;
    }

    @Override
    protected void prepare(List<Message> messages2) throws DeserializerException {
        this.messages = messages2;
    }

    @Override
    public void close() throws IOException {
        getFirehoseInstrumentation().logInfo("GRPC connection closing");
        this.messages = new ArrayList<>();
        stencilClient.close();
    }

    private void setRetryableErrorInfo(Message message, DynamicMessage dynamicMessage) {
        boolean eligibleToRetry = retryEvaluator.evaluate(dynamicMessage);
        if (eligibleToRetry) {
            message.setErrorInfo(new ErrorInfo(new DefaultException("Retryable gRPC Error"), ErrorType.SINK_RETRYABLE_ERROR));
            return;
        }
        message.setErrorInfo(new ErrorInfo(new DefaultException("Non Retryable gRPC Error"), ErrorType.SINK_NON_RETRYABLE_ERROR));
    }
}
