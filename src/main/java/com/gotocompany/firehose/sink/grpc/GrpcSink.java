package com.gotocompany.firehose.sink.grpc;


import com.gotocompany.depot.error.ErrorInfo;
import com.gotocompany.depot.error.ErrorType;
import com.gotocompany.firehose.config.GrpcSinkConfig;
import com.gotocompany.firehose.evaluator.CELPayloadEvaluator;
import com.gotocompany.firehose.exception.DefaultException;
import com.gotocompany.firehose.exception.DeserializerException;
import com.gotocompany.firehose.message.Message;
import com.gotocompany.firehose.metrics.FirehoseInstrumentation;
import com.gotocompany.firehose.sink.AbstractSink;
import com.gotocompany.firehose.sink.grpc.client.GrpcClient;
import com.google.protobuf.DynamicMessage;
import com.gotocompany.stencil.client.StencilClient;
import org.apache.commons.lang3.StringUtils;

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
    private final GrpcSinkConfig grpcSinkConfig;
    private List<Message> messages;
    private CELPayloadEvaluator retryEvaluator;

    public GrpcSink(FirehoseInstrumentation firehoseInstrumentation,
                    GrpcClient grpcClient,
                    StencilClient stencilClient,
                    GrpcSinkConfig grpcSinkConfig) {
        super(firehoseInstrumentation, "grpc");
        this.grpcClient = grpcClient;
        this.stencilClient = stencilClient;
        this.grpcSinkConfig = grpcSinkConfig;
        if (StringUtils.isNotBlank(grpcSinkConfig.getSinkGrpcResponseRetryCELExpression())) {
            this.retryEvaluator = new CELPayloadEvaluator(
                    stencilClient.get(grpcSinkConfig.getSinkGrpcResponseSchemaProtoClass()),
                    grpcSinkConfig.getSinkGrpcResponseRetryCELExpression());
        }
    }

    @Override
    protected List<Message> execute() throws Exception {
        ArrayList<Message> failedMessages = new ArrayList<>();

        for (Message message : this.messages) {
            DynamicMessage response = grpcClient.execute(message.getLogMessage(), message.getHeaders());
            getFirehoseInstrumentation().logDebug("Response: {}", response);

            if (retryEvaluator.evaluate(response)) {
                message.setErrorInfo(new ErrorInfo(new DefaultException("DEFAULT"), ErrorType.SINK_RETRYABLE_ERROR));
                failedMessages.add(message);
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
}
