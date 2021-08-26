package io.odpf.firehose.sink.grpc;



import io.odpf.firehose.consumer.Message;
import io.odpf.firehose.exception.DeserializerException;
import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.sink.AbstractSink;
import io.odpf.firehose.sink.grpc.client.GrpcClient;
import com.google.protobuf.DynamicMessage;
import io.odpf.stencil.client.StencilClient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * GrpcSink allows messages consumed from kafka to be relayed to a http service.
 * The related configurations for HTTPSink can be found here: {@see io.odpf.firehose.config.HTTPSinkConfig}
 */
public class GrpcSink extends AbstractSink {

    private final GrpcClient grpcClient;
    private List<Message> messages;
    private StencilClient stencilClient;

    public GrpcSink(Instrumentation instrumentation, GrpcClient grpcClient, StencilClient stencilClient) {
        super(instrumentation, "grpc");
        this.grpcClient = grpcClient;
        this.stencilClient = stencilClient;
    }

    @Override
    protected List<Message> execute() throws Exception {
        ArrayList<Message> failedMessages = new ArrayList<>();

        for (Message message : this.messages) {
            DynamicMessage response = grpcClient.execute(message.getLogMessage(), message.getHeaders());
            getInstrumentation().logDebug("Response: {}", response);
            Object m = response.getField(response.getDescriptorForType().findFieldByName("success"));
            boolean success = (m != null) ? Boolean.valueOf(String.valueOf(m)) : false;

            if (!success) {
                getInstrumentation().logWarn("Grpc Service returned error");
                failedMessages.add(message);
            }
        }
        getInstrumentation().logDebug("Failed messages count: {}", failedMessages.size());
        return failedMessages;
    }

    @Override
    protected void prepare(List<Message> messages2) throws DeserializerException {
        this.messages = messages2;
    }

    @Override
    public void close() throws IOException {
        getInstrumentation().logInfo("GRPC connection closing");
        this.messages = new ArrayList<>();
        stencilClient.close();
    }
}
