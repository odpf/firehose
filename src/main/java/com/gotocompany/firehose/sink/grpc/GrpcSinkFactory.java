package com.gotocompany.firehose.sink.grpc;


import com.google.protobuf.Message;
import com.gotocompany.firehose.config.GrpcSinkConfig;
import com.gotocompany.firehose.evaluator.DefaultGrpcPayloadEvaluator;
import com.gotocompany.firehose.evaluator.GrpcResponseCelPayloadEvaluator;
import com.gotocompany.firehose.evaluator.PayloadEvaluator;
import com.gotocompany.firehose.metrics.FirehoseInstrumentation;
import com.gotocompany.firehose.sink.grpc.client.GrpcClient;
import com.gotocompany.depot.metrics.StatsDReporter;
import com.gotocompany.firehose.sink.AbstractSink;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import com.gotocompany.stencil.client.StencilClient;
import org.aeonbits.owner.ConfigFactory;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Factory class to create the GrpcSink.
 * <p>
 * The consumer framework would reflectively instantiate this factory
 * using the configurations supplied and invoke {@see #create(Map < String, String > configuration, StatsDClient client)}
 * to obtain the GrpcSink sink implementation.
 */
public class GrpcSinkFactory {

    public static AbstractSink create(Map<String, String> configuration, StatsDReporter statsDReporter, StencilClient stencilClient) {
        GrpcSinkConfig grpcConfig = ConfigFactory.create(GrpcSinkConfig.class, configuration);
        FirehoseInstrumentation firehoseInstrumentation = new FirehoseInstrumentation(statsDReporter, GrpcSinkFactory.class);
        String grpcSinkConfig = String.format("\n\tService host: %s\n\tService port: %s\n\tMethod url: %s\n\tResponse proto schema: %s",
                grpcConfig.getSinkGrpcServiceHost(), grpcConfig.getSinkGrpcServicePort(), grpcConfig.getSinkGrpcMethodUrl(), grpcConfig.getSinkGrpcResponseSchemaProtoClass());
        firehoseInstrumentation.logDebug(grpcSinkConfig);

        ManagedChannel managedChannel = ManagedChannelBuilder.forAddress(grpcConfig.getSinkGrpcServiceHost(), grpcConfig.getSinkGrpcServicePort())
                        .keepAliveTime(grpcConfig.getSinkGrpcArgKeepaliveTimeMS(), TimeUnit.MILLISECONDS)
                        .keepAliveTimeout(grpcConfig.getSinkGrpcArgKeepaliveTimeoutMS(), TimeUnit.MILLISECONDS)
                        .usePlaintext().build();

        GrpcClient grpcClient = new GrpcClient(new FirehoseInstrumentation(statsDReporter, GrpcClient.class), grpcConfig, managedChannel, stencilClient);
        firehoseInstrumentation.logInfo("GRPC connection established");
        PayloadEvaluator<Message> grpcResponseRetryEvaluator = instantiateRetryEvaluator(grpcConfig, stencilClient);
        return new GrpcSink(new FirehoseInstrumentation(statsDReporter, GrpcSink.class), grpcClient, stencilClient, grpcConfig, grpcResponseRetryEvaluator);
    }

    private static PayloadEvaluator<Message> instantiateRetryEvaluator(GrpcSinkConfig grpcSinkConfig, StencilClient stencilClient) {
        if (StringUtils.isNotBlank(grpcSinkConfig.getSinkGrpcResponseRetryCELExpression())) {
            return new GrpcResponseCelPayloadEvaluator(
                    stencilClient.get(grpcSinkConfig.getSinkGrpcResponseSchemaProtoClass()),
                    grpcSinkConfig.getSinkGrpcResponseRetryCELExpression());
        }
        return new DefaultGrpcPayloadEvaluator();
    }

}
