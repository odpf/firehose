package org.raystack.firehose.sink.grpc;


import org.raystack.firehose.config.GrpcSinkConfig;
import org.raystack.firehose.metrics.FirehoseInstrumentation;
import org.raystack.firehose.sink.grpc.client.GrpcClient;
import org.raystack.depot.metrics.StatsDReporter;
import org.raystack.firehose.sink.AbstractSink;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.raystack.stencil.client.StencilClient;
import org.aeonbits.owner.ConfigFactory;

import java.util.Map;

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

        ManagedChannel managedChannel = ManagedChannelBuilder.forAddress(grpcConfig.getSinkGrpcServiceHost(), grpcConfig.getSinkGrpcServicePort()).usePlaintext().build();

        GrpcClient grpcClient = new GrpcClient(new FirehoseInstrumentation(statsDReporter, GrpcClient.class), grpcConfig, managedChannel, stencilClient);
        firehoseInstrumentation.logInfo("GRPC connection established");

        return new GrpcSink(new FirehoseInstrumentation(statsDReporter, GrpcSink.class), grpcClient, stencilClient);
    }

}
