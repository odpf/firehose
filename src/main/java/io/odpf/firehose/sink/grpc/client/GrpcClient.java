package io.odpf.firehose.sink.grpc.client;



import io.odpf.firehose.config.GrpcSinkConfig;
import io.odpf.firehose.metrics.Instrumentation;
import com.google.protobuf.DynamicMessage;
import com.newrelic.api.agent.Trace;

import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.MetadataUtils;
import io.grpc.Channel;
import io.grpc.ClientInterceptors;
import io.grpc.CallOptions;
import io.odpf.stencil.client.StencilClient;
import io.odpf.stencil.parser.ProtoParser;
import org.apache.commons.io.IOUtils;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;


/**
 * Custom GRPC client for all GRPC communication.
 */
public class GrpcClient {

    private Instrumentation instrumentation;
    private final GrpcSinkConfig grpcSinkConfig;
    private ProtoParser protoParser;
    private StencilClient stencilClient;
    private ManagedChannel managedChannel;

    public GrpcClient(Instrumentation instrumentation, GrpcSinkConfig grpcSinkConfig, ManagedChannel managedChannel, StencilClient stencilClient) {
        this.instrumentation = instrumentation;
        this.grpcSinkConfig = grpcSinkConfig;
        this.protoParser = new ProtoParser(stencilClient, grpcSinkConfig.getSinkGrpcResponseSchemaProtoClass());
        this.stencilClient = stencilClient;
        this.managedChannel = managedChannel;
    }

    @Trace(dispatcher = true)
    public DynamicMessage execute(byte[] logMessage, Headers headers) {

        MethodDescriptor.Marshaller<byte[]> marshaller = getMarshaller();
        DynamicMessage dynamicMessage;

        try {


            Metadata metadata = new Metadata();
            for (Header header : headers) {
                metadata.put(Metadata.Key.of(header.key(), Metadata.ASCII_STRING_MARSHALLER), new String(header.value()));
            }

            Channel decoratedChannel = ClientInterceptors.intercept(managedChannel,
                     MetadataUtils.newAttachHeadersInterceptor(metadata));
            byte[] response = ClientCalls.blockingUnaryCall(
                    decoratedChannel,
                    MethodDescriptor.newBuilder(marshaller, marshaller)
                            .setType(MethodDescriptor.MethodType.UNARY)
                            .setFullMethodName(grpcSinkConfig.getSinkGrpcMethodUrl())
                            .build(),
                    CallOptions.DEFAULT,
                    logMessage);

            dynamicMessage = protoParser.parse(response);

        } catch (Exception e) {
            instrumentation.logWarn(e.getMessage());
            dynamicMessage = DynamicMessage.newBuilder(this.stencilClient.get(this.grpcSinkConfig.getSinkGrpcResponseSchemaProtoClass())).build();

        }

        return dynamicMessage;
    }

    private MethodDescriptor.Marshaller<byte[]> getMarshaller() {
        return new MethodDescriptor.Marshaller<byte[]>() {
            @Override
            public InputStream stream(byte[] value) {
                return new ByteArrayInputStream(value);
            }

            @Override
            public byte[] parse(InputStream stream) {
                try {
                    return IOUtils.toByteArray(stream);
                } catch (IOException e) {
                    return null;
                }
            }
        };
    }
}
