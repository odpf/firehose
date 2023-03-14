package com.gotocompany.firehose.sink.grpc.client;



import com.gotocompany.firehose.config.GrpcSinkConfig;
import com.gotocompany.firehose.metrics.FirehoseInstrumentation;
import com.google.protobuf.DynamicMessage;

import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.MetadataUtils;
import io.grpc.Channel;
import io.grpc.ClientInterceptors;
import io.grpc.CallOptions;
import com.gotocompany.stencil.client.StencilClient;
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

    private FirehoseInstrumentation firehoseInstrumentation;
    private final GrpcSinkConfig grpcSinkConfig;
    private StencilClient stencilClient;
    private ManagedChannel managedChannel;

    public GrpcClient(FirehoseInstrumentation firehoseInstrumentation, GrpcSinkConfig grpcSinkConfig, ManagedChannel managedChannel, StencilClient stencilClient) {
        this.firehoseInstrumentation = firehoseInstrumentation;
        this.grpcSinkConfig = grpcSinkConfig;
        this.stencilClient = stencilClient;
        this.managedChannel = managedChannel;
    }

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

            dynamicMessage = stencilClient.parse(grpcSinkConfig.getSinkGrpcResponseSchemaProtoClass(), response);

        } catch (Exception e) {
            firehoseInstrumentation.logWarn(e.getMessage());
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
