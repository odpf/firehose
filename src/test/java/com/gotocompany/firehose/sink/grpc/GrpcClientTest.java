package com.gotocompany.firehose.sink.grpc;

import com.gotocompany.firehose.config.GrpcSinkConfig;
import com.gotocompany.firehose.metrics.FirehoseInstrumentation;
import com.gotocompany.firehose.sink.grpc.client.GrpcClient;
import com.gotocompany.firehose.consumer.Error;
import com.gotocompany.firehose.consumer.TestGrpcRequest;
import com.gotocompany.firehose.consumer.TestGrpcResponse;
import com.gotocompany.firehose.consumer.TestServerGrpc;
import com.google.protobuf.AbstractMessage;
import com.google.protobuf.DynamicMessage;
import io.grpc.*;
import io.grpc.stub.StreamObserver;
import com.gotocompany.stencil.StencilClientFactory;
import com.gotocompany.stencil.client.StencilClient;
import org.aeonbits.owner.ConfigFactory;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.stubbing.Stubber;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

public class GrpcClientTest {

    private Server server;
    private GrpcClient grpcClient;
    private TestServerGrpc.TestServerImplBase testGrpcService;
    private RecordHeaders headers;
    private static final List<String> HEADER_KEYS = Arrays.asList("test-header-key-1", "test-header-key-2");
    private HeaderTestInterceptor headerTestInterceptor;

    @Before
    public void setup() throws IOException {
        FirehoseInstrumentation firehoseInstrumentation = Mockito.mock(FirehoseInstrumentation.class);
        testGrpcService = Mockito.mock(TestServerGrpc.TestServerImplBase.class, CALLS_REAL_METHODS);
        headerTestInterceptor = new HeaderTestInterceptor();
        headerTestInterceptor.setHeaderKeys(HEADER_KEYS);
        ServerServiceDefinition serviceDefinition = ServerInterceptors.intercept(testGrpcService.bindService(), Arrays.asList(headerTestInterceptor));
        server = ServerBuilder.forPort(5000)
                .addService(serviceDefinition)
                .build()
                .start();
        Map<String, String> config = new HashMap<>();
        config.put("SINK_GRPC_SERVICE_HOST", "localhost");
        config.put("SINK_GRPC_SERVICE_PORT", "5000");
        config.put("SINK_GRPC_METHOD_URL", "com.gotocompany.firehose.consumer.TestServer/TestRpcMethod");
        config.put("SINK_GRPC_RESPONSE_SCHEMA_PROTO_CLASS", "com.gotocompany.firehose.consumer.TestGrpcResponse");

        GrpcSinkConfig grpcSinkConfig = ConfigFactory.create(GrpcSinkConfig.class, config);
        StencilClient stencilClient = StencilClientFactory.getClient();
        ManagedChannel managedChannel = ManagedChannelBuilder.forAddress(grpcSinkConfig.getSinkGrpcServiceHost(), grpcSinkConfig.getSinkGrpcServicePort()).usePlaintext().build();
        grpcClient = new GrpcClient(firehoseInstrumentation, grpcSinkConfig, managedChannel, stencilClient);
        headers = new RecordHeaders();
    }

    @After
    public void tearDown() {
        if (server != null) {
            server.shutdown();
        }
    }

    public class HeaderTestInterceptor implements ServerInterceptor {

        private Map<String, String> headers = new HashMap<>();
        private List<String> headerKeys;
        private List<String> keyValues = new ArrayList<>();

        @Override
        public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> serverCall, Metadata metadata, ServerCallHandler<ReqT, RespT> serverCallHandler) {
            for (String key : headerKeys) {
                keyValues.add(metadata.get(Metadata.Key.of(key, Metadata.ASCII_STRING_MARSHALLER)));
            }
            return serverCallHandler.startCall(serverCall, metadata);
        }

        public Map<String, String> getHeaders() {
            return headers;
        }

        public void setHeaderKeys(List headerKey) {
            this.headerKeys = headerKey;
        }

        public List<String> getKeyValues() {
            return keyValues;
        }
    }

    @Test
    public void shouldCallTheGivenRpcMethodAndGetSuccessResponse() {
        doAnswerProtoReponse(TestGrpcResponse.newBuilder()
                .setSuccess(true)
                .build()).when(testGrpcService).testRpcMethod(any(TestGrpcRequest.class), any());
        TestGrpcRequest request = TestGrpcRequest.newBuilder()
                .setField1("field1")
                .setField2("field2")
                .build();
        DynamicMessage response = grpcClient.execute(request.toByteArray(), headers);
        System.out.println(response.toString());
        assertTrue(Boolean.parseBoolean(String.valueOf(response.getField(TestGrpcResponse.getDescriptor().findFieldByName("success")))));
    }

    @Test
    public void shouldCallTheGivenRpcMethodWithHeaders() {
        doAnswerProtoReponse(TestGrpcResponse.newBuilder()
                .setSuccess(true)
                .build()).when(testGrpcService).testRpcMethod(any(TestGrpcRequest.class), any());
        TestGrpcRequest request = TestGrpcRequest.newBuilder()
                .setField1("field1")
                .setField2("field2")
                .build();
        String headerValue1 = "test-value-1";
        String headerValue2 = "test-value-2";
        headers.add(new RecordHeader(HEADER_KEYS.get(0), headerValue1.getBytes()));
        headers.add(new RecordHeader(HEADER_KEYS.get(1), headerValue2.getBytes()));
        grpcClient.execute(request.toByteArray(), headers);

        assertEquals(headerTestInterceptor.getKeyValues(), Arrays.asList(headerValue1, headerValue2));
    }

    @Test
    public void shouldCallTheGivenRpcMethodAndGetErrorResponse() {
        doAnswerProtoReponse(TestGrpcResponse.newBuilder()
                .setSuccess(false)
                .addError(Error.newBuilder().
                        setCode("101")
                        .setEntity("some-entity").build())
                .build()).when(testGrpcService).testRpcMethod(any(TestGrpcRequest.class), any());
        TestGrpcRequest request = TestGrpcRequest.newBuilder()
                .setField1("field1")
                .setField2("field2")
                .build();
        DynamicMessage response = grpcClient.execute(request.toByteArray(), headers);
        assertFalse(Boolean.parseBoolean(String.valueOf(response.getField(response.getDescriptorForType().findFieldByName("success")))));
    }

    @Test
    public void shouldReturnErrorWhenBytesAreNull() {
        DynamicMessage response = grpcClient.execute(null, headers);
        assertFalse(Boolean.parseBoolean(String.valueOf(response.getField(response.getDescriptorForType().findFieldByName("success")))));
    }

    @Test
    public void shouldReturnErrorWhenGrpcException() {
        doThrow(new RuntimeException("error")).when(testGrpcService).testRpcMethod(any(TestGrpcRequest.class), any());
        TestGrpcRequest request = TestGrpcRequest.newBuilder()
                .setField1("field1")
                .setField2("field2")
                .build();
        DynamicMessage response = grpcClient.execute(request.toByteArray(), headers);
        assertFalse(Boolean.parseBoolean(String.valueOf(response.getField(response.getDescriptorForType().findFieldByName("success")))));
    }


    private <T extends AbstractMessage> Stubber doAnswerProtoReponse(T response) {
        return doAnswer(invocation -> {
            StreamObserver<T> responseObserver = (StreamObserver<T>) invocation.getArguments()[1];
            responseObserver.onNext(response);
            responseObserver.onCompleted();
            return null;
        });
    }
}
