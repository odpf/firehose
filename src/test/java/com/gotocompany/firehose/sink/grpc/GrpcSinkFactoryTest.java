package com.gotocompany.firehose.sink.grpc;

import com.gotocompany.firehose.config.GrpcSinkConfig;
import com.gotocompany.firehose.consumer.TestGrpcRequest;
import com.gotocompany.firehose.consumer.TestGrpcResponse;
import com.gotocompany.firehose.exception.DeserializerException;
import com.gotocompany.firehose.sink.Sink;
import com.gotocompany.depot.metrics.StatsDReporter;
import com.gotocompany.firehose.consumer.TestServerGrpc;
import com.gotocompany.stencil.StencilClientFactory;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import com.gotocompany.stencil.client.StencilClient;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.when;

import static org.mockito.MockitoAnnotations.initMocks;

public class GrpcSinkFactoryTest {

    @Mock
    private StatsDReporter statsDReporter;

    @Mock
    private TestServerGrpc.TestServerImplBase testGrpcService;

    private StencilClient stencilClient;

    @Mock
    private GrpcSinkConfig grpcConfig;

    @Mock
    private ManagedChannelBuilder channelBuilder;

    private Server server;

    @Before
    public void setUp() {
        initMocks(this);
        stencilClient = StencilClientFactory.getClient();
    }

    @After
    public void tearDown() {
        if (server != null) {
            server.shutdown();
        }
    }


    @Test
    public void shouldCreateChannelPoolWithHostAndPort() throws IOException, DeserializerException {

        when(testGrpcService.bindService()).thenCallRealMethod();
        server = ServerBuilder
                .forPort(5000)
                .addService(testGrpcService.bindService())
                .build()
                .start();

        Map<String, String> config = new HashMap<>();
        config.put("SINK_GRPC_METHOD_URL", "com.gotocompany.firehose.consumer.TestServer/TestRpcMethod");
        config.put("SINK_GRPC_SERVICE_HOST", "localhost");
        config.put("SINK_GRPC_SERVICE_PORT", "5000");
        config.put("SINK_GRPC_RESPONSE_SCHEMA_PROTO_CLASS", TestGrpcResponse.class.getName());
        config.put("INPUT_SCHEMA_PROTO_CLASS", TestGrpcRequest.getDescriptor().getFullName());

        Sink sink = GrpcSinkFactory.create(config, statsDReporter, stencilClient);

        Assert.assertNotNull(sink);
    }
}
