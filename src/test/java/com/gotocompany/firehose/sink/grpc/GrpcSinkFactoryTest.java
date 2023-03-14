package com.gotocompany.firehose.sink.grpc;

import com.gotocompany.firehose.exception.DeserializerException;
import com.gotocompany.firehose.sink.Sink;
import com.gotocompany.depot.metrics.StatsDReporter;
import com.gotocompany.firehose.consumer.TestServerGrpc;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import com.gotocompany.stencil.client.StencilClient;
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

    @Mock
    private StencilClient stencilClient;

//    private static ConsulClient consulClient;

    @Before
    public void setUp() {
        initMocks(this);
    }


    @Test
    public void shouldCreateChannelPoolWithHostAndPort() throws IOException, DeserializerException {
        when(testGrpcService.bindService()).thenCallRealMethod();

        Server server = ServerBuilder
                .forPort(5000)
                .addService(testGrpcService.bindService())
                .build()
                .start();

        Map<String, String> config = new HashMap<>();
        config.put("SINK_GRPC_METHOD_URL", "com.gotocompany.firehose.consumer.TestServer/TestRpcMethod");
        config.put("SINK_GRPC_SERVICE_HOST", "localhost");
        config.put("SINK_GRPC_SERVICE_PORT", "5000");


        Sink sink = GrpcSinkFactory.create(config, statsDReporter, stencilClient);

        Assert.assertNotNull(sink);
        server.shutdownNow();
    }
}
