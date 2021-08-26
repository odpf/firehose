package io.odpf.firehose.sink.redis;


import io.odpf.firehose.metrics.StatsDReporter;
import io.odpf.firehose.sink.AbstractSink;
import io.odpf.stencil.client.StencilClient;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class RedisSinkFactoryTest {
    private Map<String, String> configuration;

    @Mock
    private StatsDReporter statsDReporter;

    @Mock
    private StencilClient stencilClient;

    @Before
    public void setUp() {
        configuration = new HashMap<>();
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void shouldCreateRedisSink() {
        configuration.put("SINK_REDIS_URLS", "localhost:6379");
        configuration.put("SINK_REDIS_KEY_TEMPLATE", "test_%%s,6");
        configuration.put("SINK_REDIS_LIST_DATA_PROTO_INDEX", "3");

        RedisSinkFactory redisSinkFactory = new RedisSinkFactory();
        AbstractSink sink = redisSinkFactory.create(configuration, statsDReporter, stencilClient);
        assertEquals(RedisSink.class, sink.getClass());
    }
}
