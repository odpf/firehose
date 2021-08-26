package io.odpf.firehose.sink.prometheus;


import io.odpf.firehose.exception.DeserializerException;
import io.odpf.firehose.metrics.StatsDReporter;
import io.odpf.firehose.sink.AbstractSink;
import io.odpf.stencil.client.StencilClient;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

@RunWith(MockitoJUnitRunner.class)
public class PromSinkFactoryTest {
    @Mock
    private StatsDReporter statsDReporter;
    @Mock
    private StencilClient stencilClient;

    @Test
    public void shouldCreatePromSink() throws DeserializerException {

        Map<String, String> configuration = new HashMap<>();
        configuration.put("SINK_PROM_SERVICE_URL", "dummyEndpoint");
        AbstractSink sink = new PromSinkFactory().create(configuration, statsDReporter, stencilClient);

        assertEquals(PromSink.class, sink.getClass());
    }
}
