package io.odpf.firehose.sink.prometheus.request;


import io.odpf.firehose.config.PromSinkConfig;
import io.odpf.firehose.metrics.StatsDReporter;
import io.odpf.stencil.parser.ProtoParser;
import org.aeonbits.owner.ConfigFactory;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.util.Properties;

import static org.junit.Assert.assertNotNull;
import static org.mockito.MockitoAnnotations.initMocks;

public class PromRequestCreatorTest {

    @Mock
    private StatsDReporter statsDReporter;

    @Mock
    private ProtoParser protoParser;

    @Before
    public void setup() {
        initMocks(this);
    }

    @Test
    public void shouldCreateNotNullRequest() {

        Properties promConfigProps = new Properties();

        PromSinkConfig promSinkConfig = ConfigFactory.create(PromSinkConfig.class, promConfigProps);
        PromRequestCreator promRequestCreator = new PromRequestCreator(statsDReporter, promSinkConfig, protoParser);

        PromRequest request = promRequestCreator.createRequest();

        assertNotNull(request);
    }
}
