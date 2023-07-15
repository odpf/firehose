package org.raystack.firehose.sink.prometheus.request;


import org.raystack.firehose.config.PromSinkConfig;
import org.raystack.depot.metrics.StatsDReporter;
import org.raystack.stencil.Parser;
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
    private Parser protoParser;

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
