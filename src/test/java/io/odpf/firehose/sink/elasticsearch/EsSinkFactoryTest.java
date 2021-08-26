package io.odpf.firehose.sink.elasticsearch;


import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.metrics.StatsDReporter;
import io.odpf.firehose.sink.Sink;
import io.odpf.stencil.client.StencilClient;
import org.apache.http.HttpHost;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class EsSinkFactoryTest {

    private Map<String, String> configuration;

    @Mock
    private StatsDReporter statsDReporter;

    @Mock
    private Instrumentation instrumentation;

    @Mock
    private StencilClient stencilClient;

    @Before
    public void setUp() {
        configuration = new HashMap<>();
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void shouldCreateESSink() {
        configuration.put("SINK_ES_CONNECTION_URLS", "localhost:9200 , localhost:9200 ");
        EsSinkFactory esSinkFactory = new EsSinkFactory();
        Sink sink = esSinkFactory.create(configuration, statsDReporter, stencilClient);
        assertEquals(EsSink.class, sink.getClass());
    }

    @Test
    public void shouldThrowIllegalArgumentExceptionForEmptyESConnectionURLs() {
        EsSinkFactory esSinkFactory = new EsSinkFactory();
        try {
            esSinkFactory.getHttpHosts("", instrumentation);
        } catch (Exception e) {
            assertEquals(IllegalArgumentException.class, e.getClass());
            assertEquals("SINK_ES_CONNECTION_URLS is empty or null", e.getMessage());
        }
    }

    @Test
    public void shouldThrowIllegalArgumentExceptionForNullESConnectionURLs() {
        EsSinkFactory esSinkFactory = new EsSinkFactory();
        try {
            esSinkFactory.getHttpHosts(null, instrumentation);
        } catch (Exception e) {
            assertEquals(IllegalArgumentException.class, e.getClass());
            assertEquals("SINK_ES_CONNECTION_URLS is empty or null", e.getMessage());
        }
    }

    @Test
    public void shouldThrowIllegalArgumentExceptionForEmptyHostName() {
        EsSinkFactory esSinkFactory = new EsSinkFactory();
        String esConnectionURLs = ":1000";
        try {
            esSinkFactory.getHttpHosts(esConnectionURLs, instrumentation);
        } catch (Exception e) {
            assertEquals(IllegalArgumentException.class, e.getClass());
        }
    }

    @Test
    public void shouldThrowIllegalArgumentExceptionForEmptyPort() {
        EsSinkFactory esSinkFactory = new EsSinkFactory();
        String esConnectionURLs = "localhost:";
        try {
            esSinkFactory.getHttpHosts(esConnectionURLs, instrumentation);
        } catch (Exception e) {
            assertEquals(IllegalArgumentException.class, e.getClass());
        }
    }

    @Test
    public void shouldGetHttpHostsForValidESConnectionURLs() {
        EsSinkFactory esSinkFactory = new EsSinkFactory();
        String esConnectionURLs = "localhost_1:1000,localhost_2:1000";
        HttpHost[] httpHosts = esSinkFactory.getHttpHosts(esConnectionURLs, instrumentation);

        assertEquals("localhost_1", httpHosts[0].getHostName());
        assertEquals(1000, httpHosts[0].getPort());
        assertEquals("localhost_2", httpHosts[1].getHostName());
        assertEquals(1000, httpHosts[1].getPort());
    }

    @Test
    public void shouldGetHttpHostsForValidESConnectionURLsWithSpacesInBetween() {
        EsSinkFactory esSinkFactory = new EsSinkFactory();
        String esConnectionURLs = " localhost_1: 1000,  localhost_2:1000";
        HttpHost[] httpHosts = esSinkFactory.getHttpHosts(esConnectionURLs, instrumentation);

        assertEquals("localhost_1", httpHosts[0].getHostName());
        assertEquals(1000, httpHosts[0].getPort());
        assertEquals("localhost_2", httpHosts[1].getHostName());
        assertEquals(1000, httpHosts[1].getPort());
    }

    @Test
    public void shouldGetHttpHostsForIPInESConnectionURLs() {
        EsSinkFactory esSinkFactory = new EsSinkFactory();
        String esConnectionURLs = "172.28.32.156:1000";
        HttpHost[] httpHosts = esSinkFactory.getHttpHosts(esConnectionURLs, instrumentation);

        assertEquals("172.28.32.156", httpHosts[0].getHostName());
        assertEquals(1000, httpHosts[0].getPort());
    }

    @Test
    public void shouldThrowExceptionIfHostAndPortNotProvidedProperly() {
        EsSinkFactory esSinkFactory = new EsSinkFactory();
        String esConnectionURLs = "test";
        try {
            esSinkFactory.getHttpHosts(esConnectionURLs, instrumentation);
        } catch (Exception e) {
            assertEquals(IllegalArgumentException.class, e.getClass());
            assertEquals("SINK_ES_CONNECTION_URLS should contain host and port both", e.getMessage());
        }
    }

    @Test
    public void shouldReturnBlackListRetryStatusCodesAsList() {
        EsSinkFactory esSinkFactory = new EsSinkFactory();
        String inputRetryStatusCodeBlacklist = "404, 502";
        List<String> statusCodesAsList = esSinkFactory.getStatusCodesAsList(inputRetryStatusCodeBlacklist);
        assertEquals("404", statusCodesAsList.get(0));
        assertEquals("502", statusCodesAsList.get(1));
    }

    @Test
    public void shouldReturnEmptyBlackListRetryStatusCodesAsEmptyList() {
        EsSinkFactory esSinkFactory = new EsSinkFactory();
        String inputRetryStatusCodeBlacklist = "";
        List<String> statusCodesAsList = esSinkFactory.getStatusCodesAsList(inputRetryStatusCodeBlacklist);
        assertEquals(0, statusCodesAsList.size());
    }
}
