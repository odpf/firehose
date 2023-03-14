package com.gotocompany.firehose.sink.elasticsearch;


import com.gotocompany.firehose.metrics.FirehoseInstrumentation;
import com.gotocompany.firehose.sink.Sink;
import com.gotocompany.depot.metrics.StatsDReporter;
import com.gotocompany.stencil.client.StencilClient;
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
    private FirehoseInstrumentation firehoseInstrumentation;

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
        Sink sink = EsSinkFactory.create(configuration, statsDReporter, stencilClient);
        assertEquals(EsSink.class, sink.getClass());
    }

    @Test
    public void shouldThrowIllegalArgumentExceptionForEmptyESConnectionURLs() {
        try {
            EsSinkFactory.getHttpHosts("", firehoseInstrumentation);
        } catch (Exception e) {
            assertEquals(IllegalArgumentException.class, e.getClass());
            assertEquals("SINK_ES_CONNECTION_URLS is empty or null", e.getMessage());
        }
    }

    @Test
    public void shouldThrowIllegalArgumentExceptionForNullESConnectionURLs() {
        try {
            EsSinkFactory.getHttpHosts(null, firehoseInstrumentation);
        } catch (Exception e) {
            assertEquals(IllegalArgumentException.class, e.getClass());
            assertEquals("SINK_ES_CONNECTION_URLS is empty or null", e.getMessage());
        }
    }

    @Test
    public void shouldThrowIllegalArgumentExceptionForEmptyHostName() {
        String esConnectionURLs = ":1000";
        try {
            EsSinkFactory.getHttpHosts(esConnectionURLs, firehoseInstrumentation);
        } catch (Exception e) {
            assertEquals(IllegalArgumentException.class, e.getClass());
        }
    }

    @Test
    public void shouldThrowIllegalArgumentExceptionForEmptyPort() {
        String esConnectionURLs = "localhost:";
        try {
            EsSinkFactory.getHttpHosts(esConnectionURLs, firehoseInstrumentation);
        } catch (Exception e) {
            assertEquals(IllegalArgumentException.class, e.getClass());
        }
    }

    @Test
    public void shouldGetHttpHostsForValidESConnectionURLs() {
        String esConnectionURLs = "localhost_1:1000,localhost_2:1000";
        HttpHost[] httpHosts = EsSinkFactory.getHttpHosts(esConnectionURLs, firehoseInstrumentation);

        assertEquals("localhost_1", httpHosts[0].getHostName());
        assertEquals(1000, httpHosts[0].getPort());
        assertEquals("localhost_2", httpHosts[1].getHostName());
        assertEquals(1000, httpHosts[1].getPort());
    }

    @Test
    public void shouldGetHttpHostsForValidESConnectionURLsWithSpacesInBetween() {
        String esConnectionURLs = " localhost_1: 1000,  localhost_2:1000";
        HttpHost[] httpHosts = EsSinkFactory.getHttpHosts(esConnectionURLs, firehoseInstrumentation);

        assertEquals("localhost_1", httpHosts[0].getHostName());
        assertEquals(1000, httpHosts[0].getPort());
        assertEquals("localhost_2", httpHosts[1].getHostName());
        assertEquals(1000, httpHosts[1].getPort());
    }

    @Test
    public void shouldGetHttpHostsForIPInESConnectionURLs() {
        String esConnectionURLs = "172.28.32.156:1000";
        HttpHost[] httpHosts = EsSinkFactory.getHttpHosts(esConnectionURLs, firehoseInstrumentation);

        assertEquals("172.28.32.156", httpHosts[0].getHostName());
        assertEquals(1000, httpHosts[0].getPort());
    }

    @Test
    public void shouldThrowExceptionIfHostAndPortNotProvidedProperly() {
        String esConnectionURLs = "test";
        try {
            EsSinkFactory.getHttpHosts(esConnectionURLs, firehoseInstrumentation);
        } catch (Exception e) {
            assertEquals(IllegalArgumentException.class, e.getClass());
            assertEquals("SINK_ES_CONNECTION_URLS should contain host and port both", e.getMessage());
        }
    }

    @Test
    public void shouldReturnBlackListRetryStatusCodesAsList() {
        String inputRetryStatusCodeBlacklist = "404, 502";
        List<String> statusCodesAsList = EsSinkFactory.getStatusCodesAsList(inputRetryStatusCodeBlacklist);
        assertEquals("404", statusCodesAsList.get(0));
        assertEquals("502", statusCodesAsList.get(1));
    }

    @Test
    public void shouldReturnEmptyBlackListRetryStatusCodesAsEmptyList() {
        String inputRetryStatusCodeBlacklist = "";
        List<String> statusCodesAsList = EsSinkFactory.getStatusCodesAsList(inputRetryStatusCodeBlacklist);
        assertEquals(0, statusCodesAsList.size());
    }
}
