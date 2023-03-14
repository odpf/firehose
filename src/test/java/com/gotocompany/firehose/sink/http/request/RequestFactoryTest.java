package com.gotocompany.firehose.sink.http.request;


import com.gotocompany.firehose.config.HttpSinkConfig;
import com.gotocompany.firehose.sink.http.request.uri.UriParser;
import com.gotocompany.depot.metrics.StatsDReporter;
import com.gotocompany.firehose.sink.http.request.types.SimpleRequest;
import com.gotocompany.firehose.sink.http.request.types.DynamicUrlRequest;
import com.gotocompany.firehose.sink.http.request.types.ParameterizedHeaderRequest;
import com.gotocompany.firehose.sink.http.request.types.ParameterizedUriRequest;
import com.gotocompany.firehose.sink.http.request.types.Request;
import com.gotocompany.stencil.client.StencilClient;
import org.aeonbits.owner.ConfigFactory;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertTrue;
import static org.mockito.MockitoAnnotations.initMocks;

public class RequestFactoryTest {
    @Mock
    private StencilClient stencilClient;
    @Mock
    private StatsDReporter statsDReporter;
    @Mock
    private UriParser uriParser;
    private HttpSinkConfig httpSinkConfig;

    private Map<String, String> configuration = new HashMap<>();

    @Before
    public void setup() {
        initMocks(this);
        configuration = new HashMap<String, String>();
    }

    @Test
    public void shouldReturnBatchRequestWhenPrameterSourceIsDisabledAndServiceUrlIsConstant() {
        configuration.put("SINK_HTTP_SERVICE_URL", "http://127.0.0.1:1080/api");
        httpSinkConfig = ConfigFactory.create(HttpSinkConfig.class, configuration);

        Request request = new RequestFactory(statsDReporter, httpSinkConfig, stencilClient, uriParser).createRequest();

        assertTrue(request instanceof SimpleRequest);
    }

    @Test
    public void shouldReturnDynamicUrlRequestWhenPrameterSourceIsDisabledAndServiceUrlIsNotParametrised() {
        configuration.put("SINK_HTTP_SERVICE_URL", "http://127.0.0.1:1080/api,%s");
        httpSinkConfig = ConfigFactory.create(HttpSinkConfig.class, configuration);

        Request request = new RequestFactory(statsDReporter, httpSinkConfig, stencilClient, uriParser).createRequest();

        assertTrue(request instanceof DynamicUrlRequest);
    }

    @Test
    public void shouldReturnParameterizedRequstWhenParameterSourceIsNotDisableAndPlacementTypeIsHeader() {
        configuration.put("SINK_HTTP_PARAMETER_SOURCE", "key");
        configuration.put("SINK_HTTP_PARAMETER_PLACEMENT", "header");
        configuration.put("SINK_HTTP_SERVICE_URL", "http://127.0.0.1:1080/api,%s");
        httpSinkConfig = ConfigFactory.create(HttpSinkConfig.class, configuration);

        Request request = new RequestFactory(statsDReporter, httpSinkConfig, stencilClient, uriParser).createRequest();

        assertTrue(request instanceof ParameterizedHeaderRequest);
    }

    @Test
    public void shouldReturnParameterizedRequstWhenParameterSourceIsNotDisableAndPlacementTypeIsQuery() {
        configuration.put("SINK_HTTP_PARAMETER_SOURCE", "key");
        configuration.put("SINK_HTTP_PARAMETER_PLACEMENT", "query");
        configuration.put("SINK_HTTP_SERVICE_URL", "http://127.0.0.1:1080/api,%s");
        httpSinkConfig = ConfigFactory.create(HttpSinkConfig.class, configuration);

        Request request = new RequestFactory(statsDReporter, httpSinkConfig, stencilClient, uriParser).createRequest();

        assertTrue(request instanceof ParameterizedUriRequest);
    }
}
