package io.odpf.firehose.sink.http;


import io.odpf.firehose.consumer.Message;
import io.odpf.firehose.exception.DeserializerException;
import io.odpf.firehose.metrics.StatsDReporter;
import io.odpf.firehose.sink.AbstractSink;
import io.odpf.firehose.util.Clock;
import io.odpf.stencil.client.StencilClient;
import org.gradle.internal.impldep.org.junit.Before;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.verify.VerificationTimes;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.mockserver.integration.ClientAndServer.startClientAndServer;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;

@RunWith(MockitoJUnitRunner.class)
public class HttpSinkFactoryTest {
    @Mock
    private StatsDReporter statsDReporter;
    @Mock
    private StencilClient stencilClient;

    private List<Message> messages = Collections.singletonList(
            new Message(new byte[]{10, 20}, new byte[]{1, 2}, "sample-topic", 0, 100));

    private static ClientAndServer mockServer;

    @Before
    public void setup() {
        initMocks(this);
    }

    @BeforeClass
    public static void startServer() {
        mockServer = startClientAndServer(1080);
    }

    @AfterClass
    public static void stopServer() {
        mockServer.stop();
    }

    @org.junit.Before
    public void startMockServer() {
        mockServer.reset();
        mockServer.when(request().withPath("/oauth2/token"))
                .respond(response().withStatusCode(200).withBody("{\"access_token\":\"ACCESSTOKEN\",\"expires_in\":3599,\"scope\":\"order:read order:write\",\"token_type\":\"bearer\"}"));
        mockServer.when(request().withPath("/api"))
                .respond(response().withStatusCode(200).withBody("OK"));
    }

    @Test(expected = Test.None.class)
    public void shouldNotEmbedAccessTokenIfGoAuthDisabled() throws IOException, DeserializerException {
        Map<String, String> configuration = new HashMap<>();
        configuration.put("SINK_HTTP_OAUTH2_ENABLE", "false");
        configuration.put("SINK_HTTP_OAUTH2_ACCESS_TOKEN_URL", "http://127.0.0.1:1080/oauth2/token");
        configuration.put("SINK_HTTP_SERVICE_URL", "http://127.0.0.1:1080/api");
        AbstractSink sink = new HttpSinkFactory().create(configuration, statsDReporter, stencilClient);

        when(statsDReporter.getClock()).thenReturn(new Clock());
        sink.pushMessage(messages);

        mockServer.verify(request().withPath("/oauth2/token"), VerificationTimes.exactly(0));
    }

    @Test(expected = Test.None.class)
    public void shouldEmbedAccessTokenIfGoAuthEnabled() throws IOException, DeserializerException {
        Map<String, String> configuration = new HashMap<>();
        configuration.put("SINK_HTTP_OAUTH2_ENABLE", "true");
        configuration.put("SINK_HTTP_OAUTH2_ACCESS_TOKEN_URL", "http://127.0.0.1:1080/oauth2/token");
        configuration.put("SINK_HTTP_SERVICE_URL", "http://127.0.0.1:1080/api");
        AbstractSink sink = new HttpSinkFactory().create(configuration, statsDReporter, stencilClient);

        when(statsDReporter.getClock()).thenReturn(new Clock());
        sink.pushMessage(messages);

        mockServer.verify(request().withPath("/oauth2/token"), VerificationTimes.exactly(1));
    }
}
