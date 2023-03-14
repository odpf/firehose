package com.gotocompany.firehose.sink.http.auth;

import com.gotocompany.firehose.metrics.FirehoseInstrumentation;
import com.gotocompany.depot.metrics.StatsDReporter;
import com.google.gson.JsonSyntaxException;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import org.apache.http.Header;
import org.apache.http.RequestLine;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClients;
import org.joda.time.DateTimeUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.model.HttpRequest;
import org.mockserver.verify.VerificationTimes;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockserver.integration.ClientAndServer.startClientAndServer;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;
import static org.mockserver.model.NottableString.not;
import static org.mockserver.model.NottableString.string;

public class OAuth2CredentialTest {
    private ClientAndServer mockServer;
    private HttpGet httpRequest;
    private OAuth2Credential oAuth2Credential;
    private HttpClient httpClient;
    private OkHttpClient okHttpClient;

    @Mock
    private StatsDReporter statsDReporter;

    @Before
    public void setUp() {
        DateTimeUtils.setCurrentMillisFixed(System.currentTimeMillis());
        mockServer = startClientAndServer(1080);
        httpRequest = new HttpGet("http://127.0.0.1:1080/api");
        String clientId = "clientId";
        String clientSecret = "clientSecret";
        String scope = "order:read";
        String accessTokenEndpoint = "http://127.0.0.1:1080/oauth2/token";
        oAuth2Credential = new OAuth2Credential(new FirehoseInstrumentation(statsDReporter, OAuth2Credential.class), clientId, clientSecret, scope, accessTokenEndpoint);
        httpClient = oAuth2Credential.initialize(HttpClients.custom()).build();
        okHttpClient = new OkHttpClient.Builder().addInterceptor(oAuth2Credential).build();
    }

    @After
    public void tearDown() {
        mockServer.stop();
    }

    @Test
    public void shouldEmbedBearerToken() throws IOException {
        HttpRequest oauthRequest = request().withPath("/oauth2/token")
                .withBody("client_id=clientId&client_secret=clientSecret&scope=order%3Aread&grant_type=client_credentials");
        mockServer.when(oauthRequest)
                .respond(response().withStatusCode(200).withBody("{\"access_token\":\"ACCESSTOKEN\",\"expires_in\":3599,\"scope\":\"order:read order:write\",\"token_type\":\"bearer\"}"));
        HttpRequest getRequest = request().withPath("/api")
                .withMethod("GET")
                .withHeader("Authorization", "Bearer ACCESSTOKEN")
                .withHeader("foo", "bar");
        mockServer.when(getRequest)
                .respond(response().withStatusCode(200).withBody("OK"));
        httpRequest.addHeader("foo", "bar");
        httpClient.execute(httpRequest);
        mockServer.verify(oauthRequest, VerificationTimes.exactly(1));
        mockServer.verify(getRequest, VerificationTimes.exactly(1));

        oAuth2Credential.setAccessToken(null);
        okHttpClient.newCall(transformRequest(httpRequest)).execute();
        mockServer.verify(oauthRequest, VerificationTimes.exactly(2));
        mockServer.verify(getRequest, VerificationTimes.exactly(2));
    }


    @Test
    public void shouldReuseTokenWhenTokenExist() throws Exception {
        HttpRequest getRequest = request().withPath("/api")
                .withMethod("GET")
                .withHeader("Authorization", "Bearer ACCESSTOKEN")
                .withHeader("foo", "bar");
        mockServer.when(getRequest)
                .respond(response().withStatusCode(200).withBody("OK"));
        httpRequest.addHeader("foo", "bar");
        oAuth2Credential.setAccessToken(new OAuth2AccessToken("ACCESSTOKEN", 300));
        httpClient.execute(httpRequest);
        mockServer.verify(getRequest, VerificationTimes.exactly(1));

        okHttpClient.newCall(transformRequest(httpRequest)).execute();
        mockServer.verify(getRequest, VerificationTimes.exactly(2));
    }

    @Test
    public void shouldRequestTokenWhenTokenIsExpired() throws Exception {
        HttpRequest oauthRequest = request().withPath("/oauth2/token")
                .withBody("client_id=clientId&client_secret=clientSecret&scope=order%3Aread&grant_type=client_credentials");
        mockServer.when(oauthRequest)
                .respond(response().withStatusCode(200).withBody("{\"access_token\":\"ACCESSTOKEN\",\"expires_in\":3599,\"scope\":\"order:read order:write\",\"token_type\":\"bearer\"}"));
        HttpRequest getRequest = request().withPath("/api")
                .withMethod("GET")
                .withHeader("Authorization", "Bearer ACCESSTOKEN")
                .withHeader("foo", "bar");
        mockServer.when(getRequest)
                .respond(response().withStatusCode(200).withBody("OK"));
        httpRequest.addHeader("foo", "bar");
        httpClient.execute(httpRequest);
        oAuth2Credential.setAccessToken(new OAuth2AccessToken("ACCESSTOKEN", -1));
        httpClient.execute(httpRequest);
        mockServer.verify(oauthRequest, VerificationTimes.exactly(2));
        mockServer.verify(getRequest, VerificationTimes.exactly(2));

        oAuth2Credential.setAccessToken(null);
        okHttpClient.newCall(transformRequest(httpRequest)).execute();
        oAuth2Credential.setAccessToken(new OAuth2AccessToken("ACCESSTOKEN", -1));
        okHttpClient.newCall(transformRequest(httpRequest)).execute();
        mockServer.verify(oauthRequest, VerificationTimes.exactly(4));
        mockServer.verify(getRequest, VerificationTimes.exactly(4));
    }

    @Test
    public void shouldRequestTokenWhenTokenIsExpiring() throws Exception {
        HttpRequest oauthRequest = request().withPath("/oauth2/token")
                .withBody("client_id=clientId&client_secret=clientSecret&scope=order%3Aread&grant_type=client_credentials");
        mockServer.when(oauthRequest)
                .respond(response().withStatusCode(200).withBody("{\"access_token\":\"ACCESSTOKEN\",\"expires_in\":3599,\"scope\":\"order:read order:write\",\"token_type\":\"bearer\"}"));
        HttpRequest getRequest = request().withPath("/api")
                .withMethod("GET")
                .withHeader("Authorization", "Bearer ACCESSTOKEN")
                .withHeader("foo", "bar");
        mockServer.when(getRequest)
                .respond(response().withStatusCode(200).withBody("OK"));
        httpRequest.addHeader("foo", "bar");
        httpClient.execute(httpRequest);
        oAuth2Credential.setAccessToken(new OAuth2AccessToken("ACCESSTOKEN", 60));
        httpClient.execute(httpRequest);
        mockServer.verify(oauthRequest, VerificationTimes.exactly(2));
        mockServer.verify(getRequest, VerificationTimes.exactly(2));

        oAuth2Credential.setAccessToken(null);
        okHttpClient.newCall(transformRequest(httpRequest)).execute();
        oAuth2Credential.setAccessToken(new OAuth2AccessToken("ACCESSTOKEN", 60));
        okHttpClient.newCall(transformRequest(httpRequest)).execute();
        mockServer.verify(oauthRequest, VerificationTimes.exactly(4));
        mockServer.verify(getRequest, VerificationTimes.exactly(4));
    }


    @Test
    public void shouldUseDefaultExpirationWhenAccessTokenIsIncomplete() throws Exception {
        HttpRequest oauthRequest = request().withPath("/oauth2/token")
                .withBody("client_id=clientId&client_secret=clientSecret&scope=order%3Aread&grant_type=client_credentials");
        mockServer.when(oauthRequest)
                .respond(response().withStatusCode(200).withBody("{\"access_token\":\"ACCESSTOKEN\",\"scope\":\"order:read order:write\",\"token_type\":\"bearer\"}"));
        HttpRequest getRequest = request().withPath("/api")
                .withMethod("GET")
                .withHeader("Authorization", "Bearer ACCESSTOKEN")
                .withHeader("foo", "bar");
        mockServer.when(getRequest)
                .respond(response().withStatusCode(200).withBody("OK"));
        httpRequest.addHeader("foo", "bar");
        httpClient.execute(httpRequest);

        mockServer.verify(oauthRequest, VerificationTimes.exactly(1));
        mockServer.verify(getRequest, VerificationTimes.exactly(1));
        assertEquals(3600, (long) oAuth2Credential.getAccessToken().getExpiresIn());

        oAuth2Credential.setAccessToken(null);
        okHttpClient.newCall(transformRequest(httpRequest)).execute();
        mockServer.verify(oauthRequest, VerificationTimes.exactly(2));
        mockServer.verify(getRequest, VerificationTimes.exactly(2));
        assertEquals(3600, (long) oAuth2Credential.getAccessToken().getExpiresIn());
    }

    @Test
    public void shouldClearTokenWhenServerReturnedHTTP401() throws Exception {
        HttpRequest oauthRequest = request().withPath("/oauth2/token")
                .withBody("client_id=clientId&client_secret=clientSecret&scope=order%3Aread&grant_type=client_credentials");
        mockServer.when(oauthRequest)
                .respond(response().withStatusCode(200).withBody("{\"access_token\":\"ACCESSTOKEN\",\"expires_in\":3599,\"scope\":\"order:read order:write\",\"token_type\":\"bearer\"}"));
        HttpRequest getRequest = request().withPath("/api")
                .withMethod("GET")
                .withHeader("foo", "bar");
        mockServer.when(getRequest)
                .respond(response().withStatusCode(401).withBody("{\n"
                        + "  \"error\": \"invalid_request\",\n"
                        + "  \"error_description\": \"Some Description\",\n"
                        + "  \"error_uri\": \"See the full API docs at https://authorization-server.com/docs/access_token\"\n"
                        + "}"));
        httpRequest.addHeader("foo", "bar");
        httpClient.execute(httpRequest);
        mockServer.verify(oauthRequest, VerificationTimes.exactly(1));
        mockServer.verify(getRequest, VerificationTimes.exactly(1));
        assertNull(oAuth2Credential.getAccessToken());

        oAuth2Credential.setAccessToken(null);
        okHttpClient.newCall(transformRequest(httpRequest)).execute();
        mockServer.verify(oauthRequest, VerificationTimes.exactly(2));
        mockServer.verify(getRequest, VerificationTimes.exactly(2));
        assertNull(oAuth2Credential.getAccessToken());
    }

    @Test(expected = JsonSyntaxException.class)
    public void shouldThrowExceptionWhenServerHasJsonException() throws Exception {
        HttpRequest oauthRequest = request().withPath("/oauth2/token")
                .withBody("client_id=clientId&client_secret=clientSecret&scope=order%3Aread&grant_type=client_credentials");
        mockServer.when(oauthRequest)
                .respond(response().withStatusCode(503).withBody("Gateway not available"));
        httpRequest.addHeader("foo", "bar");
        httpClient.execute(httpRequest);
    }

    @Test(expected = JsonSyntaxException.class)
    public void shouldThrowExceptionWhenServerHasJsonExceptionOkHttp() throws Exception {
        HttpRequest oauthRequest = request().withPath("/oauth2/token")
                .withBody("client_id=clientId&client_secret=clientSecret&scope=order%3Aread&grant_type=client_credentials");
        mockServer.when(oauthRequest)
                .respond(response().withStatusCode(503).withBody("Gateway not available"));
        httpRequest.addHeader("foo", "bar");
        okHttpClient.newCall(transformRequest(httpRequest)).execute();
    }

    @Test
    public void shouldNotThrowExceptionWhenServerHasOAuthException() throws Exception {
        HttpRequest oauthRequest = request().withPath("/oauth2/token")
                .withBody("client_id=clientId&client_secret=clientSecret&scope=order%3Aread&grant_type=client_credentials");
        mockServer.when(oauthRequest)
                .respond(response().withStatusCode(400).withBody("{\n"
                        + "  \"error\": \"invalid_request\",\n"
                        + "  \"error_description\": \"Request was missing the 'redirect_uri' parameter.\",\n"
                        + "  \"error_uri\": \"See the full API docs at https://authorization-server.com/docs/access_token\"\n"
                        + "}"));
        HttpRequest getRequest = request().withPath("/api")
                .withMethod("GET");
        mockServer.when(getRequest)
                .respond(response().withStatusCode(401).withBody("Authentication error"));
        httpClient.execute(httpRequest);
        mockServer.verify(oauthRequest, VerificationTimes.exactly(1));
        mockServer.verify(getRequest, VerificationTimes.exactly(1));

        oAuth2Credential.setAccessToken(null);
        okHttpClient.newCall(transformRequest(httpRequest)).execute();
        mockServer.verify(oauthRequest, VerificationTimes.exactly(2));
        mockServer.verify(getRequest, VerificationTimes.exactly(2));
    }

    @Test
    public void shouldNotThrowExceptionWhenServerHasOAuthTimeout() throws Exception {
        HttpRequest oauthRequest = request().withPath("/oauth2/token")
                .withBody("client_id=clientId&client_secret=clientSecret&scope=order%3Aread&grant_type=client_credentials");
        mockServer.when(oauthRequest)
                .respond(response().withStatusCode(200)
                        .withDelay(TimeUnit.SECONDS, 6).applyDelay()
                        .withBody("{\"access_token\":\"ACCESSTOKEN\",\"expires_in\":3599,\"scope\":\"order:read order:write\",\"token_type\":\"bearer\"}"));
        HttpRequest getRequest = request().withPath("/api")
                .withHeader(not("Authorization"), string(".*"))
                .withMethod("GET");
        mockServer.when(getRequest)
                .respond(response().withStatusCode(200).withBody("No error"));

        httpClient.execute(httpRequest);
        mockServer.verify(getRequest, VerificationTimes.exactly(1));

        oAuth2Credential.setAccessToken(null);
        okHttpClient.newCall(transformRequest(httpRequest)).execute();
        mockServer.verify(getRequest, VerificationTimes.exactly(2));
    }

    private Request transformRequest(org.apache.http.HttpRequest request) {
        Request.Builder builder = new Request.Builder();
        RequestLine requestLine = request.getRequestLine();
        Header[] headers = request.getAllHeaders();
        for (Header header : headers) {
            builder = builder.header(header.getName(), header.getValue());
        }
        builder.header("User-Agent", "Apache-HttpClient/4.5.2 (Java/1.8.0_252)");
        builder.header("Accept-Encoding", "gzip,deflate");
        return builder.url(requestLine.getUri()).build();
    }
}
