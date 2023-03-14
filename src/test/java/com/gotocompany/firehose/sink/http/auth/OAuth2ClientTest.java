package com.gotocompany.firehose.sink.http.auth;

import com.gotocompany.firehose.exception.OAuth2Exception;
import org.joda.time.DateTimeUtils;
import org.junit.*;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.model.HttpRequest;

import java.io.IOException;

import static org.mockserver.integration.ClientAndServer.startClientAndServer;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;

public class OAuth2ClientTest {

    private OAuth2Client oAuth2Client;
    private static ClientAndServer mockServer;

    @BeforeClass
    public static void startServer() {
        mockServer = startClientAndServer(1080);
    }

    @AfterClass
    public static void stopServer() {
        mockServer.stop();
    }

    @Before
    public void setUp() {
        DateTimeUtils.setCurrentMillisFixed(System.currentTimeMillis());
        mockServer.reset();
        String clientId = "clientId";
        String clientSecret = "clientSecret";
        String scope = "order:read";
        String accessTokenEndpoint = "http://127.0.0.1:1080/oauth2/token";
        oAuth2Client = new OAuth2Client(clientId, clientSecret, scope, accessTokenEndpoint);

    }

    @Test(expected = OAuth2Exception.class)
    public void shouldThrowOAuth2ExceptionIfResponseReturnedIsNon2XX() throws IOException {
        HttpRequest oauthRequest = request().withPath("/oauth2/token")
                .withBody("client_id=clientId&client_secret=clientSecret&scope=order%3Aread&grant_type=client_credentials");
        mockServer.when(oauthRequest)
                .respond(response().withStatusCode(400).withBody("{\"access_token\":\"ACCESSTOKEN\",\"expires_in\":3599,\"scope\":\"order:read order:write\",\"token_type\":\"bearer\"}"));

        oAuth2Client.requestClientCredentialsGrantAccessToken();
    }

    @Test
    public void shouldReturnOAuth2AccessTokenIfResponseReturnedIs200() throws IOException {
        Long expiresIn = 3599L;
        HttpRequest oauthRequest = request().withPath("/oauth2/token")
                .withBody("client_id=clientId&client_secret=clientSecret&scope=order%3Aread&grant_type=client_credentials");
        mockServer.when(oauthRequest)
                .respond(response().withStatusCode(200).withBody("{\"access_token\":\"ACCESSTOKEN\",\"expires_in\":3599,\"scope\":\"order:read order:write\",\"token_type\":\"bearer\"}"));

        OAuth2AccessToken oAuth2AccessToken = oAuth2Client.requestClientCredentialsGrantAccessToken();

        Assert.assertEquals("ACCESSTOKEN", oAuth2AccessToken.toString());
        Assert.assertEquals(expiresIn, oAuth2AccessToken.getExpiresIn());
    }

    @Test
    public void shouldReturnOAuth2AccessTokenIfResponseReturnedIs201() throws IOException {
        Long expiresIn = 3599L;
        HttpRequest oauthRequest = request().withPath("/oauth2/token")
                .withBody("client_id=clientId&client_secret=clientSecret&scope=order%3Aread&grant_type=client_credentials");
        mockServer.when(oauthRequest)
                .respond(response().withStatusCode(201).withBody("{\"access_token\":\"ACCESSTOKEN\",\"expires_in\":3599,\"scope\":\"order:read order:write\",\"token_type\":\"bearer\"}"));

        OAuth2AccessToken oAuth2AccessToken = oAuth2Client.requestClientCredentialsGrantAccessToken();

        Assert.assertEquals("ACCESSTOKEN", oAuth2AccessToken.toString());
        Assert.assertEquals(expiresIn, oAuth2AccessToken.getExpiresIn());
    }
}
