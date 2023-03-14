package com.gotocompany.firehose.sink.http.auth;

import com.gotocompany.firehose.metrics.FirehoseInstrumentation;
import okhttp3.Interceptor;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.HttpResponseInterceptor;
import org.apache.http.HttpStatus;
import org.apache.http.impl.client.HttpClientBuilder;

import java.io.IOException;

/**
 * Interceptor to add oauth token in http request.
 */
public class OAuth2Credential implements Interceptor {

    private final OAuth2Client client;
    private OAuth2AccessToken accessToken;
    private FirehoseInstrumentation firehoseInstrumentation;

    public OAuth2Credential(FirehoseInstrumentation firehoseInstrumentation, String clientId, String clientSecret, String scope, String accessTokenEndpoint) {
        this.firehoseInstrumentation = firehoseInstrumentation;
        this.client = new OAuth2Client(clientId, clientSecret, scope, accessTokenEndpoint);
    }

    public void requestAccessToken() throws IOException {
        firehoseInstrumentation.logInfo("Requesting Access Token, expires in: {0}",
                (this.accessToken == null ? "<none>" : this.accessToken.getExpiresIn()));
        OAuth2AccessToken token = client.requestClientCredentialsGrantAccessToken();
        setAccessToken(token);
    }

    public HttpRequestInterceptor requestInterceptor() {
        return (request, context) -> {
            try {
                if (getAccessToken() == null || getAccessToken().isExpired()) {
                    requestAccessToken();
                }
                request.addHeader("Authorization", "Bearer " + getAccessToken().toString());
            } catch (IOException e) {
                firehoseInstrumentation.logWarn("OAuth2 request access token failed: {0}", e.getMessage());
            }
        };
    }

    public HttpResponseInterceptor responseInterceptor() {
        return (response, context) -> {
            boolean isTokenExpired = response.getStatusLine().getStatusCode() == HttpStatus.SC_UNAUTHORIZED;
            if (isTokenExpired) {
                setAccessToken(null);
            }
        };
    }

    public HttpClientBuilder initialize(HttpClientBuilder builder) {
        return builder.addInterceptorFirst(this.requestInterceptor()).addInterceptorLast(this.responseInterceptor());
    }

    public OAuth2AccessToken getAccessToken() {
        return accessToken;
    }

    public void setAccessToken(OAuth2AccessToken accessToken) {
        this.accessToken = accessToken;
    }

    @Override
    public Response intercept(Chain chain) throws IOException {
        Request request = chain.request();
        try {
            if (getAccessToken() == null || getAccessToken().isExpired()) {
                requestAccessToken();
            }
            request = request.newBuilder().header("Authorization", "Bearer " + getAccessToken().toString()).build();
        } catch (IOException e) {
            firehoseInstrumentation.logWarn("OAuth2 request access token failed: {0}", e.getMessage());
        }

        Response response = chain.proceed(request);
        boolean isTokenExpired = response.code() == HttpStatus.SC_UNAUTHORIZED;
        if (isTokenExpired) {
            setAccessToken(null);
        }
        return response;
    }
}

