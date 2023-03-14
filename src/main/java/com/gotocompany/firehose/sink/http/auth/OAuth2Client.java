package com.gotocompany.firehose.sink.http.auth;

import com.gotocompany.firehose.exception.OAuth2Exception;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;

public class OAuth2Client {
    private final HttpClient client;
    private final String clientId;
    private final String clientSecret;
    private final String scope;
    private final String accessTokenEndpoint;
    private final int timeoutMs = 5000;
    private static final String SUCCESS_CODE_PATTERN = "^2.*";

    public OAuth2Client(String clientId, String clientSecret, String scope, String accessTokenEndpoint) {
        this.clientId = clientId;
        this.clientSecret = clientSecret;
        this.scope = scope;
        this.accessTokenEndpoint = accessTokenEndpoint;
        this.client = this.httpClient();
    }

    private CloseableHttpClient httpClient() {
        RequestConfig config = RequestConfig.custom().setConnectTimeout(timeoutMs).setConnectionRequestTimeout(timeoutMs).setSocketTimeout(timeoutMs).build();
        return HttpClientBuilder.create().setDefaultRequestConfig(config).build();
    }

    public OAuth2AccessToken requestClientCredentialsGrantAccessToken() throws IOException {
        HttpPost req = new HttpPost(this.accessTokenEndpoint);
        req.setHeader("Content-Type", "application/x-www-form-urlencoded");
        List<NameValuePair> kv = new ArrayList();
        kv.add(new BasicNameValuePair("client_id", this.clientId));
        kv.add(new BasicNameValuePair("client_secret", this.clientSecret));
        kv.add(new BasicNameValuePair("scope", this.scope));
        kv.add(new BasicNameValuePair("grant_type", "client_credentials"));
        req.setEntity(new UrlEncodedFormEntity(kv, "UTF-8"));
        HttpResponse response = this.client.execute(req);
        String body = EntityUtils.toString(response.getEntity());
        Type responseMapType = (new TypeToken<Map<String, String>>() {
        }).getType();
        Map<String, String> map = new Gson().fromJson(body, responseMapType);

        if (!Pattern.compile(SUCCESS_CODE_PATTERN).matcher(String.valueOf(response.getStatusLine().getStatusCode())).matches()) {
            throw new OAuth2Exception("OAuthException: " + map.get("error"));
        } else {
            String accessToken = map.get("access_token");
            String expiresInRaw = map.get("expires_in");
            Integer expiresIn = expiresInRaw == null ? null : Integer.valueOf(expiresInRaw);
            return new OAuth2AccessToken(accessToken, expiresIn);
        }
    }
}
