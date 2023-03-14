package com.gotocompany.firehose.sink.http.auth;

import org.joda.time.DateTimeUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class OAuth2AccessTokenTest {

    private OAuth2AccessToken oAuth2AccessToken;
    private String accessToken;

    @Before
    public void setUp() {
        DateTimeUtils.setCurrentMillisFixed(System.currentTimeMillis());
        accessToken = "SAMPLE-TOKEN";
    }

    @Test
    public void shouldReturnTrueWhenExpireTimeIsLessThan60Sec() {
        oAuth2AccessToken = new OAuth2AccessToken(accessToken, 55);

        Assert.assertTrue(oAuth2AccessToken.isExpired());
    }

    @Test
    public void shouldReturnTrueWhenExpireTimeIs60Sec() {
        oAuth2AccessToken = new OAuth2AccessToken(accessToken, 60);

        Assert.assertTrue(oAuth2AccessToken.isExpired());
    }

    @Test
    public void shouldReturnFalseWhenExpireTimeIsMoreThan60Sec() {
        oAuth2AccessToken = new OAuth2AccessToken(accessToken, 62);

        Assert.assertFalse(oAuth2AccessToken.isExpired());
    }

    @Test
    public void shouldReturnExpirationTimeAsPassedInParamaters() {
        Long expiresIn = 65L;
        oAuth2AccessToken = new OAuth2AccessToken(accessToken, 65);

        Assert.assertEquals(expiresIn, oAuth2AccessToken.getExpiresIn());
    }

    @Test
    public void shouldReturnDefaultExpirationTimeWhenNotPassedInParamaters() {
        Long expiresIn = 3600L;
        oAuth2AccessToken = new OAuth2AccessToken(accessToken, null);

        Assert.assertEquals(expiresIn, oAuth2AccessToken.getExpiresIn());
    }
}
