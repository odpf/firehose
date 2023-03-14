package com.gotocompany.firehose.sink.http.auth;

import org.joda.time.DateTimeUtils;

public class OAuth2AccessToken {
    private final String accessToken;
    private final Long expirationTimeMs;
    private static final int DEFAULT_EXPIRATION_TIME = 3600;
    private static final long MILLIS = 1000L;

    public OAuth2AccessToken(String accessToken, Integer expiresIn) {
        this.accessToken = accessToken;
        expiresIn = expiresIn == null ? DEFAULT_EXPIRATION_TIME : expiresIn;
        this.expirationTimeMs = DateTimeUtils.currentTimeMillis() + (expiresIn * MILLIS);
    }

    public boolean isExpired() {
        final long oneMinute = 60L;
        return this.getExpiresIn() <= oneMinute;
    }

    public String toString() {
        return this.accessToken;
    }

    public Long getExpiresIn() {
        return (this.expirationTimeMs - DateTimeUtils.currentTimeMillis()) / MILLIS;
    }
}

