package com.gotocompany.firehose.exception;

import java.io.IOException;

public class OAuth2Exception extends IOException {
    public OAuth2Exception(String message) {
        super(message);
    }
}

