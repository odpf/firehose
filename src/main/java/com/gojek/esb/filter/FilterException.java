package com.gojek.esb.filter;

public class FilterException extends Exception {

    public FilterException(String message, Exception e) {
        super(message, e);
    }

    public FilterException(String message) {
        super(message);
    }
}
