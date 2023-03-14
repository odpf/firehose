package com.gotocompany.firehose.sink.common.blobstorage.gcs.error;

import java.util.HashMap;
import java.util.Map;

/**
 * Error types from exception thrown by google cloud storage client.
 * There might be newer error codes that thrown by gcs client, need to update this list of error.
 */
public enum GCSErrorType {
    FOUND(302),
    SEE_OTHER(303),
    NOT_MODIFIED(304),
    TEMPORARY_REDIRECT(305),
    RESUME_INCOMPLETE(308),
    UNAUTHORIZED(401),
    FORBIDDEN(403),
    NOT_FOUND(404),
    METHOD_NOT_ALLOWED(405),
    CONFLICT(409),
    GONE(410),
    LENGTH_REQUIRED(411),
    PRECONDITION_FAILED(412),
    PAYLOAD_TOO_LARGE(413),
    REQUESTED_RANGE_NOT_SATISFIABLE(416),
    CLIENT_CLOSED_REQUEST(499),
    BAD_REQUEST(400),
    GATEWAY_TIMEOUT(504),
    SERVICE_UNAVAILABLE(503),
    BAD_GATEWAY(502),
    INTERNAL_SERVER_ERROR(500),
    TOO_MANY_REQUEST(429),
    REQUEST_TIMEOUT(408),
    DEFAULT_ERROR;

    private static final Map<Integer, GCSErrorType> ERROR_NUMBER_TYPE_MAP = new HashMap<>();

    static {
        for (GCSErrorType errorType : values()) {
            ERROR_NUMBER_TYPE_MAP.put(errorType.codeValue, errorType);
        }
    }

    public static GCSErrorType valueOfCode(int code) {
        return ERROR_NUMBER_TYPE_MAP.getOrDefault(code, DEFAULT_ERROR);
    }

    private int codeValue;

    GCSErrorType(int codeValue) {
        this.codeValue = codeValue;
    }

    GCSErrorType() {

    }
}
