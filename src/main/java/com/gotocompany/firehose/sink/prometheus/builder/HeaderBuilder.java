package com.gotocompany.firehose.sink.prometheus.builder;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import static com.gotocompany.firehose.sink.prometheus.PromSinkConstants.*;

/**
 * Builder for prometheus request header.
 */
public class HeaderBuilder {

    private final Map<String, String> baseHeaders;

    /**
     * Instantiates a new Header builder.
     *
     * @param headerConfig  the header config
     */
    public HeaderBuilder(String headerConfig) {
        baseHeaders = Arrays.stream(headerConfig.split(","))
                        .filter(kv -> !kv.trim().isEmpty()).map(kv -> kv.split(":"))
                        .collect(Collectors.toMap(kv -> kv[0], kv -> kv[1]));
    }

    /**
     * build prometheus request header.
     *
     * @return prometheus request header
     */
    public Map<String, String> build() {
        Map<String, String> headers = new HashMap<>(baseHeaders);
        headers.put(CONTENT_ENCODING, CONTENT_ENCODING_DEFAULT);
        headers.put(PROMETHEUS_REMOTE_WRITE_VERSION, PROMETHEUS_REMOTE_WRITE_VERSION_DEFAULT);
        return headers;
    }
}
