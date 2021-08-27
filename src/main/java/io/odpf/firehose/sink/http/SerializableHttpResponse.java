package io.odpf.firehose.sink.http;

import org.apache.http.HttpResponse;
import org.apache.logging.log4j.util.Strings;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Collectors;

public class SerializableHttpResponse implements Serializable {
    private HttpResponse httpResponse;

    public SerializableHttpResponse(HttpResponse httpResponse) {
        this.httpResponse = httpResponse;
    }

    @Override
    public String toString() {
        InputStream inputStream = null;
        try {
            inputStream = httpResponse.getEntity().getContent();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return Strings.join(readContent(inputStream), '\n');
    }

    private List<String> readContent(InputStream inputStream) {
        return new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8)).lines().collect(Collectors.toList());
    }
}
