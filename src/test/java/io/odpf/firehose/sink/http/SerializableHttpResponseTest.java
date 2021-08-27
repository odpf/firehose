package io.odpf.firehose.sink.http;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.tools.ant.filters.StringInputStream;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;

import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class SerializableHttpResponseTest {

    @Mock
    private HttpResponse httpResponse;
    @Mock
    private HttpEntity httpEntity;
    private SerializableHttpResponse serializableHttpResponse;

    @Before
    public void setUp() throws Exception {
        serializableHttpResponse = new SerializableHttpResponse(httpResponse);
    }

    @Test
    public void shouldReturnTheResponseBodyStringInCaseOfNonNullResponse() throws IOException {
        when(httpResponse.getEntity()).thenReturn(httpEntity);
        when(httpEntity.getContent()).thenReturn(new StringInputStream("[{\"key\":\"value1\"}, {\"key\":\"value2\"}]"));

        String responseBody = serializableHttpResponse.toString();
        Assert.assertEquals("[{\"key\":\"value1\"}, {\"key\":\"value2\"}]", responseBody);
    }
}
