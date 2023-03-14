package com.gotocompany.firehose.sink.http.request.entity;

import org.apache.commons.io.IOUtils;
import org.apache.http.entity.StringEntity;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.mockito.MockitoAnnotations.initMocks;

public class RequestEntityBuilderTest {

    private String bodyContent;

    @Before
    public void setUp() {
        initMocks(this);
        bodyContent = "dummyContent";
    }

    @Test
    public void shouldCreateStringEntity() throws IOException {
        RequestEntityBuilder requestEntityBuilder = new RequestEntityBuilder();

        StringEntity stringEntity = requestEntityBuilder.buildHttpEntity(bodyContent);
        byte[] bytes = IOUtils.toByteArray(stringEntity.getContent());
        Assert.assertEquals("dummyContent", new String(bytes));
    }

    @Test
    public void shouldWrapEntityIfSet() throws IOException {
        RequestEntityBuilder requestEntityBuilder = new RequestEntityBuilder();

        StringEntity stringEntity = requestEntityBuilder.setWrapping(true).buildHttpEntity(bodyContent);
        byte[] bytes = IOUtils.toByteArray(stringEntity.getContent());
        Assert.assertEquals("[dummyContent]", new String(bytes));
    }

    @Test
    public void shouldNotWrapEntityIfNotSet() throws IOException {
        RequestEntityBuilder requestEntityBuilder = new RequestEntityBuilder();

        StringEntity stringEntity = requestEntityBuilder.setWrapping(false).buildHttpEntity(bodyContent);
        byte[] bytes = IOUtils.toByteArray(stringEntity.getContent());
        Assert.assertEquals("dummyContent", new String(bytes));
    }
}
