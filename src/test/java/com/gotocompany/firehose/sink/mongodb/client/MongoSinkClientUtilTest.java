package com.gotocompany.firehose.sink.mongodb.client;

import com.gotocompany.firehose.metrics.FirehoseInstrumentation;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class MongoSinkClientUtilTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Mock
    private FirehoseInstrumentation firehoseInstrumentation;

    @Mock
    private MongoClient mongoClient;

    @Mock
    private MongoDatabase mongoDatabase;

    @Test
    public void shouldReturnBlackListRetryStatusCodesAsList() {
        String inputRetryStatusCodeBlacklist = "404, 502";
        List<Integer> statusCodesAsList = MongoSinkClientUtil.getStatusCodesAsList(inputRetryStatusCodeBlacklist);
        assertEquals(404, statusCodesAsList.get(0).intValue());
        assertEquals(502, statusCodesAsList.get(1).intValue());
    }

    @Test
    public void shouldReturnEmptyBlackListRetryStatusCodesAsEmptyList() {
        String inputRetryStatusCodeBlacklist = "";
        List<Integer> statusCodesAsList = MongoSinkClientUtil.getStatusCodesAsList(inputRetryStatusCodeBlacklist);
        assertEquals(0, statusCodesAsList.size());
    }

    @Test
    public void shouldReturnMultipleEmptyBlackListStatusCodesAsEmptyList() {
        String inputRetryStatusCodeBlacklist = ",,,,,";
        List<Integer> statusCodesAsList = MongoSinkClientUtil.getStatusCodesAsList(inputRetryStatusCodeBlacklist);
        assertEquals(0, statusCodesAsList.size());
    }

    @Test
    public void shouldThrowExceptionWhenStatusCodeNotAnInteger() {
        String inputRetryStatusCodeBlacklist = "jahxh";
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Status code must be an integer");
        MongoSinkClientUtil.getStatusCodesAsList(inputRetryStatusCodeBlacklist);
    }

    @Test
    public void shouldThrowExceptionWhenAllStatusCodesNotAnInteger() {
        String inputRetryStatusCodeBlacklist = "jahxh,rbtt,fbne";
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Status code must be an integer");
        MongoSinkClientUtil.getStatusCodesAsList(inputRetryStatusCodeBlacklist);
    }

    @Test
    public void shouldThrowExceptionWhenSomeOfTheStatusCodesNotAnInteger() {
        String inputRetryStatusCodeBlacklist = "jahxh,608,wfrf";
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Status code must be an integer");
        MongoSinkClientUtil.getStatusCodesAsList(inputRetryStatusCodeBlacklist);
    }

    @Test
    public void shouldThrowExceptionWhenStatusCodesHaveInvalidCharacters() {
        String inputRetryStatusCodeBlacklist = "608,++=$>#";
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Status code must be an integer");
        MongoSinkClientUtil.getStatusCodesAsList(inputRetryStatusCodeBlacklist);
    }

    @Test
    public void shouldThrowExceptionWhenNullDatabaseName() {
        thrown.expect(IllegalArgumentException.class);
        MongoSinkClientUtil.checkDatabaseExists(null, mongoClient, firehoseInstrumentation);
    }

    @Test
    public void shouldThrowExceptionWhenNullCollectionName() {
        thrown.expect(IllegalArgumentException.class);
        MongoSinkClientUtil.checkCollectionExists(null, mongoDatabase, firehoseInstrumentation);
    }
}
