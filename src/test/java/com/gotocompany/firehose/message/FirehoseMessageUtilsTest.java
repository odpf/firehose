package com.gotocompany.firehose.message;


import com.gotocompany.depot.common.Tuple;
import org.gradle.internal.impldep.org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class FirehoseMessageUtilsTest {

    @Test
    public void shouldConvertToDepotMessage() {
        Message m1 = new Message(
                "testKey".getBytes(),
                "testMessage".getBytes(),
                "topic",
                12,
                1
        );
        Message m2 = new Message(
                "testKey1".getBytes(),
                "testMessage1".getBytes(),
                "topic1",
                11,
                2
        );
        List<com.gotocompany.depot.message.Message> actualMessages = FirehoseMessageUtils.convertToDepotMessage(new ArrayList<Message>() {{
            add(m1);
            add(m2);
        }});
        Assert.assertEquals(2, actualMessages.size());
        com.gotocompany.depot.message.Message expectedMessage1 = new com.gotocompany.depot.message.Message(
                "testKey".getBytes(),
                "testMessage".getBytes(),
                new Tuple<>("message_topic", "topic"),
                new Tuple<>("message_partition", 12),
                new Tuple<>("message_offset", 1L),
                new Tuple<>("message_headers", null),
                new Tuple<>("message_timestamp", 0L),
                new Tuple<>("load_time", 0L));
        com.gotocompany.depot.message.Message expectedMessage2 = new com.gotocompany.depot.message.Message(
                "testKey1".getBytes(),
                "testMessage1".getBytes(),
                new Tuple<>("message_topic", "topic1"),
                new Tuple<>("message_partition", 11),
                new Tuple<>("message_offset", 2L),
                new Tuple<>("message_headers", null),
                new Tuple<>("message_timestamp", 0L),
                new Tuple<>("load_time", 0L));

        com.gotocompany.depot.message.Message actualMessage1 = actualMessages.get(0);
        com.gotocompany.depot.message.Message actualMessage2 = actualMessages.get(1);

        Assert.assertTrue(Arrays.equals((byte[]) expectedMessage1.getLogKey(), (byte[]) actualMessage1.getLogKey()));
        Assert.assertTrue(Arrays.equals((byte[]) expectedMessage1.getLogMessage(), (byte[]) actualMessage1.getLogMessage()));
        Assert.assertEquals(expectedMessage1.getMetadata(), actualMessage1.getMetadata());

        Assert.assertTrue(Arrays.equals((byte[]) expectedMessage2.getLogKey(), (byte[]) actualMessage2.getLogKey()));
        Assert.assertTrue(Arrays.equals((byte[]) expectedMessage2.getLogMessage(), (byte[]) actualMessage2.getLogMessage()));
        Assert.assertEquals(expectedMessage2.getMetadata(), actualMessage2.getMetadata());
    }

    @Test
    public void shouldReturnEmptyList() {
        List<com.gotocompany.depot.message.Message> actualMessages = FirehoseMessageUtils.convertToDepotMessage(new ArrayList<>());
        Assert.assertEquals(Collections.emptyList(), actualMessages);
    }
}
