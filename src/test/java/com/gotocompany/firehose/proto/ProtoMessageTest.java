package com.gotocompany.firehose.proto;

import com.gotocompany.firehose.exception.DeserializerException;
import com.gotocompany.firehose.message.Message;
import com.gotocompany.firehose.consumer.TestBookingLogKey;
import com.gotocompany.firehose.consumer.TestFeedbackLogKey;
import com.gotocompany.firehose.consumer.TestFeedbackLogMessage;
import com.google.protobuf.Timestamp;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class ProtoMessageTest {
    private static final int TIMESTAMP_IN_EPOCH_SECONDS = 149860000;
    private Message message;
    private static final String EXPECTED_ORDER_NUMBER = "R-123";
    private static final String EXPECTED_FEEDBACK = "good";
    private static final int ORDER_NUMBER_INDEX = 1;
    private static final int FEEDBACK_INDEX = 6;

    @Before
    public void setUp() {
        setupEsbMessages(EXPECTED_ORDER_NUMBER, EXPECTED_FEEDBACK);
    }

    @Test
    public void shouldGetFieldValueBasedOnIndex() throws DeserializerException {

        ProtoMessage protoMessage = new ProtoMessage(TestFeedbackLogMessage.class.getName());

        assertEquals(EXPECTED_ORDER_NUMBER, protoMessage.get(message, ORDER_NUMBER_INDEX));
        assertEquals(EXPECTED_FEEDBACK, protoMessage.get(message, FEEDBACK_INDEX));
    }

    @Test
    public void shouldThrowExceptionForUnknownClass() {

        try {
            new ProtoMessage("unknown.class.name");

            fail("Should have thrown eglc configuration exception");
        } catch (RuntimeException e) {
            assertEquals(ProtoMessage.CLASS_NAME_NOT_FOUND, e.getMessage());
        }
    }

    @Test
    public void shouldThrowExceptionForInvalidProtoClass() {
        try {
            new ProtoMessage(String.class.getName());

            fail("Should have thrown eglc configuration exception");
        } catch (RuntimeException e) {
            assertEquals(ProtoMessage.INVALID_PROTOCOL_CLASS_MESSAGE, e.getMessage());
        }
    }

    @Test
    public void shouldThrowExceptionForCorruptedEsbMessages() {
        ProtoMessage protoMessage = new ProtoMessage(TestBookingLogKey.class.getName());

        try {
            protoMessage.get(message, FEEDBACK_INDEX);

            fail("Should throw deserialzer exception on recieving corrupted messages");
        } catch (DeserializerException e) {
            assertEquals(ProtoMessage.DESERIALIZE_ERROR_MESSAGE, e.getMessage());
        }
    }

    private void setupEsbMessages(String expectedOrderNumber, String expectedFeedback) {
        TestFeedbackLogMessage feedbackLogMessage = TestFeedbackLogMessage.newBuilder().setOrderNumber(expectedOrderNumber).setFeedbackComment(expectedFeedback).setEventTimestamp(getTimestamp(TIMESTAMP_IN_EPOCH_SECONDS)).build();
        TestFeedbackLogKey feedbackLogKey = TestFeedbackLogKey.newBuilder().build();
        message = new Message(feedbackLogKey.toByteArray(), feedbackLogMessage.toByteArray(), "topic", 1, 1);
    }

    private Timestamp getTimestamp(int seconds) {
        return Timestamp.newBuilder().setSeconds(seconds).build();
    }
}
