package io.odpf.firehose.sink.http.request.uri;




import io.odpf.firehose.consumer.Message;
import io.odpf.firehose.consumer.TestBookingLogMessage;
import io.odpf.firehose.consumer.TestKey;
import io.odpf.firehose.consumer.TestMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import io.odpf.stencil.client.ClassLoadStencilClient;
import io.odpf.stencil.client.StencilClient;
import io.odpf.stencil.parser.ProtoParser;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class UriParserTest {
    @Mock
    private ProtoParser protoParser;
    private ProtoParser testMessageProtoParser;
    private ProtoParser bookingMessageProtoParser;
    private Message message;
    private Message bookingMessage;

    @Before
    public void setUp() {
        initMocks(this);
        TestKey testKey = TestKey.newBuilder().setOrderNumber("ORDER-1-FROM-KEY").build();
        TestMessage testMessage = TestMessage.newBuilder().setOrderNumber("test-order").setOrderDetails("ORDER-DETAILS").build();
        TestBookingLogMessage testBookingLogMessage = TestBookingLogMessage.newBuilder().setOrderNumber("bookingOrderNumber").setCustomerTotalFareWithoutSurge(2000L).setAmountPaidByCash(12.3F).build();

        this.message = new Message(testKey.toByteArray(), testMessage.toByteArray(), "test", 1, 11);
        this.bookingMessage = new Message(testKey.toByteArray(), testBookingLogMessage.toByteArray(), "test", 1, 11);

        StencilClient stencilClient = new ClassLoadStencilClient();
        testMessageProtoParser = new ProtoParser(stencilClient, TestMessage.class.getCanonicalName());
        bookingMessageProtoParser = new ProtoParser(stencilClient, TestBookingLogMessage.class.getCanonicalName());
    }

    @Test
    public void shouldReturnTheServiceUrlAsItIsWhenServiceUrlIsNotParametrizedAndParserModeIsMessage() {
        UriParser uriParser = new UriParser(testMessageProtoParser, "message");
        String serviceUrl = "http://dummyurl.com";
        String parsedUrl = uriParser.parse(message, serviceUrl);

        assertEquals(serviceUrl, parsedUrl);
    }

    @Test
    public void shouldReturnTheServiceUrlAsItIsWhenServiceUrlIsNotParametrizedAndParserModeIsKey() {
        UriParser uriParser = new UriParser(testMessageProtoParser, "key");
        String serviceUrl = "http://dummyurl.com";
        String parsedUrl = uriParser.parse(message, serviceUrl);

        assertEquals(serviceUrl, parsedUrl);
    }

    @Test
    public void shouldSetTheValueInServiceUrlFromGivenProtoIndexWhenServiceUrlIsParametrizedAndParserModeIsMessage() {
        UriParser uriParser = new UriParser(testMessageProtoParser, "message");
        String serviceUrl = "http://dummyurl.com/%s,1";
        String parsedUrl = uriParser.parse(message, serviceUrl);

        assertEquals("http://dummyurl.com/test-order", parsedUrl);
    }

    @Test
    public void shouldSetTheValueInServiceUrlFromGivenProtoIndexWhenServiceUrlIsParametrizedAndParserModeIsKey() {
        UriParser uriParser = new UriParser(testMessageProtoParser, "key");
        String serviceUrl = "http://dummyurl.com/%s,1";
        String parsedUrl = uriParser.parse(message, serviceUrl);

        assertEquals("http://dummyurl.com/ORDER-1-FROM-KEY", parsedUrl);
    }

    @Test
    public void shouldSetTheFloatValueInServiceUrlFromGivenProtoIndexWhenServiceUrlIsParametrizedAndParserModeIsMessage() {
        UriParser uriParser = new UriParser(bookingMessageProtoParser, "message");
        String serviceUrl = "http://dummyurl.com/%.2f,16";
        String parsedUrl = uriParser.parse(bookingMessage, serviceUrl);

        assertEquals("http://dummyurl.com/12.30", parsedUrl);
    }

    @Test
    public void shouldSetTheLongValueInServiceUrlFromGivenProtoIndexWhenServiceUrlIsParametrizedAndParserModeIsMessage() {
        UriParser uriParser = new UriParser(bookingMessageProtoParser, "message");
        String serviceUrl = "http://dummyurl.com/%d,52";
        String parsedUrl = uriParser.parse(bookingMessage, serviceUrl);

        assertEquals("http://dummyurl.com/2000", parsedUrl);
    }

    @Test
    public void shouldCatchInvalidProtocolBufferExceptionFromProtoParserAndThrowIllegalArgumentException() throws InvalidProtocolBufferException {
        when(protoParser.parse(any())).thenThrow(new InvalidProtocolBufferException(""));
        UriParser uriParser = new UriParser(protoParser, "message");
        String serviceUrl = "http://dummyurl.com/%s,1";

        try {
            uriParser.parse(message, serviceUrl);
        } catch (IllegalArgumentException e) {
            assertEquals("Unable to parse Service URL", e.getMessage());
        }

    }

    @Test
    public void shouldThrowIllegalArgumentExceptionWhenEmptyServiceUrlIsProvided() {
        UriParser uriParser = new UriParser(testMessageProtoParser, "message");
        String serviceUrl = "";
        try {
            uriParser.parse(message, serviceUrl);
        } catch (IllegalArgumentException e) {
            assertEquals("Service URL '" + serviceUrl + "' is invalid", e.getMessage());
        }

    }

    @Test
    public void shouldThrowInvalidConfigurationExceptionWhenNoUrlAndArgumentsAreProvided() {
        UriParser uriParser = new UriParser(testMessageProtoParser, "message");
        String serviceUrl = ",,,";
        try {
            uriParser.parse(message, serviceUrl);
        } catch (InvalidConfigurationException e) {
            assertEquals("Empty Service URL configuration: '" + serviceUrl + "'", e.getMessage());
        }

    }

    @Test
    public void shouldCatchNumberFormatExceptionAndThrowIllegalArgumentsException() {
        UriParser uriParser = new UriParser(testMessageProtoParser, "message");
        String serviceUrl = "http://dummy.com/%s, 6a";
        try {
            uriParser.parse(message, serviceUrl);
        } catch (IllegalArgumentException e) {
            assertEquals("Invalid Proto Index", e.getMessage());
        }

    }

    @Test
    public void shouldThrowIllegalArgumentsExceptionWhenDescriptorIsNotFoundForTheProtoIndexProvided() {
        UriParser uriParser = new UriParser(testMessageProtoParser, "message");
        String serviceUrl = "http://dummy.com/%s, 1000";
        try {
            uriParser.parse(message, serviceUrl);
        } catch (IllegalArgumentException e) {
            assertEquals("Descriptor not found for index: 1000", e.getMessage());
        }

    }

}
