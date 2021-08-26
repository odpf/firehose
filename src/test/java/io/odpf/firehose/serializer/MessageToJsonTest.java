package io.odpf.firehose.serializer;




import io.odpf.firehose.consumer.Message;
import io.odpf.firehose.consumer.TestAggregatedSupplyMessage;
import io.odpf.firehose.exception.DeserializerException;
import io.odpf.stencil.StencilClientFactory;
import io.odpf.stencil.client.StencilClient;
import io.odpf.stencil.parser.ProtoParser;
import org.junit.Before;
import org.junit.Test;

import java.util.Base64;

import static org.junit.Assert.assertEquals;

public class MessageToJsonTest {
    private String logMessage;
    private String logKey;
    private ProtoParser protoParser;

    @Before
    public void setUp() {
        StencilClient stencilClient = StencilClientFactory.getClient();
        protoParser = new ProtoParser(stencilClient, TestAggregatedSupplyMessage.class.getName());
        logMessage = "CgYIyOm+xgUSBgiE6r7GBRgNIICAgIDA9/y0LigCMAM\u003d";
        logKey = "CgYIyOm+xgUSBgiE6r7GBRgNIICAgIDA9/y0LigC";
    }

    @Test
    public void shouldProperlySerializeEsbMessage() throws DeserializerException {
        MessageToJson messageToJson = new MessageToJson(protoParser, false, true);

        Message message = new Message(Base64.getDecoder().decode(logKey.getBytes()),
                Base64.getDecoder().decode(logMessage.getBytes()), "sample-topic", 0, 100);
        String actualOutput = messageToJson.serialize(message);
        assertEquals(actualOutput, "{\"logMessage\":\"{\\\"uniqueDrivers\\\":\\\"3\\\","
                + "\\\"windowStartTime\\\":\\\"Mar 20, 2017 10:54:00 AM\\\","
                + "\\\"windowEndTime\\\":\\\"Mar 20, 2017 10:55:00 AM\\\",\\\"s2IdLevel\\\":13,\\\"vehicleType\\\":\\\"BIKE\\\","
                + "\\\"s2Id\\\":\\\"3344472187078705152\\\"}\",\"topic\":\"sample-topic\",\"logKey\":\"{"
                + "\\\"windowStartTime\\\":\\\"Mar 20, 2017 10:54:00 AM\\\","
                + "\\\"windowEndTime\\\":\\\"Mar 20, 2017 10:55:00 AM\\\",\\\"s2IdLevel\\\":13,\\\"vehicleType\\\":\\\"BIKE\\\","
                + "\\\"s2Id\\\":\\\"3344472187078705152\\\"}\"}");
    }

    @Test
    public void shouldSerializeWhenKeyIsMissing() throws DeserializerException {
        MessageToJson messageToJson = new MessageToJson(protoParser, false, true);

        Message message = new Message(null, Base64.getDecoder().decode(logMessage.getBytes()), "sample-topic", 0,
                100);
        String actualOutput = messageToJson.serialize(message);
        assertEquals("{\"logMessage\":\"{\\\"uniqueDrivers\\\":\\\"3\\\","
                + "\\\"windowStartTime\\\":\\\"Mar 20, 2017 10:54:00 AM\\\","
                + "\\\"windowEndTime\\\":\\\"Mar 20, 2017 10:55:00 AM\\\",\\\"s2IdLevel\\\":13,\\\"vehicleType\\\":\\\"BIKE\\\","
                + "\\\"s2Id\\\":\\\"3344472187078705152\\\"}\",\"topic\":\"sample-topic\"}", actualOutput);
    }

    @Test
    public void shouldSerializeWhenKeyIsEmptyWithTimestampsAsSimpleDateFormatWhenFlagIsEnabled() throws DeserializerException {
        MessageToJson messageToJson = new MessageToJson(protoParser, false, true);

        Message message = new Message(new byte[]{}, Base64.getDecoder().decode(logMessage.getBytes()),
                "sample-topic", 0, 100);

        String actualOutput = messageToJson.serialize(message);
        assertEquals("{\"logMessage\":\"{\\\"uniqueDrivers\\\":\\\"3\\\","
                + "\\\"windowStartTime\\\":\\\"Mar 20, 2017 10:54:00 AM\\\","
                + "\\\"windowEndTime\\\":\\\"Mar 20, 2017 10:55:00 AM\\\",\\\"s2IdLevel\\\":13,\\\"vehicleType\\\":\\\"BIKE\\\","
                + "\\\"s2Id\\\":\\\"3344472187078705152\\\"}\",\"topic\":\"sample-topic\"}", actualOutput);
    }

    @Test
    public void shouldSerializeWhenKeyIsEmptyWithTimestampsAsISOFormatWhenFlagIsDisabled() throws DeserializerException {
        MessageToJson messageToJson = new MessageToJson(protoParser, false, false);

        Message message = new Message(new byte[]{}, Base64.getDecoder().decode(logMessage.getBytes()),
                "sample-topic", 0, 100);

        String actualOutput = messageToJson.serialize(message);
        assertEquals("{\"logMessage\":\"{\\\"uniqueDrivers\\\":\\\"3\\\","
                + "\\\"windowStartTime\\\":\\\"2017-03-20T10:54:00Z\\\","
                + "\\\"windowEndTime\\\":\\\"2017-03-20T10:55:00Z\\\",\\\"s2IdLevel\\\":13,\\\"vehicleType\\\":\\\"BIKE\\\","
                + "\\\"s2Id\\\":\\\"3344472187078705152\\\"}\",\"topic\":\"sample-topic\"}", actualOutput);
    }

    @Test
    public void shouldWrappedSerializedJsonInArrayWithTimestampsAsSimpleDateFormatWhenFlagsAreEnabled() throws DeserializerException {
        boolean wrappedInsideArray = true;
        MessageToJson messageToJson = new MessageToJson(protoParser, false, wrappedInsideArray, true);

        Message message = new Message(new byte[]{}, Base64.getDecoder().decode(logMessage.getBytes()),
                "sample-topic", 0, 100);

        String actualOutput = messageToJson.serialize(message);
        assertEquals("[{\"logMessage\":\"{\\\"uniqueDrivers\\\":\\\"3\\\","
                + "\\\"windowStartTime\\\":\\\"Mar 20, 2017 10:54:00 AM\\\","
                + "\\\"windowEndTime\\\":\\\"Mar 20, 2017 10:55:00 AM\\\",\\\"s2IdLevel\\\":13,\\\"vehicleType\\\":\\\"BIKE\\\","
                + "\\\"s2Id\\\":\\\"3344472187078705152\\\"}\",\"topic\":\"sample-topic\"}]", actualOutput);
    }

    @Test
    public void shouldReturnTheTimestampFieldsInISOFormatIfSimpleDateFormatIsDisabled() throws DeserializerException {
        boolean wrappedInsideArray = true;
        MessageToJson messageToJson = new MessageToJson(protoParser, false, wrappedInsideArray, false);

        Message message = new Message(new byte[]{}, Base64.getDecoder().decode(logMessage.getBytes()),
                "sample-topic", 0, 100);

        String actualOutput = messageToJson.serialize(message);
        assertEquals("[{\"logMessage\":\"{\\\"uniqueDrivers\\\":\\\"3\\\","
                + "\\\"windowStartTime\\\":\\\"2017-03-20T10:54:00Z\\\","
                + "\\\"windowEndTime\\\":\\\"2017-03-20T10:55:00Z\\\",\\\"s2IdLevel\\\":13,\\\"vehicleType\\\":\\\"BIKE\\\","
                + "\\\"s2Id\\\":\\\"3344472187078705152\\\"}\",\"topic\":\"sample-topic\"}]", actualOutput);
    }
}
