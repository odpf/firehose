package io.odpf.firehose.serializer;




import io.odpf.firehose.consumer.Message;
import io.odpf.firehose.consumer.TestAggregatedSupplyMessage;
import io.odpf.firehose.exception.DeserializerException;
import io.odpf.firehose.exception.EglcConfigurationException;
import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.stencil.StencilClientFactory;
import io.odpf.stencil.client.StencilClient;
import io.odpf.stencil.parser.ProtoParser;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Base64;
import java.util.HashSet;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.mockito.MockitoAnnotations.initMocks;

public class MessageToTemplatizedJsonTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Mock
    private Instrumentation instrumentation;

    @Mock
    private ProtoParser protoParser;

    private String logMessage;
    private String logKey;

    @Before
    public void setup() {
        initMocks(this);
        logMessage = "CgYIyOm+xgUSBgiE6r7GBRgNIICAgIDA9/y0LigCMAM\u003d";
        logKey = "CgYIyOm+xgUSBgiE6r7GBRgNIICAgIDA9/y0LigC";
    }

    @Test
    public void shouldProperlySerializeMessageToTemplateWithSingleUnknownField() {
        String template = "{\"test\":\"$.vehicle_type\"}";
        StencilClient stencilClient = StencilClientFactory.getClient();
        protoParser = new ProtoParser(stencilClient, TestAggregatedSupplyMessage.class.getName());
        MessageToTemplatizedJson messageToTemplatizedJson = MessageToTemplatizedJson
                .create(instrumentation, template, protoParser);
        Message message = new Message(Base64.getDecoder().decode(logKey.getBytes()),
                Base64.getDecoder().decode(logMessage.getBytes()), "sample-topic", 0, 100);

        String serializedMessage = messageToTemplatizedJson.serialize(message);
        String expectedMessage = "{\"test\":\"BIKE\"}";
        Assert.assertEquals(expectedMessage, serializedMessage);
    }

    @Test
    public void shouldProperlySerializeMessageToTemplateWithAsItIs() {
        String template = "\"$._all_\"";
        StencilClient stencilClient = StencilClientFactory.getClient();
        protoParser = new ProtoParser(stencilClient, TestAggregatedSupplyMessage.class.getName());
        MessageToTemplatizedJson messageToTemplatizedJson = MessageToTemplatizedJson
                .create(instrumentation, template, protoParser);
        Message message = new Message(Base64.getDecoder().decode(logKey.getBytes()),
                Base64.getDecoder().decode(logMessage.getBytes()), "sample-topic", 0, 100);

        String serializedMessage = messageToTemplatizedJson.serialize(message);
        String expectedMessage = "{\n"
                + "  \"window_start_time\": \"2017-03-20T10:54:00Z\",\n"
                + "  \"window_end_time\": \"2017-03-20T10:55:00Z\",\n"
                + "  \"s2_id_level\": 13,\n"
                + "  \"s2_id\": \"3344472187078705152\",\n"
                + "  \"vehicle_type\": \"BIKE\",\n"
                + "  \"unique_drivers\": \"3\"\n"
                + "}";
        Assert.assertEquals(expectedMessage, serializedMessage);
    }

    @Test
    public void shouldThrowIfNoPathsFoundInTheProto() {
        expectedException.expect(DeserializerException.class);
        expectedException.expectMessage("No results for path: $['invalidPath']");

        String template = "{\"test\":\"$.invalidPath\"}";
        StencilClient stencilClient = StencilClientFactory.getClient();
        protoParser = new ProtoParser(stencilClient, TestAggregatedSupplyMessage.class.getName());
        MessageToTemplatizedJson messageToTemplatizedJson = MessageToTemplatizedJson
                .create(instrumentation, template, protoParser);
        Message message = new Message(Base64.getDecoder().decode(logKey.getBytes()),
                Base64.getDecoder().decode(logMessage.getBytes()), "sample-topic", 0, 100);

        messageToTemplatizedJson.serialize(message);
    }

    @Test
    public void shouldFailForNonJsonTemplate() {
        expectedException.expect(EglcConfigurationException.class);
        expectedException.expectMessage("must be a valid JSON.");

        String template = "{\"test:\"$.routes[0]\", \"$.order_number\" : \"xxx\"}";
        MessageToTemplatizedJson.create(instrumentation, template, protoParser);
    }


    @Test
    public void shouldDoRegexMatchingToReplaceThingsFromProtobuf() {
        expectedException.expect(EglcConfigurationException.class);
        expectedException.expectMessage("must be a valid JSON.");

        String template = "{\"test:\"$.routes[0]\", \"$.order_number\" : \"xxx\"}";
        MessageToTemplatizedJson.create(instrumentation, template, protoParser);
    }

    @Test
    public void shouldLogPaths() {
        HashSet<String> paths = new HashSet<>();
        String template = "\"$._all_\"";
        String templatePathRegex = "\"\\$\\.[^\\s\\\\]*?\"";

        Pattern pattern = Pattern.compile(templatePathRegex);
        Matcher matcher = pattern.matcher(template);
        while (matcher.find()) {
            paths.add(matcher.group(0));
        }
        List<String> pathList = new ArrayList<>(paths);

        StencilClient stencilClient = StencilClientFactory.getClient();
        protoParser = new ProtoParser(stencilClient, TestAggregatedSupplyMessage.class.getName());
        MessageToTemplatizedJson.create(instrumentation, template, protoParser);

        Mockito.verify(instrumentation, Mockito.times(1)).logDebug("\nPaths: {}", pathList);
    }
}
