package com.gotocompany.firehose.serializer;

import com.gotocompany.firehose.config.HttpSinkConfig;
import com.gotocompany.firehose.exception.DeserializerException;
import com.gotocompany.firehose.message.Message;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import lombok.extern.slf4j.Slf4j;

import java.util.Optional;

/***
 * MessageSerializer wrapping other JSON MessageSerializer which add capability to typecast some of the fields from the inner serializer.
 */
@Slf4j
public class TypecastedJsonSerializer implements MessageSerializer {

    private final MessageSerializer messageSerializer;
    private final HttpSinkConfig httpSinkConfig;
    private final Configuration jsonPathConfiguration;

    /**
     * Constructor for TypecastedJsonSerializer.
     *
     * @param messageSerializer the inner serializer to be wrapped
     * @param httpSinkConfig    the HTTP Sink config configuration containing typecasting parameters,
     *                          where each map entry contains a JSON path and the desired type
     */
    public TypecastedJsonSerializer(MessageSerializer messageSerializer,
                                    HttpSinkConfig httpSinkConfig) {
        this.messageSerializer = messageSerializer;
        this.httpSinkConfig = httpSinkConfig;
        this.jsonPathConfiguration = Configuration.builder()
                .options(Option.SUPPRESS_EXCEPTIONS)
                .build();
    }

    /**
     * Serializes the given message, then applies typecasting to specified fields in the resulting JSON.
     *
     * @param message the message to be serialized
     * @return the serialized and typecasted JSON string
     * @throws DeserializerException if an error occurs during serialization or typecasting
     */
    @Override
    public String serialize(Message message) throws DeserializerException {
        String jsonString = messageSerializer.serialize(message);
        DocumentContext documentContext = JsonPath
                .using(jsonPathConfiguration)
                .parse(jsonString);
        httpSinkConfig.getSinkHttpSerializerJsonTypecast()
                .forEach((jsonPath, typecastFunction) -> documentContext.map(jsonPath,
                        (currentValue, configuration) -> Optional.ofNullable(currentValue)
                                .map(v -> typecastFunction.apply(v.toString()))
                                .orElse(null)
                ));
        return documentContext.jsonString();
    }
}
