package com.gotocompany.firehose.config.converter;

import org.aeonbits.owner.Converter;

import java.lang.reflect.Method;
import java.util.Arrays;
import io.grpc.Metadata;

public class GrpcMetadataConverter implements Converter<Metadata> {

    @Override
    public Metadata convert(Method method, String input) {
        Metadata metadata = new Metadata();
        Arrays.stream(input.split(","))
                .filter(metadataKeyValue -> !metadataKeyValue.trim().isEmpty())
                .map(metadataKeyValue -> metadataKeyValue.split(":", 2))
                .forEach(keyValue -> {
                    if (keyValue.length != 2) {
                        throw new IllegalArgumentException(String.format("provided metadata %s is invalid", input));
                    }
                    metadata.put(Metadata.Key.of(keyValue[0].trim(), Metadata.ASCII_STRING_MARSHALLER), keyValue[1].trim());
                });

        return metadata;
    }
}
