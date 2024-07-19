package com.gotocompany.firehose.config.converter;

import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.util.Map;

import static org.junit.Assert.assertEquals;

public class GrpcMetadataConverterTest {

    @Test
    public void shouldConvertConfigToMetadata() {
        Map<String, String> actualMetadata = new GrpcMetadataConverter().convert(null, ",k1:v1,k2:v2, k3:v3, $Generic.Field: $Generic.Value");
        assertEquals("v1", actualMetadata.get("k1"));
        assertEquals("v2", actualMetadata.get("k2"));
        assertEquals("v3", actualMetadata.get("k3"));
        assertEquals("$Generic.Value", actualMetadata.get("$Generic.Field"));
    }

    @Test
    public void shouldConvertToEmptyMapGivenEmptyStringConfig() {
        Map<String, String> result = new GrpcMetadataConverter().convert(null, "");
        Assertions.assertTrue(result.isEmpty());
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowOnIncompleteMetadataValue() {
        new GrpcMetadataConverter().convert(null, "k1:v1,k2:v2,k3:v3,k4");
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowOnIncompleteMetadataKey() {
        new GrpcMetadataConverter().convert(null, "k1:v1,:v2,k3:v3");
    }


}
