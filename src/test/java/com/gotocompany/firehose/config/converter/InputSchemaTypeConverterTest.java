package com.gotocompany.firehose.config.converter;

import com.gotocompany.firehose.config.enums.InputSchemaType;
import org.junit.Assert;
import org.junit.Test;

public class InputSchemaTypeConverterTest {

    @Test
    public void shouldConvertSchemaType() {
        InputSchemaTypeConverter converter = new InputSchemaTypeConverter();
        InputSchemaType schemaType = converter.convert(null, "PROTOBUF");
        Assert.assertEquals(InputSchemaType.PROTOBUF, schemaType);
        schemaType = converter.convert(null, "JSON");
        Assert.assertEquals(InputSchemaType.JSON, schemaType);
    }

    @Test
    public void shouldConvertSchemaTypeWithLowerCase() {
        InputSchemaTypeConverter converter = new InputSchemaTypeConverter();
        InputSchemaType schemaType = converter.convert(null, " json ");
        Assert.assertEquals(InputSchemaType.JSON, schemaType);
    }
}
