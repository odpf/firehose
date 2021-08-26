package io.odpf.firehose.sink.bigquery.proto;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.protobuf.DescriptorProtos;
import io.odpf.firehose.sink.bigquery.models.Constants;
import io.odpf.firehose.sink.bigquery.models.ProtoField;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class ProtoMapperTest {

    private final ProtoMapper protoMapper = new ProtoMapper();
    private final ObjectMapper objectMapper = new ObjectMapper();

    private final Map<DescriptorProtos.FieldDescriptorProto.Type, LegacySQLTypeName> expectedType = new HashMap<DescriptorProtos.FieldDescriptorProto.Type, LegacySQLTypeName>() {{
        put(DescriptorProtos.FieldDescriptorProto.Type.TYPE_BYTES, LegacySQLTypeName.BYTES);
        put(DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING, LegacySQLTypeName.STRING);
        put(DescriptorProtos.FieldDescriptorProto.Type.TYPE_ENUM, LegacySQLTypeName.STRING);
        put(DescriptorProtos.FieldDescriptorProto.Type.TYPE_BOOL, LegacySQLTypeName.BOOLEAN);
        put(DescriptorProtos.FieldDescriptorProto.Type.TYPE_DOUBLE, LegacySQLTypeName.FLOAT);
        put(DescriptorProtos.FieldDescriptorProto.Type.TYPE_FLOAT, LegacySQLTypeName.FLOAT);
    }};

    @Test
    public void shouldTestShouldCreateFirstLevelColumnMappingSuccessfully() throws IOException {
        ProtoField protoField = new ProtoField(new ArrayList<ProtoField>() {{
            add(new ProtoField("order_number", 1));
            add(new ProtoField("order_url", 2));
            add(new ProtoField("order_details", 3));
            add(new ProtoField("created_at", 4));
            add(new ProtoField("status", 5));
        }});

        ObjectNode objNode = JsonNodeFactory.instance.objectNode();
        objNode.put("1", "order_number");
        objNode.put("2", "order_url");
        objNode.put("3", "order_details");
        objNode.put("4", "created_at");
        objNode.put("5", "status");

        String columnMapping = protoMapper.generateColumnMappings(protoField.getFields());

        String expectedProtoMapping = objectMapper.writeValueAsString(objNode);
        assertEquals(expectedProtoMapping, columnMapping);
    }

    @Test
    public void shouldTestShouldCreateNestedMapping() throws IOException {
        ProtoField protoField = new ProtoField(new ArrayList<ProtoField>() {{
            add(new ProtoField("order_number", 1));
            add(new ProtoField("order_url", "some.type.name", DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE, 2, new ArrayList<ProtoField>() {{
                add(new ProtoField("host", 1));
                add(new ProtoField("url", 2));
            }}));
            add(new ProtoField("order_details", 3));
        }});

        ObjectNode objNode = JsonNodeFactory.instance.objectNode();
        ObjectNode innerObjNode = JsonNodeFactory.instance.objectNode();
        innerObjNode.put("1", "host");
        innerObjNode.put("2", "url");
        innerObjNode.put("record_name", "order_url");
        objNode.put("1", "order_number");
        objNode.put("2", innerObjNode);
        objNode.put("3", "order_details");


        String columnMapping = protoMapper.generateColumnMappings(protoField.getFields());
        String expectedProtoMapping = objectMapper.writeValueAsString(objNode);
        assertEquals(expectedProtoMapping, columnMapping);
    }

    @Test
    public void generateColumnMappingsForNoFields() throws IOException {
        String protoMapping = protoMapper.generateColumnMappings(new ArrayList<>());
        assertEquals(protoMapping, "{}");
    }

    @Test
    public void shouldTestConvertToSchemaSuccessful() {
        List<ProtoField> nestedBQFields = new ArrayList<>();
        nestedBQFields.add(new ProtoField("field0_bytes", DescriptorProtos.FieldDescriptorProto.Type.TYPE_BYTES, DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL));
        nestedBQFields.add(new ProtoField("field1_string", DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING, DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL));
        nestedBQFields.add(new ProtoField("field2_bool", DescriptorProtos.FieldDescriptorProto.Type.TYPE_BOOL, DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL));
        nestedBQFields.add(new ProtoField("field3_enum", DescriptorProtos.FieldDescriptorProto.Type.TYPE_ENUM, DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL));
        nestedBQFields.add(new ProtoField("field4_double", DescriptorProtos.FieldDescriptorProto.Type.TYPE_DOUBLE, DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL));
        nestedBQFields.add(new ProtoField("field5_float", DescriptorProtos.FieldDescriptorProto.Type.TYPE_FLOAT, DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL));


        List<Field> fields = protoMapper.generateBigquerySchema(new ProtoField(nestedBQFields));
        assertEquals(nestedBQFields.size(), fields.size());
        IntStream.range(0, nestedBQFields.size())
                .forEach(index -> {
                    assertEquals(Field.Mode.NULLABLE, fields.get(index).getMode());
                    assertEquals(nestedBQFields.get(index).getName(), fields.get(index).getName());
                    assertEquals(expectedType.get(nestedBQFields.get(index).getType()), fields.get(index).getType());
                });

    }

    @Test
    public void shouldTestShouldConvertIntegerDataTypes() {
        List<DescriptorProtos.FieldDescriptorProto.Type> allIntTypes = new ArrayList<DescriptorProtos.FieldDescriptorProto.Type>() {{
            add(DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64);
            add(DescriptorProtos.FieldDescriptorProto.Type.TYPE_UINT64);
            add(DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32);
            add(DescriptorProtos.FieldDescriptorProto.Type.TYPE_UINT32);
            add(DescriptorProtos.FieldDescriptorProto.Type.TYPE_FIXED64);
            add(DescriptorProtos.FieldDescriptorProto.Type.TYPE_FIXED32);
            add(DescriptorProtos.FieldDescriptorProto.Type.TYPE_SFIXED32);
            add(DescriptorProtos.FieldDescriptorProto.Type.TYPE_SFIXED64);
            add(DescriptorProtos.FieldDescriptorProto.Type.TYPE_SINT32);
            add(DescriptorProtos.FieldDescriptorProto.Type.TYPE_SINT64);
        }};

        List<ProtoField> nestedBQFields = IntStream.range(0, allIntTypes.size())
                .mapToObj(index -> new ProtoField("field-" + index, allIntTypes.get(index), DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL))
                .collect(Collectors.toList());


        List<Field> fields = protoMapper.generateBigquerySchema(new ProtoField(nestedBQFields));
        assertEquals(nestedBQFields.size(), fields.size());
        IntStream.range(0, nestedBQFields.size())
                .forEach(index -> {
                    assertEquals(Field.Mode.NULLABLE, fields.get(index).getMode());
                    assertEquals(nestedBQFields.get(index).getName(), fields.get(index).getName());
                    assertEquals(LegacySQLTypeName.INTEGER, fields.get(index).getType());
                });
    }

    @Test
    public void shouldTestShouldConvertNestedField() {
        List<ProtoField> nestedBQFields = new ArrayList<>();
        nestedBQFields.add(new ProtoField("field1_level2_nested", DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING, DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL));
        nestedBQFields.add(new ProtoField("field2_level2_nested", DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING, DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL));

        ProtoField protoField = new ProtoField(new ArrayList<ProtoField>() {{
            add(new ProtoField("field1_level1",
                    DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING,
                    DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL));
            add(new ProtoField("field2_level1_message",
                    "some.type.name",
                    DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE,
                    DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL,
                    nestedBQFields));
        }});


        List<Field> fields = protoMapper.generateBigquerySchema(protoField);

        assertEquals(protoField.getFields().size(), fields.size());
        assertEquals(nestedBQFields.size(), fields.get(1).getSubFields().size());

        assertBqField(protoField.getFields().get(0).getName(), LegacySQLTypeName.STRING, Field.Mode.NULLABLE, fields.get(0));
        assertBqField(protoField.getFields().get(1).getName(), LegacySQLTypeName.RECORD, Field.Mode.NULLABLE, fields.get(1));
        assertBqField(nestedBQFields.get(0).getName(), LegacySQLTypeName.STRING, Field.Mode.NULLABLE, fields.get(1).getSubFields().get(0));
        assertBqField(nestedBQFields.get(1).getName(), LegacySQLTypeName.STRING, Field.Mode.NULLABLE, fields.get(1).getSubFields().get(1));

    }

    @Test
    public void shouldTestShouldConvertMultiNestedFields() {
        List<ProtoField> nestedBQFields = new ArrayList<ProtoField>() {{
            add(new ProtoField("field1_level3_nested",
                    DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING,
                    DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL));
            add(new ProtoField("field2_level3_nested",
                    DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING,
                    DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL));
        }};

        ProtoField protoField = new ProtoField(new ArrayList<ProtoField>() {{
            add(new ProtoField("field1_level1",
                    DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING,
                    DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL));

            add(new ProtoField(
                    "field2_level1_message",
                    "some.type.name",
                    DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE,
                    DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL,
                    new ArrayList<ProtoField>() {{
                        add(new ProtoField(
                                "field1_level2",
                                DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING,
                                DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL));
                        add(new ProtoField(
                                "field2_level2_message",
                                "some.type.name",
                                DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE,
                                DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL,
                                nestedBQFields));
                        add(new ProtoField(
                                "field3_level2",
                                DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING,
                                DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL));
                        add(new ProtoField(
                                "field4_level2_message",
                                "some.type.name",
                                DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE,
                                DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL,
                                nestedBQFields));
                    }}
            ));
        }});

        List<Field> fields = protoMapper.generateBigquerySchema(protoField);


        assertEquals(protoField.getFields().size(), fields.size());
        assertEquals(4, fields.get(1).getSubFields().size());
        assertEquals(2, fields.get(1).getSubFields().get(1).getSubFields().size());
        assertEquals(2, fields.get(1).getSubFields().get(3).getSubFields().size());
        assertMultipleFields(nestedBQFields, fields.get(1).getSubFields().get(1).getSubFields());
        assertMultipleFields(nestedBQFields, fields.get(1).getSubFields().get(3).getSubFields());
    }

    @Test
    public void shouldTestConvertToSchemaForTimestamp() {
        ProtoField protoField = new ProtoField(new ArrayList<ProtoField>() {{
            add(new ProtoField("field1_timestamp",
                    Constants.ProtobufTypeName.TIMESTAMP_PROTOBUF_TYPE_NAME,
                    DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE,
                    DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL));
        }});

        List<Field> fields = protoMapper.generateBigquerySchema(protoField);

        assertEquals(protoField.getFields().size(), fields.size());
        assertBqField(protoField.getFields().get(0).getName(), LegacySQLTypeName.TIMESTAMP, Field.Mode.NULLABLE, fields.get(0));
    }

    @Test
    public void shouldTestConvertToSchemaForSpecialFields() {
        ProtoField protoField = new ProtoField(new ArrayList<ProtoField>() {{
            add(new ProtoField("field1_struct",
                    Constants.ProtobufTypeName.STRUCT_PROTOBUF_TYPE_NAME,
                    DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE,
                    DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL));
            add(new ProtoField("field2_bytes",
                    DescriptorProtos.FieldDescriptorProto.Type.TYPE_BYTES,
                    DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL));

            add(new ProtoField("field3_duration",
                    "." + com.google.protobuf.Duration.getDescriptor().getFullName(),
                    DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE,
                    DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL,
                    new ArrayList<ProtoField>() {
                        {
                            add(new ProtoField("duration_seconds",
                                    DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64,
                                    DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL));

                            add(new ProtoField("duration_nanos",
                                    DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32,
                                    DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL));

                        }
                    }));

            add(new ProtoField("field3_date",
                    "." + com.google.type.Date.getDescriptor().getFullName(),
                    DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE,
                    DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL,
                    new ArrayList<ProtoField>() {
                        {
                            add(new ProtoField("year",
                                    DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64,
                                    DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL));

                            add(new ProtoField("month",
                                    DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32,
                                    DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL));

                            add(new ProtoField("day",
                                    DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32,
                                    DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL));

                        }
                    }));

        }});

        List<Field> fields = protoMapper.generateBigquerySchema(protoField);

        assertEquals(protoField.getFields().size(), fields.size());
        assertBqField(protoField.getFields().get(0).getName(), LegacySQLTypeName.STRING, Field.Mode.NULLABLE, fields.get(0));
        assertBqField(protoField.getFields().get(1).getName(), LegacySQLTypeName.BYTES, Field.Mode.NULLABLE, fields.get(1));
        assertBqField(protoField.getFields().get(2).getName(), LegacySQLTypeName.RECORD, Field.Mode.NULLABLE, fields.get(2));
        assertBqField(protoField.getFields().get(3).getName(), LegacySQLTypeName.RECORD, Field.Mode.NULLABLE, fields.get(3));
        assertEquals(2, fields.get(2).getSubFields().size());
        assertBqField("duration_seconds", LegacySQLTypeName.INTEGER, Field.Mode.NULLABLE, fields.get(2).getSubFields().get(0));
        assertBqField("duration_nanos", LegacySQLTypeName.INTEGER, Field.Mode.NULLABLE, fields.get(2).getSubFields().get(1));

        assertEquals(3, fields.get(3).getSubFields().size());
        assertBqField("year", LegacySQLTypeName.INTEGER, Field.Mode.NULLABLE, fields.get(3).getSubFields().get(0));
        assertBqField("month", LegacySQLTypeName.INTEGER, Field.Mode.NULLABLE, fields.get(3).getSubFields().get(1));
        assertBqField("day", LegacySQLTypeName.INTEGER, Field.Mode.NULLABLE, fields.get(3).getSubFields().get(2));
    }


    @Test
    public void shouldTestConverterToSchemaForNullFields() {
        List<Field> fields = protoMapper.generateBigquerySchema(null);
        assertNull(fields);
    }

    @Test
    public void shouldTestConvertToSchemaForRepeatedFields() {
        ProtoField protoField = new ProtoField(new ArrayList<ProtoField>() {{
            add(new ProtoField("field1_map",
                    DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32,
                    DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED));
            add(new ProtoField("field2_repeated",
                    DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING,
                    DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED));

        }});

        List<Field> fields = protoMapper.generateBigquerySchema(protoField);

        assertEquals(protoField.getFields().size(), fields.size());
        assertBqField(protoField.getFields().get(0).getName(), LegacySQLTypeName.INTEGER, Field.Mode.REPEATED, fields.get(0));
        assertBqField(protoField.getFields().get(1).getName(), LegacySQLTypeName.STRING, Field.Mode.REPEATED, fields.get(1));
    }

    public void assertMultipleFields(List<ProtoField> pfields, List<Field> bqFields) {
        IntStream.range(0, bqFields.size())
                .forEach(index -> {
                    assertBqField(pfields.get(index).getName(), expectedType.get(pfields.get(index).getType()), Field.Mode.NULLABLE, bqFields.get(index));
                });
    }

    public void assertBqField(String name, LegacySQLTypeName ftype, Field.Mode mode, Field bqf) {
        assertEquals(mode, bqf.getMode());
        assertEquals(name, bqf.getName());
        assertEquals(ftype, bqf.getType());
    }
}
