package io.odpf.firehose.sink.bigquery.proto;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Duration;
import com.google.protobuf.Struct;
import com.google.protobuf.Timestamp;
import com.google.type.Date;
import io.odpf.firehose.TestMessageBQ;
import io.odpf.firehose.TestNestedMessageBQ;
import io.odpf.firehose.TestRecursiveMessageBQ;
import io.odpf.firehose.sink.bigquery.models.ProtoField;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

@RunWith(MockitoJUnitRunner.class)
public class ProtoFieldParserTest {
    private ProtoFieldParser protoMappingParser;

    @Before
    public void setup() {
        this.protoMappingParser = new ProtoFieldParser();
    }

    @Test(expected = RuntimeException.class)
    public void shouldThrowExceptionIfProtoNotFound() {
        protoMappingParser.parseFields(null, "test", new HashMap<>(), new HashMap<>());
    }

    @Test(expected = RuntimeException.class)
    public void shouldThrowExceptionIfNestedProtoNotFound() {
        Map<String, Descriptors.Descriptor> descriptorMap = new HashMap<String, Descriptors.Descriptor>() {{
            put("io.odpf.firehose.TestMessageBQ", TestMessageBQ.getDescriptor());
        }};
        ProtoField protoField = new ProtoField();
        protoMappingParser.parseFields(protoField, "io.odpf.firehose.TestNestedMessageBQ", descriptorMap, new HashMap<>());
    }

    @Test
    public void shouldParseProtoSchemaForNonNestedFields() {
        ArrayList<Descriptors.FileDescriptor> fileDescriptors = new ArrayList<>();

        fileDescriptors.add(TestMessageBQ.getDescriptor().getFile());
        fileDescriptors.add(Duration.getDescriptor().getFile());
        fileDescriptors.add(Date.getDescriptor().getFile());
        fileDescriptors.add(Struct.getDescriptor().getFile());
        fileDescriptors.add(Timestamp.getDescriptor().getFile());

        Map<String, Descriptors.Descriptor> descriptorMap = getDescriptors(fileDescriptors);

        Map<String, String> typeNameToPackageNameMap = new HashMap<String, String>() {{
            put(".odpf.firehose.TestMessageBQ.CurrentStateEntry", "io.odpf.firehose.TestMessageBQ.CurrentStateEntry");
            put(".google.protobuf.Struct.FieldsEntry", "com.google.protobuf.Struct.FieldsEntry");
            put(".google.protobuf.Duration", "com.google.protobuf.Duration");
            put(".google.type.Date", "com.google.type.Date");
        }};

        ProtoField protoField = new ProtoField();
        protoField = protoMappingParser.parseFields(protoField, "io.odpf.firehose.TestMessageBQ", descriptorMap, typeNameToPackageNameMap);
        assertTestMessage(protoField.getFields());
    }

    @Test
    public void shouldParseProtoSchemaForRecursiveFieldTillMaxLevel() {
        ArrayList<Descriptors.FileDescriptor> fileDescriptors = new ArrayList<>();

        fileDescriptors.add(TestRecursiveMessageBQ.getDescriptor().getFile());

        Map<String, Descriptors.Descriptor> descriptorMap = getDescriptors(fileDescriptors);

        Map<String, String> typeNameToPackageNameMap = new HashMap<String, String>() {{
            put(".odpf.firehose.TestRecursiveMessageBQ", "io.odpf.firehose.TestRecursiveMessageBQ");
        }};

        ProtoField protoField = new ProtoField();
        protoField = protoMappingParser.parseFields(protoField, "io.odpf.firehose.TestRecursiveMessageBQ", descriptorMap, typeNameToPackageNameMap);
        assertField(protoField.getFields().get(0), "string_value", DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING, DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL, 1);
        assertField(protoField.getFields().get(1), "float_value", DescriptorProtos.FieldDescriptorProto.Type.TYPE_FLOAT, DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL, 2);

        ProtoField recursiveField = protoField;
        int totalLevel = 1;
        while (recursiveField.getFields().size() == 3) {
            assertField(protoField.getFields().get(0), "string_value", DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING, DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL, 1);
            assertField(protoField.getFields().get(1), "float_value", DescriptorProtos.FieldDescriptorProto.Type.TYPE_FLOAT, DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL, 2);
            recursiveField = recursiveField.getFields().get(2);
            totalLevel++;
        }
        assertEquals(15, totalLevel);
    }

    @Test
    public void shouldParseProtoSchemaForNestedFields() {
        ArrayList<Descriptors.FileDescriptor> fileDescriptors = new ArrayList<>();

        fileDescriptors.add(TestMessageBQ.getDescriptor().getFile());
        fileDescriptors.add(Duration.getDescriptor().getFile());
        fileDescriptors.add(Date.getDescriptor().getFile());
        fileDescriptors.add(Struct.getDescriptor().getFile());
        fileDescriptors.add(TestNestedMessageBQ.getDescriptor().getFile());

        Map<String, Descriptors.Descriptor> descriptorMap = getDescriptors(fileDescriptors);

        Map<String, String> typeNameToPackageNameMap = new HashMap<String, String>() {{
            put(".odpf.firehose.TestMessageBQ.CurrentStateEntry", "io.odpf.firehose.TestMessageBQ.CurrentStateEntry");
            put(".google.protobuf.Struct.FieldsEntry", "com.google.protobuf.Struct.FieldsEntry");
            put(".google.protobuf.Duration", "com.google.protobuf.Duration");
            put(".google.type.Date", "com.google.type.Date");
            put(".odpf.firehose.TestMessageBQ", "io.odpf.firehose.TestMessageBQ");
        }};

        ProtoField protoField = new ProtoField();
        protoField = protoMappingParser.parseFields(protoField, "io.odpf.firehose.TestNestedMessageBQ", descriptorMap, typeNameToPackageNameMap);
        assertField(protoField.getFields().get(0), "nested_id", DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING, DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL, 1);
        assertField(protoField.getFields().get(1), "single_message", DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE, DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL, 2);

        assertTestMessage(protoField.getFields().get(1).getFields());
    }

    private Map<String, Descriptors.Descriptor> getDescriptors(ArrayList<Descriptors.FileDescriptor> fileDescriptors) {
        Map<String, Descriptors.Descriptor> descriptorMap = new HashMap<>();
        fileDescriptors.forEach(fd -> {
            String javaPackage = fd.getOptions().getJavaPackage();
            fd.getMessageTypes().forEach(desc -> {
                String className = desc.getName();
                desc.getNestedTypes().forEach(nestedDesc -> {
                    String nestedClassName = nestedDesc.getName();
                    descriptorMap.put(String.format("%s.%s.%s", javaPackage, className, nestedClassName), nestedDesc);
                });
                descriptorMap.put(String.format("%s.%s", javaPackage, className), desc);
            });
        });
        return descriptorMap;
    }

    private void assertTestMessage(List<ProtoField> fields) {
        assertEquals(16, fields.size());
        assertField(fields.get(0), "order_number", DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING, DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL, 1);
        assertField(fields.get(1), "order_url", DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING, DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL, 2);
        assertField(fields.get(2), "order_details", DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING, DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL, 3);
        assertField(fields.get(3), "created_at", DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE, DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL, 4);
        assertField(fields.get(4), "status", DescriptorProtos.FieldDescriptorProto.Type.TYPE_ENUM, DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL, 5);
        assertField(fields.get(5), "discount", DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64, DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL, 6);
        assertField(fields.get(6), "success", DescriptorProtos.FieldDescriptorProto.Type.TYPE_BOOL, DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL, 7);
        assertField(fields.get(7), "price", DescriptorProtos.FieldDescriptorProto.Type.TYPE_FLOAT, DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL, 8);
        assertField(fields.get(8), "current_state", DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE, DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED, 9);
        assertField(fields.get(9), "user_token", DescriptorProtos.FieldDescriptorProto.Type.TYPE_BYTES, DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL, 10);
        assertField(fields.get(10), "trip_duration", DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE, DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL, 11);
        assertField(fields.get(11), "aliases", DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING, DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED, 12);
        assertField(fields.get(12), "properties", DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE, DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL, 13);
        assertField(fields.get(13), "order_date", DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE, DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL, 14);
        assertField(fields.get(14), "updated_at", DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE, DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED, 15);
        assertField(fields.get(15), "attributes", DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE, DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED, 16);

        assertEquals(String.format(".%s", Duration.getDescriptor().getFullName()), fields.get(10).getTypeName());
        assertEquals(2, fields.get(10).getFields().size());
        assertField(fields.get(10).getFields().get(0), "seconds", DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64, DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL, 1);
        assertField(fields.get(10).getFields().get(1), "nanos", DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32, DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL, 2);

        assertEquals(String.format(".%s", Date.getDescriptor().getFullName()), fields.get(13).getTypeName());
        assertEquals(3, fields.get(13).getFields().size());
        assertField(fields.get(13).getFields().get(0), "year", DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32, DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL, 1);
        assertField(fields.get(13).getFields().get(1), "month", DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32, DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL, 2);
        assertField(fields.get(13).getFields().get(2), "day", DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32, DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL, 3);
    }

    private void assertField(ProtoField field, String name, DescriptorProtos.FieldDescriptorProto.Type ftype, DescriptorProtos.FieldDescriptorProto.Label flabel, int index) {
        assertEquals(name, field.getName());
        assertEquals(ftype, field.getType());
        assertEquals(flabel, field.getLabel());
        assertEquals(index, field.getIndex());
    }
}
