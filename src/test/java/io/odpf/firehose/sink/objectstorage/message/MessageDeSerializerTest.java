package io.odpf.firehose.sink.objectstorage.message;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Int64Value;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.StringValue;
import com.gojek.de.stencil.parser.Parser;

import com.google.protobuf.UnknownFieldSet;
import io.odpf.firehose.config.ObjectStorageSinkConfig;
import io.odpf.firehose.consumer.Message;
import io.odpf.firehose.exception.DeserializerException;
import io.odpf.firehose.sink.objectstorage.proto.KafkaMetadataProto;
import io.odpf.firehose.sink.objectstorage.proto.KafkaMetadataProtoFile;
import io.odpf.firehose.sink.objectstorage.proto.NestedKafkaMetadataProto;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class MessageDeSerializerTest {

    @Mock
    private KafkaMetadataUtils kafkaMetadataUtils;

    @Mock
    private Parser protoParser;

    private MessageDeSerializer deSerializer;

    private final byte[] logKey = "key".getBytes();
    private final byte[] logMessage = "msg".getBytes();
    private Message message;

    @Mock
    private ObjectStorageSinkConfig sinkConfig;

    @Before
    public void setUp() throws Exception {
        message = new Message(logKey, logMessage, "topic1", 0, 100);
        deSerializer = new MessageDeSerializer(kafkaMetadataUtils, protoParser, sinkConfig);
        when(sinkConfig.getWriteKafkaMetadata()).thenReturn(true);
    }

    @Test
    public void shouldCreateRecord() throws InvalidProtocolBufferException {
        DynamicMessage dynamicMessage = DynamicMessage.newBuilder(StringValue.of("abc")).build();
        DynamicMessage metadataMessage = DynamicMessage.newBuilder(Int64Value.of(112)).build();

        when(protoParser.parse(logMessage)).thenReturn(dynamicMessage);
        when(kafkaMetadataUtils.createKafkaMetadata(message)).thenReturn(metadataMessage);

        Record record = deSerializer.deSerialize(message);

        Assert.assertEquals(metadataMessage, record.getMetadata());
        Assert.assertEquals(dynamicMessage, record.getMessage());

        verify(protoParser, times(1)).parse(logMessage);
        verify(kafkaMetadataUtils, times(1)).createKafkaMetadata(message);
    }

    @Test(expected = DeserializerException.class)
    public void shouldThrowDeserializerExceptionWhenProtoParsingThrowException() throws InvalidProtocolBufferException {
        InvalidProtocolBufferException invalidProtocolBufferException = new InvalidProtocolBufferException("");
        when(protoParser.parse(logMessage)).thenThrow(invalidProtocolBufferException);

        deSerializer.deSerialize(message);
    }

    @Test(expected = DeserializerException.class)
    public void shouldThrowExceptionWhenLogMessageIsEmpty() {
        Message emptyMessage = new Message("".getBytes(), "".getBytes(), "default", 1, 1);
        deSerializer.deSerialize(emptyMessage);
    }

    @Test(expected = DeserializerException.class)
    public void shouldThrowExceptionWhenLogMessageIsNull() {
        Message emptyMessage = new Message("".getBytes(), null, "default", 1, 1);
        deSerializer.deSerialize(emptyMessage);
    }

    @Test(expected = DeserializerException.class)
    public void shouldThrowExceptionWhenUnknownFieldExist() throws InvalidProtocolBufferException {
        Descriptors.FileDescriptor fileDescriptor = KafkaMetadataProtoFile.createFileDescriptor("meta_field");
        Descriptors.Descriptor nestedDescriptor = fileDescriptor.findMessageTypeByName(NestedKafkaMetadataProto.getTypeName());
        Descriptors.Descriptor metaDescriptor = fileDescriptor.findMessageTypeByName(KafkaMetadataProto.getTypeName());

        Descriptors.FieldDescriptor fieldDescriptor = nestedDescriptor.findFieldByName("meta_field");
        DynamicMessage dynamicMessage = DynamicMessage.newBuilder(nestedDescriptor)
                .setField(fieldDescriptor, DynamicMessage.newBuilder(metaDescriptor)
                        .setUnknownFields(UnknownFieldSet.newBuilder()
                                .addField(1, UnknownFieldSet.Field.getDefaultInstance())
                                .build())
                        .build())
                .setUnknownFields(UnknownFieldSet.newBuilder()
                        .addField(1, UnknownFieldSet.Field.getDefaultInstance())
                        .addField(2, UnknownFieldSet.Field.getDefaultInstance())
                        .build())
                .build();

        when(protoParser.parse(logMessage)).thenReturn(dynamicMessage);
        when(sinkConfig.getInputSchemaProtoAllowUnknownFieldsEnable()).thenReturn(false);
        deSerializer.deSerialize(message);
    }

    @Test
    public void shouldNotThrowExceptionWhenUnknownFieldExistWhenConfigIsNotSet() throws InvalidProtocolBufferException {
        DynamicMessage dynamicMessage = DynamicMessage.newBuilder(StringValue.of("abc"))
                .setUnknownFields(UnknownFieldSet.newBuilder()
                        .addField(1, UnknownFieldSet.Field.getDefaultInstance())
                        .addField(2, UnknownFieldSet.Field.getDefaultInstance())
                        .build()).build();
        DynamicMessage metadataMessage = DynamicMessage.newBuilder(Int64Value.of(112)).build();
        when(sinkConfig.getInputSchemaProtoAllowUnknownFieldsEnable()).thenReturn(true);

        when(protoParser.parse(logMessage)).thenReturn(dynamicMessage);
        when(kafkaMetadataUtils.createKafkaMetadata(message)).thenReturn(metadataMessage);

        Record record = deSerializer.deSerialize(message);

        Assert.assertEquals(dynamicMessage, record.getMessage());
        Assert.assertEquals(metadataMessage, record.getMetadata());

        Assert.assertEquals((UnknownFieldSet.newBuilder()
                .addField(1, UnknownFieldSet.Field.getDefaultInstance())
                .addField(2, UnknownFieldSet.Field.getDefaultInstance())
                .build()), record.getMessage().getUnknownFields());
        verify(protoParser, times(1)).parse(logMessage);
        verify(kafkaMetadataUtils, times(1)).createKafkaMetadata(message);
    }
}
