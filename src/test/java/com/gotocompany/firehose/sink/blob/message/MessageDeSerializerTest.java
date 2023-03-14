package com.gotocompany.firehose.sink.blob.message;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.StringValue;

import com.google.protobuf.UnknownFieldSet;
import com.gotocompany.firehose.config.BlobSinkConfig;
import com.gotocompany.firehose.exception.DeserializerException;
import com.gotocompany.firehose.message.Message;
import com.gotocompany.firehose.sink.blob.proto.KafkaMetadataProtoMessage;
import com.gotocompany.firehose.sink.blob.proto.KafkaMetadataProtoMessageUtils;
import com.gotocompany.firehose.sink.blob.proto.NestedKafkaMetadataProtoMessage;
import com.gotocompany.stencil.Parser;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;


import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class MessageDeSerializerTest {

    @Mock
    private Parser protoParser;

    private MessageDeSerializer deSerializer;

    private final byte[] logKey = "key".getBytes();
    private final byte[] logMessage = "msg".getBytes();
    private Message message;

    @Mock
    private BlobSinkConfig sinkConfig;

    @Before
    public void setUp() throws Exception {
        message = new Message(logKey, logMessage, "topic1", 0, 100);

        Descriptors.FileDescriptor fileDescriptor = KafkaMetadataProtoMessageUtils.createFileDescriptor("");
        deSerializer = new MessageDeSerializer(fileDescriptor, protoParser, sinkConfig);
        when(sinkConfig.getOutputIncludeKafkaMetadataEnable()).thenReturn(true);
        when(sinkConfig.getOutputKafkaMetadataColumnName()).thenReturn("");
    }

    @Test
    public void shouldCreateRecord() throws InvalidProtocolBufferException {
        DynamicMessage dynamicMessage = DynamicMessage.newBuilder(StringValue.of("abc")).build();

        when(protoParser.parse(logMessage)).thenReturn(dynamicMessage);

        Record record = deSerializer.deSerialize(message);

        Assert.assertNotNull(record.getMetadata());
        Assert.assertEquals(dynamicMessage, record.getMessage());

        verify(protoParser, times(1)).parse(logMessage);
    }

    @Test
    public void shouldAlwaysCreateRecordWithMetadata() throws InvalidProtocolBufferException {
        when(sinkConfig.getOutputIncludeKafkaMetadataEnable()).thenReturn(false);

        DynamicMessage dynamicMessage = DynamicMessage.newBuilder(StringValue.of("abc")).build();

        when(protoParser.parse(logMessage)).thenReturn(dynamicMessage);

        Record record = deSerializer.deSerialize(message);

        Assert.assertNotNull(record.getMetadata());
        Assert.assertEquals(dynamicMessage, record.getMessage());

        verify(protoParser, times(1)).parse(logMessage);
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
        Descriptors.FileDescriptor fileDescriptor = KafkaMetadataProtoMessageUtils.createFileDescriptor("meta_field");
        Descriptors.Descriptor nestedDescriptor = fileDescriptor.findMessageTypeByName(NestedKafkaMetadataProtoMessage.getTypeName());
        Descriptors.Descriptor metaDescriptor = fileDescriptor.findMessageTypeByName(KafkaMetadataProtoMessage.getTypeName());

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
        when(sinkConfig.getInputSchemaProtoAllowUnknownFieldsEnable()).thenReturn(true);

        when(protoParser.parse(logMessage)).thenReturn(dynamicMessage);

        Record record = deSerializer.deSerialize(message);

        Assert.assertEquals(dynamicMessage, record.getMessage());
        Assert.assertNotNull(record.getMetadata());

        Assert.assertEquals((UnknownFieldSet.newBuilder()
                .addField(1, UnknownFieldSet.Field.getDefaultInstance())
                .addField(2, UnknownFieldSet.Field.getDefaultInstance())
                .build()), record.getMessage().getUnknownFields());
        verify(protoParser, times(1)).parse(logMessage);
    }
}
